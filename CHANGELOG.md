# CHANGELOG

История правок по итогам отладочных запусков.

## v1.1.0 — устойчивость к нагрузке Zabbix (timeout/retry tuning)

После эпизодов `TimeoutError` в PROD-DAG на запросе `auditlog.get`
(инсталляция ~10.4 млн записей в audit log, активные правки) — комплекс
изменений, делающий синхронизацию устойчивой к временной нагрузке на
Zabbix-сервер. Существующая логика (правило отсрочки на час, short-circuit
DEFER) не меняется.

### Изменения параметров (с дефолтами)

| Параметр | Было | Стало | Variable |
|---|---|---|---|
| Базовый таймаут Zabbix API | 30 с | 60 с | `<env>_zabbix_timeout_sec` |
| Таймаут `auditlog.get` | 30 с | 180 с | `<env>_zabbix_audit_timeout_sec` |
| Окно `auditlog.get` запроса | `max(2×quiet_period, 7200)` (PROD: 7200 с) | `quiet_period + padding` (PROD: 4500 с) | — |
| Padding окна | (хардкод) | 900 с | `<env>_audit_window_padding_sec` |
| Лимит `auditlog.get` | 50000 | 5000 | `<env>_audit_query_limit` |
| Retries PROD | 2 | 3 | в `ENVIRONMENTS` |
| Retry delay PROD | 5 мин | 15 мин | в `ENVIRONMENTS` |
| Retries TEST | 2 | 2 (без изменений) | в `ENVIRONMENTS` |
| Retry delay TEST | 5 мин | 5 мин (без изменений) | в `ENVIRONMENTS` |

### Изменения в коде

- **`ZabbixAPI._call()`** — добавлен опциональный параметр `timeout_override`,
  позволяющий перекрыть базовый таймаут для конкретного запроса. Используется
  для `auditlog.get`.
- **`ZabbixAPI.get_recently_modified_templates()`** — теперь принимает
  параметры `audit_timeout` и `limit`.
- **`SyncConfig`** — добавлены 4 новых поля: `zabbix_timeout_sec`,
  `zabbix_audit_timeout_sec`, `audit_window_padding_sec`, `audit_query_limit`.
- **DAG** — все 4 параметра конфигурируются через Airflow Variables
  с префиксом среды; retries и retry_delay вынесены в `ENVIRONMENTS`
  с разделением TEST/PROD.
- **`ZabbixAPI._call()` → handler для `TimeoutError`** — отдельная ветка,
  выдающая понятное сообщение с конкретными CLI-командами для подстройки
  таймаутов/окна/лимита через Airflow Variables. Раньше был криптический
  stacktrace на 15 строк.

### Совместимость с предыдущими версиями

Полностью обратно-совместимо. Если новые Variables не созданы — используются
дефолты в коде. Чтобы получить новые значения для PROD, дополнительных
действий не требуется — дефолты применятся автоматически.

При желании можно настроить под конкретную нагрузку:
```bash
# Если timeout таки случается — поднять выше:
airflow variables set prod_zabbix_audit_timeout_sec 300

# Если очень активный audit log — уменьшить окно:
airflow variables set prod_audit_window_padding_sec 300

# Если в окне реально единицы правок — снизить лимит ещё:
airflow variables set prod_audit_query_limit 1000
```

## v1.0.9 — диагностика сетевых ошибок (SSL, DNS, refused, timeout)

### Улучшения

- **Понятные сообщения при сбоях SSL/сети.** При получении
  `URLError`/`SSLCertVerificationError` от Zabbix или GitLab вместо
  криптического stacktrace пользователь получает подсказку с двумя
  готовыми вариантами решения:

  ```
  → SSL-сертификат сервера https://zabbix.example.com не проходит проверку
    (нет в trust store Python). Это обычно self-signed сертификат или
    сертификат, подписанный внутренним корпоративным CA.

    ВАРИАНТЫ РЕШЕНИЯ:
    1) Быстрое: добавьте в Extra Connection-а флаг "verify_ssl": false
       Команда:
       airflow connections delete <conn_id>
       airflow connections add <conn_id> \
         --conn-type http --conn-host '...' \
         --conn-login '...' --conn-password '...' \
         --conn-extra '{"verify_ssl": false}'

    2) Правильное: установите корневой CA сертификат в trust store
       контейнера Airflow:
         cp corporate-ca.crt /usr/local/share/ca-certificates/
         update-ca-certificates
       либо через env-переменные REQUESTS_CA_BUNDLE / SSL_CERT_FILE.
  ```

  Также распознаются и подсвечиваются: DNS-сбои («Name or service not known»),
  отказ соединения («Connection refused»), таймауты — для каждого случая
  даётся короткая подсказка, что проверить.

## v1.0.8 — полный аудит на совместимость с Airflow 3.1.2

Систематическая ревизия DAG-файла по чек-листу breaking changes
и best practices Airflow 3.1+. Все 15 проверок пройдены, плюс одна
улучшающая правка.

### Улучшения

- **`start_date` через pendulum с timezone.** Раньше использовался
  `datetime(2025, 1, 1)` без таймзоны — это работало в Airflow 3,
  но генерировало warnings и не соответствует рекомендации из
  документации Airflow 3 «Time zone aware Dags». Теперь:
  ```python
  try:
      import pendulum
      _START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")
  except ImportError:
      _START_DATE = datetime(2025, 1, 1)   # fallback
  ```
  pendulum поставляется вместе с Airflow на любой современной
  инсталляции — fallback на случай очень старых сред.

### Подтверждённая совместимость (что проверено и работает)

Каждый пункт проверен статическим анализом + симуляцией импорта в
обоих режимах:

| # | Проверка | Статус |
|---|---|---|
| 1 | Синтаксис Python валиден (AST) | ✓ |
| 2 | Параметр DAG — `schedule=` (не `schedule_interval=`) | ✓ |
| 3 | На топ-уровне DAG нет `Variable.get()` / `BaseHook.get_connection()` | ✓ |
| 4 | Используется `airflow.sdk` namespace (рекомендация Airflow 3) | ✓ |
| 5 | `PythonOperator` из `providers.standard` (требование Airflow 3) | ✓ |
| 6 | `Variable.get()` обёрнут в `_var_get()` (обходит `default`/`default_var` несовместимость) | ✓ |
| 7 | `start_date` timezone-aware через pendulum | ✓ |
| 8 | `catchup=False` задан явно | ✓ |
| 9 | Не используются удалённые context-переменные (`execution_date`, `prev_ds` и др.) | ✓ |
| 10 | Нет `provide_context=True` (deprecated с Airflow 2.0) | ✓ |
| 11 | Нет `SubDagOperator` (удалён в Airflow 3) | ✓ |
| 12 | Нет прямого доступа к метабазе (`create_session`, `provide_session`, sqlalchemy) | ✓ |
| 13 | Нет SLA параметров (`sla=`, `sla_miss_callback`) — удалены в Airflow 3 | ✓ |
| 14 | Нет `from airflow.models import DAG` (deprecated) | ✓ |
| 15 | `airflow.operators.python` используется только как fallback для Airflow 2.x | ✓ |

### Подтверждённая работа в обоих режимах импорта

| Сценарий | `_USE_SDK` | DAG-импорт | Variable-импорт | BaseHook-импорт | `Variable.get` сигнатура |
|---|---|---|---|---|---|
| Airflow 3.x (есть `airflow.sdk`) | `True` | `airflow.sdk.DAG` | `airflow.sdk.Variable` | `airflow.sdk.bases.hook.BaseHook` | `default=` |
| Airflow 2.x (нет `airflow.sdk`) | `False` | `airflow.DAG` | `airflow.models.Variable` | `airflow.hooks.base.BaseHook` | `default_var=` |

## v1.0.7 — фикс несовместимой сигнатуры Variable.get() в Airflow 3 SDK

### Исправления

- **`Variable.get(default_var=...)` → `_var_get()` обёртка.**
  В Airflow 3 SDK сигнатура `Variable.get()` изменилась: параметр для
  значения по умолчанию называется **`default`**, а в Airflow 2 legacy
  API — **`default_var`**. Так как DAG поддерживает обе ветки одновременно
  (импорт через try/except), один и тот же вызов работать не мог:

  | API | Сигнатура |
  |---|---|
  | `airflow.models.Variable.get` (Airflow 2) | `get(key, default_var=...)` |
  | `airflow.sdk.Variable.get` (Airflow 3) | `get(key, default=...)` |

  Добавлена обёртка `_var_get(key, default)`, которая по флагу `_USE_SDK`
  (выставляется в зависимости от того, какой импорт сработал) вызывает
  правильный вариант. Все Variable.get-обращения в `_build_config()`
  заменены на эту обёртку. Старый код падал в Airflow 3 с
  `TypeError: Variable.get() got an unexpected keyword argument 'default_var'`.

## v1.0.6 — поддержка нескольких окружений (TEST + PROD)

### Изменения

- **DAG-фабрика для multi-env синхронизации.** Из одного DAG-файла
  регистрируются несколько независимых DAG'ов — по одному на каждую среду
  из словаря `ENVIRONMENTS` в начале файла. По умолчанию это `test` и
  `prod`. Добавить новую среду — одна запись в словаре + создать
  соответствующие Connections/Variables.

  Каждый DAG имеет собственные:
  - `dag_id`: `zabbix_templates_to_gitlab_test`, `zabbix_templates_to_gitlab_prod`;
  - расписание (TEST `*/5`, PROD `*/15` — настраивается в `ENVIRONMENTS`);
  - `default_quiet_period_sec` (TEST 5 мин, PROD 1 час — по ТЗ);
  - получателей email-алертов и owner'а;
  - тег в UI (`test`/`prod`) для фильтрации.

- **Изоляция секретов по средам.** Connections — `zabbix_<env>` и
  `gitlab_<env>`. Variables — с префиксом среды:
  `test_gitlab_project_id`, `prod_gitlab_project_id` и т.д.
  Один токен/пароль никогда не shared между TEST и PROD.

- **Раздел README про архитектурный выбор.** Объяснение, почему для
  multi-env используются Airflow Connections/Variables, а не
  `config.yaml`: безопасность (Fernet-шифрование секретов), ротация без
  передеплоя, совместимость с архитектурой Airflow 3 (нет top-level
  DB-чтений), интеграция с внешними secrets backends.

## v1.0.5 — полная совместимость с Airflow 3.x

### Исправления

- **Убраны top-level DB-обращения из DAG.**
  В Airflow 3 архитектура изменилась: DAG processor работает изолированно
  от метабазы. Любой вызов `Variable.get()` или `BaseHook.get_connection()`
  на ТОП-УРОВНЕ DAG-файла ломает регистрацию DAG'а с ошибкой
  `Dag not found during start up`. Все DB-обращения вынесены строго
  в тело таска `_build_config()`. Расписание `schedule="*/15 * * * *"`
  захардкожено в коде (раньше шло из Variable — это и было причиной
  ошибки регистрации).

- **Импорты через `airflow.sdk` для Airflow 3.x.**
  Официальный публичный API авторов DAG в Airflow 3 — пакет `airflow.sdk`.
  Старые пути (`from airflow import DAG`, `from airflow.hooks.base import
  BaseHook`, `from airflow.models import Variable`) ещё работают через
  deprecation shims, но генерируют warnings. DAG переведён на
  условные импорты: сначала пробуется `airflow.sdk`, fallback — legacy
  для Airflow 2.x. Поддерживаются обе ветки одним файлом.

- **Понятные сообщения если Variables/Connections не созданы.**
  Если `gitlab_project_id` Variable отсутствует или Connection не настроен,
  таск падает с конкретной инструкцией: какую команду запустить
  (`airflow variables set ...` / `airflow connections add ...`), чтобы
  это исправить. Раньше получался необработанный traceback.

### Документация

- В README добавлен подробный раздел про настройку Airflow 3:
  - команды `airflow connections add` / `airflow variables set` для CLI;
  - таблица всех Variables с пометкой «обязательная / опциональная»;
  - таблица диагностики типичных проблем с решениями.
- Пояснение почему `schedule` нельзя сделать настраиваемым через Variable
  в Airflow 3.

## v1.0.4 — совместимость с Airflow 3.x

### Исправления

- **`schedule_interval` → `schedule` в DAG.**
  В Airflow 3.0+ параметр `schedule_interval` полностью удалён (был
  deprecated с 2.4). Без этого фикса DAG не парсится в Airflow 3 с ошибкой
  `TypeError: DAG.__init__() got an unexpected keyword argument 'schedule_interval'`.
  Параметр `schedule` работает идентично и поддерживается всеми версиями
  Airflow 2.4+, так что DAG теперь совместим с 2.4-2.x и 3.x одновременно.

- **`PythonOperator` через provider-пакет.**
  В Airflow 3.0+ `PythonOperator` переехал из `airflow.operators.python`
  в `airflow.providers.standard.operators.python` (provider
  `apache-airflow-providers-standard`, ставится по умолчанию). Старый путь
  ещё работает в 3.0 с deprecation warning, но может быть удалён.
  В DAG добавлен try/except — сначала пробуем новый путь, fallback на
  старый для Airflow 2.x.

## v1.0.3 — production-ready

### Исправления

- **`auditlog.get`: правильный синтаксис фильтра для Zabbix 7.**
  Top-level параметров `resourcetypes`/`resourceids` в Zabbix 7 не существует.
  Фильтрация делается через `filter: {resourcetype: 30, resourceid: ...}`.
  Без этого фикса bulk-запрос audit log падал с
  `Invalid parameter "/": unexpected parameter "resourcetypes"`,
  и логика отсрочки на час не работала.

- **Замена bulk-стратегии на time-windowed запрос.**
  Раньше: «дай записи аудита для вот этих 400 id».
  Теперь: `get_recently_modified_templates(since)` — «дай все записи
  аудита по resourcetype=TEMPLATE за последние 2 часа». Один компактный
  запрос вместо одного огромного, плюс работает на любых правах.

### Улучшения производительности

- **Short-circuit для DEFER без `configuration.export`.**
  Если шаблон есть в audit log с правкой моложе `quiet_period_sec` И
  файл уже существует в GitLab — сразу ставим DEFER. Тяжёлый
  `configuration.export` (который сериализует весь шаблон с items,
  triggers, LLD, graphs) при этом НЕ вызывается. На больших
  инсталляциях с активным редактированием экономит минуты.

- **Прогресс-лог каждые 25 шаблонов.** На 400+ шаблонах sync идёт
  несколько минут — раньше выглядело как зависание. Теперь видно:
  `прогресс: 50/401 (created=2, updated=5, deferred=1, unchanged=42)`.

### UX-фиксы

- **Pre-flight check GitLab проекта (`verify_access`).**
  Делается ДО выгрузки шаблонов из Zabbix. Если в `config.yaml` неверный
  `project_id`, ветка или токен — падаем мгновенно, а не после
  пятиминутной выгрузки.

- **Поддержка пустого репозитория.** GitLab отдаёт 404 на
  `/repository/branches/main` и на `/repository/tree` для свежесозданного
  проекта без коммитов, хотя `default_branch: main` уже в метаданных.
  Это нормальный кейс — первый коммит создаст ветку. `verify_access()`
  проверяет поле `empty_repo` и пропускает проверку ветки;
  `list_files()` корректно возвращает `[]`.

- **Понятная диагностика для частых ошибок GitLab.**
  404 «Project Not Found» теперь подсказывает, какие значения положить
  в `gitlab.project_id` (путь vs числовой ID); 401/403 — что проверить
  в токене.

### Внутренние фиксы

- **`safe_filename`: схлопывание двойных подчёркиваний.**
  `"Windows: server | 2022"` → `"Windows_server_2022"`
  (раньше получалось `Windows__server___2022`).

- **Поддержка пустого `templates_subdir`.** Если файлы должны лежать
  в корне репозитория, а не в подпапке — указывайте `templates_subdir: ""`.

## v1.0.0 — первая версия

Изначальное решение по ТЗ:
1. Экспорт шаблонов Zabbix 7 → GitLab в формате YAML.
2. Шаблоны, отсутствующие в GitLab, копируются сразу.
3. Изменённые — через час после последнего изменения (через `auditlog.get`).
4. Планировщик — Apache Airflow (каждые 15 минут).
5. Полная поддержка кириллицы (UTF-8 на всех этапах).
