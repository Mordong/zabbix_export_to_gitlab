# CHANGELOG

История правок по итогам отладочных запусков.

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
