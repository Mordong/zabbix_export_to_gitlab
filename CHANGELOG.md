# CHANGELOG

История правок по итогам отладочных запусков.

## v1.6.2 — отдельный таймаут под батч-экспорт хостов

Батчевый `configuration.export` отвечает дольше одиночного, поэтому ему задан
собственный увеличенный таймаут — по аналогии с выделенным таймаутом у
`auditlog.get`. Раньше батч использовал общий `zabbix_timeout_sec` (60 с) и
на крупных пачках мог упереться в таймаут.

- Новое поле `SyncConfig.zabbix_export_timeout_sec` (дефолт 300).
- Проброс: Variable `<env>_zabbix_export_timeout_sec`, env
  `ZABBIX_EXPORT_TIMEOUT_SEC`, yaml `zabbix_export_timeout_sec`.
- `export_yaml_by_ids` и `export_hosts_batched` принимают
  `timeout_override`/`export_timeout`; в `_call` уходит как `timeout_override`.
- Затронут только батч-экспорт хостов; остальные `configuration.export`
  (шаблоны, группы, media types) — на обычном таймауте.

## v1.6.1 — оптимизация экспорта хостов (батчинг)

Исправлено узкое место в `core`-экспорте: при 10–15 тыс. хостов выгрузка
занимала часы, т.к. `configuration.export` вызывался по одному хосту
(10–15 тыс. последовательных вызовов API).

### Что изменилось

- Хосты теперь экспортируются **пачками** (`configuration.export` с
  `options:{hosts:[...]}`), затем результат **нарезается** обратно на
  отдельные файлы. Для 15 000 хостов при batch_size=500 это ~30 вызовов
  вместо 15 000 — выгрузка укладывается в считанные минуты.
- Формат сохранён: по-прежнему **файл на хост** (`core/hosts/<имя>.yaml`),
  самодостаточный и импортируемый (общие секции version/host_groups/
  templates/value_maps копируются в каждый файл; volatile-поле `date`
  вырезается — стабильный дифф).
- Размер пачки настраивается: `SyncConfig.host_export_batch_size` (дефолт
  500), Variable `<env>_host_export_batch_size`, env `HOST_EXPORT_BATCH_SIZE`,
  yaml `host_export_batch_size`.

### Изменения в коде

- **`zabbix_client.py`** — `export_hosts_batched(hostids, batch_size)` +
  module-level `_slice_hosts_export()` (парсинг батча и пересборка одиночных
  документов на PyYAML, без новых зависимостей).
- **`config_sync.py`** — `_build_core` использует батч-экспорт; сигнатура
  builders унифицирована до `(zbx, folder, cfg)`.
- **`sync.py`** — поле `host_export_batch_size` в `SyncConfig`.
- **`dag_factory.py`**, **`cli.py`** — проброс размера пачки.

### Совместимость

- **Zabbix 7.4.5 / Airflow 3.1.2** — батч-`configuration.export` в форме
  API 7.4; затронут только `core`-экспорт хостов. Templates, auth, infra/ui,
  users/alerting не изменены. 17 smoke-тестов проходят.

## v1.6.0 — DR-экспорт infra и ui (proxies, maps, dashboards и др.)

Завершён disaster-recovery набор: добавлены оставшиеся объекты конфигурации
двумя новыми группами/DAG. Запуск раз в сутки в 19:00, без quiet period.

### Что нового

- **infra** (DAG `zabbix_infra_to_gitlab_<env>`):
  - `proxies/<имя>.yaml` — прокси поимённо (TLS PSK маскируется `[SECRET]`);
  - `proxygroups.yaml`, `drules.yaml` (сетевое обнаружение), `maintenance.yaml`
    — одним файлом в корне.
- **ui** (DAG `zabbix_ui_to_gitlab_<env>`):
  - `maps/<имя>.yaml` — карты поимённо (configuration.export);
  - `dashboards/<имя>.yaml` — дашборды поимённо;
  - `scripts/<имя>.yaml` — скрипты поимённо (password маскируется `[SECRET]`).
- Оба DAG: расписание `0 19 * * *`, без quiet period.

### Метод и секреты

- maps — `configuration.export`; остальное — `*.get` + `dump_yaml`.
- proxy groups (`proxygroup.get`, Zabbix 7.0+) — с мягким fallback на пустой
  список, если метод недоступен по версии/правам.
- Секреты: proxy TLS PSK и script password, где приходят непустыми,
  заменяются на `[SECRET]`.

### Изменения в коде

- **`zabbix_client.py`** — `get_proxies`/`list_proxies_brief`/`get_proxy`
  (маскировка PSK), `get_proxy_groups` (fallback), `get_discovery_rules`,
  `get_maintenances`, `list_maps`/`export_map_yaml`,
  `list_dashboards`/`get_dashboard`, `list_scripts`/`get_script` (маскировка
  пароля).
- **`config_sync.py`** — группы `infra` и `ui` + хелпер `_named_items`
  (поимённый экспорт в свою папку).
- **`dag_factory.py`** — параметр `schedule` (дефолт `0 21 * * *` сохранён).
- **`dags/zabbix_{infra,ui}_to_gitlab.py`** (новые) — обёртки с `0 19 * * *`.

### Совместимость

- **Zabbix 7.4.5 / Airflow 3.1.2** — методы в форме API 7.4; новые DAG по тем
  же паттернам Airflow 3 (проверено: регистрация 4 новых DAG в ветках импорта
  SDK и legacy). 16 smoke-тестов проходят.
- Обратно-совместимо: предыдущие экспорты не затронуты.

## v1.5.0 — disaster-recovery экспорт конфигурации (users / alerting / core)

Добавлен экспорт минимального набора объектов для восстановления Zabbix «с
нуля» (ребилд серверов, потеря БД, стирание объектов) — помимо шаблонов и
auth. Три новые группы, по папке и DAG на каждую.

### Что нового

- **users/** — `roles.yaml`, `usergroups.yaml` (с правами на host/template
  groups и tag-фильтрами), `users.yaml` (включая локальных, которые НЕ
  пересоздаются LDAP-провижнингом).
- **alerting/** — `mediatypes.yaml` (configuration.export), `actions.yaml`
  (условия + операции оповещений/эскалаций).
- **core/** — `hostgroups.yaml`, `templategroups.yaml`, `macros.yaml`
  (глобальные) + `core/hosts/<имя>.yaml` (по файлу на каждый хост через
  configuration.export).
- **3 новых DAG** (`zabbix_users/alerting/core_to_gitlab_<env>`): расписание
  `0 21 * * *` (ежедневно в 21:00), **без quiet period** — любое расхождение
  коммитится сразу.

### Метод и секреты

- Где Zabbix поддерживает `configuration.export` (hosts, host/template
  groups, media types) — используется он (нативный импортируемый YAML);
  остальное (roles, user groups, users, actions, global macros) — `*.get` +
  `dump_yaml`.
- Секреты API не отдаёт. Секретные макросы (type=1) с пустым значением
  помечаются маркером `[SECRET]`. Пароли пользователей в `user.get`
  отсутствуют как поле — восстанавливаются вручную (см. README). Токены в
  media types Zabbix также не экспортирует.

### Изменения в коде

- **`zabbix_client.py`** — `get_roles`, `get_usergroups`, `get_users`,
  `get_global_macros` (маскировка `[SECRET]`), `get_actions`;
  `export_yaml_by_ids`, `list_hosts`, `export_host_yaml`; константа
  `SECRET_PLACEHOLDER`.
- **`config_sync.py`** (новый) — `ConfigSynchronizer(config, group)` с
  декларативным описанием групп; без quiet period, hosts пофайлово, пустые
  `configuration.export` пропускаются.
- **`dag_factory.py`** (новый) — общая фабрика DR-DAG (чтобы не дублировать
  инфраструктуру трижды).
- **`dags/zabbix_{users,alerting,core}_to_gitlab.py`** (новые) — тонкие обёртки.

### Совместимость

- **Zabbix 7.4.5 / Airflow 3.1.2** — `configuration.export` и `*.get` в форме
  API 7.x; новые DAG по тем же паттернам Airflow 3 (проверено: регистрация 6
  DAG в ветках импорта SDK и legacy, schedule `0 21 * * *`).
- Обратно-совместимо: шаблоны и auth не затронуты.

## v1.4.0 — MD-маппинг групп + новые расписания DAG

### Что нового

- **MD-файл маппинга групп** рядом с CSV — по одному на каждый directory:
  `auth/userdirectory_<имя>.md`. Заголовок `# <имя directory>` +
  Markdown-таблица с теми же тремя столбцами
  (`LDAP group pattern | User groups | User role`). User groups через
  запятую, имена резолвятся (fallback на ID). Символ `|` в значениях
  экранируется. Пустые provision_groups → заголовок + шапка таблицы без
  строк (единообразно с CSV). CSV-файлы сохранены — MD добавляется
  параллельно.
- **Изменены расписания DAG по умолчанию (все среды):**
  - auth (`zabbix_auth_to_gitlab`): `0 0 * * *` (ежедневно в полночь);
  - templates (`zabbix_templates_to_gitlab`): `*/30 * * * *` (каждые 30 минут).

### Изменения в коде

- **`utils.py`** — `dump_user_group_mapping_md()`; общий обход
  `_iter_group_mapping_rows()` для CSV и MD (единая логика данных) и
  `_md_escape()` для безопасных ячеек таблицы. Без новых зависимостей.
- **`auth_sync.py`** — в directory-цикле рядом с CSV генерируется MD
  (тот же resourcetype 49, то же правило отсрочки, общий атомарный коммит).
- **`dags/*.py`** — обновлены значения `schedule` для test и prod.

### Совместимость

- **Zabbix 7.4.5 / Airflow 3.1.2** — MD строится из уже получаемых полей
  `userdirectory.get`; правка `schedule` не меняет паттерны Airflow 3
  (значение `schedule=`, без top-level DB-обращений). Расписания и
  регистрация обоих DAG проверены тестами в ветках импорта SDK и legacy.
- Обратно-совместимо: YAML/CSV auth и экспорт шаблонов не затронуты по
  логике (изменилась только частота запуска шаблонного DAG).

## v1.3.0 — CSV-маппинг групп LDAP/SAML

К экспорту user directories добавлен CSV-файл маппинга групп — по одному на
каждый directory, рядом с `userdirectories.yaml`. Удобно открывать в Excel и
быстро видеть соответствие LDAP-групп ролям и группам Zabbix.

### Что нового

- **CSV на каждый directory:** `auth/userdirectory_<имя>.csv` (имя через
  `safe_filename`, кириллица сохраняется).
- **Три столбца через `;`:** `LDAP group pattern;User groups;User role`.
  - `LDAP group pattern` — `provision_groups[].name`;
  - `User groups` — имена user-групп через запятую (`_grp_name`);
  - `User role` — имя роли (`_role_name`).
- Если у directory нет provision_groups — файл содержит только строку
  заголовков.

### Детали реализации

- **`utils.py`** — `dump_user_group_mapping_csv()` на стандартном модуле
  `csv` (новых зависимостей нет). Разделитель `;`, кодировка UTF-8 без BOM,
  переводы строк `\n` (стабильный дифф независимо от ОС). Значения с `;`,
  `,`, кавычками или переносом строки квотируются автоматически — запятые
  внутри «User groups» не ломают структуру файла. Порядок строк — как в
  ответе API. При отсутствии резолвленных имён — fallback на ID.
- **`auth_sync.py`** — `_process_resource` обобщён: принимает готовое
  содержимое и функцию сравнения (`yaml_semantic_equal` для YAML,
  побайтовое для CSV). Результат `get_userdirectories()` переиспользуется
  для YAML и всех CSV — без второго вызова API. CSV подчиняются тому же
  правилу отсрочки (resourcetype 49) и попадают в общий атомарный коммит.

### Совместимость

- **Zabbix 7.4.5 / Airflow 3.1.2** — CSV строится из уже получаемых полей
  `userdirectory.get`; новых API-вызовов и изменений в DAG-паттернах нет.
  Регистрация обоих DAG проверена в ветках импорта SDK и legacy.
- Полностью обратно-совместимо: экспорт шаблонов и YAML auth не затронут.

## v1.2.0 — экспорт настроек аутентификации (LDAP/SAML provisioning/mapping)

Добавлен экспорт настроек аутентификации Zabbix в GitLab в формате YAML,
параллельно с экспортом шаблонов. Логика портирована из проверенного
`zabbix_reporter.py`. PDF-экспорт намеренно отложен, чтобы сохранить
минимализм зависимостей (только PyYAML + urllib).

### Что нового

- **Два YAML-файла в поддиректории `auth/`:**
  - `auth/authentication.yaml` — глобальные флаги (`authentication.get`);
  - `auth/userdirectories.yaml` — LDAP/SAML directories с JIT provisioning
    (`provision_groups`, `provision_media`).
- **Резолвинг ID + имя.** В provision-mapping рядом с `roleid`/`usrgrpid`/
  `mediatypeid` добавлены `_role_name`/`_grp_name`/`_mt_name`.
- **Отдельный DAG** `zabbix_auth_to_gitlab_<env>` с более редким расписанием
  (PROD `@daily`, TEST каждые 6 часов) — auth-конфиг меняется редко.
- **CLI-флаги** `--auth` и `--auth-subdir`.

### Изменения в коде

- **`zabbix_client.py`** — константы `AUDIT_RESOURCE_AUTHENTICATION = 42` и
  `AUDIT_RESOURCE_USERDIRECTORY = 49` (подтверждены по официальной документации
  auditlog Zabbix 7.4); методы `get_authentication()`, `get_userdirectories()`
  (с fallback для версий без provisioning), обобщённый
  `get_latest_audit_clock(resourcetype, ...)` и helper `_resolve_names()`.
- **`utils.py`** — `dump_yaml()`: стабильная сериализация dict→YAML
  (`allow_unicode=True`, `sort_keys=True`) для устойчивого диффа.
- **`auth_sync.py`** (новый) — `AuthSynchronizer`: pre-flight GitLab → экспорт
  обоих ресурсов → раздельное решение (NEW/UPDATE/DEFER/unchanged) с правилом
  отсрочки по соответствующему resourcetype. Переиспользует `GitLabClient` и
  `yaml_semantic_equal`.
- **`dags/zabbix_auth_to_gitlab.py`** (новый) — самодостаточная DAG-фабрика по
  тем же паттернам Airflow 3.x, что и DAG шаблонов.

### Правило отсрочки для auth

Применяется раздельно к каждому файлу через свой тип ресурса audit log
(42 для authentication, 49 для user directory). При недоступности
`auditlog.get` — fallback на «коммитить любое расхождение», как и у шаблонов.

### Совместимость

- **Zabbix 7.4.5** — resourcetype-коды и форма `auditlog.get`/`userdirectory.get`
  проверены по документации ветки 7.4.
- **Airflow 3.1.2** — новый DAG прошёл тот же чек-лист, что и DAG шаблонов
  (см. v1.0.8): `schedule=`, нет top-level DB-обращений, `airflow.sdk` +
  legacy fallback, `PythonOperator` из `providers.standard`, обёртка
  `_var_get`, tz-aware `start_date`, `catchup=False`. Регистрация обоих DAG
  проверена симуляцией в обеих ветках импорта (SDK / legacy).
- Полностью обратно-совместимо: синхронизация шаблонов не затронута.

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
