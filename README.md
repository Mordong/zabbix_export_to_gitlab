# Zabbix Template Sync → GitLab

Экспорт шаблонов мониторинга **Zabbix 7** в **GitLab** в формате **YAML**,
с планировщиком на **Apache Airflow**.

## Что делает

1. Подключается к Zabbix API (по образцу из `zabbix_reporter.py`: версия → `user.login` → Bearer-токен для 6.0+).
2. Получает список всех шаблонов (`template.get`) — опционально с фильтром по группам.
3. Экспортирует каждый шаблон в YAML через `configuration.export` (UTF-8, кириллица сохраняется как есть).
4. Сравнивает с тем, что лежит в GitLab репозитории.
5. Применяет правила:
   - **Шаблон отсутствует в GitLab** → коммитит сразу.
   - **Шаблон в GitLab отличается от Zabbix** → проверяет `auditlog.get`. Если со времени последнего изменения прошло **≥ 1 часа** — коммитит. Иначе откладывает до следующего запуска DAG.
   - **Содержимое идентично** → пропускает.
6. Все изменения за один прогон объединяются в один атомарный коммит (опционально).

## Почему «1 час» и почему через `auditlog.get`

Правило защищает от того, чтобы коммитить шаблон, который инженер ещё доделывает в веб-интерфейсе. Берём время последней записи в Zabbix audit log, относящейся к данному `templateid` — это точное «когда последний раз кто-то трогал шаблон». DAG крутится каждые 15 минут, так что максимальная задержка от завершения правок до коммита — `1 час + 15 минут`.

Если у учётки нет доступа к `auditlog.get` (типично для read-only API users) — синхронизатор падает в режим «коммитить любое расхождение». Это безопасно (хуже не будет), просто теряется отсрочка. Для read-only синхронизации достаточно ролевого доступа `Super admin` или специальной роли с правами на `auditlog.get`.

## Структура проекта

```
zabbix-template-sync/
├── zabbix_template_sync/
│   ├── __init__.py
│   ├── zabbix_client.py      # JSON-RPC клиент Zabbix (auth по образцу)
│   ├── gitlab_client.py      # REST клиент GitLab Repository API
│   ├── sync.py               # Логика синхронизации шаблонов
│   ├── auth_sync.py          # Логика синхронизации настроек аутентификации
│   ├── utils.py              # safe_filename + сравнение/сериализация YAML
│   └── cli.py                # python -m zabbix_template_sync.cli
├── dags/
│   ├── zabbix_templates_to_gitlab.py    # Airflow DAG (шаблоны)
│   └── zabbix_auth_to_gitlab.py         # Airflow DAG (аутентификация LDAP/SAML)
├── config.example.yaml
├── requirements.txt
└── README.md
```

## Экспорт настроек аутентификации (LDAP/SAML)

Помимо шаблонов, можно версионировать настройки аутентификации Zabbix —
глобальные флаги и JIT provisioning/mapping LDAP/SAML серверов. Экспортируются
два YAML-файла в поддиректорию `auth/`:

| Файл | Источник API | resourcetype в audit log |
|---|---|---|
| `auth/authentication.yaml` | `authentication.get` — глобальные флаги LDAP/SAML, JIT, политика паролей | 42 (Authentication) |
| `auth/userdirectories.yaml` | `userdirectory.get` — LDAP/SAML серверы с `provision_groups`/`provision_media` | 49 (User directory) |
| `auth/userdirectory_<имя>.csv` | то же, маппинг групп в CSV (по файлу на каждый directory) | 49 (User directory) |
| `auth/userdirectory_<имя>.md` | то же, маппинг групп в Markdown-таблице | 49 (User directory) |

В provision-mapping рядом с сырыми ID (`roleid`, `usrgrpid`, `mediatypeid`)
добавлены резолвленные имена в полях `_role_name` / `_grp_name` / `_mt_name` —
YAML остаётся читаемым, но исходные ID не теряются.

Дополнительно для каждого directory выгружается CSV-маппинг групп
`auth/userdirectory_<имя>.csv` (имя через `safe_filename`, кириллица
сохраняется) — три столбца через `;`:

```
LDAP group pattern;User groups;User role
cn=admins,ou=groups,dc=corp;Zabbix administrators;Super admin role
cn=ops,dc=corp;Группа А,Группа Б;User role
```

`User groups` перечисляются через запятую. Кодировка UTF-8 без BOM; значения
с `;` или `,` квотируются автоматически. Если у directory нет
provision_groups — в файле только строка заголовков.

Параллельно для каждого directory выгружается тот же маппинг в виде
Markdown-таблицы `auth/userdirectory_<имя>.md` (заголовок `# <имя>` + таблица
с теми же тремя столбцами). Удобно просматривать прямо в веб-интерфейсе
GitLab. Символ `|` в значениях экранируется; пустой directory — заголовок и
шапка таблицы без строк.

Правило отсрочки (quiet period) работает так же, как для шаблонов, но
раздельно по каждому файлу, через свой тип ресурса audit log (42 и 49).
CSV подчиняются тому же правилу, что и `userdirectories.yaml` (resourcetype 49).
Поскольку auth-конфиг меняется редко, для него отдельный DAG с более редким
расписанием (PROD — `@daily`, TEST — каждые 6 часов).

Секреты (`bind_password` и т.п.) Zabbix API в ответе не отдаёт — поле
приходит пустым, поэтому маскирование не требуется.

### Запуск через CLI

```bash
# Синхронизировать auth-конфиг (вместо шаблонов)
python -m zabbix_template_sync.cli --config config.yaml --auth

# В корень репозитория, а не в auth/
python -m zabbix_template_sync.cli --config config.yaml --auth --auth-subdir ""

# Первичная заливка — игнорируем правило отсрочки
python -m zabbix_template_sync.cli --config config.yaml --auth --force-all
```

### Запуск через Airflow

DAG-файл `dags/zabbix_auth_to_gitlab.py` регистрирует
`zabbix_auth_to_gitlab_test` и `zabbix_auth_to_gitlab_prod`. Он переиспользует
те же Connections (`zabbix_<env>`, `gitlab_<env>`) и общую Variable
`<env>_gitlab_project_id`, что и DAG шаблонов. Дополнительные опциональные
Variables: `<env>_zabbix_auth_subdir` (default `auth`),
`<env>_zabbix_auth_quiet_period`, `<env>_zabbix_auth_single_commit`.

Учётке Zabbix для auth-экспорта нужны права на `authentication.get`,
`userdirectory.get` (и опционально `auditlog.get` для правила отсрочки).
В Zabbix эти методы доступны роли Super admin.

## Поддержка кириллицы

Гарантирована на каждом этапе:

| Этап | Что делается |
|---|---|
| Zabbix API | `configuration.export` сервер сам отдаёт YAML в UTF-8 с `allow_unicode=true` |
| Парсинг | `yaml.safe_load` работает с unicode-строками без преобразований |
| Имена файлов | `safe_filename()` нормализует Unicode (NFC), удаляет только запрещённые ФС-символы (`/\:*?"<>|`), кириллицу оставляет |
| Запись в файл | `open(..., encoding="utf-8")` |
| GitLab API | контент кодируется в `base64`, JSON-тело сериализуется с `ensure_ascii=False` |

Никаких `\u0414\u043B\u044F` в коммитах не появится.

## Установка

### 1. Зависимости

```bash
pip install -r requirements.txt
```

### 2. Учётка Zabbix

Создайте API-пользователя с ролью, дающей права:
- `template.get`, `templategroup.get`, `configuration.export` — чтение конфигурации;
- `auditlog.get` — для правила «1 час» (опционально).

### 3. Токен GitLab

Project Access Token (или Personal Access Token) со scope:
- `api`
- `write_repository`

## Запуск вручную (CLI)

```bash
# Через YAML-конфиг
cp config.example.yaml config.yaml
$EDITOR config.yaml
python -m zabbix_template_sync.cli --config config.yaml

# Или через переменные окружения
export ZABBIX_URL=https://zabbix.example.com
export ZABBIX_USER=api-readonly
export ZABBIX_PASSWORD='...'
export GITLAB_URL=https://gitlab.example.com
export GITLAB_PROJECT_ID='monitoring/zabbix-templates'
export GITLAB_TOKEN='glpat-...'
python -m zabbix_template_sync.cli

# Первичная заливка — игнорируем правило 1 часа
python -m zabbix_template_sync.cli --config config.yaml --force-all
```

## Запуск через Airflow (несколько сред: TEST + PROD)

Один DAG-файл регистрирует **несколько DAG'ов** — по одному на каждую среду
из списка `ENVIRONMENTS` внутри `zabbix_templates_to_gitlab.py`. У каждой
среды свои Connections, свои Variables (с префиксом среды) и своё расписание.

Поддерживаются Airflow 2.4+ и Airflow 3.x.

### Почему Variables, а не config.yaml

Все настройки и секреты хранятся в Airflow Connections и Variables, а не в
файле. Конкретно для среды TEST+PROD это даёт:

- **Безопасность.** Пароли и токены в Connections шифруются Fernet'ом в
  метабазе Airflow. В git-репозитории с config.yaml они лежали бы в открытом
  виде. Также легко подключить внешний secrets backend (Vault, AWS SM, GCP SM)
  без правок кода — Airflow умеет это из коробки.
- **Ротация без передеплоя.** Меняете значение в UI — DAG подхватит на
  следующем прогоне. С config.yaml потребуется git commit → deploy → reload.
- **RBAC и аудит.** Airflow логирует кто и когда менял Connection/Variable.
- **Без top-level чтения из DAG.** В Airflow 3 DAG processor работает
  изолированно от метабазы — любое чтение `config.yaml` на парсинге DAG
  сразу делает деплой хрупким.

`config.yaml` оставлен для CLI-режима (`python -m zabbix_template_sync.cli`)
— это удобно для разовых ручных прогонов и отладки вне Airflow.

### 1. Положите пакет туда, откуда Airflow его увидит

```
$AIRFLOW_HOME/dags/
├── zabbix_templates_to_gitlab.py
└── zabbix_template_sync/        # сам пакет, рядом с DAG
```

или установите как пакет: `pip install /path/to/zabbix-template-sync`.

### 2. Создайте Connections — по одному набору на каждую среду

```bash
# TEST
airflow connections add zabbix_test \
    --conn-type http \
    --conn-host 'https://zabbix-test.example.com' \
    --conn-login 'api-readonly' \
    --conn-password 'ПАРОЛЬ_TEST' \
    --conn-extra '{"verify_ssl": true}'

airflow connections add gitlab_test \
    --conn-type http \
    --conn-host 'https://gitlab.example.com' \
    --conn-password 'glpat-TOKEN_TEST' \
    --conn-extra '{"verify_ssl": true}'

# PROD
airflow connections add zabbix_prod \
    --conn-type http \
    --conn-host 'https://zabbix-prod.example.com' \
    --conn-login 'api-readonly' \
    --conn-password 'ПАРОЛЬ_PROD' \
    --conn-extra '{"verify_ssl": true}'

airflow connections add gitlab_prod \
    --conn-type http \
    --conn-host 'https://gitlab.example.com' \
    --conn-password 'glpat-TOKEN_PROD' \
    --conn-extra '{"verify_ssl": true}'
```

### 3. Создайте Variables — обязательные с префиксом среды

```bash
# TEST — обязательная
airflow variables set test_gitlab_project_id "observability/zabbix-cis/templates/test"

# PROD — обязательная
airflow variables set prod_gitlab_project_id "observability/zabbix-cis/templates/prod"

# Опциональные — можно не создавать, есть defaults в коде
airflow variables set test_zabbix_sync_subdir ""           # в корень репо
airflow variables set prod_zabbix_sync_subdir "templates"
airflow variables set prod_zabbix_sync_template_groups '["Templates/OS","Шаблоны/СУБД"]'
```

#### Полная таблица Variables (на каждую среду свой префикс)

| Имя | Обязательная | Default | Заметки |
|---|---|---|---|
| `<env>_gitlab_project_id` | да | — | путь или ID проекта GitLab |
| `<env>_gitlab_branch` | нет | `main` | целевая ветка |
| `<env>_zabbix_sync_subdir` | нет | `templates` | поддиректория; `""` = в корень |
| `<env>_zabbix_sync_quiet_period` | нет | 300 (TEST) / 3600 (PROD) | секунд «тишины» перед коммитом |
| `<env>_zabbix_sync_template_groups` | нет | `[]` | JSON-массив групп для фильтра |
| `<env>_zabbix_sync_single_commit` | нет | `true` | объединять изменения в 1 коммит |
| `<env>_zabbix_timeout_sec` | нет | `60` | базовый HTTP-таймаут к Zabbix API |
| `<env>_zabbix_audit_timeout_sec` | нет | `180` | таймаут для тяжёлого `auditlog.get` запроса |
| `<env>_audit_window_padding_sec` | нет | `900` | запас к окну `auditlog.get` сверх `quiet_period` |
| `<env>_audit_query_limit` | нет | `5000` | верхняя граница записей в одном `auditlog.get` |

### 4. Настройте расписание / параметры среды в коде DAG'а

Откройте `zabbix_templates_to_gitlab.py` и подправьте `ENVIRONMENTS` под себя:

```python
ENVIRONMENTS = {
    "test": {
        "schedule": "*/5 * * * *",            # TEST — каждые 5 минут
        "default_quiet_period_sec": 300,      # 5 минут «тишины»
        "owner": "monitoring-team",
        "email": ["[email protected]"],
        "env_tag": "test",
    },
    "prod": {
        "schedule": "*/15 * * * *",           # PROD — каждые 15 минут
        "default_quiet_period_sec": 3600,     # 1 час «тишины» (по ТЗ)
        "owner": "monitoring-team",
        "email": ["[email protected]"],
        "env_tag": "prod",
    },
}
```

### 5. Включите DAG'и

В UI появятся два DAG'а: `zabbix_templates_to_gitlab_test` и
`zabbix_templates_to_gitlab_prod`. Их можно включать/выключать независимо,
фильтровать по тегам `test`/`prod`.

### Чтобы добавить новую среду (например, STAGING)

1. Добавить запись `"staging": {...}` в `ENVIRONMENTS` в DAG-файле.
2. Создать Connections `zabbix_staging` и `gitlab_staging`.
3. Создать Variable `staging_gitlab_project_id`.
4. После следующего парсинга DAG `zabbix_templates_to_gitlab_staging`
   появится сам — никаких других правок не нужно.

### Диагностика проблем

| Симптом | Причина | Решение |
|---|---|---|
| `Dag not found during start up` | DB-обращение на топ-уровне DAG (в Airflow 3) | Берите свежую версию DAG'а — все Variable/Connection вызовы должны быть внутри `_build_config()` |
| `Airflow Variable 'test_gitlab_project_id' не задана` | Не создана обязательная Variable | Создайте её для нужной среды (см. шаг 3) |
| `Connection 'zabbix_test' не найден` | Не создан Connection | См. шаг 2 для нужной среды |
| `[SSL: CERTIFICATE_VERIFY_FAILED] unable to get local issuer certificate` | Сертификат Zabbix/GitLab подписан внутренним CA, которого нет в trust store контейнера | Быстро: в Extra Connection-а поставьте `{"verify_ssl": false}`. Правильно: положите CA в `/usr/local/share/ca-certificates/` контейнера и запустите `update-ca-certificates`. |
| `Запрос '<method>' к Zabbix не уложился в N сек` | Zabbix не успел ответить за таймаут. Чаще всего на `auditlog.get` при большой/нагруженной таблице audit log | Поднять `<env>_zabbix_audit_timeout_sec` (например до 300). Дополнительно — уменьшить `<env>_audit_window_padding_sec` или `<env>_audit_query_limit`. Проверить индексы `auditlog` в Zabbix БД (см. ниже). |
| `Name or service not known` / DNS-ошибки | Из контейнера Airflow не резолвится hostname | Проверьте DNS и `/etc/resolv.conf`, корпоративные DNS-серверы должны быть доступны воркеру |
| `Connection refused` | Порт закрыт или сервис не запущен | Проверьте firewall/NAT/маршруты между Airflow worker и Zabbix/GitLab |
| `TypeError: ... 'schedule_interval'` | Старая версия DAG в Airflow 3 | Берите свежий `zabbix_templates_to_gitlab.py` |
| `TypeError: Variable.get() got an unexpected keyword argument 'default_var'` | Старая версия DAG, не использует обёртку `_var_get` | Подмените DAG-файл свежей версией |
| Deprecation warnings про `airflow.hooks.base.BaseHook` | Сработал legacy-fallback импорта | На Airflow 3 проверьте, что установлен `apache-airflow-providers-standard` |

## Структура репозитория-приёмника

После работы DAG'а в GitLab будет:

```
templates/
├── Linux_by_Zabbix_agent.yaml
├── Windows_by_Zabbix_agent.yaml
├── PostgreSQL_by_Zabbix_agent_active.yaml
├── Шаблон_СУБД_Oracle.yaml
└── ...
```

Импортировать обратно в Zabbix можно через UI (Configuration → Templates → Import) или через `configuration.import` API.

## Про ZabbixCI

[ZabbixCI](https://github.com/Slamdunk/ZabbixCI) — известная утилита, делающая в целом то же самое. Я её не использую, потому что:

1. ZabbixCI коммитит изменения **немедленно**, без отсрочки. Правило «1 час после последнего изменения» в неё не встроено и потребовало бы оборачивания всё равно.
2. Лишняя зависимость на pip-пакет, который придётся устанавливать на Airflow worker.
3. Текущая реализация на стандартной библиотеке Python (только `PyYAML` извне) — проще аудитировать и сопровождать.

Если ZabbixCI всё-таки предпочтителен, его можно вызывать из `BashOperator`, но логику «1 час» придётся реализовывать поверх (например, через стейт-файл с хешами).

## Поведение в граничных случаях

| Ситуация | Что происходит |
|---|---|
| Шаблон удалён в Zabbix | В GitLab остаётся (мы не делаем `delete`). При желании — расширить `_apply_actions`. |
| Шаблон переименован (`host` сменился) | Считается новым: создаётся файл с новым именем. Старый остаётся (см. выше). |
| Имя содержит `/` | Заменяется на `_` в `safe_filename()`. |
| Имя на чистой кириллице | Сохраняется как есть в UTF-8: `Шаблон_СУБД_Oracle.yaml`. |
| `auditlog.get` запрещён | Логируется warning, fallback на «коммитить любое расхождение». |
| GitLab вернул 5xx | Коммит падает, DAG получает failure → retry через 5 минут (2 попытки). |
| Несколько изменений за прогон | Все вместе в один коммит с понятным сообщением (если `single_commit=true`). |

## Лицензия

Внутреннее решение, лицензируйте по политике организации.
