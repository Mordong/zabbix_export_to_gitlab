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
│   ├── sync.py               # Логика синхронизации
│   ├── utils.py              # safe_filename + сравнение YAML
│   └── cli.py                # python -m zabbix_template_sync.cli
├── dags/
│   └── zabbix_templates_to_gitlab.py    # Airflow DAG
├── config.example.yaml
├── requirements.txt
└── README.md
```

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

## Запуск через Airflow

### 1. Положите пакет туда, откуда Airflow его увидит

Вариант A — установка как пакет:
```bash
pip install /path/to/zabbix-template-sync
```

Вариант B — синхронизировать в `dags/` через git-sync sidecar:
```
$AIRFLOW_HOME/dags/
├── zabbix_templates_to_gitlab.py
└── zabbix_template_sync/        # сам пакет, рядом с DAG
```

### 2. Connections в Airflow UI

**zabbix_default** (тип: HTTP):
- Host: `https://zabbix.example.com`
- Login: `api-readonly`
- Password: `<пароль>`
- Extra: `{"verify_ssl": true}`

**gitlab_default** (тип: HTTP):
- Host: `https://gitlab.example.com`
- Password: `<PRIVATE-TOKEN>` (поле password = токен)
- Extra: `{"verify_ssl": true}`

### 3. Variables

| Имя | Значение | Default |
|---|---|---|
| `gitlab_project_id` | `monitoring/zabbix-templates` | — (обязательно) |
| `gitlab_branch` | `main` | `main` |
| `zabbix_sync_subdir` | `templates` | `templates` |
| `zabbix_sync_quiet_period` | `3600` | `3600` |
| `zabbix_sync_template_groups` | `["Templates/OS","Шаблоны/СУБД"]` | `[]` (все) |
| `zabbix_sync_single_commit` | `true` | `true` |
| `zabbix_sync_schedule` | `*/15 * * * *` | `*/15 * * * *` |

### 4. Включите DAG

DAG `zabbix_templates_to_gitlab` появится в списке. По умолчанию запускается каждые 15 минут.

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
