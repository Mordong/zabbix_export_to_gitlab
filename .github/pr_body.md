## Summary

Добавляет экспорт настроек аутентификации Zabbix (LDAP/SAML + JIT provisioning/mapping) в GitLab, параллельно существующему экспорту шаблонов. Логика портирована из проверенного `zabbix_reporter.py`. Только YAML; PDF отложен, чтобы сохранить минимализм зависимостей (PyYAML + urllib).

**Target branch:** `test`

## Что добавлено

- Два YAML-файла в `auth/`:
  - `authentication.yaml` — `authentication.get`, resourcetype 42;
  - `userdirectories.yaml` — `userdirectory.get`, resourcetype 49, с `provision_groups`/`provision_media`.
- В provision-mapping рядом с ID (`roleid`/`usrgrpid`/`mediatypeid`) добавлены резолвленные имена (`_role_name`/`_grp_name`/`_mt_name`).
- Правило отсрочки (quiet period) применяется раздельно к каждому файлу через свой тип ресурса audit log.
- Отдельный DAG `zabbix_auth_to_gitlab_<env>` (PROD `@daily`, TEST каждые 6 ч), переиспользует существующие Connections и `<env>_gitlab_project_id`.
- CLI-флаги `--auth` и `--auth-subdir`.

## Изменённые / новые файлы

- Правки: `zabbix_client.py`, `utils.py`, `cli.py`, `__init__.py`.
- Новые: `auth_sync.py`, `dags/zabbix_auth_to_gitlab.py`, `tests/test_smoke.py`.
- Документация: README, CHANGELOG (v1.2.0), config.example.yaml.

## Тестирование

- [x] Все модули компилируются; пакет импортируется (v1.2.0).
- [x] Smoke-тесты: create / unchanged / defer / update; кириллица сохраняется; ID+имя рядом.
- [x] Новый DAG прошёл чек-лист совместимости Airflow 3.1.2.
- [x] Регистрация обоих DAG проверена в ветках импорта SDK и legacy.
- [x] Регрессия: DAG шаблонов не затронут.
- [x] resourcetype-коды и форма API сверены с документацией Zabbix 7.4.

## Совместимость

Zabbix 7.4.5, Airflow 3.1.2. Полностью обратно-совместимо — синхронизация шаблонов не изменена.
