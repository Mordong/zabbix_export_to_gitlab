## Summary

Экспорт настроек аутентификации Zabbix (LDAP/SAML) в GitLab + CSV-маппинг групп. Логика портирована из проверенного `zabbix_reporter.py`. Только YAML/CSV; PDF отложен, чтобы сохранить минимализм зависимостей (PyYAML + urllib + stdlib `csv`).

**Target branch:** `test`

## Что добавлено

YAML-экспорт настроек аутентификации:
- `auth/authentication.yaml` — `authentication.get`, resourcetype 42;
- `auth/userdirectories.yaml` — `userdirectory.get`, resourcetype 49, с `provision_groups`/`provision_media`.
- В provision-mapping рядом с ID добавлены резолвленные имена (`_role_name`/`_grp_name`/`_mt_name`).

CSV-маппинг групп (по файлу на каждый directory):
- `auth/userdirectory_<имя>.csv` — три столбца через `;`: `LDAP group pattern;User groups;User role`.
- `User groups` через запятую; имена резолвятся (fallback на ID); UTF-8 без BOM; значения квотируются по необходимости; пустые provision_groups → только шапка.

Инфраструктура:
- Отдельный DAG `zabbix_auth_to_gitlab_<env>` (PROD `@daily`, TEST каждые 6 ч).
- CLI-флаги `--auth` и `--auth-subdir`.
- Правило отсрочки (quiet period) раздельно по каждому файлу через свой resourcetype audit log (42 / 49); CSV — по 49.

## Тестирование

- [x] Все модули компилируются; пакет импортируется (v1.3.0).
- [x] Smoke-тесты (9/9): create / unchanged / defer / update; CSV-квотирование `;` и `,`; кириллица; пустые группы → шапка; safe_filename; один вызов API.
- [x] DAG прошёл чек-лист Airflow 3.1.2; регистрация проверена в ветках импорта SDK и legacy.
- [x] Регрессия: экспорт шаблонов не затронут.
- [x] resourcetype-коды и форма API сверены с документацией Zabbix 7.4.

## Совместимость

Zabbix 7.4.5, Airflow 3.1.2. Полностью обратно-совместимо.
