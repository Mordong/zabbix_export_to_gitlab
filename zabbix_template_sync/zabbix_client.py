"""
Zabbix JSON-RPC API клиент для экспорта шаблонов в YAML.

Авторизация выполнена по образцу из zabbix_reporter.py:
  - apiinfo.version и user.login вызываются БЕЗ авторизационного токена;
  - в Zabbix 6.0+ токен передаётся в HTTP-заголовке Authorization: Bearer <token>;
  - в Zabbix 5.x   токен передаётся в поле "auth" тела JSON-RPC запроса.

Для синхронизации шаблонов используются методы:
  - template.get          — список шаблонов
  - configuration.export  — экспорт шаблона в YAML (поддерживает кириллицу, UTF-8)
  - auditlog.get          — время последнего изменения конкретного шаблона
"""

from __future__ import annotations

import json
import logging
import ssl
import urllib.error
import urllib.request
from typing import Any

log = logging.getLogger(__name__)

# Типы ресурсов в audit log Zabbix 7.x.
# Источник: официальная документация auditlog/object (release/7.4) —
#   30 - Template, 42 - Authentication, 49 - User directory.
# Эти коды стабильны в ветке 7.x; проверено по docs Zabbix 7.4.
AUDIT_RESOURCE_TEMPLATE = 30
AUDIT_RESOURCE_AUTHENTICATION = 42
AUDIT_RESOURCE_USERDIRECTORY = 49

# Поддерживаемые форматы экспорта в Zabbix 7.x
EXPORT_FORMAT_YAML = "yaml"

# Маркер на месте секретов, которые Zabbix API не отдаёт в открытом виде
# (значение секретного макроса/токена приходит пустым). Подставляется в
# экспорт, чтобы при восстановлении было видно: поле есть, значение — вручную.
SECRET_PLACEHOLDER = "[SECRET]"
EXPORT_FORMAT_XML = "xml"
EXPORT_FORMAT_JSON = "json"


class ZabbixAPIError(RuntimeError):
    """Ошибка, возвращённая Zabbix API в поле 'error' JSON-RPC ответа."""


class ZabbixAPI:
    """
    Минималистичный клиент Zabbix JSON-RPC API.
    Стиль авторизации (Bearer / body.auth) воспроизведён из ZabbixAPI в zabbix_reporter.py.
    """

    def __init__(self, url: str, verify_ssl: bool = True, timeout: int = 60):
        """
        :param timeout: базовый таймаут для всех JSON-RPC запросов (секунды).
            Тяжёлые запросы (например auditlog.get) могут переопределять его
            через параметр timeout_override в _call(); см. также
            get_recently_modified_templates(audit_timeout).
        """
        self.url = url.rstrip("/") + "/api_jsonrpc.php"
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.auth_token: str | None = None
        self._rid = 1
        self._api_version: tuple[int, int] | None = None

    # ──────────────────────────────────────────────────────────────────────────
    # Низкоуровневый JSON-RPC вызов
    # ──────────────────────────────────────────────────────────────────────────
    def _call(
        self,
        method: str,
        params: Any,
        _no_auth: bool = False,
        timeout_override: int | None = None,
    ) -> Any:
        body: dict[str, Any] = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._rid,
        }
        self._rid += 1

        headers = {"Content-Type": "application/json-rpc"}
        if not _no_auth and self.auth_token:
            if self._api_version and self._api_version >= (6, 0):
                headers["Authorization"] = f"Bearer {self.auth_token}"
            else:
                body["auth"] = self.auth_token

        log.debug("Zabbix → %s", method)

        req = urllib.request.Request(
            self.url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers=headers,
        )

        ctx = ssl.create_default_context()
        if not self.verify_ssl:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE

        effective_timeout = timeout_override if timeout_override is not None else self.timeout

        try:
            with urllib.request.urlopen(req, context=ctx, timeout=effective_timeout) as r:
                # Zabbix всегда отвечает в UTF-8
                response = json.loads(r.read().decode("utf-8"))
        except TimeoutError as e:
            # Соединение установилось, но Zabbix не ответил за отведённое время.
            # Чаще всего это значит, что запрос дорогой (большая выборка из
            # auditlog / configuration.export для тяжёлого шаблона) или сам
            # Zabbix перегружен.
            log.error(
                "Zabbix API timeout on %s после %d сек чтения",
                method, effective_timeout,
            )
            raise ConnectionError(
                f"Запрос '{method}' к Zabbix не уложился в {effective_timeout} сек.\n"
                f"\n"
                f"  ВАРИАНТЫ РЕШЕНИЯ:\n"
                f"  1) Увеличьте таймаут для этой среды:\n"
                f"     airflow variables set <env>_zabbix_timeout_sec 120         "
                f"# базовый\n"
                f"     airflow variables set <env>_zabbix_audit_timeout_sec 300   "
                f"# для auditlog.get\n"
                f"  2) Если падает на auditlog.get — уменьшите окно или лимит:\n"
                f"     airflow variables set <env>_audit_window_padding_sec 300\n"
                f"     airflow variables set <env>_audit_query_limit 1000\n"
                f"  3) Проверьте нагрузку на Zabbix-сервер и индексы таблицы auditlog\n"
                f"     (см. README, раздел «Диагностика проблем»)."
            ) from e
        except urllib.error.URLError as e:
            reason = e.reason
            log.error("Zabbix API connection error on %s: %s", method, reason)

            # Диагностика частых случаев — даём пользователю подсказку,
            # вместо криптического stacktrace.
            hint = ""
            reason_str = str(reason)
            if "CERTIFICATE_VERIFY_FAILED" in reason_str or "unable to get local issuer" in reason_str:
                hint = (
                    f"\n\n→ SSL-сертификат сервера {self.url.split('/api_jsonrpc.php')[0]} "
                    f"не проходит проверку (нет в trust store Python).\n"
                    f"  Это обычно self-signed сертификат или сертификат, "
                    f"подписанный внутренним корпоративным CA.\n"
                    f"\n"
                    f"  ВАРИАНТЫ РЕШЕНИЯ:\n"
                    f"  1) Быстрое: добавьте в Extra Connection-а флаг "
                    f"\"verify_ssl\": false\n"
                    f"     Команда:\n"
                    f"     airflow connections delete <conn_id>\n"
                    f"     airflow connections add <conn_id> \\\n"
                    f"       --conn-type http --conn-host '{self.url.split('/api_jsonrpc.php')[0]}' \\\n"
                    f"       --conn-login '...' --conn-password '...' \\\n"
                    f"       --conn-extra '{{\"verify_ssl\": false}}'\n"
                    f"\n"
                    f"  2) Правильное: установите корневой CA сертификат "
                    f"в trust store контейнера Airflow:\n"
                    f"     cp corporate-ca.crt /usr/local/share/ca-certificates/\n"
                    f"     update-ca-certificates\n"
                    f"     либо через env-переменные REQUESTS_CA_BUNDLE / SSL_CERT_FILE."
                )
            elif "Name or service not known" in reason_str or "nodename nor servname" in reason_str:
                hint = (
                    f"\n→ Не удалось разрешить DNS-имя хоста. "
                    f"Проверьте URL в Connection и доступность DNS из Airflow worker."
                )
            elif "Connection refused" in reason_str:
                hint = (
                    f"\n→ Сервер Zabbix недоступен по этому адресу/порту. "
                    f"Проверьте, что HTTPS-порт открыт и сервер запущен."
                )
            elif "timed out" in reason_str.lower():
                hint = (
                    f"\n→ Таймаут подключения (не чтения). "
                    f"Проверьте сетевую доступность Zabbix из Airflow worker "
                    f"(firewall, прокси, маршруты)."
                )

            raise ConnectionError(
                f"Ошибка подключения к Zabbix: {reason}{hint}"
            ) from e

        if "error" in response:
            err = response["error"]
            raise ZabbixAPIError(
                f"Zabbix API [{err.get('code')}]: {err.get('message')} — {err.get('data')}"
            )
        return response["result"]

    # ──────────────────────────────────────────────────────────────────────────
    # Жизненный цикл сессии
    # ──────────────────────────────────────────────────────────────────────────
    def login(self, user: str, password: str) -> None:
        """
        Получает версию API (определяет, какой способ передачи токена использовать),
        затем выполняет user.login без авторизации.
        """
        ver_str = self._call("apiinfo.version", {}, _no_auth=True)
        try:
            parts = ver_str.split(".")
            self._api_version = (int(parts[0]), int(parts[1]))
        except (ValueError, IndexError):
            self._api_version = (6, 0)
        log.info("Подключение к Zabbix API версии %s", ver_str)

        self.auth_token = self._call(
            "user.login",
            {"username": user, "password": password},
            _no_auth=True,
        )
        log.info("Аутентификация в Zabbix успешна")

    def logout(self) -> None:
        if self.auth_token:
            try:
                self._call("user.logout", [])
            except Exception as e:  # noqa: BLE001
                log.warning("Ошибка при logout: %s", e)
            self.auth_token = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    # ──────────────────────────────────────────────────────────────────────────
    # Шаблоны
    # ──────────────────────────────────────────────────────────────────────────
    def get_templates(
        self,
        host_groups: list[str] | None = None,
        name_filter: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Возвращает список шаблонов с минимальным набором полей,
        достаточным для синхронизации. Возвращает поля: templateid, host, name.

        :param host_groups: имена групп шаблонов для фильтрации (опционально).
        :param name_filter: список host-имён шаблонов для точной выборки.
        """
        params: dict[str, Any] = {
            "output": ["templateid", "host", "name", "description"],
        }
        if host_groups:
            # template.get принимает groupids — резолвим имена в id
            groups = self._call(
                "templategroup.get",
                {"output": ["groupid"], "filter": {"name": host_groups}},
            )
            if not groups:
                log.warning("Группы шаблонов не найдены: %s", host_groups)
                return []
            params["groupids"] = [g["groupid"] for g in groups]

        if name_filter:
            params["filter"] = {"host": name_filter}

        return self._call("template.get", params)

    def export_template_yaml(self, templateid: str) -> str:
        """
        Экспортирует один шаблон в YAML. Возвращает строку UTF-8.

        Zabbix 7 экспортирует YAML с allow_unicode=True на стороне сервера,
        поэтому кириллица сохраняется в исходном виде (не экранируется).
        """
        result: str = self._call(
            "configuration.export",
            {
                "format": EXPORT_FORMAT_YAML,
                "options": {"templates": [templateid]},
            },
        )
        # configuration.export всегда возвращает строку
        if not isinstance(result, str):
            raise ZabbixAPIError(
                f"configuration.export вернул не строку: {type(result).__name__}"
            )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Audit log — время последнего изменения шаблона
    # ──────────────────────────────────────────────────────────────────────────
    def get_template_last_modified(self, templateid: str) -> int | None:
        """
        Возвращает unix timestamp (clock) последней записи аудита,
        относящейся к данному шаблону, либо None если записей нет
        или если у пользователя нет доступа к auditlog.

        В audit log попадают только действия из веб-интерфейса/API
        (создание, обновление, удаление). Изменения, прилетевшие из обнаружения
        (LLD) и автоматических операций — туда обычно не пишутся, что для нашей
        задачи как раз корректно (нас интересуют именно человеческие правки).
        """
        try:
            records = self._call(
                "auditlog.get",
                {
                    "output": ["clock"],
                    # В Zabbix 7 фильтрация по типу/id ресурса идёт через filter,
                    # а не top-level resourcetypes/resourceids (которых нет).
                    "filter": {
                        "resourcetype": AUDIT_RESOURCE_TEMPLATE,
                        "resourceid": templateid,
                    },
                    "sortfield": "clock",
                    "sortorder": "DESC",
                    "limit": 1,
                },
            )
        except ZabbixAPIError as e:
            log.warning(
                "auditlog.get недоступен (templateid=%s): %s. "
                "Правило '1 час с момента изменения' не будет применено для этого шаблона.",
                templateid,
                e,
            )
            return None

        if not records:
            return None

        try:
            return int(records[0]["clock"])
        except (KeyError, TypeError, ValueError):
            return None

    def get_recently_modified_templates(
        self,
        since: int,
        audit_timeout: int = 180,
        limit: int = 5000,
    ) -> dict[str, int]:
        """
        Возвращает {templateid: latest_clock} — словарь шаблонов, у которых
        ЕСТЬ записи в audit log с clock >= since (unix timestamp).

        Один лёгкий запрос вместо запросов по каждому шаблону:
        фильтр по resourcetype=TEMPLATE и time_from. Записей за окно
        в час-полтора обычно единицы-десятки, нагрузки практически нет.

        Шаблоны, отсутствующие в результате, либо не правились в указанном
        окне, либо были изменены настолько давно, что записи уже вычищены
        housekeeper'ом — в обоих случаях нам они «стабильны».

        :param since: Unix-timestamp, с которого ищем правки.
        :param audit_timeout: таймаут именно для этого запроса (сек).
            Может быть кратно больше базового таймаута Zabbix API,
            т.к. auditlog.get на больших инсталляциях иногда занимает
            десятки секунд при холодном кэше БД.
        :param limit: верхняя граница числа возвращаемых записей.
            Реально за окно в час правок шаблонов бывают единицы-десятки,
            но запас на случай массовых правок не помешает.
        """
        try:
            records = self._call(
                "auditlog.get",
                {
                    "output": ["clock", "resourceid"],
                    "filter": {"resourcetype": AUDIT_RESOURCE_TEMPLATE},
                    "time_from": int(since),
                    "sortfield": "clock",
                    "sortorder": "DESC",
                    "limit": int(limit),
                },
                timeout_override=int(audit_timeout),
            )
        except ZabbixAPIError as e:
            log.warning("auditlog.get (bulk): %s", e)
            return {}

        latest: dict[str, int] = {}
        for rec in records:
            rid = str(rec.get("resourceid", ""))
            if not rid:
                continue
            try:
                clock = int(rec["clock"])
            except (KeyError, TypeError, ValueError):
                continue
            if rid not in latest or clock > latest[rid]:
                latest[rid] = clock
        return latest

    def get_latest_audit_clock(
        self,
        resourcetype: int,
        since: int,
        audit_timeout: int = 180,
        limit: int = 5000,
    ) -> int | None:
        """
        Возвращает unix timestamp последней записи audit log заданного
        resourcetype с clock >= since, либо None если таких записей нет
        (или если auditlog.get недоступен).

        В отличие от get_recently_modified_templates() здесь не важен
        resourceid: для глобальной конфигурации (authentication) её просто
        нет, а для user directories мы синхронизируем их единым файлом и
        достаточно знать, было ли ВООБЩЕ изменение любого ресурса этого
        типа в окне. Поэтому отдаём один максимальный clock по всему типу.

        :param resourcetype: код ресурса (см. AUDIT_RESOURCE_* выше).
        :param since: Unix-timestamp, с которого ищем правки.
        :param audit_timeout: таймаут именно для этого запроса (сек).
        :param limit: верхняя граница числа записей в ответе.
        """
        try:
            records = self._call(
                "auditlog.get",
                {
                    "output": ["clock"],
                    "filter": {"resourcetype": int(resourcetype)},
                    "time_from": int(since),
                    "sortfield": "clock",
                    "sortorder": "DESC",
                    "limit": int(limit),
                },
                timeout_override=int(audit_timeout),
            )
        except ZabbixAPIError as e:
            log.warning(
                "auditlog.get (resourcetype=%s): %s. "
                "Правило отсрочки для этого ресурса не будет применено.",
                resourcetype, e,
            )
            return None

        latest: int | None = None
        for rec in records:
            try:
                clock = int(rec["clock"])
            except (KeyError, TypeError, ValueError):
                continue
            if latest is None or clock > latest:
                latest = clock
        return latest

    # ──────────────────────────────────────────────────────────────────────────
    # Аутентификация и LDAP/SAML user directories
    # ──────────────────────────────────────────────────────────────────────────
    def get_authentication(self) -> dict[str, Any]:
        """
        Глобальные настройки аутентификации Zabbix (authentication.get).
        Это одна запись на весь сервер: общие флаги LDAP/SAML, параметры JIT,
        политика паролей и т.п. Секретов в открытом виде не содержит.
        """
        result = self._call("authentication.get", {"output": "extend"})
        # authentication.get возвращает один объект (dict), не список.
        if isinstance(result, list):
            return result[0] if result else {}
        return result

    def get_userdirectories(self) -> list[dict[str, Any]]:
        """
        Все LDAP/SAML user directories с JIT provisioning
        (provision_groups, provision_media).

        Для каждого provision-mapping рядом с сырыми ID (roleid, usrgrpid,
        mediatypeid) добавляются резолвленные имена в полях с префиксом '_':
        _role_name, _grp_name, _mt_name. Это делает YAML читаемым, не теряя
        исходные ID (как и просил заказчик — «ID + имя рядом»).

        Поведение портировано из проверенного zabbix_reporter.py.
        bind_password и подобные секреты Zabbix API в ответе не отдаёт
        (поле приходит пустым), поэтому маскирование не требуется.

        На Zabbix < 6.4 (где нет provisioning) — мягкий fallback: directories
        возвращаются без provision_*-полей.
        """
        try:
            dirs = self._call("userdirectory.get", {
                "output": "extend",
                "selectProvisionMedia": "extend",
                "selectProvisionGroups": "extend",
            })
        except ZabbixAPIError as e:
            # Старые версии без provisioning — отдаём без selectProvision*
            if "selectProvision" in str(e) or "unexpected parameter" in str(e).lower():
                dirs = self._call("userdirectory.get", {"output": "extend"})
                for d in dirs:
                    d.setdefault("provision_groups", [])
                    d.setdefault("provision_media", [])
            else:
                raise

        # Собираем ID для резолвинга имён
        role_ids: set[str] = set()
        grp_ids: set[str] = set()
        mt_ids: set[str] = set()
        for d in dirs:
            for pg in d.get("provision_groups", []):
                if pg.get("roleid"):
                    role_ids.add(pg["roleid"])
                for ug in pg.get("user_groups", []):
                    if ug.get("usrgrpid"):
                        grp_ids.add(ug["usrgrpid"])
            for pm in d.get("provision_media", []):
                if pm.get("mediatypeid"):
                    mt_ids.add(pm["mediatypeid"])

        role_map = self._resolve_names(
            "role.get", "roleid", "name", role_ids,
        )
        grp_map = self._resolve_names(
            "usergroup.get", "usrgrpid", "name", grp_ids,
        )
        mt_map = self._resolve_names(
            "mediatype.get", "mediatypeid", "name", mt_ids,
        )

        # Встраиваем имена рядом с ID
        for d in dirs:
            for pg in d.get("provision_groups", []):
                pg["_role_name"] = role_map.get(pg.get("roleid", ""), "")
                for ug in pg.get("user_groups", []):
                    ug["_grp_name"] = grp_map.get(ug.get("usrgrpid", ""), "")
            for pm in d.get("provision_media", []):
                pm["_mt_name"] = mt_map.get(pm.get("mediatypeid", ""), "")

        return dirs

    def _resolve_names(
        self,
        method: str,
        id_field: str,
        name_field: str,
        ids: set[str],
    ) -> dict[str, str]:
        """
        Вспомогательный резолвинг {id: name} через *.get методы.
        Ошибки доступа не фатальны — возвращаем то, что удалось получить
        (имена в экспорте опциональны, сырые ID остаются в любом случае).
        """
        if not ids:
            return {}
        try:
            rows = self._call(method, {
                "output": [id_field, name_field],
                f"{id_field}s": list(ids),
            })
            return {r[id_field]: r.get(name_field, "") for r in rows}
        except ZabbixAPIError as e:
            log.warning("Резолвинг имён через %s не удался: %s", method, e)
            return {}

    # ──────────────────────────────────────────────────────────────────────────
    # Disaster-recovery экспорт: объекты конфигурации для восстановления.
    # Где Zabbix поддерживает configuration.export — используем его (нативный
    # импортируемый YAML); остальное — *.get + ручная сериализация в YAML.
    # Секреты Zabbix API не отдаёт; где значение приходит пустым — подставляем
    # маркер SECRET_PLACEHOLDER, чтобы в файле было видно, что поле есть, но
    # его значение нужно восстановить вручную.
    # ──────────────────────────────────────────────────────────────────────────
    def get_roles(self) -> list[dict[str, Any]]:
        """Роли пользователей (role.get) с правилами доступа (UI/API/модули)."""
        return self._call("role.get", {
            "output": "extend",
            "selectRules": "extend",
        })

    def get_usergroups(self) -> list[dict[str, Any]]:
        """
        Группы пользователей (usergroup.get) с правами на host groups,
        template groups и tag-фильтрами. На этих правах держится видимость
        для LDAP-провижненных пользователей.
        """
        return self._call("usergroup.get", {
            "output": "extend",
            "selectHostGroupRights": "extend",
            "selectTemplateGroupRights": "extend",
            "selectTagFilters": "extend",
            "selectUsers": ["userid", "username"],
        })

    def get_users(self) -> list[dict[str, Any]]:
        """
        Пользователи (user.get) — в т.ч. локальные, которые НЕ пересоздаются
        LDAP-провижнингом. Пароли API не отдаёт (поля passwd в ответе нет),
        поэтому маскировать нечего — факт отсутствия пароля отмечается в
        документации, не в данных.
        """
        return self._call("user.get", {
            "output": "extend",
            "selectUsrgrps": ["usrgrpid", "name"],
            "selectRole": ["roleid", "name"],
            "selectMedias": "extend",
        })

    def get_global_macros(self) -> list[dict[str, Any]]:
        """
        Глобальные макросы (usermacro.get globalmacro=True). Секретные макросы
        (type=1) приходят с пустым value — подставляем SECRET_PLACEHOLDER.
        """
        macros = self._call("usermacro.get", {
            "globalmacro": True,
            "output": "extend",
        })
        for m in macros:
            # type: 0=text, 1=secret, 2=vault
            if str(m.get("type", "0")) == "1" and not m.get("value"):
                m["value"] = SECRET_PLACEHOLDER
        return macros

    def get_actions(self) -> list[dict[str, Any]]:
        """
        Действия (action.get): условия и операции оповещений/эскалаций.
        Включает trigger/discovery/autoregistration/internal/service actions.
        """
        return self._call("action.get", {
            "output": "extend",
            "selectOperations": "extend",
            "selectRecoveryOperations": "extend",
            "selectUpdateOperations": "extend",
            "selectFilter": "extend",
        })

    def export_yaml_by_ids(self, option_key: str, ids: list[str]) -> str:
        """
        Обёртка configuration.export для произвольной группы объектов.

        :param option_key: ключ в options ('hosts', 'host_groups',
            'template_groups', 'mediaTypes' и т.п. — имена согласно API 7.x).
        :param ids: список id объектов.
        Возвращает YAML-строку (UTF-8). Если ids пуст — возвращает ''.
        """
        if not ids:
            return ""
        result = self._call("configuration.export", {
            "format": EXPORT_FORMAT_YAML,
            "options": {option_key: list(ids)},
        })
        if not isinstance(result, str):
            raise ZabbixAPIError(
                f"configuration.export ({option_key}) вернул не строку: "
                f"{type(result).__name__}"
            )
        return result

    def list_hosts(self) -> list[dict[str, Any]]:
        """Лёгкий список хостов (hostid + host + name) для поимённого экспорта."""
        return self._call("host.get", {"output": ["hostid", "host", "name"]})

    def export_host_yaml(self, hostid: str) -> str:
        """Экспортирует один хост в YAML через configuration.export."""
        return self.export_yaml_by_ids("hosts", [hostid])

    # ──────────────────────────────────────────────────────────────────────────
    # Infra-объекты: прокси, прокси-группы, сетевое обнаружение, обслуживание.
    # ──────────────────────────────────────────────────────────────────────────
    def get_proxies(self) -> list[dict[str, Any]]:
        """
        Прокси (proxy.get). TLS-секреты (tls_psk, tls_psk_identity) Zabbix API
        отдаёт только при наличии прав; где они приходят непустыми — маскируем
        в SECRET_PLACEHOLDER, чтобы PSK не утекал в git.
        """
        proxies = self._call("proxy.get", {"output": "extend"})
        return [self._mask_proxy_secrets(p) for p in proxies]

    @staticmethod
    def _mask_proxy_secrets(p: dict[str, Any]) -> dict[str, Any]:
        for secret_field in ("tls_psk", "tls_psk_identity"):
            if p.get(secret_field):
                p[secret_field] = SECRET_PLACEHOLDER
        return p

    def list_proxies_brief(self) -> list[dict[str, Any]]:
        """Лёгкий список прокси (proxyid + name) для поимённого экспорта."""
        return self._call("proxy.get", {"output": ["proxyid", "name"]})

    def get_proxy(self, proxyid: str) -> dict[str, Any]:
        """Один прокси (proxy.get) с маскировкой TLS-секретов."""
        rows = self._call("proxy.get", {"output": "extend", "proxyids": [proxyid]})
        return self._mask_proxy_secrets(rows[0]) if rows else {}

    def get_proxy_groups(self) -> list[dict[str, Any]]:
        """
        Прокси-группы (proxygroup.get, Zabbix 7.0+). На версиях/правах, где
        метод недоступен, — мягкий fallback на пустой список.
        """
        try:
            return self._call("proxygroup.get", {"output": "extend"})
        except ZabbixAPIError as e:
            log.warning(
                "proxygroup.get недоступен (%s) — пропускаю прокси-группы.", e)
            return []

    def get_discovery_rules(self) -> list[dict[str, Any]]:
        """Правила сетевого обнаружения (drule.get) с проверками (dchecks)."""
        return self._call("drule.get", {
            "output": "extend",
            "selectDChecks": "extend",
        })

    def get_maintenances(self) -> list[dict[str, Any]]:
        """Окна обслуживания (maintenance.get) с таймпериодами и группами."""
        return self._call("maintenance.get", {
            "output": "extend",
            "selectTimeperiods": "extend",
            "selectHostGroups": ["groupid", "name"],
            "selectHosts": ["hostid", "host"],
            "selectTags": "extend",
        })

    # ──────────────────────────────────────────────────────────────────────────
    # UI-объекты: карты, дашборды, скрипты.
    # ──────────────────────────────────────────────────────────────────────────
    def list_maps(self) -> list[dict[str, Any]]:
        """Лёгкий список карт (sysmapid + name) для поимённого экспорта."""
        return self._call("map.get", {"output": ["sysmapid", "name"]})

    def export_map_yaml(self, sysmapid: str) -> str:
        """Экспортирует одну карту в YAML через configuration.export."""
        return self.export_yaml_by_ids("maps", [sysmapid])

    def list_dashboards(self) -> list[dict[str, Any]]:
        """Лёгкий список дашбордов (dashboardid + name)."""
        return self._call("dashboard.get", {"output": ["dashboardid", "name"]})

    def get_dashboard(self, dashboardid: str) -> dict[str, Any]:
        """
        Один дашборд (dashboard.get) с виджетами, страницами и пользователями.
        Возвращает dict (или {} если не найден).
        """
        rows = self._call("dashboard.get", {
            "output": "extend",
            "dashboardids": [dashboardid],
            "selectPages": "extend",
            "selectUsers": "extend",
            "selectUserGroups": "extend",
        })
        return rows[0] if rows else {}

    def list_scripts(self) -> list[dict[str, Any]]:
        """Лёгкий список скриптов (scriptid + name)."""
        return self._call("script.get", {"output": ["scriptid", "name"]})

    def get_script(self, scriptid: str) -> dict[str, Any]:
        """
        Один скрипт (script.get). Пароли (password) для типов SSH/Telnet, где
        приходят непустыми, маскируются в SECRET_PLACEHOLDER.
        """
        rows = self._call("script.get", {
            "output": "extend",
            "scriptids": [scriptid],
        })
        if not rows:
            return {}
        s = rows[0]
        if s.get("password"):
            s["password"] = SECRET_PLACEHOLDER
        return s
