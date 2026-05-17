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

# Тип ресурса в audit log Zabbix 6+/7+: TEMPLATE = 30
# Источник: zabbix/include/audit.inc.php (AUDIT_RESOURCE_TEMPLATE)
AUDIT_RESOURCE_TEMPLATE = 30

# Поддерживаемые форматы экспорта в Zabbix 7.x
EXPORT_FORMAT_YAML = "yaml"
EXPORT_FORMAT_XML = "xml"
EXPORT_FORMAT_JSON = "json"


class ZabbixAPIError(RuntimeError):
    """Ошибка, возвращённая Zabbix API в поле 'error' JSON-RPC ответа."""


class ZabbixAPI:
    """
    Минималистичный клиент Zabbix JSON-RPC API.
    Стиль авторизации (Bearer / body.auth) воспроизведён из ZabbixAPI в zabbix_reporter.py.
    """

    def __init__(self, url: str, verify_ssl: bool = True, timeout: int = 30):
        self.url = url.rstrip("/") + "/api_jsonrpc.php"
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.auth_token: str | None = None
        self._rid = 1
        self._api_version: tuple[int, int] | None = None

    # ──────────────────────────────────────────────────────────────────────────
    # Низкоуровневый JSON-RPC вызов
    # ──────────────────────────────────────────────────────────────────────────
    def _call(self, method: str, params: Any, _no_auth: bool = False) -> Any:
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

        try:
            with urllib.request.urlopen(req, context=ctx, timeout=self.timeout) as r:
                # Zabbix всегда отвечает в UTF-8
                response = json.loads(r.read().decode("utf-8"))
        except urllib.error.URLError as e:
            log.error("Zabbix API connection error on %s: %s", method, e.reason)
            raise ConnectionError(f"Ошибка подключения к Zabbix: {e.reason}") from e

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

    def get_recently_modified_templates(self, since: int) -> dict[str, int]:
        """
        Возвращает {templateid: latest_clock} — словарь шаблонов, у которых
        ЕСТЬ записи в audit log с clock >= since (unix timestamp).

        Один лёгкий запрос вместо запросов по каждому шаблону:
        фильтр по resourcetype=TEMPLATE и time_from. Записей за окно
        в пару часов обычно единицы-десятки, нагрузки практически нет.

        Шаблоны, отсутствующие в результате, либо не правились в указанном
        окне, либо были изменены настолько давно, что записи уже вычищены
        housekeeper'ом — в обоих случаях нам они «стабильны».
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
                    "limit": 50000,  # с большим запасом; обычно записей сильно меньше
                },
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
