"""
Disaster-recovery экспорт объектов конфигурации Zabbix → GitLab.

Покрывает минимальный набор для восстановления системы «с нуля», помимо
шаблонов и настроек аутентификации:

  users/     — roles.yaml, usergroups.yaml, users.yaml
  alerting/  — mediatypes.yaml, actions.yaml
  core/      — hostgroups.yaml, templategroups.yaml, macros.yaml,
               hosts/<имя>.yaml (по файлу на каждый хост)

Отличия от синхронизации шаблонов и auth:
  - БЕЗ правила отсрочки (quiet period): эти объекты выгружаются раз в сутки,
    любое расхождение коммитится сразу (cfg.quiet_period_sec игнорируется);
  - где Zabbix поддерживает configuration.export — используется он (нативный
    импортируемый YAML); остальное — *.get + dump_yaml;
  - секреты, которые API не отдаёт, помечены маркером [SECRET] (см.
    zabbix_client.SECRET_PLACEHOLDER) либо отсутствуют (пароли users) —
    при восстановлении эти значения вводятся вручную.

Переиспользуются GitLabClient и dump_yaml — никаких новых зависимостей.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from .gitlab_client import GitLabClient
from .sync import SyncConfig
from .utils import dump_yaml, yaml_semantic_equal, safe_filename
from .zabbix_client import ZabbixAPI

log = logging.getLogger(__name__)


@dataclass
class ConfigSyncStats:
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"created={len(self.created)} updated={len(self.updated)} "
            f"unchanged={len(self.unchanged)} errors={len(self.errors)}"
        )


# Описание одной единицы экспорта: путь в репозитории + функция, которая
# возвращает содержимое (YAML-строку). Функция вызывается лениво, уже под
# активной сессией Zabbix.
@dataclass
class ExportItem:
    file_path: str
    produce: Callable[[], str]


# Группа экспорта = поддиректория + функция, строящая список ExportItem
# по активному клиенту Zabbix.
@dataclass
class ExportGroup:
    name: str
    build_items: Callable[[ZabbixAPI, str, SyncConfig], list[ExportItem]]


class ConfigSynchronizer:
    """
    Координатор DR-экспорта. Принимает имя группы ('users'/'alerting'/'core')
    и синхронизирует соответствующий набор объектов в одноимённую папку.
    """

    def __init__(self, config: SyncConfig, group: str):
        self.cfg = config
        self.group = group
        if group not in _GROUPS:
            raise ValueError(
                f"Неизвестная группа экспорта '{group}'. "
                f"Доступны: {', '.join(sorted(_GROUPS))}"
            )

    def run(self) -> ConfigSyncStats:
        stats = ConfigSyncStats()

        gl = GitLabClient(
            gitlab_url=self.cfg.gitlab_url,
            project_id=self.cfg.gitlab_project_id,
            token=self.cfg.gitlab_token,
            branch=self.cfg.gitlab_branch,
            verify_ssl=self.cfg.gitlab_verify_ssl,
        )
        gl.verify_access()

        zbx = ZabbixAPI(
            self.cfg.zabbix_url,
            verify_ssl=self.cfg.zabbix_verify_ssl,
            timeout=self.cfg.zabbix_timeout_sec,
        )

        group = _GROUPS[self.group]
        actions: list[dict[str, Any]] = []

        with zbx:
            zbx.login(self.cfg.zabbix_user, self.cfg.zabbix_password)
            try:
                items = group.build_items(zbx, group.name, self.cfg)
            except Exception as e:  # noqa: BLE001
                log.error("[%s] Ошибка построения списка экспорта: %s", self.group, e)
                stats.errors.append((self.group, f"build: {e}"))
                return stats

            for item in items:
                self._process_item(gl, item, stats, actions)

        self._apply_actions(gl, actions, stats)
        return stats

    # ──────────────────────────────────────────────────────────────────────────
    def _process_item(
        self,
        gl: GitLabClient,
        item: ExportItem,
        stats: ConfigSyncStats,
        actions: list[dict[str, Any]],
    ) -> None:
        name = item.file_path
        try:
            content = item.produce()
        except Exception as e:  # noqa: BLE001
            log.error("Ошибка экспорта %s: %s", name, e)
            stats.errors.append((name, f"export: {e}"))
            return

        # Пустой результат (например, нет объектов данного типа) — пропускаем,
        # чтобы не коммитить пустые файлы.
        if content == "":
            log.info("[SKIP]   %s (нет данных)", name)
            return

        try:
            current = gl.get_file_content(name)
        except Exception as e:  # noqa: BLE001
            log.error("Ошибка чтения %s из GitLab: %s", name, e)
            stats.errors.append((name, f"gitlab read: {e}"))
            return

        if current is None:
            log.info("[NEW]    %s", name)
            actions.append({"action": "create", "file_path": name,
                            "content": content, "_kind": "created"})
            return

        # YAML сравниваем по смыслу (configuration.export добавляет date),
        # иначе побайтово.
        same = (yaml_semantic_equal(current, content)
                if name.endswith((".yaml", ".yml")) else current == content)
        if same:
            stats.unchanged.append(name)
            return

        log.info("[UPDATE] %s", name)
        actions.append({"action": "update", "file_path": name,
                        "content": content, "_kind": "updated"})

    def _apply_actions(
        self,
        gl: GitLabClient,
        actions: list[dict[str, Any]],
        stats: ConfigSyncStats,
    ) -> None:
        if not actions:
            log.info("[%s] Изменений нет.", self.group)
            return

        for a in actions:
            (stats.created if a["_kind"] == "created" else stats.updated).append(a["file_path"])

        if self.cfg.single_commit:
            msg = self._commit_message(actions)
            try:
                gl.commit_multiple(
                    actions=[{"action": a["action"], "file_path": a["file_path"],
                              "content": a["content"]} for a in actions],
                    commit_message=msg,
                    author_name=self.cfg.commit_author_name,
                    author_email=self.cfg.commit_author_email,
                )
            except Exception as e:  # noqa: BLE001
                log.error("[%s] Атомарный коммит упал: %s", self.group, e)
                stats.errors.extend((a["file_path"], f"commit: {e}") for a in actions)
                stats.created.clear()
                stats.updated.clear()
        else:
            for a in actions:
                kind = "Add" if a["action"] == "create" else "Update"
                try:
                    gl.upsert_file(
                        file_path=a["file_path"], content=a["content"],
                        commit_message=f"{kind} Zabbix config: {a['file_path']}",
                        author_name=self.cfg.commit_author_name,
                        author_email=self.cfg.commit_author_email,
                    )
                except Exception as e:  # noqa: BLE001
                    log.error("Коммит %s упал: %s", a["file_path"], e)
                    stats.errors.append((a["file_path"], f"commit: {e}"))
                    lst = stats.created if a["_kind"] == "created" else stats.updated
                    if a["file_path"] in lst:
                        lst.remove(a["file_path"])

    def _commit_message(self, actions: list[dict[str, Any]]) -> str:
        created = [a["file_path"] for a in actions if a["_kind"] == "created"]
        updated = [a["file_path"] for a in actions if a["_kind"] == "updated"]
        parts = []
        if created:
            parts.append(f"add {len(created)}")
        if updated:
            parts.append(f"update {len(updated)}")
        title = f"Sync Zabbix {self.group} config: {', '.join(parts)}"
        body = []
        if created:
            body.append("\nAdded:")
            body.extend(f"  + {f}" for f in created)
        if updated:
            body.append("\nUpdated:")
            body.extend(f"  ~ {f}" for f in updated)
        return title + "\n" + "\n".join(body)


# ──────────────────────────────────────────────────────────────────────────────
# Определения групп экспорта
# ──────────────────────────────────────────────────────────────────────────────
def _yaml(data_label: str, value: Any) -> str:
    """Сериализует {label: value} в стабильный YAML."""
    return dump_yaml({data_label: value})


def _build_users(zbx: ZabbixAPI, folder: str, cfg: SyncConfig) -> list[ExportItem]:
    return [
        ExportItem(f"{folder}/roles.yaml", lambda: _yaml("roles", zbx.get_roles())),
        ExportItem(f"{folder}/usergroups.yaml", lambda: _yaml("usergroups", zbx.get_usergroups())),
        ExportItem(f"{folder}/users.yaml", lambda: _yaml("users", zbx.get_users())),
    ]


def _build_alerting(zbx: ZabbixAPI, folder: str, cfg: SyncConfig) -> list[ExportItem]:
    def media_types() -> str:
        # configuration.export поддерживает media types целиком (без id).
        rows = zbx._call("mediatype.get", {"output": ["mediatypeid"]})
        ids = [r["mediatypeid"] for r in rows]
        return zbx.export_yaml_by_ids("mediaTypes", ids)

    return [
        ExportItem(f"{folder}/mediatypes.yaml", media_types),
        ExportItem(f"{folder}/actions.yaml", lambda: _yaml("actions", zbx.get_actions())),
    ]


def _build_core(zbx: ZabbixAPI, folder: str, cfg: SyncConfig) -> list[ExportItem]:
    def host_groups() -> str:
        rows = zbx._call("hostgroup.get", {"output": ["groupid"]})
        return zbx.export_yaml_by_ids("host_groups", [r["groupid"] for r in rows])

    def template_groups() -> str:
        rows = zbx._call("templategroup.get", {"output": ["groupid"]})
        return zbx.export_yaml_by_ids("template_groups", [r["groupid"] for r in rows])

    items = [
        ExportItem(f"{folder}/hostgroups.yaml", host_groups),
        ExportItem(f"{folder}/templategroups.yaml", template_groups),
        ExportItem(f"{folder}/macros.yaml", lambda: _yaml("macros", zbx.get_global_macros())),
    ]

    # Хосты — по файлу на каждый, в подпапке core/hosts/.
    # ОПТИМИЗАЦИЯ: configuration.export вызывается ПАЧКАМИ по batch_size хостов
    # (один вызов на пачку), результат нарезается обратно на отдельные хосты.
    # Для 10–15 тыс. хостов это ~20–30 вызовов вместо 10–15 тысяч.
    #
    # Экспорт делается здесь (под активной сессией Zabbix) разом, а каждый
    # ExportItem лишь отдаёт уже готовую строку — так сохраняется существующая
    # поштучная логика сравнения/коммита (файл на хост) без повторных вызовов.
    hostids = [h["hostid"] for h in zbx.list_hosts()]
    batch_size = getattr(cfg, "host_export_batch_size", 500)
    export_timeout = getattr(cfg, "zabbix_export_timeout_sec", None)
    for host_name, host_yaml in zbx.export_hosts_batched(
            hostids, batch_size, export_timeout=export_timeout):
        fname = safe_filename(host_name)
        items.append(ExportItem(
            f"{folder}/hosts/{fname}.yaml",
            (lambda content=host_yaml: content),
        ))
    return items


_GROUPS: dict[str, ExportGroup] = {
    "users": ExportGroup("users", _build_users),
    "alerting": ExportGroup("alerting", _build_alerting),
    "core": ExportGroup("core", _build_core),
}


def _named_items(rows, subdir, name_key, id_key, exporter):
    """
    Хелпер для «крупных поимённо»: по файлу на объект в своей папке
    (<subdir>/<имя>.yaml). exporter(obj_id) -> YAML-строка.
    """
    items = []
    for r in rows:
        oid = r[id_key]
        fname = safe_filename(r.get(name_key) or oid)
        items.append(ExportItem(
            f"{subdir}/{fname}.yaml",
            (lambda _id=oid: exporter(_id)),
        ))
    return items


def _build_infra(zbx: ZabbixAPI, folder: str, cfg: SyncConfig) -> list[ExportItem]:
    """
    proxies — поимённо в proxies/; proxy groups, discovery rules, maintenance
    — одним файлом в корне. (folder не используется: раскладка «по типу».)
    """
    items: list[ExportItem] = []
    # proxies поимённо (через proxy.get — у каждого свой файл)
    items += _named_items(
        zbx.list_proxies_brief(), "proxies", "name", "proxyid",
        lambda pid: _yaml("proxy", zbx.get_proxy(pid)),
    )
    # мелкие — одним файлом в корне
    items += [
        ExportItem("proxygroups.yaml", lambda: _yaml("proxy_groups", zbx.get_proxy_groups())),
        ExportItem("drules.yaml", lambda: _yaml("discovery_rules", zbx.get_discovery_rules())),
        ExportItem("maintenance.yaml", lambda: _yaml("maintenance", zbx.get_maintenances())),
    ]
    return items


def _build_ui(zbx: ZabbixAPI, folder: str, cfg: SyncConfig) -> list[ExportItem]:
    """
    maps / dashboards / scripts — все поимённо, каждый в свою папку.
    maps через configuration.export, остальное через *.get.
    """
    items: list[ExportItem] = []
    items += _named_items(
        zbx.list_maps(), "maps", "name", "sysmapid",
        lambda sid: zbx.export_map_yaml(sid),
    )
    items += _named_items(
        zbx.list_dashboards(), "dashboards", "name", "dashboardid",
        lambda did: _yaml("dashboard", zbx.get_dashboard(did)),
    )
    items += _named_items(
        zbx.list_scripts(), "scripts", "name", "scriptid",
        lambda scid: _yaml("script", zbx.get_script(scid)),
    )
    return items


_GROUPS["infra"] = ExportGroup("infra", _build_infra)
_GROUPS["ui"] = ExportGroup("ui", _build_ui)
