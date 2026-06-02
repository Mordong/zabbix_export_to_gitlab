"""
Логика синхронизации настроек аутентификации Zabbix → GitLab.

Экспортирует два YAML-файла в поддиректорию auth/ репозитория:

  auth/authentication.yaml   — глобальные флаги аутентификации
                               (authentication.get): общие настройки LDAP/SAML,
                               JIT, политика паролей.
  auth/userdirectories.yaml  — список LDAP/SAML user directories с JIT
                               provisioning (provision_groups, provision_media);
                               рядом с ID добавлены резолвленные имена.

Правило отсрочки (quiet period) применяется к КАЖДОМУ файлу раздельно, по
своему типу ресурса в audit log:
  - authentication.yaml   → resourcetype 42 (Authentication);
  - userdirectories.yaml  → resourcetype 49 (User directory).

Это отдельный путь от синхронизации шаблонов: auth-конфиг меняется редко,
поэтому DAG для него запускается реже (см. dags/zabbix_auth_to_gitlab.py).
Переиспользуются GitLabClient и сравнение YAML из основного пакета —
никаких новых зависимостей.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .gitlab_client import GitLabClient
from .sync import SyncConfig
from .utils import dump_yaml, yaml_semantic_equal, format_duration
from .zabbix_client import (
    ZabbixAPI,
    AUDIT_RESOURCE_AUTHENTICATION,
    AUDIT_RESOURCE_USERDIRECTORY,
)

log = logging.getLogger(__name__)

# Имена файлов в поддиректории auth/ (см. модульный docstring).
AUTH_FILE = "authentication.yaml"
USERDIR_FILE = "userdirectories.yaml"


@dataclass
class AuthSyncStats:
    """Сводная статистика прогона auth-синхронизации."""
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    deferred: list[tuple[str, int]] = field(default_factory=list)  # (file, secs_left)
    unchanged: list[str] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"created={len(self.created)} "
            f"updated={len(self.updated)} "
            f"deferred={len(self.deferred)} "
            f"unchanged={len(self.unchanged)} "
            f"errors={len(self.errors)}"
        )


class AuthSynchronizer:
    """
    Координатор синхронизации настроек аутентификации.

    Использует тот же SyncConfig, что и синхронизатор шаблонов — поля Zabbix,
    GitLab, quiet_period_sec, audit_* и т.д. применяются одинаково. Поле
    templates_subdir для auth НЕ используется; вместо него фиксированная
    поддиректория auth/ (настраивается через subdir в конструкторе при
    необходимости).
    """

    def __init__(self, config: SyncConfig, subdir: str = "auth"):
        self.cfg = config
        # Поддиректория для auth-файлов. "" = корень репозитория.
        self.subdir = subdir.strip("/")

    def _path(self, file_name: str) -> str:
        return f"{self.subdir}/{file_name}" if self.subdir else file_name

    def run(self) -> AuthSyncStats:
        stats = AuthSyncStats()
        now = int(time.time())

        # 0. Pre-flight: доступ к GitLab проекту ДО обращения к Zabbix.
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

        actions: list[dict[str, Any]] = []
        with zbx:
            zbx.login(self.cfg.zabbix_user, self.cfg.zabbix_password)

            # Окно для auditlog.get (общее для обоих типов ресурса).
            window = self.cfg.quiet_period_sec + self.cfg.audit_window_padding_sec

            # 1. authentication.yaml (resourcetype 42)
            self._process_resource(
                gl=gl, zbx=zbx, now=now, window=window, stats=stats,
                actions=actions,
                file_name=AUTH_FILE,
                resourcetype=AUDIT_RESOURCE_AUTHENTICATION,
                fetch=lambda: {"authentication": zbx.get_authentication()},
                label="authentication",
            )

            # 2. userdirectories.yaml (resourcetype 49)
            self._process_resource(
                gl=gl, zbx=zbx, now=now, window=window, stats=stats,
                actions=actions,
                file_name=USERDIR_FILE,
                resourcetype=AUDIT_RESOURCE_USERDIRECTORY,
                fetch=lambda: {"userdirectories": zbx.get_userdirectories()},
                label="userdirectories",
            )

        # 3. Применяем накопленные действия.
        self._apply_actions(gl, actions, stats)
        return stats

    # ──────────────────────────────────────────────────────────────────────────
    def _process_resource(
        self,
        gl: GitLabClient,
        zbx: ZabbixAPI,
        now: int,
        window: int,
        stats: AuthSyncStats,
        actions: list[dict[str, Any]],
        file_name: str,
        resourcetype: int,
        fetch,
        label: str,
    ) -> None:
        """
        Обрабатывает один ресурс (один файл): экспорт → сравнение → решение
        с учётом правила отсрочки по audit log.
        """
        file_path = self._path(file_name)

        # Что лежит в GitLab сейчас?
        try:
            current = gl.get_file_content(file_path)
        except Exception as e:  # noqa: BLE001
            log.error("Ошибка чтения %s из GitLab: %s", file_path, e)
            stats.errors.append((file_name, f"gitlab read: {e}"))
            return

        # Экспортируем данные ресурса из Zabbix.
        try:
            data = fetch()
        except Exception as e:  # noqa: BLE001
            log.error("Ошибка экспорта %s из Zabbix: %s", label, e)
            stats.errors.append((file_name, f"export: {e}"))
            return

        new_yaml = dump_yaml(data)

        # Файла нет → создаём СРАЗУ (как и для новых шаблонов).
        if current is None:
            log.info("[NEW]    %s", file_path)
            actions.append({
                "action": "create",
                "file_path": file_path,
                "content": new_yaml,
                "_file": file_name,
                "_kind": "created",
            })
            return

        # Содержимое совпадает — ничего не делаем.
        if yaml_semantic_equal(current, new_yaml):
            stats.unchanged.append(file_name)
            return

        # Контент отличается — проверяем правило отсрочки по audit log.
        last_modified = zbx.get_latest_audit_clock(
            resourcetype=resourcetype,
            since=now - window,
            audit_timeout=self.cfg.zabbix_audit_timeout_sec,
            limit=self.cfg.audit_query_limit,
        )

        if last_modified is None:
            # В audit log нет недавних правок этого типа (или auditlog.get
            # недоступен) — считаем стабильным, коммитим расхождение.
            log.info(
                "[UPDATE] %s (в audit log нет недавних правок — считаем стабильным)",
                file_path,
            )
            actions.append({
                "action": "update",
                "file_path": file_path,
                "content": new_yaml,
                "_file": file_name,
                "_kind": "updated",
            })
            return

        age = now - last_modified
        if age >= self.cfg.quiet_period_sec:
            log.info(
                "[UPDATE] %s (последнее изменение %s назад)",
                file_path, format_duration(age),
            )
            actions.append({
                "action": "update",
                "file_path": file_path,
                "content": new_yaml,
                "_file": file_name,
                "_kind": "updated",
            })
        else:
            remaining = self.cfg.quiet_period_sec - age
            log.info(
                "[DEFER]  %s — правка %s назад, ждём ещё %s",
                file_path, format_duration(age), format_duration(remaining),
            )
            stats.deferred.append((file_name, remaining))

    # ──────────────────────────────────────────────────────────────────────────
    def _apply_actions(
        self,
        gl: GitLabClient,
        actions: list[dict[str, Any]],
        stats: AuthSyncStats,
    ) -> None:
        if not actions:
            log.info("Изменений auth для применения нет.")
            return

        for a in actions:
            if a["_kind"] == "created":
                stats.created.append(a["_file"])
            else:
                stats.updated.append(a["_file"])

        if self.cfg.single_commit:
            commit_msg = self._compose_commit_message(actions)
            try:
                gl.commit_multiple(
                    actions=[
                        {
                            "action": a["action"],
                            "file_path": a["file_path"],
                            "content": a["content"],
                        }
                        for a in actions
                    ],
                    commit_message=commit_msg,
                    author_name=self.cfg.commit_author_name,
                    author_email=self.cfg.commit_author_email,
                )
            except Exception as e:  # noqa: BLE001
                log.error("Атомарный коммит auth упал: %s", e)
                stats.errors.extend((a["_file"], f"commit: {e}") for a in actions)
                stats.created.clear()
                stats.updated.clear()
        else:
            for a in actions:
                kind = "Add" if a["action"] == "create" else "Update"
                msg = f"{kind} Zabbix auth config: {a['_file']}"
                try:
                    gl.upsert_file(
                        file_path=a["file_path"],
                        content=a["content"],
                        commit_message=msg,
                        author_name=self.cfg.commit_author_name,
                        author_email=self.cfg.commit_author_email,
                    )
                except Exception as e:  # noqa: BLE001
                    log.error("Коммит %s упал: %s", a["file_path"], e)
                    stats.errors.append((a["_file"], f"commit: {e}"))
                    if a["_kind"] == "created":
                        stats.created.remove(a["_file"])
                    else:
                        stats.updated.remove(a["_file"])

    @staticmethod
    def _compose_commit_message(actions: list[dict[str, Any]]) -> str:
        created = [a["_file"] for a in actions if a["_kind"] == "created"]
        updated = [a["_file"] for a in actions if a["_kind"] == "updated"]
        parts = []
        if created:
            parts.append(f"add {len(created)}")
        if updated:
            parts.append(f"update {len(updated)}")
        title = f"Sync Zabbix auth config: {', '.join(parts)}"

        body_lines: list[str] = []
        if created:
            body_lines.append("\nAdded:")
            body_lines.extend(f"  + {f}" for f in created)
        if updated:
            body_lines.append("\nUpdated:")
            body_lines.extend(f"  ~ {f}" for f in updated)
        return title + "\n" + "\n".join(body_lines)
