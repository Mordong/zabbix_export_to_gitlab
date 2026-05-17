"""
Логика синхронизации шаблонов Zabbix → GitLab.

Реализует требования ТЗ:
  1. Все шаблоны копируются в GitLab.
  2. Шаблоны, отсутствующие в GitLab, копируются СРАЗУ (без задержки).
  3. Изменённые шаблоны копируются только если со времени их последнего
     изменения прошло >= 1 часа (защита от незавершённых правок —
     не хотим коммитить шаблон, который инженер ещё доделывает).
  4. Кириллица в именах и содержимом сохраняется (UTF-8 на всех этапах).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from .gitlab_client import GitLabClient
from .utils import safe_filename, yaml_semantic_equal, format_duration
from .zabbix_client import ZabbixAPI

log = logging.getLogger(__name__)

# Стандартная задержка перед коммитом изменённых шаблонов (сек)
DEFAULT_QUIET_PERIOD_SEC = 3600


@dataclass
class SyncStats:
    """Сводная статистика по результатам прогона."""
    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    deferred: list[tuple[str, int]] = field(default_factory=list)  # (host, secs_remaining)
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


@dataclass
class SyncConfig:
    # Zabbix
    zabbix_url: str
    zabbix_user: str
    zabbix_password: str
    zabbix_verify_ssl: bool = True

    # GitLab
    gitlab_url: str = "https://gitlab.com"
    gitlab_project_id: str = ""    # числовой ID или "group/subgroup/project"
    gitlab_token: str = ""
    gitlab_branch: str = "main"
    gitlab_verify_ssl: bool = True

    # Логика
    templates_subdir: str = "templates"
    quiet_period_sec: int = DEFAULT_QUIET_PERIOD_SEC
    template_groups: list[str] | None = None    # фильтр по группам шаблонов; None = все
    template_hosts: list[str] | None = None     # фильтр по host-именам; None = все

    # Коммиты
    single_commit: bool = True   # объединять все изменения в один коммит
    commit_author_name: str = "Zabbix Sync Bot"
    commit_author_email: str = "zabbix-sync@example.com"


class TemplateSynchronizer:
    """Координатор синхронизации."""

    def __init__(self, config: SyncConfig):
        self.cfg = config

    def run(self) -> SyncStats:
        stats = SyncStats()
        now = int(time.time())

        # 0. Pre-flight: проверяем доступ к GitLab проекту ДО подключения к Zabbix.
        #    Это быстро (один GET) и даёт понятную ошибку, если в config.yaml
        #    неправильный project_id, токен или ветка. Иначе мы бы сначала
        #    выкачали все шаблоны из Zabbix и только потом получили 404.
        gl = GitLabClient(
            gitlab_url=self.cfg.gitlab_url,
            project_id=self.cfg.gitlab_project_id,
            token=self.cfg.gitlab_token,
            branch=self.cfg.gitlab_branch,
            verify_ssl=self.cfg.gitlab_verify_ssl,
        )
        gl.verify_access()

        # 1. Подключаемся к Zabbix и получаем список шаблонов
        zbx = ZabbixAPI(self.cfg.zabbix_url, verify_ssl=self.cfg.zabbix_verify_ssl)
        with zbx:
            zbx.login(self.cfg.zabbix_user, self.cfg.zabbix_password)
            templates = zbx.get_templates(
                host_groups=self.cfg.template_groups,
                name_filter=self.cfg.template_hosts,
            )
            log.info("Получено шаблонов из Zabbix: %d", len(templates))

            if not templates:
                return stats

            # 2. Список существующих файлов в репозитории
            existing_files = set(gl.list_files(self.cfg.templates_subdir))
            log.info(
                "В GitLab (%s/%s) уже есть файлов в %s/: %d",
                self.cfg.gitlab_project_id,
                self.cfg.gitlab_branch,
                self.cfg.templates_subdir,
                len(existing_files),
            )

            # 3. Audit log: один лёгкий запрос за окно «2 × quiet_period» (с запасом).
            #    Получаем словарь {templateid: latest_clock} для НЕДАВНО изменённых.
            #    Шаблоны, отсутствующие в словаре, считаем «стабильными»
            #    (их давно никто не трогал).
            window = max(2 * self.cfg.quiet_period_sec, 7200)
            recent_modified = zbx.get_recently_modified_templates(
                since=now - window
            )
            log.info(
                "Audit log: шаблонов с правками за последние %s — %d",
                format_duration(window),
                len(recent_modified),
            )

            # 4. Итерация по шаблонам, сбор действий
            actions: list[dict[str, Any]] = []
            total = len(templates)
            for idx, tpl in enumerate(templates, 1):
                # Прогресс-лог, чтобы было видно, что процесс жив.
                if idx % 25 == 0 or idx == total:
                    log.info(
                        "  прогресс: %d/%d  (created=%d, updated=%d, deferred=%d, unchanged=%d)",
                        idx, total,
                        sum(1 for a in actions if a["_kind"] == "created"),
                        sum(1 for a in actions if a["_kind"] == "updated"),
                        len(stats.deferred),
                        len(stats.unchanged),
                    )

                tplid = tpl["templateid"]
                host = tpl["host"]
                visible_name = tpl.get("name") or host
                file_name = f"{safe_filename(host)}.yaml"
                file_path = (
                    f"{self.cfg.templates_subdir}/{file_name}"
                    if self.cfg.templates_subdir else file_name
                )

                # 4.0 SHORT-CIRCUIT: если файл уже есть в GitLab И шаблон
                #     недавно правился (клок в окне quiet_period) — откладываем
                #     БЕЗ дорогого configuration.export. Экономит десятки
                #     секунд на каждый активно-редактируемый шаблон.
                last_modified = recent_modified.get(tplid)
                if (
                    file_path in existing_files
                    and last_modified is not None
                ):
                    age = now - last_modified
                    if age < self.cfg.quiet_period_sec:
                        remaining = self.cfg.quiet_period_sec - age
                        log.info(
                            "[DEFER]  %s — правка %s назад, ждём ещё %s",
                            visible_name,
                            format_duration(age),
                            format_duration(remaining),
                        )
                        stats.deferred.append((host, remaining))
                        continue

                try:
                    # 4.1 Экспорт в YAML (UTF-8)
                    yaml_content = zbx.export_template_yaml(tplid)
                except Exception as e:  # noqa: BLE001
                    log.error("Ошибка экспорта шаблона %s (%s): %s", host, tplid, e)
                    stats.errors.append((host, f"export: {e}"))
                    continue

                # 4.2 Что лежит в GitLab сейчас?
                if file_path in existing_files:
                    try:
                        gitlab_content = gl.get_file_content(file_path)
                    except Exception as e:  # noqa: BLE001
                        log.error("Ошибка чтения %s из GitLab: %s", file_path, e)
                        stats.errors.append((host, f"gitlab read: {e}"))
                        continue
                else:
                    gitlab_content = None

                # 4.3 Принимаем решение
                if gitlab_content is None:
                    # Сценарий 1: шаблона нет — копируем СРАЗУ (требование п.2 ТЗ)
                    log.info("[NEW]    %s → %s", visible_name, file_path)
                    actions.append({
                        "action": "create",
                        "file_path": file_path,
                        "content": yaml_content,
                        "_host": host,
                        "_kind": "created",
                    })
                    continue

                if yaml_semantic_equal(gitlab_content, yaml_content):
                    # Содержимое совпадает — ничего делать не нужно
                    stats.unchanged.append(host)
                    continue

                # Сценарий 2: контент отличается — проверяем правило 1 часа
                if last_modified is None:
                    # Записей в audit log нет (или старше окна) — шаблон считается
                    # стабильным, коммитим расхождение. Это срабатывает например,
                    # если правка была сделана импортом из API без записи в аудит,
                    # или если housekeeper уже вычистил старую запись.
                    log.info(
                        "[UPDATE] %s → %s (в audit log нет недавних правок — "
                        "считаем стабильным)",
                        visible_name, file_path,
                    )
                    actions.append({
                        "action": "update",
                        "file_path": file_path,
                        "content": yaml_content,
                        "_host": host,
                        "_kind": "updated",
                    })
                    continue

                age = now - last_modified
                if age >= self.cfg.quiet_period_sec:
                    log.info(
                        "[UPDATE] %s → %s (последнее изменение %s назад)",
                        visible_name, file_path, format_duration(age),
                    )
                    actions.append({
                        "action": "update",
                        "file_path": file_path,
                        "content": yaml_content,
                        "_host": host,
                        "_kind": "updated",
                    })
                else:
                    # На самом деле сюда уже не попадёт благодаря short-circuit
                    # выше — оставляю для случая «файла нет, но в audit недавно».
                    # Если шаблон НОВЫЙ — мы должны его создать сразу (п.2 ТЗ),
                    # даже если он только что отредактирован.
                    remaining = self.cfg.quiet_period_sec - age
                    log.info(
                        "[DEFER]  %s — изменён %s назад, ждём ещё %s",
                        visible_name,
                        format_duration(age),
                        format_duration(remaining),
                    )
                    stats.deferred.append((host, remaining))

            # 5. Применяем накопленные действия в GitLab
            self._apply_actions(gl, actions, stats)

        return stats

    # ──────────────────────────────────────────────────────────────────────────
    def _apply_actions(
        self,
        gl: GitLabClient,
        actions: list[dict[str, Any]],
        stats: SyncStats,
    ) -> None:
        if not actions:
            log.info("Изменений для применения нет.")
            return

        # Пишем результат в stats заранее (если single_commit упадёт целиком,
        # — переведём созданные/обновлённые в errors)
        for a in actions:
            if a["_kind"] == "created":
                stats.created.append(a["_host"])
            else:
                stats.updated.append(a["_host"])

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
                log.error("Атомарный коммит упал: %s", e)
                # Откатываем стат и пишем ошибку
                stats.errors.extend((a["_host"], f"commit: {e}") for a in actions)
                stats.created.clear()
                stats.updated.clear()
        else:
            # По одному файлу = по одному коммиту
            for a in actions:
                kind = "Add" if a["action"] == "create" else "Update"
                msg = f"{kind} Zabbix template: {a['_host']}"
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
                    stats.errors.append((a["_host"], f"commit: {e}"))
                    if a["_kind"] == "created":
                        stats.created.remove(a["_host"])
                    else:
                        stats.updated.remove(a["_host"])

    @staticmethod
    def _compose_commit_message(actions: list[dict[str, Any]]) -> str:
        created = [a["_host"] for a in actions if a["_kind"] == "created"]
        updated = [a["_host"] for a in actions if a["_kind"] == "updated"]
        parts = []
        if created:
            parts.append(f"add {len(created)}")
        if updated:
            parts.append(f"update {len(updated)}")
        title = f"Sync Zabbix templates: {', '.join(parts)}"

        body_lines: list[str] = []
        if created:
            body_lines.append("\nAdded:")
            body_lines.extend(f"  + {h}" for h in created)
        if updated:
            body_lines.append("\nUpdated:")
            body_lines.extend(f"  ~ {h}" for h in updated)
        return title + "\n" + "\n".join(body_lines)
