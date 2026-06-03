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
    # Базовый таймаут HTTP запросов к Zabbix API (сек). Применяется ко всем
    # вызовам кроме audit log (тот использует zabbix_audit_timeout_sec).
    zabbix_timeout_sec: int = 60
    # Таймаут именно для auditlog.get bulk-запроса. Этот запрос на больших
    # инсталляциях может занимать значительно дольше остальных — особенно
    # при холодном кэше БД Zabbix. Поэтому отдельный, заметно более щедрый.
    zabbix_audit_timeout_sec: int = 180

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

    # Окно audit log запроса = quiet_period_sec + audit_window_padding_sec.
    # Запас нужен, чтобы покрыть один-два пропущенных запуска DAG'а
    # (если он недолго был выключен/упал). Слишком большое значение увеличивает
    # объём данных, читаемых Zabbix-сервером, и риск таймаута.
    audit_window_padding_sec: int = 900   # 15 минут — один интервал DAG

    # Верхняя граница записей в одном auditlog.get запросе. Защита от того,
    # что в окне внезапно окажется аномально много записей и Zabbix-сервер
    # начнёт материализовывать большой набор. Реально за час правок шаблонов
    # бывают единицы-десятки, 5000 — с большим запасом.
    audit_query_limit: int = 5000

    # Коммиты
    single_commit: bool = True   # объединять изменения в коммиты (чанками)
    commit_author_name: str = "Zabbix Sync Bot"
    commit_author_email: str = "zabbix-sync@example.com"
    # Чанкование коммитов: при тысячах файлов один POST /commits отбивается
    # WAF/прокси (Qrator) с 413. Поэтому действия дробятся на пачки.
    commit_chunk_size: int = 150
    commit_max_retries: int = 3

    # DR-экспорт хостов: число хостов на один configuration.export.
    # Хосты экспортируются пачками (один вызов API на пачку) и нарезаются
    # обратно на файлы — это снимает узкое место при 10–15 тыс. хостов.
    host_export_batch_size: int = 500
    # Таймаут на один батч-вызов configuration.export для хостов (сек).
    # Батч тяжелее одиночного экспорта, поэтому таймаут отдельный и больше
    # обычного (по аналогии с zabbix_audit_timeout_sec у auditlog).
    zabbix_export_timeout_sec: int = 300


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
        zbx = ZabbixAPI(
            self.cfg.zabbix_url,
            verify_ssl=self.cfg.zabbix_verify_ssl,
            timeout=self.cfg.zabbix_timeout_sec,
        )
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

            # 3. Audit log: один запрос за окно (quiet_period + padding).
            #    Получаем словарь {templateid: latest_clock} для НЕДАВНО изменённых.
            #    Шаблоны, отсутствующие в словаре, считаем «стабильными»
            #    (их давно никто не трогал, либо запись уже вычищена housekeeper'ом).
            #    Параметры audit_window/timeout/limit вынесены в SyncConfig,
            #    чтобы их можно было подстроить под нагрузку Zabbix без правок кода.
            window = self.cfg.quiet_period_sec + self.cfg.audit_window_padding_sec
            recent_modified = zbx.get_recently_modified_templates(
                since=now - window,
                audit_timeout=self.cfg.zabbix_audit_timeout_sec,
                limit=self.cfg.audit_query_limit,
            )
            log.info(
                "Audit log: шаблонов с правками за последние %s — %d "
                "(timeout=%ds, limit=%d)",
                format_duration(window),
                len(recent_modified),
                self.cfg.zabbix_audit_timeout_sec,
                self.cfg.audit_query_limit,
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
            failed = gl.commit_multiple(
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
                chunk_size=self.cfg.commit_chunk_size,
                max_retries=self.cfg.commit_max_retries,
            )
            if failed:
                path_to_host = {a["file_path"]: a["_host"] for a in actions}
                failed_hosts = {path_to_host.get(p, p) for p, _ in failed}
                stats.errors.extend(
                    (path_to_host.get(p, p), reason) for p, reason in failed)
                stats.created[:] = [h for h in stats.created if h not in failed_hosts]
                stats.updated[:] = [h for h in stats.updated if h not in failed_hosts]
                log.error("Не закоммичено шаблонов: %d", len(failed_hosts))
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
