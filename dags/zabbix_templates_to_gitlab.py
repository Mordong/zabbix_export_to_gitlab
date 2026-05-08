"""
Airflow DAG: синхронизация шаблонов Zabbix 7 в GitLab.

Расписание:
    Каждые 15 минут — это компромисс между «реакция в течение часа после
    последнего изменения» и нагрузкой на API. Можно настроить через
    Airflow Variable `zabbix_sync_schedule`.

Конфигурация — через Airflow Connections и Variables:
  Connection «zabbix_default»:
      host:     https://zabbix.example.com
      login:    api-readonly
      password: <pwd>
      extra:    {"verify_ssl": true}

  Connection «gitlab_default»:
      host:     https://gitlab.example.com
      password: <PRIVATE-TOKEN>     ← поле password используется как токен
      extra:    {"verify_ssl": true}

  Variables (опционально):
      gitlab_project_id          — числовой ID или "group/project"   (обязательно)
      gitlab_branch              — ветка                              (default: main)
      zabbix_sync_subdir         — поддиректория для шаблонов        (default: templates)
      zabbix_sync_quiet_period   — период «тишины» в секундах         (default: 3600)
      zabbix_sync_template_groups— JSON-массив групп шаблонов        (default: [] = все)
      zabbix_sync_single_commit  — "true"/"false"                     (default: true)

Зависимости (на Airflow worker):
      pip install pyyaml
      # Никаких python-gitlab/pyzabbix не требуется — клиенты на urllib.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from zabbix_template_sync import SyncConfig, TemplateSynchronizer

log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Загрузка конфига из Airflow
# ──────────────────────────────────────────────────────────────────────────────
def _build_config() -> SyncConfig:
    zbx_conn = BaseHook.get_connection("zabbix_default")
    gl_conn = BaseHook.get_connection("gitlab_default")

    zbx_extra = zbx_conn.extra_dejson or {}
    gl_extra = gl_conn.extra_dejson or {}

    template_groups_raw = Variable.get(
        "zabbix_sync_template_groups", default_var="[]"
    )
    try:
        template_groups = json.loads(template_groups_raw) or None
    except json.JSONDecodeError:
        log.warning("zabbix_sync_template_groups не валидный JSON, игнорирую")
        template_groups = None

    return SyncConfig(
        # Zabbix
        zabbix_url=zbx_conn.host,
        zabbix_user=zbx_conn.login,
        zabbix_password=zbx_conn.password,
        zabbix_verify_ssl=bool(zbx_extra.get("verify_ssl", True)),

        # GitLab
        gitlab_url=gl_conn.host,
        gitlab_project_id=Variable.get("gitlab_project_id"),
        gitlab_token=gl_conn.password,
        gitlab_branch=Variable.get("gitlab_branch", default_var="main"),
        gitlab_verify_ssl=bool(gl_extra.get("verify_ssl", True)),

        # Логика
        templates_subdir=Variable.get(
            "zabbix_sync_subdir", default_var="templates"
        ),
        quiet_period_sec=int(
            Variable.get("zabbix_sync_quiet_period", default_var="3600")
        ),
        template_groups=template_groups,
        single_commit=Variable.get(
            "zabbix_sync_single_commit", default_var="true"
        ).lower() == "true",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Основной таск
# ──────────────────────────────────────────────────────────────────────────────
def sync_templates_task(**context) -> dict:
    cfg = _build_config()
    log.info(
        "Старт синхронизации Zabbix→GitLab. "
        "URL Zabbix=%s, GitLab=%s, project=%s, branch=%s, quiet_period=%ds",
        cfg.zabbix_url, cfg.gitlab_url, cfg.gitlab_project_id,
        cfg.gitlab_branch, cfg.quiet_period_sec,
    )

    syncer = TemplateSynchronizer(cfg)
    stats = syncer.run()

    log.info("Итоги: %s", stats.summary())
    if stats.created:
        log.info("Созданы: %s", ", ".join(stats.created))
    if stats.updated:
        log.info("Обновлены: %s", ", ".join(stats.updated))
    if stats.deferred:
        log.info(
            "Отложены (правило 1 часа): %s",
            ", ".join(f"{h} (через {s}с)" for h, s in stats.deferred),
        )
    if stats.errors:
        log.error(
            "Ошибки по шаблонам:\n%s",
            "\n".join(f"  {h}: {e}" for h, e in stats.errors),
        )
        # Падаем, чтобы Airflow зафиксировал failure и сработали алерты
        raise RuntimeError(
            f"Синхронизация завершилась с {len(stats.errors)} ошибками"
        )

    return {
        "created": stats.created,
        "updated": stats.updated,
        "deferred": [(h, s) for h, s in stats.deferred],
        "unchanged": len(stats.unchanged),
    }


# ──────────────────────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "monitoring-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="zabbix_templates_to_gitlab",
    description="Sync Zabbix 7 templates into GitLab as YAML",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    # Каждые 15 минут — изменения старше часа гарантированно попадут в коммит
    # за следующие 15 минут после истечения «тихого периода».
    schedule_interval=Variable.get(
        "zabbix_sync_schedule", default_var="*/15 * * * *"
    ),
    catchup=False,
    max_active_runs=1,
    tags=["zabbix", "gitlab", "monitoring", "ci"],
) as dag:

    sync = PythonOperator(
        task_id="sync_templates",
        python_callable=sync_templates_task,
    )
