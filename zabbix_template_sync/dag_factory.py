"""
Общая фабрика DAG'ов для disaster-recovery экспорта конфигурации Zabbix.

Используется тремя DAG-файлами (users / alerting / core), чтобы не дублировать
~150 строк инфраструктурного кода. Логика вынесена в установленный пакет
(надёжный импорт), а сами DAG-файлы — тонкие обёртки, вызывающие
build_config_dags(group=...).

Совместимость: импорты Airflow с fallback (SDK 3.x → legacy 2.x), schedule=,
никаких top-level обращений к БД (Variable/Connection читаются внутри таска).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

try:
    import pendulum
    _START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")
except ImportError:
    _START_DATE = datetime(2025, 1, 1)

try:
    from airflow.sdk import DAG, Variable
    from airflow.sdk.bases.hook import BaseHook
    _USE_SDK = True
except ImportError:
    from airflow import DAG  # type: ignore[no-redef]
    from airflow.models import Variable  # type: ignore[no-redef]
    from airflow.hooks.base import BaseHook  # type: ignore[no-redef]
    _USE_SDK = False

try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    from airflow.operators.python import PythonOperator  # type: ignore[no-redef]

from zabbix_template_sync import SyncConfig, ConfigSynchronizer

log = logging.getLogger(__name__)

# Все DR-экспорты идут раз в сутки в 21:00, без quiet period.
DR_SCHEDULE = "0 21 * * *"
ENVIRONMENTS = ("test", "prod")


def _var_get(key: str, default):
    if _USE_SDK:
        return Variable.get(key, default=default)
    return Variable.get(key, default_var=default)


def _build_config(env: str) -> SyncConfig:
    zbx_conn_id = f"zabbix_{env}"
    gl_conn_id = f"gitlab_{env}"

    try:
        zbx_conn = BaseHook.get_connection(zbx_conn_id)
    except Exception as e:
        raise RuntimeError(
            f"Connection '{zbx_conn_id}' не найден. "
            f"Создайте http-connection с host/login/password."
        ) from e
    try:
        gl_conn = BaseHook.get_connection(gl_conn_id)
    except Exception as e:
        raise RuntimeError(
            f"Connection '{gl_conn_id}' не найден. "
            f"Создайте http-connection с host и password (PRIVATE-TOKEN)."
        ) from e

    zbx_extra = zbx_conn.extra_dejson or {}
    gl_extra = gl_conn.extra_dejson or {}

    project_id = _var_get(f"{env}_gitlab_project_id", None)
    if not project_id:
        raise RuntimeError(
            f"Airflow Variable '{env}_gitlab_project_id' не задана."
        )

    return SyncConfig(
        zabbix_url=zbx_conn.host,
        zabbix_user=zbx_conn.login,
        zabbix_password=zbx_conn.password,
        zabbix_verify_ssl=bool(zbx_extra.get("verify_ssl", True)),
        zabbix_timeout_sec=int(_var_get(f"{env}_zabbix_timeout_sec", "60")),
        gitlab_url=gl_conn.host,
        gitlab_project_id=project_id,
        gitlab_token=gl_conn.password,
        gitlab_branch=_var_get(f"{env}_gitlab_branch", "main"),
        gitlab_verify_ssl=bool(gl_extra.get("verify_ssl", True)),
        # DR-экспорт без правила отсрочки.
        quiet_period_sec=0,
        single_commit=_var_get(f"{env}_config_single_commit", "true").lower() == "true",
        commit_author_name=f"Zabbix Sync Bot ({env})",
        commit_author_email=f"zabbix-sync-{env}@example.com",
    )


def _sync_task(env: str, group: str, **context) -> dict:
    cfg = _build_config(env)
    log.info("[%s/%s] Старт DR-экспорта Zabbix→GitLab. project=%s",
             env.upper(), group, cfg.gitlab_project_id)
    stats = ConfigSynchronizer(cfg, group).run()
    log.info("[%s/%s] Итоги: %s", env.upper(), group, stats.summary())
    if stats.errors:
        log.error("[%s/%s] Ошибки:\n%s", env.upper(), group,
                  "\n".join(f"  {f}: {e}" for f, e in stats.errors))
        raise RuntimeError(
            f"[{env}/{group}] завершилось с {len(stats.errors)} ошибками")
    return {"env": env, "group": group,
            "created": stats.created, "updated": stats.updated,
            "unchanged": len(stats.unchanged)}


def build_config_dags(group: str, extra_tags: list[str] | None = None) -> dict:
    """
    Создаёт по DAG на среду для заданной группы ('users'/'alerting'/'core')
    и возвращает {dag_id: DAG}. Вызывающий регистрирует их в globals().
    """
    retries = {"test": 2, "prod": 3}
    retry_delay = {"test": 5, "prod": 15}
    dags: dict = {}

    for env in ENVIRONMENTS:
        dag_id = f"zabbix_{group}_to_gitlab_{env}"
        default_args = {
            "owner": "monitoring-team",
            "depends_on_past": False,
            "email": [],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": retries[env],
            "retry_delay": timedelta(minutes=retry_delay[env]),
        }
        dag = DAG(
            dag_id=dag_id,
            description=f"DR export of Zabbix {group} config into GitLab ({env.upper()})",
            default_args=default_args,
            start_date=_START_DATE,
            schedule=DR_SCHEDULE,
            catchup=False,
            max_active_runs=1,
            tags=["zabbix", "gitlab", "monitoring", "dr", group, env]
                 + (extra_tags or []),
        )
        with dag:
            PythonOperator(
                task_id=f"sync_{group}",
                python_callable=_sync_task,
                op_kwargs={"env": env, "group": group},
            )
        dags[dag_id] = dag

    return dags
