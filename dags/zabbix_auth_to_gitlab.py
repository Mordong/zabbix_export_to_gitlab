"""
Airflow DAG-фабрика: синхронизация настроек аутентификации Zabbix
(LDAP/SAML + JIT provisioning/mapping) в GitLab как YAML, для нескольких
окружений (TEST и PROD).

Экспортируются два файла в поддиректорию auth/ репозитория:
  auth/authentication.yaml   — глобальные флаги аутентификации;
  auth/userdirectories.yaml  — LDAP/SAML серверы с provision_groups/media.

ОТЛИЧИЕ ОТ zabbix_templates_to_gitlab.py:
  - auth-конфиг меняется редко → расписание реже (по умолчанию @daily);
  - правило отсрочки (quiet period) считается по типам ресурса audit log
    42 (Authentication) и 49 (User directory), а не 30 (Template).
Архитектурные принципы те же: секреты в Connections/Variables, никаких
top-level DB-обращений (совместимо с изоляцией DAG processor в Airflow 3),
импорты через airflow.sdk с fallback на legacy для Airflow 2.x.

──────────────────────────────────────────────────────────────────────────────
ТРЕБУЕМЫЕ Connections (те же, что и для шаблонов — переиспользуются)
──────────────────────────────────────────────────────────────────────────────
  zabbix_<env>   — host, login, password, extra={"verify_ssl": true/false}
  gitlab_<env>   — host, password (= PRIVATE-TOKEN), extra={"verify_ssl": ...}

──────────────────────────────────────────────────────────────────────────────
ТРЕБУЕМЫЕ / ОПЦИОНАЛЬНЫЕ Variables (префиксованы средой)
──────────────────────────────────────────────────────────────────────────────
  <env>_gitlab_project_id            — обязательная (общая с DAG шаблонов)
  <env>_gitlab_branch                — опц. (default: "main")
  <env>_zabbix_auth_subdir           — опц. (default: "auth"; "" = корень репо)
  <env>_zabbix_auth_quiet_period     — опц. (default: см. ENVIRONMENTS ниже)
  <env>_zabbix_auth_single_commit    — опц. (default: "true")
  <env>_zabbix_timeout_sec           — опц. (default: 60)
  <env>_zabbix_audit_timeout_sec     — опц. (default: 180)
  <env>_audit_window_padding_sec     — опц. (default: 900)
  <env>_audit_query_limit            — опц. (default: 5000)

Пример:
  airflow variables set prod_gitlab_project_id "observability/zabbix-cis/prod"
  airflow variables set prod_zabbix_auth_subdir "auth"
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

try:
    import pendulum
    _START_DATE = pendulum.datetime(2025, 1, 1, tz="UTC")
except ImportError:
    _START_DATE = datetime(2025, 1, 1)

# ── Совместимость импортов между Airflow 2.x и 3.x ────────────────────────────
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

from zabbix_template_sync import SyncConfig, AuthSynchronizer

log = logging.getLogger(__name__)


def _var_get(key: str, default):
    """
    Совместимая обёртка для Variable.get().
    Airflow 3 SDK: параметр `default`. Airflow 2 legacy: `default_var`.
    """
    if _USE_SDK:
        return Variable.get(key, default=default)
    return Variable.get(key, default_var=default)


# ──────────────────────────────────────────────────────────────────────────────
# Конфигурация окружений (auth)
# ──────────────────────────────────────────────────────────────────────────────
ENVIRONMENTS = {
    "test": {
        "schedule": "0 0 * * *",            # ежедневно в полночь
        "default_quiet_period_sec": 300,    # 5 минут «тишины»
        "retries": 2,
        "retry_delay_minutes": 5,
        "owner": "monitoring-team",
        "email": [],  # ["[email protected]"]
        "env_tag": "test",
    },
    "prod": {
        "schedule": "0 0 * * *",            # ежедневно в полночь
        "default_quiet_period_sec": 3600,   # 1 час «тишины»
        "retries": 3,
        "retry_delay_minutes": 15,
        "owner": "monitoring-team",
        "email": [],  # ["[email protected]"]
        "env_tag": "prod",
    },
}


# ──────────────────────────────────────────────────────────────────────────────
# Загрузка конфига из Airflow для конкретной среды.
# Вызывается ИЗ ТАСКА, не на парсинге DAG.
# ──────────────────────────────────────────────────────────────────────────────
def _build_config(env: str, env_defaults: dict) -> tuple[SyncConfig, str]:
    """
    Возвращает (SyncConfig, auth_subdir).
    auth_subdir — поддиректория для auth-файлов (по умолчанию "auth").
    """
    zbx_conn_id = f"zabbix_{env}"
    gl_conn_id = f"gitlab_{env}"

    try:
        zbx_conn = BaseHook.get_connection(zbx_conn_id)
    except Exception as e:
        raise RuntimeError(
            f"Connection '{zbx_conn_id}' не найден. Создайте его:\n"
            f"  airflow connections add {zbx_conn_id} \\\n"
            f"    --conn-type http --conn-host https://zabbix-{env}.example.com \\\n"
            f"    --conn-login <user> --conn-password '<pwd>' \\\n"
            f"    --conn-extra '{{\"verify_ssl\": true}}'"
        ) from e

    try:
        gl_conn = BaseHook.get_connection(gl_conn_id)
    except Exception as e:
        raise RuntimeError(
            f"Connection '{gl_conn_id}' не найден. Создайте его:\n"
            f"  airflow connections add {gl_conn_id} \\\n"
            f"    --conn-type http --conn-host https://gitlab.example.com \\\n"
            f"    --conn-password '<PRIVATE-TOKEN>' \\\n"
            f"    --conn-extra '{{\"verify_ssl\": true}}'"
        ) from e

    zbx_extra = zbx_conn.extra_dejson or {}
    gl_extra = gl_conn.extra_dejson or {}

    project_id = _var_get(f"{env}_gitlab_project_id", None)
    if not project_id:
        raise RuntimeError(
            f"Airflow Variable '{env}_gitlab_project_id' не задана. Создайте:\n"
            f"  airflow variables set {env}_gitlab_project_id "
            f"'group/subgroup/project'"
        )

    default_quiet = env_defaults["default_quiet_period_sec"]
    auth_subdir = _var_get(f"{env}_zabbix_auth_subdir", "auth")

    cfg = SyncConfig(
        # Zabbix
        zabbix_url=zbx_conn.host,
        zabbix_user=zbx_conn.login,
        zabbix_password=zbx_conn.password,
        zabbix_verify_ssl=bool(zbx_extra.get("verify_ssl", True)),
        zabbix_timeout_sec=int(_var_get(f"{env}_zabbix_timeout_sec", "60")),
        zabbix_audit_timeout_sec=int(
            _var_get(f"{env}_zabbix_audit_timeout_sec", "180")
        ),

        # GitLab
        gitlab_url=gl_conn.host,
        gitlab_project_id=project_id,
        gitlab_token=gl_conn.password,
        gitlab_branch=_var_get(f"{env}_gitlab_branch", "main"),
        gitlab_verify_ssl=bool(gl_extra.get("verify_ssl", True)),

        # Логика. templates_subdir не используется auth-синхронизатором;
        # поддиректория auth передаётся отдельно (auth_subdir).
        quiet_period_sec=int(
            _var_get(f"{env}_zabbix_auth_quiet_period", str(default_quiet))
        ),
        audit_window_padding_sec=int(
            _var_get(f"{env}_audit_window_padding_sec", "900")
        ),
        audit_query_limit=int(_var_get(f"{env}_audit_query_limit", "5000")),
        single_commit=_var_get(
            f"{env}_zabbix_auth_single_commit", "true"
        ).lower() == "true",

        commit_author_name=f"Zabbix Sync Bot ({env})",
        commit_author_email=f"zabbix-sync-{env}@example.com",
    )
    return cfg, auth_subdir


# ──────────────────────────────────────────────────────────────────────────────
# Таск
# ──────────────────────────────────────────────────────────────────────────────
def sync_auth_task(env: str, env_defaults: dict, **context) -> dict:
    cfg, auth_subdir = _build_config(env, env_defaults)
    log.info(
        "[%s] Старт синхронизации auth Zabbix→GitLab. "
        "Zabbix=%s, GitLab=%s, project=%s, branch=%s, subdir=%s, quiet_period=%ds",
        env.upper(), cfg.zabbix_url, cfg.gitlab_url, cfg.gitlab_project_id,
        cfg.gitlab_branch, auth_subdir or "<root>", cfg.quiet_period_sec,
    )

    syncer = AuthSynchronizer(cfg, subdir=auth_subdir)
    stats = syncer.run()

    log.info("[%s] Итоги auth: %s", env.upper(), stats.summary())
    if stats.created:
        log.info("[%s] Созданы: %s", env.upper(), ", ".join(stats.created))
    if stats.updated:
        log.info("[%s] Обновлены: %s", env.upper(), ", ".join(stats.updated))
    if stats.deferred:
        log.info(
            "[%s] Отложены (правило отсрочки): %s",
            env.upper(),
            ", ".join(f"{f} (через {s}с)" for f, s in stats.deferred),
        )
    if stats.errors:
        log.error(
            "[%s] Ошибки auth:\n%s",
            env.upper(),
            "\n".join(f"  {f}: {e}" for f, e in stats.errors),
        )
        raise RuntimeError(
            f"[{env}] Синхронизация auth завершилась с {len(stats.errors)} ошибками"
        )

    return {
        "env": env,
        "created": stats.created,
        "updated": stats.updated,
        "deferred": [(f, s) for f, s in stats.deferred],
        "unchanged": len(stats.unchanged),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Фабрика DAG'ов
# ──────────────────────────────────────────────────────────────────────────────
def _make_dag(env: str, env_cfg: dict) -> DAG:
    default_args = {
        "owner": env_cfg["owner"],
        "depends_on_past": False,
        "email": env_cfg.get("email", []),
        "email_on_failure": bool(env_cfg.get("email")),
        "email_on_retry": False,
        "retries": env_cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=env_cfg.get("retry_delay_minutes", 5)),
    }

    dag = DAG(
        dag_id=f"zabbix_auth_to_gitlab_{env}",
        description=f"Sync Zabbix auth config (LDAP/SAML) into GitLab as YAML ({env.upper()})",
        default_args=default_args,
        start_date=_START_DATE,
        schedule=env_cfg["schedule"],
        catchup=False,
        max_active_runs=1,
        tags=["zabbix", "gitlab", "monitoring", "auth", "ldap", env_cfg["env_tag"]],
    )

    with dag:
        PythonOperator(
            task_id="sync_auth",
            python_callable=sync_auth_task,
            op_kwargs={"env": env, "env_defaults": env_cfg},
        )

    return dag


# Регистрируем по одному DAG-объекту в globals() на каждую среду
for _env, _env_cfg in ENVIRONMENTS.items():
    globals()[f"zabbix_auth_to_gitlab_{_env}"] = _make_dag(_env, _env_cfg)
