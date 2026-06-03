"""
Airflow DAG-фабрика: синхронизация шаблонов Zabbix 7 в GitLab
для нескольких окружений (TEST и PROD).

Из одного DAG-файла регистрируется N независимых DAG'ов — по одному
на каждую среду из ENVIRONMENTS ниже. Это даёт:
  - разные расписания (TEST чаще, PROD реже);
  - разные параметры (например quiet_period: 5 мин в TEST, 1 час в PROD);
  - разные retry/email-политики;
  - возможность отключить один DAG, не трогая другой.

──────────────────────────────────────────────────────────────────────────────
АРХИТЕКТУРНЫЙ ВЫБОР: Variables vs config.yaml
──────────────────────────────────────────────────────────────────────────────
Все секреты и настройки берутся из Airflow Connections и Variables, НЕ из
config.yaml. Это сделано по двум причинам:

1. Безопасность. Пароли и токены, лежащие в Connections, шифруются Fernet'ом
   в метабазе Airflow. В config.yaml они лежали бы в открытом виде в git.
   Также легко подключить внешний secrets backend (Vault, AWS SM, GCP SM)
   без правок кода.

2. Соответствие архитектуре Airflow 3. DAG processor работает изолированно
   от метабазы — никаких top-level DB-обращений. Все Variable.get() сидят
   внутри _build_config(), который вызывается только из таска.

config.yaml оставлен для CLI-режима (zabbix_template_sync.cli) — ручные
прогоны и отладка вне Airflow.

──────────────────────────────────────────────────────────────────────────────
ТРЕБУЕМЫЕ Connections для каждой среды
──────────────────────────────────────────────────────────────────────────────
  zabbix_<env>   — host, login, password, extra={"verify_ssl": true/false}
  gitlab_<env>   — host, password (= PRIVATE-TOKEN), extra={"verify_ssl": ...}

Создание через CLI:
  airflow connections add zabbix_test \
    --conn-type http --conn-host https://zabbix-test.example.com \
    --conn-login api-readonly --conn-password '...' \
    --conn-extra '{"verify_ssl": true}'

  airflow connections add gitlab_test \
    --conn-type http --conn-host https://gitlab.example.com \
    --conn-password 'glpat-...' \
    --conn-extra '{"verify_ssl": true}'

(Аналогично для prod: zabbix_prod, gitlab_prod.)

──────────────────────────────────────────────────────────────────────────────
ТРЕБУЕМЫЕ Variables (префиксованы средой)
──────────────────────────────────────────────────────────────────────────────
  <env>_gitlab_project_id            — обязательная
  <env>_gitlab_branch                — опц. (default: "main")
  <env>_zabbix_sync_subdir           — опц. (default: "templates")
  <env>_zabbix_sync_quiet_period     — опц. (default: см. ENVIRONMENTS ниже)
  <env>_zabbix_sync_template_groups  — опц. (default: "[]" = все группы)
  <env>_zabbix_sync_single_commit    — опц. (default: "true")

Пример:
  airflow variables set test_gitlab_project_id "observability/zabbix-cis/templates/test"
  airflow variables set prod_gitlab_project_id "observability/zabbix-cis/templates/prod"

──────────────────────────────────────────────────────────────────────────────
Как добавить новую среду (например, STAGING)
──────────────────────────────────────────────────────────────────────────────
1. Добавить запись в ENVIRONMENTS ниже.
2. Создать Connections zabbix_staging / gitlab_staging.
3. Создать Variable staging_gitlab_project_id.
4. После следующего парсинга DAG появится сам.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

# pendulum поставляется вместе с Airflow на любой современной инсталляции —
# Airflow 3 документация рекомендует использовать pendulum.datetime() с явной
# таймзоной для start_date (см. "Time zone aware Dags"). Падение в naive
# datetime — это legacy-поведение, работает, но в логах появляются warnings.
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

from zabbix_template_sync import SyncConfig, TemplateSynchronizer

log = logging.getLogger(__name__)


def _var_get(key: str, default):
    """
    Совместимая обёртка для Variable.get().

    В Airflow 3 SDK (airflow.sdk.Variable) параметр называется `default`.
    В Airflow 2 legacy (airflow.models.Variable) — `default_var`.
    Скрываем разницу здесь, чтобы остальной код был чистым.
    """
    if _USE_SDK:
        return Variable.get(key, default=default)
    return Variable.get(key, default_var=default)


# ──────────────────────────────────────────────────────────────────────────────
# Конфигурация окружений
# ──────────────────────────────────────────────────────────────────────────────
# Меняйте только эту секцию, чтобы добавить/удалить среду или подстроить
# поведение под конкретное окружение.
ENVIRONMENTS = {
    "test": {
        # Cron-расписание.
        "schedule": "*/30 * * * *",
        # Default quiet_period, если соответствующая Variable не задана.
        # В TEST разумно 5 мин — изменения должны быстро докатываться.
        "default_quiet_period_sec": 300,
        # Retry-политика. В TEST оставляем короткую — быстрая обратная связь.
        "retries": 2,
        "retry_delay_minutes": 5,
        # Кто получает алерты при сбоях этого DAG'а.
        "owner": "monitoring-team",
        "email": [],  # ["[email protected]"]
        # Тег в Airflow UI для фильтрации.
        "env_tag": "test",
    },
    "prod": {
        # PROD — каждые 30 минут.
        "schedule": "*/30 * * * *",
        # PROD — строго 1 час «тишины» по ТЗ.
        "default_quiet_period_sec": 3600,
        # PROD — больше ретраев и длиннее delay: если Zabbix временно
        # перегружен, через 5 минут может быть всё ещё нагружен.
        # 15 мин × 3 = до 45 минут окно попыток после первого падения.
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
def _build_config(env: str, env_defaults: dict) -> SyncConfig:
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

    template_groups_raw = _var_get(f"{env}_zabbix_sync_template_groups", "[]")
    try:
        template_groups = json.loads(template_groups_raw) or None
    except json.JSONDecodeError:
        log.warning(
            "%s_zabbix_sync_template_groups не валидный JSON, игнорирую", env
        )
        template_groups = None

    default_quiet = env_defaults["default_quiet_period_sec"]

    return SyncConfig(
        # Zabbix
        zabbix_url=zbx_conn.host,
        zabbix_user=zbx_conn.login,
        zabbix_password=zbx_conn.password,
        zabbix_verify_ssl=bool(zbx_extra.get("verify_ssl", True)),
        # Производительность Zabbix API (см. SyncConfig для смысла каждого):
        zabbix_timeout_sec=int(
            _var_get(f"{env}_zabbix_timeout_sec", "60")
        ),
        zabbix_audit_timeout_sec=int(
            _var_get(f"{env}_zabbix_audit_timeout_sec", "180")
        ),

        # GitLab
        gitlab_url=gl_conn.host,
        gitlab_project_id=project_id,
        gitlab_token=gl_conn.password,
        gitlab_branch=_var_get(f"{env}_gitlab_branch", "main"),
        gitlab_verify_ssl=bool(gl_extra.get("verify_ssl", True)),

        # Логика
        templates_subdir=_var_get(f"{env}_zabbix_sync_subdir", "templates"),
        quiet_period_sec=int(
            _var_get(f"{env}_zabbix_sync_quiet_period", str(default_quiet))
        ),
        template_groups=template_groups,
        # Окно и лимит auditlog.get запроса (см. SyncConfig):
        audit_window_padding_sec=int(
            _var_get(f"{env}_audit_window_padding_sec", "900")
        ),
        audit_query_limit=int(
            _var_get(f"{env}_audit_query_limit", "5000")
        ),
        single_commit=_var_get(
            f"{env}_zabbix_sync_single_commit", "true"
        ).lower() == "true",
        commit_chunk_size=int(_var_get(f"{env}_commit_chunk_size", "150")),
        commit_max_retries=int(_var_get(f"{env}_commit_max_retries", "3")),

        commit_author_name=f"Zabbix Sync Bot ({env})",
        commit_author_email=f"zabbix-sync-{env}@example.com",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Таск (общий для всех сред, среда передаётся через op_kwargs)
# ──────────────────────────────────────────────────────────────────────────────
def sync_templates_task(env: str, env_defaults: dict, **context) -> dict:
    cfg = _build_config(env, env_defaults)
    log.info(
        "[%s] Старт синхронизации Zabbix→GitLab. "
        "Zabbix=%s, GitLab=%s, project=%s, branch=%s, quiet_period=%ds",
        env.upper(), cfg.zabbix_url, cfg.gitlab_url, cfg.gitlab_project_id,
        cfg.gitlab_branch, cfg.quiet_period_sec,
    )

    syncer = TemplateSynchronizer(cfg)
    stats = syncer.run()

    log.info("[%s] Итоги: %s", env.upper(), stats.summary())
    if stats.created:
        log.info("[%s] Созданы: %s", env.upper(), ", ".join(stats.created))
    if stats.updated:
        log.info("[%s] Обновлены: %s", env.upper(), ", ".join(stats.updated))
    if stats.deferred:
        log.info(
            "[%s] Отложены (правило 1 часа): %s",
            env.upper(),
            ", ".join(f"{h} (через {s}с)" for h, s in stats.deferred),
        )
    if stats.errors:
        log.error(
            "[%s] Ошибки по шаблонам:\n%s",
            env.upper(),
            "\n".join(f"  {h}: {e}" for h, e in stats.errors),
        )
        raise RuntimeError(
            f"[{env}] Синхронизация завершилась с {len(stats.errors)} ошибками"
        )

    return {
        "env": env,
        "created": stats.created,
        "updated": stats.updated,
        "deferred": [(h, s) for h, s in stats.deferred],
        "unchanged": len(stats.unchanged),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Фабрика DAG'ов
#
# КРИТИЧНО: создаваемые DAG-объекты должны быть положены в globals(), иначе
# Airflow их не зарегистрирует. Цикл `for env, env_cfg in ENVIRONMENTS.items()`
# именно это и делает.
# ──────────────────────────────────────────────────────────────────────────────
def _make_dag(env: str, env_cfg: dict) -> DAG:
    default_args = {
        "owner": env_cfg["owner"],
        "depends_on_past": False,
        "email": env_cfg.get("email", []),
        "email_on_failure": bool(env_cfg.get("email")),
        "email_on_retry": False,
        # Берём из ENVIRONMENTS — у TEST/PROD разные retry-политики.
        "retries": env_cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=env_cfg.get("retry_delay_minutes", 5)),
    }

    dag = DAG(
        dag_id=f"zabbix_templates_to_gitlab_{env}",
        description=f"Sync Zabbix 7 templates into GitLab as YAML ({env.upper()})",
        default_args=default_args,
        start_date=_START_DATE,
        schedule=env_cfg["schedule"],
        catchup=False,
        max_active_runs=1,
        tags=["zabbix", "gitlab", "monitoring", "ci", env_cfg["env_tag"]],
    )

    with dag:
        PythonOperator(
            task_id="sync_templates",
            python_callable=sync_templates_task,
            op_kwargs={"env": env, "env_defaults": env_cfg},
        )

    return dag


# Регистрируем по одному DAG-объекту в globals() на каждую среду
for _env, _env_cfg in ENVIRONMENTS.items():
    globals()[f"zabbix_templates_to_gitlab_{_env}"] = _make_dag(_env, _env_cfg)
