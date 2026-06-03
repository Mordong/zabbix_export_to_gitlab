"""
CLI: ручной запуск синхронизации без Airflow.

Использование:
    python -m zabbix_template_sync.cli --config config.yaml
    python -m zabbix_template_sync.cli         # читает env-переменные

Удобно для:
  - первичной выгрузки (когда репо ещё пуст);
  - ручной проверки прав/конфигурации;
  - локальной отладки.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import yaml

from .sync import SyncConfig, TemplateSynchronizer
from .auth_sync import AuthSynchronizer


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        # stream=sys.stderr — по умолчанию; UTF-8 на современных консолях работает,
        # на Windows может потребоваться `chcp 65001` или PYTHONIOENCODING=utf-8.
        stream=sys.stderr,
    )


def _config_from_env() -> SyncConfig:
    """Читает конфигурацию из переменных среды."""
    def env(name: str, default: str = "") -> str:
        return os.environ.get(name, default)

    return SyncConfig(
        zabbix_url=env("ZABBIX_URL"),
        zabbix_user=env("ZABBIX_USER"),
        zabbix_password=env("ZABBIX_PASSWORD"),
        zabbix_verify_ssl=env("ZABBIX_VERIFY_SSL", "true").lower() == "true",

        gitlab_url=env("GITLAB_URL", "https://gitlab.com"),
        gitlab_project_id=env("GITLAB_PROJECT_ID"),
        gitlab_token=env("GITLAB_TOKEN"),
        gitlab_branch=env("GITLAB_BRANCH", "main"),
        gitlab_verify_ssl=env("GITLAB_VERIFY_SSL", "true").lower() == "true",

        templates_subdir=env("TEMPLATES_SUBDIR", "templates"),
        quiet_period_sec=int(env("QUIET_PERIOD_SEC", "3600")),
        single_commit=env("SINGLE_COMMIT", "true").lower() == "true",
        host_export_batch_size=int(env("HOST_EXPORT_BATCH_SIZE", "500")),
        zabbix_export_timeout_sec=int(env("ZABBIX_EXPORT_TIMEOUT_SEC", "300")),
    )


def _config_from_yaml(path: str) -> SyncConfig:
    """Читает конфигурацию из YAML-файла (UTF-8)."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    z = data.get("zabbix", {})
    g = data.get("gitlab", {})
    s = data.get("sync", {})

    return SyncConfig(
        zabbix_url=z["url"],
        zabbix_user=z["user"],
        zabbix_password=z["password"],
        zabbix_verify_ssl=bool(z.get("verify_ssl", True)),
        zabbix_timeout_sec=int(z.get("timeout_sec", 60)),
        zabbix_audit_timeout_sec=int(z.get("audit_timeout_sec", 180)),

        gitlab_url=g["url"],
        gitlab_project_id=str(g["project_id"]),
        gitlab_token=g["token"],
        gitlab_branch=g.get("branch", "main"),
        gitlab_verify_ssl=bool(g.get("verify_ssl", True)),

        templates_subdir=s.get("templates_subdir", "templates"),
        quiet_period_sec=int(s.get("quiet_period_sec", 3600)),
        template_groups=s.get("template_groups") or None,
        template_hosts=s.get("template_hosts") or None,
        audit_window_padding_sec=int(s.get("audit_window_padding_sec", 900)),
        audit_query_limit=int(s.get("audit_query_limit", 5000)),
        single_commit=bool(s.get("single_commit", True)),
        host_export_batch_size=int(s.get("host_export_batch_size", 500)),
        zabbix_export_timeout_sec=int(s.get("zabbix_export_timeout_sec", 300)),
        commit_author_name=s.get("commit_author_name", "Zabbix Sync Bot"),
        commit_author_email=s.get("commit_author_email", "zabbix-sync@example.com"),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Синхронизация шаблонов Zabbix 7 → GitLab (YAML, UTF-8)."
    )
    p.add_argument("--config", "-c", help="Путь к YAML конфигу (UTF-8).")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug-логи.")
    p.add_argument(
        "--force-all",
        action="store_true",
        help="Игнорировать правило 1 часа — коммитить все изменения сразу. "
             "Используйте при первичной заливке или ручной синхронизации.",
    )
    p.add_argument(
        "--auth",
        action="store_true",
        help="Синхронизировать настройки аутентификации (LDAP/SAML) "
             "вместо шаблонов: auth/authentication.yaml + "
             "auth/userdirectories.yaml.",
    )
    p.add_argument(
        "--auth-subdir",
        default="auth",
        help="Поддиректория в репозитории для auth-файлов "
             "(по умолчанию 'auth'; '' = корень репо). "
             "Используется только с --auth.",
    )
    args = p.parse_args(argv)

    _setup_logging(args.verbose)

    cfg = _config_from_yaml(args.config) if args.config else _config_from_env()
    if args.force_all:
        cfg.quiet_period_sec = 0

    if args.auth:
        syncer = AuthSynchronizer(cfg, subdir=args.auth_subdir)
    else:
        syncer = TemplateSynchronizer(cfg)
    stats = syncer.run()

    print()
    title = "Итоги синхронизации auth" if args.auth else "Итоги синхронизации"
    print(f"═══ {title} ═══")
    print(f"  Создано:    {len(stats.created)}")
    if stats.created:
        for h in stats.created:
            print(f"      + {h}")
    print(f"  Обновлено:  {len(stats.updated)}")
    if stats.updated:
        for h in stats.updated:
            print(f"      ~ {h}")
    print(f"  Отложено:   {len(stats.deferred)}  (изменены менее часа назад)")
    if stats.deferred:
        for h, s in stats.deferred:
            print(f"      … {h}  (через {s}с)")
    print(f"  Без изменений: {len(stats.unchanged)}")
    if stats.errors:
        print(f"  Ошибки:    {len(stats.errors)}")
        for h, e in stats.errors:
            print(f"      ✗ {h}: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
