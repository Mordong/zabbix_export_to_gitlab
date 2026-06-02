"""
Самодостаточные smoke-тесты пакета zabbix_template_sync.

Запуск: python tests/test_smoke.py
Без внешних зависимостей кроме PyYAML (как и сам пакет). Airflow не требуется —
DAG-файлы проверяются через подмену фейковых модулей airflow.

Покрывает:
  - импорт пакета и наличие публичных символов;
  - стабильную сериализацию YAML (кириллица, sort_keys);
  - логику AuthSynchronizer (create / unchanged / defer / update);
  - регистрацию обоих DAG в обеих ветках импорта (Airflow SDK / legacy).
"""

from __future__ import annotations

import importlib.util
import os
import sys
import time
import types
from unittest.mock import MagicMock, patch

# Корень репозитория — на уровень выше каталога tests/
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_package_imports() -> None:
    import zabbix_template_sync as z
    for sym in (
        "SyncConfig", "TemplateSynchronizer",
        "AuthSynchronizer", "AuthSyncStats",
        "ZabbixAPI", "GitLabClient",
    ):
        assert hasattr(z, sym), f"missing export: {sym}"
    print("  test_package_imports: OK")


def test_dump_yaml_unicode_stable() -> None:
    from zabbix_template_sync.utils import dump_yaml
    a = dump_yaml({"name": "Корп LDAP", "b": 1, "a": 2})
    b = dump_yaml({"a": 2, "name": "Корп LDAP", "b": 1})
    assert "Корп LDAP" in a and "\\u" not in a, "кириллица должна быть как есть"
    assert a == b, "sort_keys должен делать вывод детерминированным"
    print("  test_dump_yaml_unicode_stable: OK")


def _cfg():
    from zabbix_template_sync import SyncConfig
    return SyncConfig(
        zabbix_url="https://zbx.example.com", zabbix_user="u", zabbix_password="p",
        gitlab_url="https://gl.example.com", gitlab_project_id="grp/proj",
        gitlab_token="tok", quiet_period_sec=3600,
    )


def _fake_zbx():
    z = MagicMock()
    z.__enter__ = lambda s: z
    z.__exit__ = lambda *a: None
    z.get_authentication.return_value = {"ldap_auth_enabled": "1"}
    z.get_userdirectories.return_value = [{
        "userdirectoryid": "3", "name": "Корп LDAP", "idp_type": "1",
        "bind_password": "",
        "provision_groups": [{"name": "cn=admins,*", "roleid": "3",
            "_role_name": "Super admin role",
            "user_groups": [{"usrgrpid": "7", "_grp_name": "Zabbix administrators"}]}],
        "provision_media": [{"name": "Email", "mediatypeid": "1",
            "_mt_name": "Email (HTML)", "attribute": "mail", "active": "0"}],
    }]
    return z


def test_auth_create_when_absent() -> None:
    from zabbix_template_sync import AuthSynchronizer
    gl = MagicMock()
    gl.get_file_content.return_value = None
    cap = {}
    gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(actions=actions)
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=_fake_zbx()):
        s = AuthSynchronizer(_cfg()).run()
    assert s.created == ["authentication.yaml", "userdirectories.yaml"], s.created
    paths = [a["file_path"] for a in cap["actions"]]
    assert all(p.startswith("auth/") for p in paths), paths
    ud = cap["actions"][1]["content"]
    assert "Корп LDAP" in ud and "roleid" in ud and "_role_name" in ud
    print("  test_auth_create_when_absent: OK")


def test_auth_unchanged() -> None:
    from zabbix_template_sync import AuthSynchronizer
    from zabbix_template_sync.utils import dump_yaml
    zbx = _fake_zbx()
    existing = {
        "auth/authentication.yaml": dump_yaml({"authentication": zbx.get_authentication.return_value}),
        "auth/userdirectories.yaml": dump_yaml({"userdirectories": zbx.get_userdirectories.return_value}),
    }
    gl = MagicMock()
    gl.get_file_content.side_effect = lambda p: existing.get(p)
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx):
        s = AuthSynchronizer(_cfg()).run()
    assert sorted(s.unchanged) == ["authentication.yaml", "userdirectories.yaml"], s.unchanged
    assert not s.created and not s.updated
    zbx.get_latest_audit_clock.assert_not_called()
    print("  test_auth_unchanged: OK")


def test_auth_defer_and_update() -> None:
    from zabbix_template_sync import AuthSynchronizer
    old = {
        "auth/authentication.yaml": "authentication:\n  ldap_auth_enabled: '0'\n",
        "auth/userdirectories.yaml": "userdirectories: []\n",
    }
    # DEFER: правка минуту назад
    gl = MagicMock()
    gl.get_file_content.side_effect = lambda p: old.get(p)
    zbx = _fake_zbx()
    zbx.get_latest_audit_clock.return_value = int(time.time()) - 60
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx):
        s = AuthSynchronizer(_cfg()).run()
    assert len(s.deferred) == 2 and not s.updated, (s.deferred, s.updated)

    # UPDATE: правка 2 часа назад
    gl2 = MagicMock()
    gl2.get_file_content.side_effect = lambda p: old.get(p)
    zbx2 = _fake_zbx()
    zbx2.get_latest_audit_clock.return_value = int(time.time()) - 7200
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl2), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx2):
        s2 = AuthSynchronizer(_cfg()).run()
    assert sorted(s2.updated) == ["authentication.yaml", "userdirectories.yaml"], s2.updated
    print("  test_auth_defer_and_update: OK")


def _build_fake_airflow(use_sdk: bool) -> None:
    for m in list(sys.modules):
        if m.startswith("airflow"):
            del sys.modules[m]

    class FakeDAG:
        def __init__(self, **kw):
            self.kw = kw
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass

    class FakeVariable:
        store: dict = {}
        @classmethod
        def get(cls, k, default=None, default_var=None):
            return cls.store.get(k, default if default is not None else default_var)

    class FakeConn:
        host = "https://x"; login = "u"; password = "p"
        @property
        def extra_dejson(self):
            return {"verify_ssl": True}

    class FakeHook:
        @staticmethod
        def get_connection(cid):
            return FakeConn()

    class FakePythonOperator:
        def __init__(self, **kw):
            self.kw = kw

    airflow = types.ModuleType("airflow")
    if use_sdk:
        sdk = types.ModuleType("airflow.sdk")
        sdk.DAG = FakeDAG; sdk.Variable = FakeVariable
        bases = types.ModuleType("airflow.sdk.bases")
        hook = types.ModuleType("airflow.sdk.bases.hook"); hook.BaseHook = FakeHook
        sys.modules.update({
            "airflow.sdk": sdk, "airflow.sdk.bases": bases,
            "airflow.sdk.bases.hook": hook,
        })
    else:
        airflow.DAG = FakeDAG
        models = types.ModuleType("airflow.models"); models.Variable = FakeVariable
        hooks = types.ModuleType("airflow.hooks")
        base = types.ModuleType("airflow.hooks.base"); base.BaseHook = FakeHook
        sys.modules.update({
            "airflow.models": models, "airflow.hooks": hooks,
            "airflow.hooks.base": base,
        })
    sys.modules["airflow"] = airflow

    prov = types.ModuleType("airflow.providers")
    std = types.ModuleType("airflow.providers.standard")
    ops = types.ModuleType("airflow.providers.standard.operators")
    py = types.ModuleType("airflow.providers.standard.operators.python")
    py.PythonOperator = FakePythonOperator
    sys.modules.update({
        "airflow.providers": prov, "airflow.providers.standard": std,
        "airflow.providers.standard.operators": ops,
        "airflow.providers.standard.operators.python": py,
    })


def _load_dag(path: str, name: str):
    for m in list(sys.modules):
        if name in m:
            del sys.modules[m]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_dags_register_both_branches() -> None:
    dags = {
        "zabbix_templates_to_gitlab": os.path.join(ROOT, "dags", "zabbix_templates_to_gitlab.py"),
        "zabbix_auth_to_gitlab": os.path.join(ROOT, "dags", "zabbix_auth_to_gitlab.py"),
    }
    for use_sdk in (True, False):
        _build_fake_airflow(use_sdk)
        for name, path in dags.items():
            mod = _load_dag(path, name)
            registered = sorted(k for k in vars(mod) if k.startswith(name))
            assert registered == [f"{name}_prod", f"{name}_test"], (name, use_sdk, registered)
    print("  test_dags_register_both_branches: OK")


def main() -> int:
    tests = [
        test_package_imports,
        test_dump_yaml_unicode_stable,
        test_auth_create_when_absent,
        test_auth_unchanged,
        test_auth_defer_and_update,
        test_dags_register_both_branches,
    ]
    print(f"Running {len(tests)} smoke tests…")
    for t in tests:
        t()
    print("ALL SMOKE TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
