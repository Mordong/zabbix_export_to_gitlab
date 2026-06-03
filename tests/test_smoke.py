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
    assert s.created == ["authentication.yaml", "userdirectories.yaml",
                         "userdirectory_Корп_LDAP.csv",
                         "userdirectory_Корп_LDAP.md"], s.created
    paths = [a["file_path"] for a in cap["actions"]]
    assert all(p.startswith("auth/") for p in paths), paths
    ud = cap["actions"][1]["content"]
    assert "Корп LDAP" in ud and "roleid" in ud and "_role_name" in ud
    print("  test_auth_create_when_absent: OK")


def test_auth_unchanged() -> None:
    from zabbix_template_sync import AuthSynchronizer
    from zabbix_template_sync.utils import (
        dump_yaml, dump_user_group_mapping_csv, dump_user_group_mapping_md)
    zbx = _fake_zbx()
    dirs = zbx.get_userdirectories.return_value
    existing = {
        "auth/authentication.yaml": dump_yaml({"authentication": zbx.get_authentication.return_value}),
        "auth/userdirectories.yaml": dump_yaml({"userdirectories": dirs}),
        "auth/userdirectory_Корп_LDAP.csv": dump_user_group_mapping_csv(dirs[0]),
        "auth/userdirectory_Корп_LDAP.md": dump_user_group_mapping_md(dirs[0]),
    }
    gl = MagicMock()
    gl.get_file_content.side_effect = lambda p: existing.get(p)
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx):
        s = AuthSynchronizer(_cfg()).run()
    assert sorted(s.unchanged) == ["authentication.yaml", "userdirectories.yaml",
                                   "userdirectory_Корп_LDAP.csv",
                                   "userdirectory_Корп_LDAP.md"], s.unchanged
    assert not s.created and not s.updated
    zbx.get_latest_audit_clock.assert_not_called()
    print("  test_auth_unchanged: OK")


def test_auth_defer_and_update() -> None:
    from zabbix_template_sync import AuthSynchronizer
    old = {
        "auth/authentication.yaml": "authentication:\n  ldap_auth_enabled: '0'\n",
        "auth/userdirectories.yaml": "userdirectories: []\n",
        # CSV и MD тоже присутствуют, но с другим содержимым →
        # участвуют в правиле отсрочки наравне с YAML.
        "auth/userdirectory_Корп_LDAP.csv": "LDAP group pattern;User groups;User role\n",
        "auth/userdirectory_Корп_LDAP.md": "# old\n",
    }
    # DEFER: правка минуту назад
    gl = MagicMock()
    gl.get_file_content.side_effect = lambda p: old.get(p)
    zbx = _fake_zbx()
    zbx.get_latest_audit_clock.return_value = int(time.time()) - 60
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx):
        s = AuthSynchronizer(_cfg()).run()
    assert len(s.deferred) == 4 and not s.updated, (s.deferred, s.updated)

    # UPDATE: правка 2 часа назад
    gl2 = MagicMock()
    gl2.get_file_content.side_effect = lambda p: old.get(p)
    zbx2 = _fake_zbx()
    zbx2.get_latest_audit_clock.return_value = int(time.time()) - 7200
    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl2), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx2):
        s2 = AuthSynchronizer(_cfg()).run()
    assert sorted(s2.updated) == ["authentication.yaml", "userdirectories.yaml",
                                  "userdirectory_Корп_LDAP.csv",
                                  "userdirectory_Корп_LDAP.md"], s2.updated
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


def test_csv_mapping_basic_and_quoting() -> None:
    """CSV маппинга групп: разделитель ';', запятые в группах, квотирование."""
    import csv as _csv
    import io as _io
    from zabbix_template_sync.utils import dump_user_group_mapping_csv

    ud = {"provision_groups": [
        {"name": "cn=admins", "_role_name": "Super admin role",
         "user_groups": [{"_grp_name": "Zabbix administrators"}, {"_grp_name": "Группа Б"}]},
        {"name": "cn=weird;grp", "_role_name": "Роль; точка с запятой",
         "user_groups": [{"_grp_name": "Группа, с запятой"}]},
    ]}
    out = dump_user_group_mapping_csv(ud)
    # без BOM, только \n
    assert "\ufeff" not in out and "\r" not in out, "UTF-8 без BOM, переводы \\n"
    rows = list(_csv.reader(_io.StringIO(out), delimiter=";"))
    assert rows[0] == ["LDAP group pattern", "User groups", "User role"]
    # группы через запятую, кириллица сохранена
    assert rows[1] == ["cn=admins", "Zabbix administrators,Группа Б", "Super admin role"]
    # имена с ';' квотируются и парсятся обратно корректно
    assert rows[2] == ["cn=weird;grp", "Группа, с запятой", "Роль; точка с запятой"], rows[2]
    print("  test_csv_mapping_basic_and_quoting: OK")


def test_csv_mapping_empty_and_fallback() -> None:
    """Пустые provision_groups → только шапка; нет имён → fallback на ID."""
    from zabbix_template_sync.utils import dump_user_group_mapping_csv

    assert dump_user_group_mapping_csv({"provision_groups": []}) == \
        "LDAP group pattern;User groups;User role\n"
    assert dump_user_group_mapping_csv({}) == \
        "LDAP group pattern;User groups;User role\n"

    ud = {"provision_groups": [
        {"name": "cn=x", "roleid": "3", "user_groups": [{"usrgrpid": "7"}]}]}
    out = dump_user_group_mapping_csv(ud)
    assert out.endswith("cn=x;7;3\n"), repr(out)
    print("  test_csv_mapping_empty_and_fallback: OK")


def test_auth_creates_csv_per_directory() -> None:
    """AuthSynchronizer создаёт по CSV на каждый directory с safe_filename."""
    from unittest.mock import MagicMock, patch
    from zabbix_template_sync import AuthSynchronizer

    dir1 = {"userdirectoryid": "3", "name": "Корп LDAP", "provision_media": [],
            "provision_groups": [{"name": "cn=a", "_role_name": "Super admin role",
                "user_groups": [{"_grp_name": "Zabbix administrators"}]}]}
    dir2 = {"userdirectoryid": "5", "name": "SAML Prod",
            "provision_groups": [], "provision_media": []}

    zbx = MagicMock()
    zbx.__enter__ = lambda s: zbx
    zbx.__exit__ = lambda *a: None
    zbx.get_authentication.return_value = {"ldap_auth_enabled": "1"}
    zbx.get_userdirectories.return_value = [dir1, dir2]

    gl = MagicMock()
    gl.get_file_content.return_value = None
    cap = {}
    gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(a=actions)

    with patch("zabbix_template_sync.auth_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.auth_sync.ZabbixAPI", return_value=zbx):
        AuthSynchronizer(_cfg()).run()

    paths = sorted(a["file_path"] for a in cap["a"])
    assert "auth/userdirectory_Корп_LDAP.csv" in paths, paths
    assert "auth/userdirectory_SAML_Prod.csv" in paths, paths
    # MD рядом с CSV для каждого directory
    assert "auth/userdirectory_Корп_LDAP.md" in paths, paths
    assert "auth/userdirectory_SAML_Prod.md" in paths, paths
    # CSV для пустого directory — только шапка
    empty = [a["content"] for a in cap["a"]
             if a["file_path"] == "auth/userdirectory_SAML_Prod.csv"][0]
    assert empty == "LDAP group pattern;User groups;User role\n", repr(empty)
    # MD для пустого directory — заголовок + шапка таблицы без строк
    empty_md = [a["content"] for a in cap["a"]
                if a["file_path"] == "auth/userdirectory_SAML_Prod.md"][0]
    assert empty_md == ("# SAML Prod\n\n| LDAP group pattern | User groups | User role |\n"
                        "| --- | --- | --- |\n"), repr(empty_md)
    # userdirectories.yaml получен одним вызовом API
    zbx.get_userdirectories.assert_called_once()
    print("  test_auth_creates_csv_per_directory: OK")


def test_md_mapping_table_and_escaping() -> None:
    """MD-маппинг: заголовок, таблица, экранирование '|', пустые группы."""
    from zabbix_template_sync.utils import dump_user_group_mapping_md

    ud = {"name": "Корп LDAP", "provision_groups": [
        {"name": "cn=admins,dc=corp", "_role_name": "Super admin role",
         "user_groups": [{"_grp_name": "Zabbix administrators"}]},
        {"name": "cn=a|b", "_role_name": "Role|X",
         "user_groups": [{"_grp_name": "Группа А"}, {"_grp_name": "Группа Б"}]},
    ]}
    out = dump_user_group_mapping_md(ud)
    assert out.startswith("# Корп LDAP\n\n"), out
    assert "| LDAP group pattern | User groups | User role |" in out
    assert "| --- | --- | --- |" in out
    # группы через запятую, кириллица
    assert "| cn=admins,dc=corp | Zabbix administrators | Super admin role |" in out
    # '|' в значениях экранируется
    assert "| cn=a\\|b | Группа А,Группа Б | Role\\|X |" in out, out
    assert "\r" not in out and out.endswith("\n")

    # пустые provision_groups → заголовок + шапка без строк
    empty = dump_user_group_mapping_md({"name": "SAML"})
    assert empty == ("# SAML\n\n| LDAP group pattern | User groups | User role |\n"
                     "| --- | --- | --- |\n"), repr(empty)
    # fallback: нет name → id в заголовке; нет имён → id в ячейках
    fb = dump_user_group_mapping_md(
        {"userdirectoryid": "9",
         "provision_groups": [{"name": "cn=x", "roleid": "3",
             "user_groups": [{"usrgrpid": "7"}]}]})
    assert fb.startswith("# 9\n"), fb
    assert "| cn=x | 7 | 3 |" in fb
    print("  test_md_mapping_table_and_escaping: OK")


def test_dag_schedules() -> None:
    """Проверка расписаний по умолчанию во всех средах обоих DAG."""
    import importlib.util
    _build_fake_airflow(True)
    for name, expected in [
        ("zabbix_auth_to_gitlab", "0 0 * * *"),
        ("zabbix_templates_to_gitlab", "*/30 * * * *"),
    ]:
        path = os.path.join(ROOT, "dags", f"{name}.py")
        spec = importlib.util.spec_from_file_location(name + "_sched", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        envs = mod.ENVIRONMENTS
        for env, cfg in envs.items():
            assert cfg["schedule"] == expected, (name, env, cfg["schedule"])
    print("  test_dag_schedules: OK")


def test_config_sync_groups_and_layout() -> None:
    """DR-экспорт: три группы кладут файлы в свои папки; hosts пофайлово."""
    from unittest.mock import MagicMock, patch
    from zabbix_template_sync.config_sync import ConfigSynchronizer

    def mkz():
        z = MagicMock()
        z.__enter__ = lambda s: z
        z.__exit__ = lambda *a: None
        z.get_roles.return_value = [{"roleid": "3", "name": "Super admin role"}]
        z.get_usergroups.return_value = [{"usrgrpid": "7", "name": "Admins"}]
        z.get_users.return_value = [{"userid": "1", "username": "Admin"}]
        z.get_actions.return_value = [{"actionid": "2", "name": "Report"}]
        z.get_global_macros.return_value = [{"macro": "{$X}", "value": "v", "type": "0"}]
        z.list_hosts.return_value = [
            {"hostid": "10"},
            {"hostid": "11"},
        ]
        _BATCH = (
            "zabbix_export:\n"
            "  hosts:\n"
            "    - {host: srv-01, name: S1}\n"
            "    - {host: db/primary, name: DB}\n"
        )
        from zabbix_template_sync.zabbix_client import ZabbixAPI as _Z
        z.export_hosts_batched.side_effect = (
            lambda hostids, bs, export_timeout=None: _Z.export_hosts_batched(
                type("X", (), {"export_yaml_by_ids": staticmethod(
                    lambda key, ids, timeout_override=None: _BATCH if ids else "")})(),
                hostids, bs, export_timeout=export_timeout))
        z.export_yaml_by_ids.side_effect = (
            lambda key, ids: f"zabbix_export:\n  {key}: {len(ids)}\n" if ids else "")
        z._call.side_effect = lambda m, p: (
            [{"mediatypeid": "1"}] if m == "mediatype.get"
            else [{"groupid": "4"}] if "group.get" in m else [])
        return z

    expect = {
        "users": ["users/roles.yaml", "users/usergroups.yaml", "users/users.yaml"],
        "alerting": ["alerting/mediatypes.yaml", "alerting/actions.yaml"],
        "core": ["core/hostgroups.yaml", "core/templategroups.yaml", "core/macros.yaml",
                 "core/hosts/srv-01.yaml", "core/hosts/db_primary.yaml"],
    }
    for grp, files in expect.items():
        gl = MagicMock()
        gl.get_file_content.return_value = None
        cap = {}
        gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(a=actions)
        with patch("zabbix_template_sync.config_sync.GitLabClient", return_value=gl), \
             patch("zabbix_template_sync.config_sync.ZabbixAPI", return_value=mkz()):
            ConfigSynchronizer(_cfg(), grp).run()
        paths = sorted(a["file_path"] for a in cap["a"])
        for f in files:
            assert f in paths, (grp, f, paths)
    print("  test_config_sync_groups_and_layout: OK")


def test_config_sync_secret_and_skip() -> None:
    """Секретный макрос маскируется в [SECRET]; пустой экспорт пропускается."""
    from unittest.mock import MagicMock, patch
    from zabbix_template_sync.config_sync import ConfigSynchronizer
    from zabbix_template_sync.zabbix_client import ZabbixAPI, SECRET_PLACEHOLDER

    # Маскировка — на уровне get_global_macros (юнит проверка логики метода)
    z = ZabbixAPI.__new__(ZabbixAPI)
    z._call = MagicMock(return_value=[
        {"macro": "{$P}", "type": "0", "value": "plain"},
        {"macro": "{$S}", "type": "1", "value": ""},
    ])
    macros = z.get_global_macros()
    assert macros[1]["value"] == SECRET_PLACEHOLDER, macros

    # Пустой configuration.export (нет media types) → файл не коммитится;
    # при этом *.get-файлы (actions.yaml с пустым списком) — коммитятся.
    def mkz():
        zz = MagicMock(); zz.__enter__ = lambda s: zz; zz.__exit__ = lambda *a: None
        zz.get_actions.return_value = []
        zz.export_yaml_by_ids.side_effect = lambda key, ids: ""
        zz._call.side_effect = lambda m, p: []  # mediatype.get → []
        return zz
    gl = MagicMock(); gl.get_file_content.return_value = None
    cap = {}
    gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(a=actions)
    with patch("zabbix_template_sync.config_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.config_sync.ZabbixAPI", return_value=mkz()):
        ConfigSynchronizer(_cfg(), "alerting").run()
    paths = [a["file_path"] for a in cap.get("a", [])]
    # mediatypes.yaml пропущен (пустой export), actions.yaml присутствует
    assert "alerting/mediatypes.yaml" not in paths, paths
    assert "alerting/actions.yaml" in paths, paths
    print("  test_config_sync_secret_and_skip: OK")


def test_config_dags_register() -> None:
    """6 DR-DAG (3 группы x 2 среды) регистрируются, schedule 0 21 * * *."""
    import importlib.util
    for use_sdk in (True, False):
        _build_fake_airflow(use_sdk)
        for m in list(sys.modules):
            if "to_gitlab" in m or "dag_factory" in m:
                del sys.modules[m]
        for g in ("users", "alerting", "core"):
            path = os.path.join(ROOT, "dags", f"zabbix_{g}_to_gitlab.py")
            spec = importlib.util.spec_from_file_location(f"zabbix_{g}_to_gitlab", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            ids = sorted(k for k in vars(mod) if k.startswith(f"zabbix_{g}_to_gitlab_"))
            assert ids == [f"zabbix_{g}_to_gitlab_prod", f"zabbix_{g}_to_gitlab_test"], (g, ids)
            for did in ids:
                assert getattr(mod, did).kw["schedule"] == "0 21 * * *"
    print("  test_config_dags_register: OK")


def test_infra_ui_groups_layout_and_secrets() -> None:
    """infra/ui: раскладка (поимённо в свою папку + мелкие в корне) + маскировка."""
    from unittest.mock import MagicMock, patch
    from zabbix_template_sync.config_sync import ConfigSynchronizer
    from zabbix_template_sync.zabbix_client import ZabbixAPI, SECRET_PLACEHOLDER

    def mkz():
        z = MagicMock(); z.__enter__ = lambda s: z; z.__exit__ = lambda *a: None
        z.list_proxies_brief.return_value = [
            {"proxyid": "1", "name": "proxy-msk"}, {"proxyid": "2", "name": "proxy/spb"}]
        z.get_proxy.side_effect = lambda pid: {"proxyid": pid, "name": "p" + pid}
        z.get_proxy_groups.return_value = [{"proxy_groupid": "9", "name": "PG1"}]
        z.get_discovery_rules.return_value = [{"druleid": "3", "name": "Net"}]
        z.get_maintenances.return_value = [{"maintenanceid": "4", "name": "Weekly"}]
        z.list_maps.return_value = [{"sysmapid": "5", "name": "DC map"}]
        z.export_map_yaml.side_effect = lambda sid: f"zabbix_export:\n  maps: [{sid}]\n"
        z.list_dashboards.return_value = [{"dashboardid": "6", "name": "Overview"}]
        z.get_dashboard.side_effect = lambda did: {"dashboardid": did, "name": "D"}
        z.list_scripts.return_value = [{"scriptid": "7", "name": "Reboot"}]
        z.get_script.side_effect = lambda scid: {"scriptid": scid, "name": "S"}
        return z

    expect = {
        "infra": ["proxies/proxy-msk.yaml", "proxies/proxy_spb.yaml",
                  "proxygroups.yaml", "drules.yaml", "maintenance.yaml"],
        "ui": ["maps/DC_map.yaml", "dashboards/Overview.yaml", "scripts/Reboot.yaml"],
    }
    for grp, files in expect.items():
        gl = MagicMock(); gl.get_file_content.return_value = None
        cap = {}
        gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(a=actions)
        with patch("zabbix_template_sync.config_sync.GitLabClient", return_value=gl), \
             patch("zabbix_template_sync.config_sync.ZabbixAPI", return_value=mkz()):
            ConfigSynchronizer(_cfg(), grp).run()
        paths = sorted(a["file_path"] for a in cap["a"])
        for f in files:
            assert f in paths, (grp, f, paths)

    # Маскировка: proxy TLS PSK и script password (юнит-проверка методов клиента)
    z = ZabbixAPI.__new__(ZabbixAPI)
    z._call = MagicMock(return_value=[{"proxyid": "1", "name": "p",
                                       "tls_psk": "abcd", "tls_psk_identity": "id"}])
    assert z.get_proxy("1")["tls_psk"] == SECRET_PLACEHOLDER
    z2 = ZabbixAPI.__new__(ZabbixAPI)
    z2._call = MagicMock(return_value=[{"scriptid": "7", "name": "r", "password": "x"}])
    assert z2.get_script("7")["password"] == SECRET_PLACEHOLDER
    print("  test_infra_ui_groups_layout_and_secrets: OK")


def test_infra_ui_dags_register() -> None:
    """infra/ui DAG регистрируются в обеих ветках Airflow, schedule 0 19 * * *."""
    import importlib.util
    for use_sdk in (True, False):
        _build_fake_airflow(use_sdk)
        for m in list(sys.modules):
            if "to_gitlab" in m or "dag_factory" in m:
                del sys.modules[m]
        for g in ("infra", "ui"):
            path = os.path.join(ROOT, "dags", f"zabbix_{g}_to_gitlab.py")
            spec = importlib.util.spec_from_file_location(f"zabbix_{g}_to_gitlab", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            ids = sorted(k for k in vars(mod) if k.startswith(f"zabbix_{g}_to_gitlab_"))
            assert ids == [f"zabbix_{g}_to_gitlab_prod", f"zabbix_{g}_to_gitlab_test"], (g, ids)
            for did in ids:
                assert getattr(mod, did).kw["schedule"] == "0 19 * * *"
    print("  test_infra_ui_dags_register: OK")


def test_host_batch_export_and_slicing() -> None:
    """Хосты: батч configuration.export + нарезка на файлы (оптимизация)."""
    from unittest.mock import MagicMock, patch
    from zabbix_template_sync.config_sync import ConfigSynchronizer
    from zabbix_template_sync.zabbix_client import ZabbixAPI, _slice_hosts_export

    BATCH = (
        "zabbix_export:\n"
        '  version: "7.4"\n'
        '  date: "2026-06-03T10:00:00Z"\n'
        "  host_groups:\n"
        "    - {uuid: g1, name: Linux}\n"
        "  hosts:\n"
        "    - {host: srv-01, name: Сервер 01}\n"
        "    - {host: srv-02, name: S2}\n"
        "    - {host: db/primary, name: DB}\n"
    )

    # Нарезка: 3 хоста → 3 самодостаточных документа, date вырезан, кириллица.
    sliced = dict(_slice_hosts_export(BATCH))
    assert sorted(sliced) == ["db/primary", "srv-01", "srv-02"], list(sliced)
    import yaml as _y
    d = _y.safe_load(sliced["srv-01"])["zabbix_export"]
    assert "date" not in d and "host_groups" in d and len(d["hosts"]) == 1
    assert "Сервер 01" in sliced["srv-01"]

    # Батчинг: 3 хоста, batch_size=2 → ровно 2 вызова configuration.export.
    z = ZabbixAPI.__new__(ZabbixAPI)
    calls = []
    z.export_yaml_by_ids = lambda key, ids, timeout_override=None: (calls.append(list(ids)) or BATCH) if ids else ""
    list(z.export_hosts_batched(["1", "2", "3"], batch_size=2))
    assert calls == [["1", "2"], ["3"]], calls

    # Интеграция в core: файлы на хост, slash в имени санитизирован.
    def mkz():
        zz = MagicMock(); zz.__enter__ = lambda s: zz; zz.__exit__ = lambda *a: None
        zz.list_hosts.return_value = [{"hostid": "1"}, {"hostid": "2"}, {"hostid": "3"}]
        zz.get_global_macros.return_value = []
        zz._call.side_effect = lambda m, p: []
        zz.export_yaml_by_ids.side_effect = lambda key, ids, timeout_override=None: (BATCH if key == "hosts" and ids else "")
        zz.export_hosts_batched.side_effect = lambda hostids, bs, export_timeout=None: ZabbixAPI.export_hosts_batched(zz, hostids, bs, export_timeout=export_timeout)
        return zz
    gl = MagicMock(); gl.get_file_content.return_value = None
    cap = {}
    gl.commit_multiple.side_effect = lambda actions, commit_message, **kw: cap.update(a=actions)
    with patch("zabbix_template_sync.config_sync.GitLabClient", return_value=gl), \
         patch("zabbix_template_sync.config_sync.ZabbixAPI", return_value=mkz()):
        ConfigSynchronizer(_cfg(), "core").run()
    paths = sorted(a["file_path"] for a in cap["a"])
    assert "core/hosts/srv-01.yaml" in paths
    assert "core/hosts/db_primary.yaml" in paths, paths
    print("  test_host_batch_export_and_slicing: OK")


def main() -> int:
    tests = [
        test_package_imports,
        test_dump_yaml_unicode_stable,
        test_auth_create_when_absent,
        test_auth_unchanged,
        test_auth_defer_and_update,
        test_csv_mapping_basic_and_quoting,
        test_csv_mapping_empty_and_fallback,
        test_md_mapping_table_and_escaping,
        test_auth_creates_csv_per_directory,
        test_config_sync_groups_and_layout,
        test_config_sync_secret_and_skip,
        test_infra_ui_groups_layout_and_secrets,
        test_host_batch_export_and_slicing,
        test_dag_schedules,
        test_dags_register_both_branches,
        test_config_dags_register,
        test_infra_ui_dags_register,
    ]
    print(f"Running {len(tests)} smoke tests…")
    for t in tests:
        t()
    print("ALL SMOKE TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
