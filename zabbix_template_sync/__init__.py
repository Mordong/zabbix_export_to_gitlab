"""
zabbix_template_sync — синхронизация шаблонов Zabbix 7 в GitLab.
"""

from .sync import SyncConfig, SyncStats, TemplateSynchronizer
from .auth_sync import AuthSynchronizer, AuthSyncStats
from .zabbix_client import ZabbixAPI, ZabbixAPIError
from .gitlab_client import GitLabClient, GitLabAPIError

__all__ = [
    "SyncConfig",
    "SyncStats",
    "TemplateSynchronizer",
    "AuthSynchronizer",
    "AuthSyncStats",
    "ZabbixAPI",
    "ZabbixAPIError",
    "GitLabClient",
    "GitLabAPIError",
]
__version__ = "1.3.0"
