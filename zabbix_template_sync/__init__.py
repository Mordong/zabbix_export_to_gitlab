"""
zabbix_template_sync — синхронизация шаблонов Zabbix 7 в GitLab.
"""

from .sync import SyncConfig, SyncStats, TemplateSynchronizer
from .zabbix_client import ZabbixAPI, ZabbixAPIError
from .gitlab_client import GitLabClient, GitLabAPIError

__all__ = [
    "SyncConfig",
    "SyncStats",
    "TemplateSynchronizer",
    "ZabbixAPI",
    "ZabbixAPIError",
    "GitLabClient",
    "GitLabAPIError",
]
__version__ = "1.1.0"
