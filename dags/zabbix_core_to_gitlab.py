"""
Airflow DAG: disaster-recovery экспорт группы 'core' конфигурации Zabbix
в GitLab (раз в сутки в 21:00, без quiet period).

ИНКРЕМЕНТАЛЬНЫЙ режим: экспортируются только хосты, изменённые за скользящее
окно (<env>_host_incremental_window_hours, дефолт 24ч) по данным auditlog.
Полный снимок и чистку осиротевших делает отдельный ручной DAG
zabbix_core_full_to_gitlab_<env>.

Регистрирует zabbix_core_to_gitlab_test и zabbix_core_to_gitlab_prod.
Переиспользует те же Connections (zabbix_<env>, gitlab_<env>) и Variable
<env>_gitlab_project_id, что и остальные DAG проекта.
"""

from __future__ import annotations

from zabbix_template_sync.dag_factory import build_config_dags

for _dag_id, _dag in build_config_dags("core", mode="incremental").items():
    globals()[_dag_id] = _dag
