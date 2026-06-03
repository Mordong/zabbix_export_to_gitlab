"""
Airflow DAG: disaster-recovery экспорт группы 'infra' конфигурации Zabbix
в GitLab (раз в сутки в 19:00, без quiet period).

Регистрирует zabbix_infra_to_gitlab_test и zabbix_infra_to_gitlab_prod.
Логика — в zabbix_template_sync.dag_factory; здесь тонкая обёртка.
Переиспользует те же Connections (zabbix_<env>, gitlab_<env>) и Variable
<env>_gitlab_project_id, что и остальные DAG проекта.
"""

from __future__ import annotations

from zabbix_template_sync.dag_factory import build_config_dags

for _dag_id, _dag in build_config_dags("infra", schedule="0 19 * * *").items():
    globals()[_dag_id] = _dag
