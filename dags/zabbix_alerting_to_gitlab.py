"""
Airflow DAG: disaster-recovery экспорт группы 'alerting' конфигурации Zabbix
в GitLab (раз в сутки в 21:00, без quiet period).

Регистрирует zabbix_alerting_to_gitlab_test и zabbix_alerting_to_gitlab_prod.
Вся логика — в zabbix_template_sync.dag_factory (общая фабрика), здесь только
тонкая обёртка. Переиспользует те же Connections (zabbix_<env>, gitlab_<env>)
и Variable <env>_gitlab_project_id, что и остальные DAG проекта.
"""

from __future__ import annotations

from zabbix_template_sync.dag_factory import build_config_dags

for _dag_id, _dag in build_config_dags("alerting").items():
    globals()[_dag_id] = _dag
