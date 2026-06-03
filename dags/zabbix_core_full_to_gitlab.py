"""
Airflow DAG: ПОЛНЫЙ (cold) disaster-recovery экспорт группы 'core' Zabbix
в GitLab. Только РУЧНОЙ запуск (schedule=None).

Отличия от инкрементного zabbix_core_to_gitlab_<env>:
  - экспортирует ВСЕ хосты (полный снимок), а не только изменённые за окно;
  - выполняет чистку осиротевших файлов в core/hosts/ (удаляет то, чего больше
    нет в Zabbix, в т.ч. старые имена файлов без hostid).

Запускать: вручную из Airflow UI ("Trigger DAG") периодически (например, раз
в неделю) и после массовых изменений/восстановления.

Регистрирует zabbix_core_full_to_gitlab_test и zabbix_core_full_to_gitlab_prod.
"""

from __future__ import annotations

from zabbix_template_sync.dag_factory import build_config_dags

for _dag_id, _dag in build_config_dags(
        "core",
        schedule=None,
        mode="full",
        dag_id_template="zabbix_core_full_to_gitlab_{env}",
        extra_tags=["full", "manual"]).items():
    globals()[_dag_id] = _dag
