"""Run dbt build after ingestion completes.

Triggered manually or on a schedule offset from ingestion DAGs.
Runs against the prod target using the airflow-runner SA — see
airflow/config/dbt_profiles.yml for the connection config.
"""

from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    dag_id="dbt_build",
    schedule=None,
    start_date=datetime(2018, 1, 1),
    max_active_runs=1,
    catchup=False,
    tags=["dbt", "transformation"],
    default_args={"retries": 1},
)
def dbt_build():

    deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt/flights && dbt deps",
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/airflow/dbt/flights && dbt build --target prod 2>&1",
    )

    deps >> build


dbt_build()
