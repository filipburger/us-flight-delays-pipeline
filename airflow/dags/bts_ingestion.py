from datetime import datetime, timedelta
import logging

from airflow.decorators import dag, task

log = logging.getLogger(__name__)

@dag(
    dag_id="bts_ingestion",
    schedule="@monthly",
    start_date=datetime(2024,1, 1),
    end_date=datetime(2024, 3, 1),
    catchup=True,
    max_active_runs=1,
    tags=["bts"],
    default_args={"retries":1},
)

def bts_ingestion():

    @task
    def show_target_month(logical_date=None):
        """Prove we understand which month each run is responsible for."""
        year, month = logical_date.year, logical_date.month
        log.info("This DAG run is resposible for %s-%02d", year, month)
        return f"{year}-{month:02d}"

    show_target_month()


bts_ingestion()

