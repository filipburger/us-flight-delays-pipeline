# dags/hello_world.py
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sanity-check"],
)
def hello_world():
    @task
    def say_hello():
        print("Airflow is alive.")
        return "ok"

    say_hello()

hello_world()