from airflow.sdk import dag, task
from pendulum import datetime

@dag(
    dag_id="hello_portfolio_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["portfolio"],
)
def hello_portfolio_pipeline():
    @task
    def hello() -> None:
        print("Portfolio data pipeline environment is working.")

    hello()


hello_portfolio_pipeline()