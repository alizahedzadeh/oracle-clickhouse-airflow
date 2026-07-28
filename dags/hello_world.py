from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="hello_world",
    description="Sanity-check DAG: confirms the scheduler picks up dags/, "
    "tasks run, and XCom passes data between them.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["exercise"],
)
def hello_world():
    @task
    def say_hello() -> str:
        return "hello from the Airflow scheduler"

    @task
    def show_message(message: str) -> None:
        print(f"Received via XCom: {message}")

    show_message(say_hello())


hello_world()
