from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from pipeline.clickhouse_client import insert_orders
from pipeline.config import clickhouse_config, oracle_config
from pipeline.oracle_client import fetch_orders


@dag(
    dag_id="oracle_to_clickhouse_full_refresh",
    description="Pulls every row from Oracle ORDERS and loads it into "
    "ClickHouse analytics.orders. Simple baseline load: safe to re-run, "
    "but re-reads the whole source table every time.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["oracle", "clickhouse", "exercise"],
)
def oracle_to_clickhouse_full_refresh():
    @task
    def extract_from_oracle() -> list[tuple]:
        rows = fetch_orders(oracle_config())
        print(f"Fetched {len(rows)} rows from Oracle ORDERS")
        return rows

    @task
    def load_into_clickhouse(rows: list[tuple]) -> None:
        insert_orders(clickhouse_config(), rows)
        print(f"Inserted {len(rows)} rows into ClickHouse analytics.orders")

    load_into_clickhouse(extract_from_oracle())


oracle_to_clickhouse_full_refresh()
