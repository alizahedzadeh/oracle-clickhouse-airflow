from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from pipeline.clickhouse_client import insert_orders, max_updated_at
from pipeline.config import clickhouse_config, oracle_config
from pipeline.oracle_client import fetch_orders


@dag(
    dag_id="oracle_to_clickhouse_incremental",
    description="Pulls only Oracle ORDERS rows updated since ClickHouse's "
    "current max(updated_at) and appends them. Cheap to run often; relies "
    "on ReplacingMergeTree(updated_at) to resolve any row re-sent because "
    "its updated_at ties with the watermark.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["oracle", "clickhouse", "exercise"],
)
def oracle_to_clickhouse_incremental():
    @task
    def get_watermark() -> str | None:
        watermark = max_updated_at(clickhouse_config())
        print(f"Current ClickHouse watermark: {watermark}")
        return watermark.isoformat() if watermark else None

    @task
    def extract_from_oracle(watermark: str | None) -> list[tuple]:
        since = datetime.fromisoformat(watermark) if watermark else None
        rows = fetch_orders(oracle_config(), since=since)
        print(f"Fetched {len(rows)} rows from Oracle ORDERS since {since}")
        return rows

    @task
    def load_into_clickhouse(rows: list[tuple]) -> None:
        insert_orders(clickhouse_config(), rows)
        print(f"Inserted {len(rows)} rows into ClickHouse analytics.orders")

    load_into_clickhouse(extract_from_oracle(get_watermark()))


oracle_to_clickhouse_incremental()
