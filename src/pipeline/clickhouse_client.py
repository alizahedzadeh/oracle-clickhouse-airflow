from typing import Any

import clickhouse_connect

from pipeline.config import ClickHouseConfig
from pipeline.oracle_client import ORDERS_COLUMNS


def insert_orders(config: ClickHouseConfig, rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        return

    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.http_port,
        username=config.user,
        password=config.password,
        database=config.database,
    )
    client.insert("orders", rows, column_names=ORDERS_COLUMNS)


def max_updated_at(config: ClickHouseConfig):
    client = clickhouse_connect.get_client(
        host=config.host,
        port=config.http_port,
        username=config.user,
        password=config.password,
        database=config.database,
    )
    result = client.query("SELECT max(updated_at) FROM orders")
    return result.result_rows[0][0]
