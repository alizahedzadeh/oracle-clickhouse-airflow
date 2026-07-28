from datetime import datetime
from typing import Any

import oracledb

from pipeline.config import OracleConfig

ORDERS_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_date",
    "amount",
    "country",
    "updated_at",
]


def fetch_orders(config: OracleConfig, since: datetime | None = None) -> list[tuple[Any, ...]]:
    """Fetch rows from ORDERS, optionally only those updated since `since`."""
    query = f"SELECT {', '.join(ORDERS_COLUMNS)} FROM orders"
    params: dict[str, Any] = {}
    if since is not None:
        query += " WHERE updated_at > :since"
        params["since"] = since
    query += " ORDER BY updated_at"

    with oracledb.connect(user=config.user, password=config.password, dsn=config.dsn) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
