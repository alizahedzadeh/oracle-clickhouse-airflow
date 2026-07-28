-- Mirrors the ORDERS table we'll later pull from Oracle.
-- ReplacingMergeTree(updated_at): when the incremental DAG re-inserts a row
-- that changed (e.g. status update), ClickHouse keeps the newest version by
-- updated_at during merges/FINAL, giving us idempotent upsert-like behavior
-- without needing ALTER UPDATE.
CREATE TABLE IF NOT EXISTS analytics.orders
(
    order_id     UInt64,
    customer_id  UInt64,
    order_status LowCardinality(String),
    order_date   DateTime,
    amount       Decimal(18, 2),
    country      LowCardinality(String),
    updated_at   DateTime,
    _loaded_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id);
