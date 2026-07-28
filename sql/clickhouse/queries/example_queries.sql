-- Revenue by month, completed orders only.
SELECT
    toStartOfMonth(order_date) AS month,
    count()                    AS orders,
    sum(amount)                AS revenue
FROM analytics.orders
WHERE order_status = 'COMPLETED'
GROUP BY month
ORDER BY month;

-- Orders by country.
SELECT
    country,
    count()      AS orders,
    sum(amount)  AS revenue
FROM analytics.orders
GROUP BY country
ORDER BY revenue DESC;

-- Status breakdown.
SELECT
    order_status,
    count() AS orders
FROM analytics.orders
GROUP BY order_status;

-- ReplacingMergeTree can hold stale duplicate rows until a background merge
-- runs, so use FINAL (or the `analytics.orders_latest` view below) to force
-- dedup by (order_date, order_id) on the newest `updated_at`.
SELECT *
FROM analytics.orders FINAL
WHERE order_id = 1003;
