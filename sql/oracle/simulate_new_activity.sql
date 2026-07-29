-- Ad-hoc script (not an init script) to simulate source activity after the
-- last sync, for exercising the incremental DAG: one brand-new order, and
-- one status change on an existing order, both with a fresh updated_at.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = APP_USER;

INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at)
VALUES (1021, 509, 'COMPLETED', DATE '2026-01-27', 84.75, 'IT', SYSTIMESTAMP);

UPDATE orders
SET order_status = 'COMPLETED', updated_at = SYSTIMESTAMP
WHERE order_id = 1005;

COMMIT;
