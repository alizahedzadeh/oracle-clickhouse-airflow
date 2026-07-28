-- gvenzl/oracle-free runs *.sql in this directory via a local "/ as sysdba"
-- bequeath connection, which lands in the root CDB, not the FREEPDB1
-- pluggable database where APP_USER lives -- so we switch container first,
-- then switch schema to create objects owned by the app user the DAGs
-- connect as, rather than under SYS.
ALTER SESSION SET CONTAINER = FREEPDB1;
ALTER SESSION SET CURRENT_SCHEMA = APP_USER;

CREATE TABLE orders
(
    order_id     NUMBER PRIMARY KEY,
    customer_id  NUMBER NOT NULL,
    order_status VARCHAR2(20) NOT NULL,
    order_date   DATE NOT NULL,
    amount       NUMBER(18, 2) NOT NULL,
    country      VARCHAR2(10) NOT NULL,
    updated_at   TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
);

INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1001, 501, 'COMPLETED', DATE '2026-01-05', 129.99, 'US', TIMESTAMP '2026-01-05 10:15:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1002, 502, 'COMPLETED', DATE '2026-01-06', 59.50, 'GB', TIMESTAMP '2026-01-06 14:22:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1003, 501, 'CANCELLED', DATE '2026-01-07', 89.00, 'US', TIMESTAMP '2026-01-08 11:00:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1004, 503, 'COMPLETED', DATE '2026-01-10', 249.00, 'DE', TIMESTAMP '2026-01-10 18:40:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1005, 504, 'PENDING', DATE '2026-01-12', 39.99, 'FR', TIMESTAMP '2026-01-12 08:00:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1006, 505, 'COMPLETED', DATE '2026-01-15', 199.95, 'US', TIMESTAMP '2026-01-15 16:30:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1007, 502, 'COMPLETED', DATE '2026-01-18', 74.20, 'GB', TIMESTAMP '2026-01-18 12:10:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1008, 506, 'COMPLETED', DATE '2026-01-20', 310.00, 'IT', TIMESTAMP '2026-01-20 20:05:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1009, 507, 'CANCELLED', DATE '2026-01-22', 45.00, 'US', TIMESTAMP '2026-01-23 09:00:00');
INSERT INTO orders (order_id, customer_id, order_status, order_date, amount, country, updated_at) VALUES (1010, 508, 'COMPLETED', DATE '2026-01-25', 159.49, 'ES', TIMESTAMP '2026-01-25 13:25:00');

COMMIT;
