-- Querying with FINAL directly is expensive at scale (forces a merge at
-- query time); wrapping it in a view keeps that concern out of application
-- SQL and gives one place to change the dedup strategy later.
CREATE VIEW IF NOT EXISTS analytics.orders_latest AS
SELECT *
FROM analytics.orders FINAL;
