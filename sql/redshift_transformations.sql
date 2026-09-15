-- Example Redshift analytics transformation layer.
-- These statements represent the type of SELECT/CTAS transformations
-- Airflow can run after loading the current S3 batch into Redshift.

CREATE TABLE IF NOT EXISTS analytics_product_performance AS
SELECT
    op.product_id,
    p.product_name,
    p.aisle_id,
    p.department_id,
    COUNT(*) AS total_purchases,
    SUM(op.reordered) AS total_reorders,
    SUM(op.reordered)::DECIMAL(18,4) / NULLIF(COUNT(*), 0) AS reorder_rate
FROM raw_order_products op
JOIN raw_products p
  ON op.product_id = p.product_id
GROUP BY
    op.product_id,
    p.product_name,
    p.aisle_id,
    p.department_id;

CREATE TABLE IF NOT EXISTS analytics_order_patterns AS
SELECT
    order_dow,
    order_hour_of_day,
    COUNT(*) AS total_orders,
    AVG(days_since_prior_order) AS avg_days_since_prior_order
FROM raw_orders
GROUP BY
    order_dow,
    order_hour_of_day;
