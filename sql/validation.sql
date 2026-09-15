-- Basic data-quality checks for the Instacart source tables.

-- Duplicate order IDs
SELECT order_id, COUNT(*)
FROM orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Missing order IDs
SELECT COUNT(*) AS null_order_ids
FROM orders
WHERE order_id IS NULL;

-- Product references that do not exist in products
SELECT COUNT(*) AS invalid_product_refs
FROM order_products__prior op
LEFT JOIN products p
  ON op.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Order references that do not exist in orders
SELECT COUNT(*) AS invalid_order_refs
FROM order_products__prior op
LEFT JOIN orders o
  ON op.order_id = o.order_id
WHERE o.order_id IS NULL;
