-- Reference DQ checks for interviewer (if using a DB)
SELECT COUNT(*) AS orders_null_violations
FROM stg_orders
WHERE order_id IS NULL
   OR customer_id IS NULL
   OR order_date IS NULL
   OR status IS NULL;

SELECT order_id, COUNT(*) AS cnt
FROM stg_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT COUNT(*) AS invalid_items
FROM stg_order_items
WHERE quantity IS NULL
   OR unit_price IS NULL
   OR unit_price <= 0;

SELECT COUNT(*) AS orphan_items
FROM stg_order_items i
LEFT JOIN stg_orders o ON i.order_id = o.order_id
WHERE o.order_id IS NULL;
