-- mart_daily_revenue.sql
-- Data mart model for daily revenue aggregation
-- This SQL represents the business logic for revenue computation

-- Step 1: Get valid orders (after DQ validation)
WITH valid_orders AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        ingested_at
    FROM stg_orders
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND order_date IS NOT NULL
      AND status IS NOT NULL
),

-- Step 2: Get valid order items (after DQ validation)
valid_items AS (
    SELECT
        item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        ingested_at
    FROM stg_order_items
    WHERE quantity IS NOT NULL
      AND unit_price IS NOT NULL
      AND unit_price > 0
      -- Orphan check: order_id must exist in valid_orders
      AND order_id IN (SELECT order_id FROM valid_orders)
),

-- Step 3: Filter for completed orders only
completed_orders AS (
    SELECT *
    FROM valid_orders
    WHERE status = 'completed'
),

-- Step 4: Calculate item-level revenue
item_revenue AS (
    SELECT
        i.order_id,
        o.order_date,
        i.quantity * i.unit_price AS amount
    FROM valid_items i
    INNER JOIN completed_orders o ON i.order_id = o.order_id
),

-- Step 5: Aggregate by order
order_revenue AS (
    SELECT
        order_date,
        order_id,
        SUM(amount) AS order_total
    FROM item_revenue
    GROUP BY order_date, order_id
)

-- Final daily revenue aggregation
SELECT
    order_date,
    SUM(order_total) AS total_revenue,
    COUNT(DISTINCT order_id) AS orders_count
FROM order_revenue
GROUP BY order_date
ORDER BY order_date;
