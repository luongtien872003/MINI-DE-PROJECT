-- stg_orders.sql
-- Staging model for orders data
-- This SQL represents the transformation logic applied in the Python pipeline

-- Step 1: Clean and standardize raw orders
WITH cleaned_orders AS (
    SELECT
        TRIM(order_id) AS order_id,
        TRIM(customer_id) AS customer_id,
        CAST(order_date AS DATE) AS order_date,
        LOWER(TRIM(status)) AS status,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM raw_orders
    WHERE order_id IS NOT NULL
      AND TRIM(order_id) != ''
),

-- Step 2: Deduplicate by order_id, keeping latest ingested_at
deduplicated_orders AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id 
                   ORDER BY ingested_at DESC NULLS LAST
               ) AS rn
        FROM cleaned_orders
    ) ranked
    WHERE rn = 1
)

-- Final staged orders
SELECT
    order_id,
    customer_id,
    order_date,
    status,
    ingested_at
FROM deduplicated_orders;
