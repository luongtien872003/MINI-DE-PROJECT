-- stg_order_items.sql
-- Staging model for order items data
-- This SQL represents the transformation logic applied in the Python pipeline

-- Clean and standardize raw order items
WITH cleaned_items AS (
    SELECT
        TRIM(item_id) AS item_id,
        TRIM(order_id) AS order_id,
        TRIM(product_id) AS product_id,
        CAST(quantity AS DECIMAL(10,2)) AS quantity,
        CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
        CAST(ingested_at AS TIMESTAMP) AS ingested_at
    FROM raw_order_items
)

-- Final staged order items
SELECT
    item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    ingested_at
FROM cleaned_items;
