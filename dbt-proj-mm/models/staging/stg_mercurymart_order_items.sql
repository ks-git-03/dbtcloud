{{ config(materialized='view') }}

WITH base AS (
   SELECT
    CAST(order_item_id AS NUMBER) AS order_item_id,
    CAST(order_id AS NUMBER) AS order_id,
    CAST(product_id AS NUMBER) AS product_id,
    CAST(quantity AS NUMBER) AS qty_ordered,
    CAST(unit_price AS NUMBER(10,2)) AS unit_price,
    ROW_NUMBER() OVER (PARTITION BY order_item_id ORDER BY (SELECT NULL)) AS rn
    from {{ source('raw', 'raw_order_items') }} 
)


SELECT
    order_item_id,
    order_id,
    product_id,
    COALESCE(qty_ordered,0) AS qty_ordered,
    COALESCE(unit_price,0) AS unit_price,
    COALESCE(unit_price * qty_ordered, 0) AS gross_order_amt

FROM base 
WHERE rn = 1