{{ config(materialized='ephemeral') }}

WITH order_item AS (
    SELECT 
    order_id,
    CAST(COALESCE(SUM(gross_order_amt),0) AS NUMBER(10,0)) AS gross_order_amt
    FROM {{ref('stg_mercurymart_order_items')}}
    GROUP BY order_id
),

orders AS (
    SELECT 
    order_id,
    customer_id,
    order_date,
    order_status,
    discount_type,
    discount_value
    FROM {{ref('stg_mercurymart_orders')}}
),

order_agg AS (
    SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    oi.gross_order_amt,
    CAST(CASE WHEN o.discount_type = 'PERCENT' THEN (o.discount_value * oi.gross_order_amt / 100)
    WHEN o.discount_type = 'FIXED' THEN o.discount_value ELSE 0 END AS NUMBER(10,0)) AS discount_amt
    FROM orders o
    LEFT JOIN order_item oi ON o.order_id = oi.order_id
)

SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.gross_order_amt,
    o.discount_amt,
    CAST(o.gross_order_amt - o.discount_amt AS NUMBER(10,0)) AS net_order_amt
FROM order_agg o