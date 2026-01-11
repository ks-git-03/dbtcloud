{{ config(materialized='view') }}

WITH orders AS (
    SELECT
    CAST(order_id AS NUMBER) AS order_id,
    CAST(customer_id AS NUMBER) AS customer_id,
    CAST(order_date AS DATE) AS order_date,
    UPPER(TRIM(status)) AS order_status,
    UPPER(TRIM(coupon_code)) AS coupon_code,
    CAST(campaign_id AS NUMBER) AS campaign_id,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
    FROM {{ source('raw', 'raw_orders') }}
),

coupons AS (
    SELECT 
    UPPER(TRIM(coupon_code)) AS coupon_code,
    UPPER(TRIM(discount_type)) AS discount_type,
    CAST(discount_value AS NUMBER(10,2)) AS discount_value,
    COALESCE(CAST(start_date AS DATE),'1900-01-01') AS start_date,
    COALESCE(CAST(end_date AS DATE),'2099-12-31') AS end_date,
    CAST(campaign_id AS NUMBER) AS campaign_id

    FROM {{source('raw','raw_coupons')}}
)

SELECT
    o.order_id,
    o.customer_id,
    COALESCE(o.order_date, '1900-01-01') AS order_date,
    o.order_status,
    o.coupon_code,
    c.discount_type,
    COALESCE(c.discount_value,0) AS discount_value,
    o.campaign_id

FROM orders o
LEFT JOIN coupons AS c ON o.coupon_code = c.coupon_code
WHERE o.rn = 1
