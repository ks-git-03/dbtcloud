{{ config(materialized='view') }}

WITH base AS (
    SELECT
    CAST(payment_id AS NUMBER) AS payment_id,
    CAST(order_id AS NUMBER) AS order_id,
    CAST(payment_date AS DATE) AS payment_date,
    UPPER(TRIM(payment_method)) AS payment_method,
    CAST(amount AS NUMBER(10,2)) AS payment_amt,
    UPPER(TRIM(status)) AS payment_status,
    ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY payment_date DESC) AS rn
from {{ source('raw', 'raw_payments') }}
)

SELECT
    payment_id,
    order_id,
    COALESCE(payment_date,'1900-01-01') AS payment_date,
    payment_method,
    COALESCE(payment_amt,0) AS payment_amt,
    payment_status
FROM base
WHERE rn = 1
