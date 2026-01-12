{{ config(materialized='view') }}

WITH base AS (

    SELECT
    CAST(refund_id AS NUMBER) AS refund_id,
    CAST(payment_id AS NUMBER) AS payment_id,
    CAST(order_id AS NUMBER) AS order_id,
    CAST(refund_date AS DATE) AS refund_date,
    CAST(amount AS NUMBER(10,2)) AS refund_amt,
    UPPER(TRIM(reason)) AS refund_reason,
    ROW_NUMBER() OVER (PARTITION BY refund_id ORDER BY refund_date DESC) AS rn
 
FROM {{ source('raw', 'raw_refunds') }}

)

SELECT
    refund_id,
    payment_id,
    order_id,
    COALESCE(refund_date,'1900-01-01') AS refund_date,
    COALESCE(refund_amt,0) AS refund_amt,
    refund_reason

FROM base 
WHERE rn = 1