{{ config(materialized='view') }}

WITH base AS (
    SELECT
    CAST(customer_id AS NUMBER) AS customer_id,
    UPPER(TRIM(first_name)) AS first_name,
    UPPER(TRIM(last_name)) AS last_name,
    LOWER(TRIM(email)) AS customer_email,
    CAST(signup_date AS DATE) AS customer_signup_date,
    UPPER(TRIM(country)) AS country,
    UPPER(TRIM(segment)) AS customer_segment,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS rn

    FROM {{ source('raw', 'raw_customers') }}
)

SELECT 
customer_id,
first_name,
last_name,
customer_email,
COALESCE(customer_signup_date,'1900-01-01') AS customer_signup_date,
country,
customer_segment

FROM base 
WHERE rn = 1
