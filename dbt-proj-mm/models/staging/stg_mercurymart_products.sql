{{ config(materialized='view') }}

WITH base AS (
    SELECT
    CAST(product_id AS NUMBER) AS product_id,
    UPPER(TRIM(product_name)) AS product_name,
    UPPER(TRIM(category)) AS product_category,
    UPPER(TRIM(brand)) AS product_brand,
    CAST(unit_price AS NUMBER(10,2)) AS unit_price,
    CAST(supplier_id AS NUMBER) AS supplier_id,
    FROM {{ source('raw', 'raw_products') }}
)

SELECT
    product_id,
    product_name,
    product_category,
    product_brand,
    COALESCE(unit_price,0) AS unit_price,
    supplier_id
FROM  base
