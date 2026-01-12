{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    p.product_brand,
    p.product_category,
    s.supplier_name,
    p.unit_price,

    case
        when p.unit_price < 100 then 'LOW'
        when p.unit_price < 500 then 'MEDIUM'
        else 'HIGH'
    end as price_bucket

from {{ ref('stg_mercurymart_products') }} p
left join {{source('raw','raw_suppliers') }} s
    on p.supplier_id = s.supplier_id
