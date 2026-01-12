{{ config(materialized='table') }}

with orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        sum(gross_order_amt) as lifetime_revenue,
        count(distinct order_id) as lifetime_orders
    from {{ ref('ephemeral_order_agg') }}
    group by customer_id
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.customer_email,
    o.first_order_date,
    o.lifetime_orders,
    o.lifetime_revenue,

    case
        when o.lifetime_revenue >= 100000 then 'PLATINUM'
        when o.lifetime_revenue >= 50000 then 'GOLD'
        when o.lifetime_revenue >= 20000 then 'SILVER'
        else 'BRONZE'
    end as loyalty_tier

from {{ ref('stg_mercurymart_customers') }} c
left join orders o
    on c.customer_id = o.customer_id
