{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy = 'merge',
    on_schema_change = 'fail'
) }}

with payments as (
    select *
    from {{ ref('ephemeral_payment_refund_agg') }}
)

select
    o.order_id,
    o.order_date,
    o.customer_id,
    o.gross_order_amt                   as gross_sales,
    o.discount_amt                      as discount_amt,
    o.net_order_amt                     as net_sales,
    p.net_paid_amount                   as net_revenue,
    p.total_payments,
    p.total_refunds,

    case
        when p.net_paid_amount >= o.net_order_amt then 'PAID'
        when p.net_paid_amount > 0 then 'PARTIAL'
        else 'UNPAID'
    end as payment_status

from {{ ref('ephemeral_order_agg') }} o
left join payments p
    on o.order_id = p.order_id

{% if is_incremental() %}
where o.order_date >= (
    select COALESCE(max(order_date),'1900-01-01') from {{ this }}
)
{% endif %}
