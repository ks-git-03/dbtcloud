{{ config(materialized='ephemeral') }}

with payments as (
    select
        cast(order_id as number) as order_id,
        sum(cast(payment_amt as number(10,2))) as total_payments
    from {{ ref('stg_mercurymart_payments') }}
    where payment_status = 'SUCCESS'
    group by order_id
),

refunds as (
    select
        cast(order_id as number) as order_id,
        sum(cast(refund_amt as number(10,2))) as total_refunds
    from {{ ref('stg_mercurymart_refunds') }}
    group by order_id
)

select
    coalesce(p.order_id, r.order_id) as order_id,
    coalesce(p.total_payments, 0)    as total_payments,
    coalesce(r.total_refunds, 0)     as total_refunds,
    coalesce(p.total_payments, 0)
      - coalesce(r.total_refunds, 0) as net_paid_amount
from payments p
full outer join refunds r
    on p.order_id = r.order_id
