select *
from {{ ref('fact_orders_daily') }}
where order_date > current_date
