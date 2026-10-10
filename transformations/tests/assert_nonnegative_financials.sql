select *
from {{ ref('mart_customer_orders') }}
where gross_amount < 0 or paid_amount < 0 or refunded_amount < 0 or refunded_amount > paid_amount
