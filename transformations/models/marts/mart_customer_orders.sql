select
  f.order_id,
  f.order_date,
  f.ordered_at,
  f.customer_id,
  c.email customer_email,
  c.country,
  f.order_status,
  f.currency,
  f.gross_amount,
  f.paid_amount,
  f.refunded_amount,
  f.net_amount,
  now64(3) published_at
from {{ ref('core_order_financial') }} f
inner join {{ ref('stg_customers') }} c using (customer_id)
