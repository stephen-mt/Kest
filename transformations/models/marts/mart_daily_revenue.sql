select
  concat(toString(order_date), ':', currency) grain_key,
  order_date,
  currency,
  countIf(paid_amount > 0) paid_orders,
  sum(gross_amount) gross_order_value,
  sum(paid_amount) captured_revenue,
  sum(refunded_amount) refunded_revenue,
  sum(net_amount) net_revenue,
  now64(3) published_at
from {{ ref('core_order_financial') }}
where paid_amount > 0
group by order_date, currency
