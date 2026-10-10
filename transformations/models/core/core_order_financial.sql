with item_totals as (
  select order_id, sum(quantity * unit_price) gross_amount
  from {{ ref('stg_order_items') }}
  group by order_id
),
payment_totals as (
  select order_id, sumIf(amount, status = 'succeeded') paid_amount
  from {{ ref('stg_payments') }}
  group by order_id
),
refund_totals as (
  select p.order_id, sumIf(r.amount, r.status = 'succeeded') refunded_amount
  from {{ ref('stg_refunds') }} r
  inner join {{ ref('stg_payments') }} p using (payment_id)
  group by p.order_id
)
select
  o.order_id as order_id,
  o.customer_id,
  o.status order_status,
  o.currency,
  o.ordered_at,
  toDate(o.ordered_at) order_date,
  coalesce(i.gross_amount, toDecimal64(0, 2)) gross_amount,
  coalesce(p.paid_amount, toDecimal64(0, 2)) paid_amount,
  coalesce(r.refunded_amount, toDecimal64(0, 2)) refunded_amount,
  coalesce(p.paid_amount, toDecimal64(0, 2)) - coalesce(r.refunded_amount, toDecimal64(0, 2)) net_amount
from {{ ref('stg_orders') }} o
left join item_totals i using (order_id)
left join payment_totals p using (order_id)
left join refund_totals r using (order_id)
