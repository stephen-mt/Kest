with event_metrics as (
  select
    toDate(occurred_at) event_date,
    campaign_id,
    uniqExactIf(session_id, event_type = 'product_viewed') viewing_sessions,
    uniqExactIf(session_id, event_type = 'add_to_cart') cart_sessions,
    uniqExactIf(session_id, event_type = 'checkout_completed') checkout_sessions,
    uniqExactIf(customer_id, event_type = 'product_viewed' and customer_id is not null) viewing_users
  from {{ ref('stg_events') }}
  where campaign_id is not null
  group by event_date, campaign_id
),
spend as (
  select spend_date event_date, campaign_id, sum(amount) campaign_spend
  from {{ ref('stg_campaign_spend') }}
  group by event_date, campaign_id
),
keys as (
  select event_date, campaign_id from event_metrics
  union distinct
  select event_date, campaign_id from spend
)
select
  k.event_date as event_date,
  k.campaign_id as campaign_id,
  c.name campaign_name,
  c.channel,
  coalesce(e.viewing_sessions, 0) viewing_sessions,
  coalesce(e.cart_sessions, 0) cart_sessions,
  coalesce(e.checkout_sessions, 0) checkout_sessions,
  coalesce(e.viewing_users, 0) viewing_users,
  coalesce(s.campaign_spend, toDecimal64(0, 2)) campaign_spend,
  if(e.viewing_sessions = 0, 0., e.checkout_sessions / e.viewing_sessions) session_conversion_rate,
  now64(3) published_at
from keys k
left join event_metrics e using (event_date, campaign_id)
left join spend s using (event_date, campaign_id)
left join {{ ref('stg_marketing_campaigns') }} c using (campaign_id)
