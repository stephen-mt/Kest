select *
from {{ ref('mart_conversion_funnel') }}
where checkout_sessions > cart_sessions or cart_sessions > viewing_sessions
