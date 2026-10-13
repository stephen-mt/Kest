-- Stable empty serving contracts let BI/catalog bootstrap before the first
-- successful data publication. dbt builds candidates independently; publish
-- exchanges each candidate into these tables atomically.
CREATE TABLE IF NOT EXISTS gold.mart_daily_revenue
(
    grain_key String,
    order_date Date,
    currency LowCardinality(String),
    paid_orders UInt64,
    gross_order_value Decimal(38, 2),
    captured_revenue Decimal(38, 2),
    refunded_revenue Decimal(38, 2),
    net_revenue Decimal(38, 2),
    published_at DateTime64(3)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS gold.mart_conversion_funnel
(
    event_date Date,
    campaign_id Nullable(String),
    campaign_name String,
    channel LowCardinality(String),
    viewing_sessions UInt64,
    cart_sessions UInt64,
    checkout_sessions UInt64,
    viewing_users UInt64,
    campaign_spend Decimal(38, 2),
    session_conversion_rate Float64,
    published_at DateTime64(3)
)
ENGINE = MergeTree
ORDER BY tuple();

CREATE TABLE IF NOT EXISTS gold.mart_customer_orders
(
    order_id String,
    order_date Date,
    ordered_at DateTime64(3, 'UTC'),
    customer_id String,
    customer_email String,
    country Nullable(String),
    order_status LowCardinality(String),
    currency LowCardinality(String),
    gross_amount Decimal(38, 2),
    paid_amount Decimal(38, 2),
    refunded_amount Decimal(38, 2),
    net_amount Decimal(38, 2),
    published_at DateTime64(3)
)
ENGINE = MergeTree
ORDER BY tuple();
