CREATE TABLE IF NOT EXISTS raw.customers (
  customer_id String, email String, country Nullable(String), created_at DateTime64(3, 'UTC'),
  updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (customer_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.orders (
  order_id String, customer_id String, status LowCardinality(String), currency LowCardinality(String),
  ordered_at DateTime64(3, 'UTC'), updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(ordered_at) ORDER BY (order_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.order_items (
  order_item_id String, order_id String, product_id String, quantity Int32, unit_price Decimal(18,2),
  updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (order_item_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.payments (
  payment_id String, order_id String, provider_transaction_id String, status LowCardinality(String),
  amount Decimal(18,2), succeeded_at Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (payment_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.refunds (
  refund_id String, payment_id String, status LowCardinality(String), amount Decimal(18,2),
  succeeded_at Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (refund_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.events (
  event_id String, event_type LowCardinality(String), session_id String, customer_id Nullable(String),
  product_id Nullable(String), campaign_id Nullable(String), occurred_at DateTime64(3, 'UTC'),
  updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(occurred_at) ORDER BY (event_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.marketing_campaigns (
  campaign_id String, name String, channel LowCardinality(String), starts_at DateTime64(3, 'UTC'),
  ends_at Nullable(DateTime64(3, 'UTC')), updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (campaign_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.campaign_spend (
  spend_id String, campaign_id String, spend_date Date, amount Decimal(18,2),
  updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(spend_date) ORDER BY (spend_id, updated_at, batch_id);

CREATE TABLE IF NOT EXISTS raw.product_catalog (
  product_id String, product_name String, category String, unit_cost Nullable(Decimal(18,2)),
  updated_at DateTime64(3, 'UTC'), is_deleted Bool,
  batch_id UUID, source_updated_at DateTime64(3, 'UTC'), ingested_at DateTime64(3, 'UTC'), record_hash String
) ENGINE = MergeTree PARTITION BY toYYYYMM(updated_at) ORDER BY (product_id, updated_at, batch_id);
