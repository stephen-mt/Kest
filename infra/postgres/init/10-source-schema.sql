\connect business_source

CREATE TABLE IF NOT EXISTS customers (
    customer_id text PRIMARY KEY,
    email text NOT NULL,
    country text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS orders (
    order_id text PRIMARY KEY,
    customer_id text NOT NULL REFERENCES customers(customer_id),
    status text NOT NULL,
    currency text NOT NULL DEFAULT 'USD',
    ordered_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS order_items (
    order_item_id text PRIMARY KEY,
    order_id text NOT NULL REFERENCES orders(order_id),
    product_id text NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(18,2) NOT NULL CHECK (unit_price >= 0),
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id text PRIMARY KEY,
    order_id text NOT NULL REFERENCES orders(order_id),
    provider_transaction_id text NOT NULL,
    status text NOT NULL,
    amount numeric(18,2) NOT NULL CHECK (amount >= 0),
    succeeded_at timestamptz,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);
CREATE UNIQUE INDEX IF NOT EXISTS payments_provider_transaction_uq
    ON payments(provider_transaction_id);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id text PRIMARY KEY,
    payment_id text NOT NULL REFERENCES payments(payment_id),
    status text NOT NULL,
    amount numeric(18,2) NOT NULL CHECK (amount >= 0),
    succeeded_at timestamptz,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS events (
    event_id text PRIMARY KEY,
    event_type text NOT NULL CHECK (event_type IN ('product_viewed', 'add_to_cart', 'checkout_completed')),
    session_id text NOT NULL,
    customer_id text,
    product_id text,
    campaign_id text,
    occurred_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS marketing_campaigns (
    campaign_id text PRIMARY KEY,
    name text NOT NULL,
    channel text NOT NULL,
    starts_at timestamptz NOT NULL,
    ends_at timestamptz,
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS campaign_spend (
    spend_id text PRIMARY KEY,
    campaign_id text NOT NULL REFERENCES marketing_campaigns(campaign_id),
    spend_date date NOT NULL,
    amount numeric(18,2) NOT NULL CHECK (amount >= 0),
    updated_at timestamptz NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false
);

ALTER TABLE customers OWNER TO business_source;
ALTER TABLE orders OWNER TO business_source;
ALTER TABLE order_items OWNER TO business_source;
ALTER TABLE payments OWNER TO business_source;
ALTER TABLE refunds OWNER TO business_source;
ALTER TABLE events OWNER TO business_source;
ALTER TABLE marketing_campaigns OWNER TO business_source;
ALTER TABLE campaign_spend OWNER TO business_source;
