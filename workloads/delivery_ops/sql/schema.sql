CREATE TABLE IF NOT EXISTS hubs (
    hub_id text PRIMARY KEY,
    hub_code text NOT NULL UNIQUE,
    hub_name text NOT NULL,
    province_raw text NOT NULL,
    timezone_name text NOT NULL,
    capacity_shipments_per_day integer NOT NULL CHECK (capacity_shipments_per_day > 0),
    opened_at timestamptz NOT NULL,
    closed_at timestamptz
);

CREATE TABLE IF NOT EXISTS service_levels (
    service_level_code text PRIMARY KEY,
    service_name text NOT NULL,
    promised_hours integer NOT NULL CHECK (promised_hours > 0),
    base_fee_vnd numeric(18, 2) NOT NULL CHECK (base_fee_vnd >= 0),
    active boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id text PRIMARY KEY,
    merchant_name text NOT NULL,
    tax_code text,
    contact_email text,
    phone_raw text,
    home_hub_id text REFERENCES hubs(hub_id),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS merchant_contracts (
    contract_id text PRIMARY KEY,
    merchant_id text NOT NULL REFERENCES merchants(merchant_id),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    merchant_tier text NOT NULL,
    discount_rate numeric(7, 4) NOT NULL,
    credit_limit_vnd numeric(18, 2) NOT NULL,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id text PRIMARY KEY,
    full_name text NOT NULL,
    phone_raw text,
    email_raw text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS addresses (
    address_id text PRIMARY KEY,
    customer_id text NOT NULL REFERENCES customers(customer_id),
    line1_raw text NOT NULL,
    ward_raw text,
    district_raw text,
    province_raw text NOT NULL,
    postal_code_raw text,
    latitude double precision,
    longitude double precision,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS couriers (
    courier_id text PRIMARY KEY,
    courier_name text NOT NULL,
    phone_raw text NOT NULL,
    hired_at timestamptz NOT NULL,
    employment_status text NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS courier_assignments (
    assignment_id text PRIMARY KEY,
    courier_id text NOT NULL REFERENCES couriers(courier_id),
    hub_id text NOT NULL REFERENCES hubs(hub_id),
    vehicle_type text NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    CHECK (valid_to IS NULL OR valid_to > valid_from)
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id text PRIMARY KEY,
    tracking_number text NOT NULL UNIQUE,
    merchant_id text NOT NULL REFERENCES merchants(merchant_id),
    customer_id text NOT NULL REFERENCES customers(customer_id),
    address_id text NOT NULL REFERENCES addresses(address_id),
    origin_hub_id text NOT NULL REFERENCES hubs(hub_id),
    destination_hub_id text NOT NULL REFERENCES hubs(hub_id),
    service_level_code text NOT NULL REFERENCES service_levels(service_level_code),
    current_status text NOT NULL,
    weight_value numeric(14, 3) NOT NULL,
    weight_unit text NOT NULL,
    declared_value_text text,
    declared_currency text,
    created_at timestamptz NOT NULL,
    promised_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    cancelled_at timestamptz,
    deleted_at timestamptz
);

CREATE TABLE IF NOT EXISTS shipment_items (
    shipment_item_id text PRIMARY KEY,
    shipment_id text NOT NULL REFERENCES shipments(shipment_id),
    sku text NOT NULL,
    description text,
    quantity integer NOT NULL CHECK (quantity > 0),
    item_value_text text,
    currency text,
    fragile boolean
);

-- Partner scan events intentionally have no shipment foreign key and no unique
-- partner_event_id. Real integrations can arrive before the shipment or retry
-- the same logical event with a corrected payload.
CREATE TABLE IF NOT EXISTS shipment_events (
    event_row_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    partner_event_id text NOT NULL,
    shipment_id text NOT NULL,
    event_type text NOT NULL,
    event_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    source_version integer NOT NULL,
    hub_id text,
    courier_id text,
    reason_code_raw text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id text PRIMARY KEY,
    shipment_id text NOT NULL REFERENCES shipments(shipment_id),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    courier_id text,
    attempted_at timestamptz NOT NULL,
    outcome text NOT NULL,
    reason_code_raw text,
    recipient_name_raw text,
    proof_url text,
    recorded_at timestamptz NOT NULL,
    UNIQUE (shipment_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS routes (
    route_id text PRIMARY KEY,
    route_date date NOT NULL,
    hub_id text NOT NULL REFERENCES hubs(hub_id),
    courier_id text NOT NULL REFERENCES couriers(courier_id),
    planned_start_at timestamptz NOT NULL,
    actual_start_at timestamptz,
    actual_end_at timestamptz,
    route_status text NOT NULL
);

CREATE TABLE IF NOT EXISTS route_stops (
    route_stop_id text PRIMARY KEY,
    route_id text NOT NULL REFERENCES routes(route_id),
    shipment_id text NOT NULL REFERENCES shipments(shipment_id),
    planned_sequence integer NOT NULL,
    actual_sequence integer,
    planned_arrival_at timestamptz,
    actual_arrival_at timestamptz,
    stop_outcome text,
    UNIQUE (route_id, planned_sequence)
);

CREATE TABLE IF NOT EXISTS charges (
    charge_id text PRIMARY KEY,
    shipment_id text NOT NULL REFERENCES shipments(shipment_id),
    charge_type text NOT NULL,
    amount_text text NOT NULL,
    currency text NOT NULL,
    charge_status text NOT NULL,
    charged_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id text PRIMARY KEY,
    charge_id text NOT NULL REFERENCES charges(charge_id),
    shipment_id text NOT NULL REFERENCES shipments(shipment_id),
    amount_text text NOT NULL,
    currency text NOT NULL,
    reason_code_raw text,
    refunded_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS shipment_events_shipment_recorded_idx
    ON shipment_events (shipment_id, recorded_at DESC, source_version DESC);
CREATE INDEX IF NOT EXISTS shipment_events_recorded_idx
    ON shipment_events (recorded_at);
CREATE INDEX IF NOT EXISTS shipments_created_idx ON shipments (created_at);
CREATE INDEX IF NOT EXISTS shipments_status_idx ON shipments (current_status);
CREATE INDEX IF NOT EXISTS delivery_attempts_time_idx ON delivery_attempts (attempted_at);
CREATE INDEX IF NOT EXISTS route_stops_shipment_idx ON route_stops (shipment_id);
CREATE INDEX IF NOT EXISTS charges_time_idx ON charges (charged_at);

COMMENT ON TABLE shipment_events IS
    'Append-oriented partner event inbox; duplicates and orphans are valid inputs.';
COMMENT ON COLUMN shipment_events.event_at IS
    'Business occurrence time supplied by the partner.';
COMMENT ON COLUMN shipment_events.recorded_at IS
    'Time DeliveryOps persisted the event; used for incremental watermarks.';
