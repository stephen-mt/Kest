CREATE SCHEMA IF NOT EXISTS delivery_lakehouse.bronze_delivery;

CREATE TABLE IF NOT EXISTS delivery_lakehouse.bronze_delivery.cdc_events (
    event_id VARCHAR,
    source_name VARCHAR,
    source_schema VARCHAR,
    source_table VARCHAR,
    operation VARCHAR,
    source_lsn VARCHAR,
    source_ts_ms VARCHAR,
    captured_at VARCHAR,
    before_json VARCHAR,
    after_json VARCHAR,
    transaction_json VARCHAR
);
