BEGIN;

-- Normalize rows created by earlier local workload versions before enforcing v2.
UPDATE "BuyerSessionAnalytics"
SET cart_removals_count = LEAST(cart_removals_count, cart_additions_count),
    checkout_completed = checkout_completed AND checkout_initiated,
    bounce_indicator = (
        bounce_indicator
        AND NOT checkout_initiated
        AND NOT checkout_completed
        AND cart_additions_count = 0
    );

UPDATE "PaymentProcessingEvents"
SET amount_processed = CASE
        WHEN processing_stage = 'settled' THEN amount_requested
        ELSE 0
    END,
    decline_reason = CASE
        WHEN processing_stage = 'failed' THEN COALESCE(decline_reason, 'processor_failure')
        ELSE NULL
    END,
    retry_count = CASE
        WHEN processing_stage = 'failed' THEN GREATEST(retry_count, 1)
        ELSE 0
    END;

DO $$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
              'markets', 'vendors', 'buyers', 'products', 'transactions',
              'transaction_products', 'BuyerSessionAnalytics',
              'PaymentProcessingEvents', 'risk_analytics',
              'RiskModelPredictions'
          )
          AND NOT (
              (table_name = 'PaymentProcessingEvents' AND column_name = 'decline_reason')
              OR (table_name = 'RiskModelPredictions' AND column_name = 'actual_outcome')
          )
    LOOP
        EXECUTE format(
            'ALTER TABLE %I ALTER COLUMN %I SET NOT NULL',
            item.table_name,
            item.column_name
        );
    END LOOP;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transactions_border_valid') THEN
        ALTER TABLE transactions ADD CONSTRAINT transactions_border_valid CHECK (
            ("CrossBorder" = 0 AND "OriginRegion" = "DestRegion"
                AND "RouteComplex" = 'direct'
                AND "Border_cross_border_pre" = 'domestic')
            OR
            ("CrossBorder" = 1 AND "OriginRegion" != "DestRegion"
                AND "RouteComplex" = 'multi-hop'
                AND "Border_cross_border_pre" = 'cross-border')
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'transaction_products_values_valid') THEN
        ALTER TABLE transaction_products ADD CONSTRAINT transaction_products_values_valid
            CHECK ("PriceAmt" >= 0 AND "QtySold" > 0);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'buyer_sessions_causal') THEN
        ALTER TABLE "BuyerSessionAnalytics" ADD CONSTRAINT buyer_sessions_causal CHECK (
            cart_removals_count <= cart_additions_count
            AND (NOT checkout_completed OR checkout_initiated)
            AND (NOT bounce_indicator OR (
                NOT checkout_initiated
                AND NOT checkout_completed
                AND cart_additions_count = 0
            ))
            AND click_through_rate BETWEEN 0 AND 1
            AND scroll_depth_pct BETWEEN 0 AND 100
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'payments_state_valid') THEN
        ALTER TABLE "PaymentProcessingEvents" ADD CONSTRAINT payments_state_valid CHECK (
            amount_requested >= 0
            AND amount_processed >= 0
            AND fraud_score BETWEEN 0 AND 1
            AND retry_count >= 0
            AND processing_stage IN ('authorized', 'settled', 'failed')
            AND (
                (processing_stage = 'authorized' AND amount_processed = 0 AND decline_reason IS NULL)
                OR (processing_stage = 'settled' AND amount_processed = amount_requested AND decline_reason IS NULL)
                OR (processing_stage = 'failed' AND amount_processed = 0 AND decline_reason IS NOT NULL AND retry_count >= 1)
            )
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'risk_probability_valid') THEN
        ALTER TABLE risk_analytics ADD CONSTRAINT risk_probability_valid CHECK (
            "FraudProb" BETWEEN 0 AND 1
            AND "ML_Risk" = CASE
                WHEN "FraudProb" >= 0.7 THEN 'high'
                WHEN "FraudProb" >= 0.3 THEN 'medium'
                ELSE 'low'
            END
        );
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'prediction_probability_valid') THEN
        ALTER TABLE "RiskModelPredictions" ADD CONSTRAINT prediction_probability_valid CHECK (
            fraud_probability BETWEEN 0 AND 1
            AND confidence_score BETWEEN 0 AND 1
            AND ensemble_agreement_rate BETWEEN 0 AND 1
            AND model_drift_indicator BETWEEN 0 AND 1
            AND risk_category_predicted = CASE
                WHEN fraud_probability >= 0.7 THEN 'high'
                WHEN fraud_probability >= 0.3 THEN 'medium'
                ELSE 'low'
            END
        );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS transactions_platform_time_idx
    ON transactions ("PlatformKey", "EventTimestamp");
CREATE INDEX IF NOT EXISTS transactions_vendor_time_idx
    ON transactions ("VendorLink", "EventTimestamp");
CREATE INDEX IF NOT EXISTS transactions_buyer_time_idx
    ON transactions ("AcqLink", "EventTimestamp");
CREATE INDEX IF NOT EXISTS transaction_products_product_idx
    ON transaction_products ("ProdCat", "Subcategory", "ListingAge", "SellerPointer");
CREATE INDEX IF NOT EXISTS buyer_sessions_buyer_time_idx
    ON "BuyerSessionAnalytics" (acq_ref, session_start_time);
CREATE INDEX IF NOT EXISTS payments_transaction_stage_idx
    ON "PaymentProcessingEvents" (transaction_ref, processing_stage);
CREATE INDEX IF NOT EXISTS predictions_transaction_time_idx
    ON "RiskModelPredictions" (txn_link_ref, prediction_timestamp);

COMMIT;
