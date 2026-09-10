BEGIN;

CREATE TABLE IF NOT EXISTS markets (
    "PlatCode" text PRIMARY KEY,
    "PlatName" text,
    "PlatformType" text,
    "AgeDays" bigint,
    "OperStatus" text,
    "RepScore" text,
    "ConfidenceLevel" text,
    "SizeCat" text,
    "DayTxnVol" real,
    "ActiveUsersMo" text,
    "SellerCount" bigint,
    "AcqCount" bigint,
    "ItemListings" bigint,
    "lastUpdated" timestamp,
    "RefreshHrs" bigint,
    platform_compliance jsonb
);

CREATE TABLE IF NOT EXISTS vendors (
    "SellerKey" text PRIMARY KEY,
    "DaysActive" bigint,
    "PerformanceRating" real,
    "TotalTxns" text,
    "CompletedTxns" bigint,
    "DisputedEvents" bigint,
    "VerTier" text,
    "LastActiveDt" timestamp,
    "AccessLevel" text,
    "InvestigationFlag" text,
    "LE_Interest" text,
    "ComplianceRisk" text,
    "RegStandeff" text,
    vendor_compliance_ratings jsonb
);

CREATE TABLE IF NOT EXISTS buyers (
    "AcqCode" text PRIMARY KEY,
    "ProfileAge" bigint,
    "PurchaseCount" bigint,
    "AuthLevel" text,
    buyer_risk_profile jsonb
);

CREATE TABLE IF NOT EXISTS products (
    "ProdCat" text,
    "Subcategory" text,
    "ListingAge" bigint,
    "SellerPointer" text REFERENCES vendors ("SellerKey"),
    product_availability jsonb,
    PRIMARY KEY ("ProdCat", "Subcategory", "ListingAge", "SellerPointer")
);

CREATE TABLE IF NOT EXISTS transactions (
    "EventCode" text PRIMARY KEY,
    "RecordTag" text,
    "EventTimestamp" timestamp,
    "PlatformKey" text REFERENCES markets ("PlatCode"),
    "VendorLink" text REFERENCES vendors ("SellerKey"),
    "AcqLink" text REFERENCES buyers ("AcqCode"),
    "OriginRegion" text,
    "DestRegion" text,
    "CrossBorder" bigint,
    "RouteComplex" text,
    "Transaction_Velocity" text,
    "Border_cross_border_pre" text,
    "GeoDistScore" text,
    transaction_financials jsonb
);

CREATE TABLE IF NOT EXISTS transaction_products (
    "EventLink" text REFERENCES transactions ("EventCode"),
    "ProdCat" text,
    "Subcategory" text,
    "ListingAge" bigint,
    "SellerPointer" text,
    "PriceAmt" real,
    "QtySold" bigint,
    PRIMARY KEY ("EventLink", "ProdCat", "Subcategory", "ListingAge", "SellerPointer"),
    FOREIGN KEY ("ProdCat", "Subcategory", "ListingAge", "SellerPointer")
        REFERENCES products ("ProdCat", "Subcategory", "ListingAge", "SellerPointer")
);

CREATE TABLE IF NOT EXISTS "BuyerSessionAnalytics" (
    "BSA_id" text PRIMARY KEY,
    acq_ref text REFERENCES buyers ("AcqCode"),
    session_start_time timestamp,
    session_duration_seconds integer,
    pages_viewed_count integer,
    products_viewed_count integer,
    cart_additions_count integer,
    cart_removals_count integer,
    search_queries_count integer,
    checkout_initiated boolean,
    checkout_completed boolean,
    bounce_indicator boolean,
    referral_source text,
    device_category text,
    geo_region text,
    avg_time_per_page_seconds real,
    click_through_rate real,
    scroll_depth_pct real,
    error_encounters_count integer,
    session_value_estimate real
);

CREATE TABLE IF NOT EXISTS "PaymentProcessingEvents" (
    "PPE_id" text PRIMARY KEY,
    transaction_ref text REFERENCES transactions ("EventCode"),
    event_timestamp timestamp,
    payment_method_type text,
    processing_stage text,
    amount_requested real,
    amount_processed real,
    currency_code text,
    processor_name text,
    authorization_code text,
    processing_fee real,
    processing_fee_pct real,
    fraud_check_passed boolean,
    fraud_score real,
    avs_response_code text,
    cvv_verification_passed boolean,
    three_ds_authenticated boolean,
    decline_reason text,
    retry_count integer,
    processing_time_ms integer
);

CREATE TABLE IF NOT EXISTS risk_analytics (
    "TxnLink" text PRIMARY KEY REFERENCES transactions ("EventCode"),
    "RiskIndicatorCount" bigint,
    "FraudProb" real,
    "ML_Risk" text,
    "LinkedEvents" bigint,
    "ChainLength" bigint,
    wallet_risk_assessment jsonb
);

CREATE TABLE IF NOT EXISTS "RiskModelPredictions" (
    "RMP_id" text PRIMARY KEY,
    txn_link_ref text REFERENCES risk_analytics ("TxnLink"),
    prediction_timestamp timestamp,
    model_name text,
    model_version text,
    fraud_probability real,
    risk_category_predicted text,
    confidence_score real,
    top_risk_factor text,
    risk_factors_count integer,
    feature_importance_velocity real,
    feature_importance_amount real,
    feature_importance_device real,
    feature_importance_behavior real,
    recommendation_action text,
    actual_outcome text,
    prediction_latency_ms integer,
    ensemble_agreement_rate real,
    manual_review_triggered boolean,
    model_drift_indicator real
);

ALTER TABLE markets REPLICA IDENTITY FULL;
ALTER TABLE vendors REPLICA IDENTITY FULL;
ALTER TABLE buyers REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE transactions REPLICA IDENTITY FULL;
ALTER TABLE transaction_products REPLICA IDENTITY FULL;
ALTER TABLE "BuyerSessionAnalytics" REPLICA IDENTITY FULL;
ALTER TABLE "PaymentProcessingEvents" REPLICA IDENTITY FULL;
ALTER TABLE risk_analytics REPLICA IDENTITY FULL;
ALTER TABLE "RiskModelPredictions" REPLICA IDENTITY FULL;

COMMIT;
