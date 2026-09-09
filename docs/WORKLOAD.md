# Kest Workload — CyberMarket Lite

> Phase contract: this is the original source-and-history specification.
> Committed CDC and the versioned Iceberg analytics layer were added in the next
> phase and are documented in `workload/README.md`.

## Goal
Use only 10 connected tables from `LiveSQLBench-Large-v1 / cybermarket_pattern_large`.
PostgreSQL is the live source. S3 is assumed to already contain ~5 GiB historical Parquet for the same 10 logical tables.
Do not store the original LiveSQLBench dump/reference files in S3.
This phase leaves CDC and final analytics/semantic modeling for Kur to the next
phase.

## Selected Graph
```text
markets ───────────────┐
vendors ─ products ─ transaction_products
   └──────── transactions ─ PaymentProcessingEvents
buyers ───────┘   │
  └─ BuyerSessionAnalytics
                  └─ risk_analytics ─ RiskModelPredictions
```

This keeps dimensions, composite keys, sessions, transactions, item grain, payment events, risk and ML predictions while avoiding the ~54-table full schema.

## Exact PostgreSQL Schema

### 1. `markets`
```text
"PlatCode" text PK; "PlatName" text; "PlatformType" text; "AgeDays" bigint;
"OperStatus" text; "RepScore" text; "ConfidenceLevel" text; "SizeCat" text;
"DayTxnVol" real; "ActiveUsersMo" text; "SellerCount" bigint; "AcqCount" bigint;
"ItemListings" bigint; "lastUpdated" timestamp; "RefreshHrs" bigint;
platform_compliance jsonb
```

### 2. `vendors`
```text
"SellerKey" text PK; "DaysActive" bigint; "PerformanceRating" real; "TotalTxns" text;
"CompletedTxns" bigint; "DisputedEvents" bigint; "VerTier" text; "LastActiveDt" timestamp;
"AccessLevel" text; "InvestigationFlag" text; "LE_Interest" text;
"ComplianceRisk" text; "RegStandeff" text; vendor_compliance_ratings jsonb
```

### 3. `buyers`
```text
"AcqCode" text PK; "ProfileAge" bigint; "PurchaseCount" bigint; "AuthLevel" text;
buyer_risk_profile jsonb
```

### 4. `products`
```text
"ProdCat" text; "Subcategory" text; "ListingAge" bigint; "SellerPointer" text;
product_availability jsonb
PK ("ProdCat","Subcategory","ListingAge","SellerPointer")
FK "SellerPointer" -> vendors."SellerKey"
```

### 5. `transactions`
```text
"EventCode" text PK; "RecordTag" text; "EventTimestamp" timestamp;
"PlatformKey" text FK -> markets."PlatCode"; "VendorLink" text FK -> vendors."SellerKey";
"AcqLink" text FK -> buyers."AcqCode"; "OriginRegion" text; "DestRegion" text;
"CrossBorder" bigint; "RouteComplex" text; "Transaction_Velocity" text;
"Border_cross_border_pre" text; "GeoDistScore" text; transaction_financials jsonb
```

### 6. `transaction_products`
```text
"EventLink" text; "ProdCat" text; "Subcategory" text; "ListingAge" bigint;
"SellerPointer" text; "PriceAmt" real; "QtySold" bigint
PK ("EventLink","ProdCat","Subcategory","ListingAge","SellerPointer")
FK "EventLink" -> transactions."EventCode"
logical composite FK ("ProdCat","Subcategory","ListingAge","SellerPointer") -> products
```

### 7. `BuyerSessionAnalytics`
```text
"BSA_id" text PK; acq_ref text FK -> buyers."AcqCode"; session_start_time timestamp;
session_duration_seconds integer; pages_viewed_count integer; products_viewed_count integer;
cart_additions_count integer; cart_removals_count integer; search_queries_count integer;
checkout_initiated boolean; checkout_completed boolean; bounce_indicator boolean;
referral_source text; device_category text; geo_region text; avg_time_per_page_seconds real;
click_through_rate real; scroll_depth_pct real; error_encounters_count integer;
session_value_estimate real
```

### 8. `PaymentProcessingEvents`
```text
"PPE_id" text PK; transaction_ref text FK -> transactions."EventCode"; event_timestamp timestamp;
payment_method_type text; processing_stage text; amount_requested real; amount_processed real;
currency_code text; processor_name text; authorization_code text; processing_fee real;
processing_fee_pct real; fraud_check_passed boolean; fraud_score real; avs_response_code text;
cvv_verification_passed boolean; three_ds_authenticated boolean; decline_reason text;
retry_count integer; processing_time_ms integer
```

### 9. `risk_analytics`
```text
"TxnLink" text PK/FK -> transactions."EventCode"; "RiskIndicatorCount" bigint;
"FraudProb" real; "ML_Risk" text; "LinkedEvents" bigint; "ChainLength" bigint;
wallet_risk_assessment jsonb
```

### 10. `RiskModelPredictions`
```text
"RMP_id" text PK; txn_link_ref text FK -> risk_analytics."TxnLink";
prediction_timestamp timestamp; model_name text; model_version text; fraud_probability real;
risk_category_predicted text; confidence_score real; top_risk_factor text;
risk_factors_count integer; feature_importance_velocity real; feature_importance_amount real;
feature_importance_device real; feature_importance_behavior real; recommendation_action text;
actual_outcome text; prediction_latency_ms integer; ensemble_agreement_rate real;
manual_review_triggered boolean; model_drift_indicator real
```

Keep these names and casing. The restored/public CyberMarket schema is the reference; do not silently rename columns.

## PostgreSQL Live Workload
Use only these 5 business event types at a fixed default rate of **20 events/sec = 1,200/min**:

| Event | /min |
|---|---:|
| buyer_session | 480 |
| purchase | 360 |
| payment_update | 180 |
| risk_prediction | 120 |
| transaction_status_update | 60 |

### `buyer_session`
INSERT 1 row into `BuyerSessionAnalytics`.

### `purchase`
One PostgreSQL transaction:
- INSERT 1 `transactions`
- INSERT exactly 2 `transaction_products`
- INSERT 1 `PaymentProcessingEvents`
- INSERT 1 `risk_analytics`
- UPDATE 1 `buyers`
- UPDATE 1 `vendors`

### `payment_update`
UPDATE 1 existing `PaymentProcessingEvents`.

### `risk_prediction`
INSERT 1 `RiskModelPredictions` linked to an existing `risk_analytics` row.

### `transaction_status_update`
UPDATE `transactions.transaction_financials` for one existing transaction.

Keep PK/FK validity, causal timestamps, valid JSONB and skewed/non-uniform buyer/vendor activity.
Do not generate each table independently with Faker.

## S3 Historical State
Assume ~5 GiB historical data already exists as Parquet for these same 10 tables:

```text
s3://<bucket>/history/<table_name>/*.parquet
```

Examples:
`history/transactions/`, `history/transaction_products/`, `history/BuyerSessionAnalytics/`,
`history/PaymentProcessingEvents/`, `history/risk_analytics/`, etc.

This history is a simulation fixture:
- it already exists before the local workload starts;
- do not upload the full LiveSQLBench dump;
- do not regenerate 5 GiB on every dev run;
- schema/logical keys must remain compatible with PostgreSQL.

Later CDC should write only newly arriving/changed source data to a separate landing path.
CDC design is intentionally out of scope here.

## Why These 10 Are Enough for Kur
They support questions spanning:
- market/vendor performance;
- buyer behavior and session conversion;
- product/category sales;
- cross-border transaction analysis;
- payment failure/fraud;
- risk scoring;
- model quality/drift.

Important grains remain different:
```text
session != transaction != transaction_item != payment_event != model_prediction
```
so useful questions can still require 4–7 table joins and careful aggregation.

## Constraints
- Exactly 10 tables in workload v1.
- Do not add tables until a concrete analytics requirement needs one.
- PostgreSQL uses the schema above; JSONB remains JSONB.
- S3 history is Parquet only.
- No schema evolution yet.
- No pre-designed fact/dimension/semantic layer in this file.
- Optimize for correctness and analytic value, not maximum throughput.

## Success
1. The 10 PostgreSQL tables exist with the schema above.
2. The generator sustains the fixed 20 events/sec workload.
3. S3 exposes ~5 GiB historical Parquet for the same logical tables.
4. History and live source use compatible keys/schema.
5. Kest can later combine history + new CDC data to build an analytics surface for Kur.
