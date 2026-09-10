import argparse
import hashlib
import json
import re
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from kest.storage import list_objects, s3_client
from workloads.cybermarket.config import Settings
from workloads.cybermarket.source.ids import (
    BUYER_COUNT,
    MARKET_COUNT,
    PRODUCT_COUNT,
    PRODUCTS_PER_VENDOR,
    VENDOR_COUNT,
)

MIB = 1024**2
FILE_TARGET_BYTES = 128 * MIB
HISTORY_SCHEMA_VERSION = 2
TRANSACTION_BYTE_WEIGHT = 0.29
TABLE_ORDER = (
    "markets",
    "vendors",
    "buyers",
    "products",
    "transactions",
    "transaction_products",
    "BuyerSessionAnalytics",
    "PaymentProcessingEvents",
    "risk_analytics",
    "RiskModelPredictions",
)
FIXED_TABLE_ROWS = {
    "markets": MARKET_COUNT,
    "vendors": VENDOR_COUNT,
    "buyers": BUYER_COUNT,
    "products": PRODUCT_COUNT,
}


SELECT_LISTS = {
    "markets": """
        printf('PLAT-%03d', i) AS "PlatCode",
        printf('CyberMarket %d %s', i, md5(i::varchar)) AS "PlatName",
        CASE i % 3 WHEN 0 THEN 'marketplace' ELSE 'specialized' END AS "PlatformType",
        (365 + i % 8000)::bigint AS "AgeDays", 'active' AS "OperStatus",
        (60 + i % 40)::varchar AS "RepScore", 'high' AS "ConfidenceLevel",
        CASE i % 3 WHEN 0 THEN 'large' ELSE 'medium' END AS "SizeCat",
        (1000 + i % 50000)::real AS "DayTxnVol", (10000 + i % 900000)::varchar AS "ActiveUsersMo",
        (100 + i % 50000)::bigint AS "SellerCount", (1000 + i % 500000)::bigint AS "AcqCount",
        (1000 + i % 1000000)::bigint AS "ItemListings",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "lastUpdated",
        24::bigint AS "RefreshHrs",
        json_object('status', 'compliant', 'audit', md5('market:' || i::varchar)) AS platform_compliance
    """,
    "vendors": """
        printf('SELLER-%06d', i) AS "SellerKey", (30 + i % 3000)::bigint AS "DaysActive",
        (3 + (i % 20) / 10.0)::real AS "PerformanceRating", (i % 50000)::varchar AS "TotalTxns",
        (i % 49000)::bigint AS "CompletedTxns", (i % 25)::bigint AS "DisputedEvents",
        CASE i % 10 WHEN 0 THEN 'enhanced' ELSE 'standard' END AS "VerTier",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "LastActiveDt",
        'active' AS "AccessLevel", CASE i % 97 WHEN 0 THEN 'review' ELSE 'none' END AS "InvestigationFlag",
        CASE i % 53 WHEN 0 THEN 'medium' ELSE 'low' END AS "LE_Interest", 'low' AS "ComplianceRisk",
        'verified' AS "RegStandeff",
        json_object('rating_id', md5('vendor:' || i::varchar), 'reviewed', i % 2 = 0) AS vendor_compliance_ratings
    """,
    "buyers": """
        printf('BUYER-%07d', i) AS "AcqCode", (i % 2500)::bigint AS "ProfileAge",
        (i % 500)::bigint AS "PurchaseCount", CASE i % 3 WHEN 0 THEN 'mfa' ELSE 'standard' END AS "AuthLevel",
        json_object('fingerprint', md5('buyer:' || i::varchar), 'score', (i % 1000) / 1000.0) AS buyer_risk_profile
    """,
    "products": """
        printf('CAT-%03d', i % 20) AS "ProdCat", printf('SUB-%05d', i % 100) AS "Subcategory",
        (i % 4)::bigint AS "ListingAge", printf('SELLER-%06d', 1 + (i - 1) // 4) AS "SellerPointer",
        json_object('stock', i % 500, 'sku_hash', md5('product:' || i::varchar)) AS product_availability
    """,
    "transactions": f"""
        printf('EVT-HIST-%014d', i) AS "EventCode", 'purchase' AS "RecordTag",
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS "EventTimestamp",
        printf('PLAT-%03d', 1 + (i - 1) % {MARKET_COUNT}) AS "PlatformKey",
        printf('SELLER-%06d', 1 + (i - 1) % {VENDOR_COUNT}) AS "VendorLink",
        printf('BUYER-%07d', 1 + (i - 1) % {BUYER_COUNT}) AS "AcqLink",
        CASE i % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS "OriginRegion",
        CASE (i // 3) % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS "DestRegion",
        ((i % 4) != ((i // 3) % 4))::bigint AS "CrossBorder",
        CASE WHEN (i % 4) != ((i // 3) % 4) THEN 'multi-hop' ELSE 'direct' END AS "RouteComplex",
        CASE i % 5 WHEN 0 THEN 'burst' ELSE 'normal' END AS "Transaction_Velocity",
        CASE WHEN (i % 4) != ((i // 3) % 4) THEN 'cross-border' ELSE 'domestic' END AS "Border_cross_border_pre",
        ((i * 37) % 10000 / 100.0)::varchar AS "GeoDistScore",
        json_object('amount', 5 + (i % 250000) / 100.0, 'currency', 'USD', 'status', 'settled',
                    'trace', md5('txn:' || i::varchar)) AS transaction_financials
    """,
    "transaction_products": f"""
        printf('EVT-HIST-%014d', 1 + (i - 1) // 2) AS "EventLink",
        printf('CAT-%03d', ((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 +
            ((((i - 1) // 2) // {VENDOR_COUNT}) % 2) * 2 + (i - 1) % 2) % 20) AS "ProdCat",
        printf('SUB-%05d', ((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 +
            ((((i - 1) // 2) // {VENDOR_COUNT}) % 2) * 2 + (i - 1) % 2) % 100) AS "Subcategory",
        (((((i - 1) // 2) % {VENDOR_COUNT}) * {PRODUCTS_PER_VENDOR} + 1 +
            ((((i - 1) // 2) // {VENDOR_COUNT}) % 2) * 2 + (i - 1) % 2) % {PRODUCTS_PER_VENDOR})::bigint AS "ListingAge",
        printf('SELLER-%06d', 1 + ((i - 1) // 2) % {VENDOR_COUNT}) AS "SellerPointer",
        ((5 + ((1 + (i - 1) // 2) % 250000) / 100.0) * CASE (i - 1) % 2 WHEN 0 THEN 0.45 ELSE 0.55 END)::real AS "PriceAmt",
        1::bigint AS "QtySold"
    """,
    "BuyerSessionAnalytics": f"""
        printf('BSA-HIST-%014d', i) AS "BSA_id", printf('BUYER-%07d', 1 + (i - 1) % {BUYER_COUNT}) AS acq_ref,
        TIMESTAMP '2024-01-01' + (i % 31536000) * INTERVAL 1 SECOND AS session_start_time,
        (10 + i % 1800)::integer AS session_duration_seconds, (1 + i % 30)::integer AS pages_viewed_count,
        (i % 15)::integer AS products_viewed_count, (i % 5)::integer AS cart_additions_count,
        ((i * 7) % (1 + i % 5))::integer AS cart_removals_count, (i % 8)::integer AS search_queries_count,
        (i % 5 > 0 AND i % 3 = 0) AS checkout_initiated,
        (i % 5 > 0 AND i % 3 = 0 AND i % 4 = 0) AS checkout_completed,
        (i % 30 = 0) AS bounce_indicator,
        CASE i % 4 WHEN 0 THEN 'direct' WHEN 1 THEN 'search' WHEN 2 THEN 'affiliate' ELSE 'social' END AS referral_source,
        CASE i % 3 WHEN 0 THEN 'desktop' WHEN 1 THEN 'mobile' ELSE 'tablet' END AS device_category,
        CASE i % 4 WHEN 0 THEN 'NA' WHEN 1 THEN 'EU' WHEN 2 THEN 'APAC' ELSE 'LATAM' END AS geo_region,
        (5 + i % 120)::real AS avg_time_per_page_seconds, ((i % 1000) / 1000.0)::real AS click_through_rate,
        (i % 101)::real AS scroll_depth_pct, (i % 3)::integer AS error_encounters_count,
        ((i * 13) % 100000 / 100.0)::real AS session_value_estimate
    """,
    "PaymentProcessingEvents": """
        printf('PPE-HIST-%014d', i) AS "PPE_id",
        printf('EVT-HIST-%014d', 1 + (i - 1) % __TRANSACTION_ROWS__) AS transaction_ref,
        TIMESTAMP '2024-01-01' + ((1 + (i - 1) % __TRANSACTION_ROWS__) % 31536000) * INTERVAL 1 SECOND
            + (10 + i % 900) * INTERVAL 1 MILLISECOND AS event_timestamp,
        CASE i % 3 WHEN 0 THEN 'card' WHEN 1 THEN 'wallet' ELSE 'bank_transfer' END AS payment_method_type,
        CASE WHEN ((i * 17) % 1000) >= 900 THEN 'failed' ELSE 'settled' END AS processing_stage,
        (5 + i % 250000 / 100.0)::real AS amount_requested,
        CASE WHEN ((i * 17) % 1000) >= 900 THEN 0 ELSE (5 + i % 250000 / 100.0) END::real AS amount_processed,
        'USD' AS currency_code,
        CASE i % 2 WHEN 0 THEN 'nova-pay' ELSE 'orbit-pay' END AS processor_name,
        upper(substr(md5('auth:' || i::varchar), 1, 12)) AS authorization_code,
        ((5 + i % 250000 / 100.0) * 0.021)::real AS processing_fee, 2.1::real AS processing_fee_pct,
        (((i * 17) % 1000) < 900) AS fraud_check_passed, ((i * 17) % 1000 / 1000.0)::real AS fraud_score,
        'Y' AS avs_response_code, (i % 50 != 0) AS cvv_verification_passed,
        (i % 4 != 0) AS three_ds_authenticated,
        CASE WHEN ((i * 17) % 1000) >= 900 THEN 'risk_decline' ELSE NULL END AS decline_reason,
        CASE WHEN ((i * 17) % 1000) >= 900 THEN 1 ELSE 0 END::integer AS retry_count,
        (25 + i % 900)::integer AS processing_time_ms
    """,
    "risk_analytics": """
        printf('EVT-HIST-%014d', i) AS "TxnLink", (i % 6)::bigint AS "RiskIndicatorCount",
        ((i * 17) % 1000 / 1000.0)::real AS "FraudProb",
        CASE WHEN ((i * 17) % 1000) >= 700 THEN 'high'
             WHEN ((i * 17) % 1000) >= 300 THEN 'medium' ELSE 'low' END AS "ML_Risk",
        (i % 9)::bigint AS "LinkedEvents", (1 + i % 5)::bigint AS "ChainLength",
        json_object('wallet_hash', md5('wallet:' || i::varchar), 'age_days', i % 2500) AS wallet_risk_assessment
    """,
    "RiskModelPredictions": """
        printf('RMP-HIST-%014d', i) AS "RMP_id",
        printf('EVT-HIST-%014d', 1 + ((i - 1) * 3) % __TRANSACTION_ROWS__) AS txn_link_ref,
        TIMESTAMP '2024-01-01' + ((1 + ((i - 1) * 3) % __TRANSACTION_ROWS__) % 31536000) * INTERVAL 1 SECOND
            + (1 + i % 300) * INTERVAL 1 SECOND AS prediction_timestamp,
        'cyber-risk-lite' AS model_name, '1.0.0' AS model_version,
        ((i * 17) % 1000 / 1000.0)::real AS fraud_probability,
        CASE WHEN ((i * 17) % 1000) >= 700 THEN 'high'
             WHEN ((i * 17) % 1000) >= 300 THEN 'medium' ELSE 'low' END AS risk_category_predicted,
        (0.6 + i % 400 / 1000.0)::real AS confidence_score,
        CASE i % 4 WHEN 0 THEN 'velocity' WHEN 1 THEN 'amount' WHEN 2 THEN 'device' ELSE 'behavior' END AS top_risk_factor,
        (1 + i % 8)::integer AS risk_factors_count, ((i * 3) % 1000 / 1000.0)::real AS feature_importance_velocity,
        ((i * 5) % 1000 / 1000.0)::real AS feature_importance_amount,
        ((i * 7) % 1000 / 1000.0)::real AS feature_importance_device,
        ((i * 11) % 1000 / 1000.0)::real AS feature_importance_behavior,
        CASE WHEN ((i * 17) % 1000) >= 700 THEN 'manual_review' ELSE 'approve' END AS recommendation_action,
        CASE i % 3 WHEN 0 THEN 'fraud' ELSE 'legitimate' END AS actual_outcome,
        (2 + i % 95)::integer AS prediction_latency_ms, (0.6 + i % 400 / 1000.0)::real AS ensemble_agreement_rate,
        (((i * 17) % 1000) >= 700) AS manual_review_triggered, (i % 200 / 1000.0)::real AS model_drift_indicator
    """,
}


def existing_parts(client, settings):
    result = defaultdict(list)
    prefix = settings.bronze_prefix + "/"
    for item in list_objects(client, settings.s3_bucket, prefix):
        if not item["Key"].endswith(".parquet"):
            continue
        relative = item["Key"][len(prefix) :]
        table, _, filename = relative.partition("/")
        match = re.fullmatch(r"part-(\d{5})\.parquet", filename)
        if not match:
            raise RuntimeError(f"Unexpected Bronze object name: {item['Key']}")
        head = client.head_object(Bucket=settings.s3_bucket, Key=item["Key"])
        metadata = head["Metadata"]
        required = {
            "content-sha256",
            "part-number",
            "row-count",
            "row-start",
            "schema-sha256",
        }
        if not required <= set(metadata):
            raise RuntimeError(
                f"Bronze object predates resumable schema v2; reset required: {item['Key']}"
            )
        part_number = int(match.group(1))
        if int(metadata["part-number"]) != part_number:
            raise RuntimeError(f"Bronze part metadata differs: {item['Key']}")
        result[table].append(
            {
                "etag": head["ETag"].strip('"'),
                "key": item["Key"],
                "part_number": part_number,
                "schema_sha256": metadata["schema-sha256"],
                "sha256": metadata["content-sha256"],
                "size": item["Size"],
                "row_start": int(metadata["row-start"]),
                "row_count": int(metadata["row-count"]),
            }
        )
    for table, table_parts in result.items():
        validate_part_sequence(table, table_parts)
    return result


def validate_part_sequence(table, parts):
    parts.sort(key=lambda part: part["part_number"])
    expected_row = 1
    for expected_part, part in enumerate(parts):
        if part["part_number"] != expected_part:
            raise RuntimeError(f"Bronze {table} has a missing or duplicate part")
        if part["row_start"] != expected_row:
            raise RuntimeError(f"Bronze {table} has an overlapping or missing range")
        expected_row += part["row_count"]


def resolved_select_list(table, transaction_rows):
    return SELECT_LISTS[table].replace("__TRANSACTION_ROWS__", str(transaction_rows))


def schema_sha256(table, transaction_rows):
    material = (
        f"{HISTORY_SCHEMA_VERSION}\n{resolved_select_list(table, transaction_rows)}"
    )
    return hashlib.sha256(material.encode()).hexdigest()


def write_parquet(connection, table, start, rows, output, transaction_rows):
    select_list = resolved_select_list(table, transaction_rows)
    connection.execute(f"""
        COPY (
            SELECT {select_list}
            FROM range({start}, {start + rows}) AS generated(i)
        ) TO '{output.as_posix()}' (
            FORMAT parquet,
            COMPRESSION zstd,
            COMPRESSION_LEVEL 1,
            ROW_GROUP_SIZE 100000
        )
    """)


def generate_table(
    connection,
    client,
    settings,
    table,
    target,
    parts,
    transaction_rows,
    target_rows=None,
):
    expected_schema = schema_sha256(table, transaction_rows)
    if any(part["schema_sha256"] != expected_schema for part in parts):
        raise RuntimeError(f"Bronze {table} schema changed; reset required")
    total = sum(part["size"] for part in parts)
    row_count = sum(part["row_count"] for part in parts)
    if target_rows is not None and row_count > target_rows:
        raise RuntimeError(f"Bronze {table} exceeds its target row count")
    start = row_count + 1
    part_number = len(parts)
    bytes_per_row = None
    with tempfile.TemporaryDirectory(prefix="kest-history-") as temp_dir:
        while row_count < target_rows if target_rows is not None else total < target:
            remaining = target - total
            if bytes_per_row is None:
                rows = 100000
            elif target_rows is not None:
                rows = max(1000, int(FILE_TARGET_BYTES / bytes_per_row))
            else:
                rows = max(1000, int(min(FILE_TARGET_BYTES, remaining) / bytes_per_row))
            if target_rows is not None:
                rows = min(rows, target_rows - row_count)
            output = Path(temp_dir) / f"part-{part_number:05d}.parquet"
            write_parquet(connection, table, start, rows, output, transaction_rows)
            size = output.stat().st_size
            if (
                target_rows is None
                and size > remaining * 1.05
                and remaining < FILE_TARGET_BYTES
            ):
                adjusted = max(1000, int(rows * remaining / size * 0.99))
                output.unlink()
                rows = adjusted
                write_parquet(connection, table, start, rows, output, transaction_rows)
                size = output.stat().st_size
            bytes_per_row = size / rows
            content_sha256 = hashlib.sha256(output.read_bytes()).hexdigest()
            key = f"{settings.bronze_prefix}/{table}/part-{part_number:05d}.parquet"
            client.upload_file(
                str(output),
                settings.s3_bucket,
                key,
                ExtraArgs={
                    "ContentType": "application/vnd.apache.parquet",
                    "Metadata": {
                        "content-sha256": content_sha256,
                        "part-number": str(part_number),
                        "row-start": str(start),
                        "row-count": str(rows),
                        "schema-sha256": expected_schema,
                    },
                },
            )
            head = client.head_object(Bucket=settings.s3_bucket, Key=key)
            parts.append(
                {
                    "etag": head["ETag"].strip('"'),
                    "key": key,
                    "part_number": part_number,
                    "row_start": start,
                    "row_count": rows,
                    "schema_sha256": expected_schema,
                    "sha256": content_sha256,
                    "size": size,
                }
            )
            output.unlink()
            total += size
            row_count += rows
            start += rows
            part_number += 1
            if target_rows is None:
                progress = f"{total / MIB:,.1f} / {target / MIB:,.1f} MiB"
            else:
                progress = f"{row_count:,} / {target_rows:,} rows"
            print(f"{table}: {progress}")
    return total, row_count


def delete_history(client, settings):
    items = list(list_objects(client, settings.s3_bucket, settings.bronze_prefix + "/"))
    for offset in range(0, len(items), 1000):
        client.delete_objects(
            Bucket=settings.s3_bucket,
            Delete={
                "Objects": [
                    {"Key": item["Key"]} for item in items[offset : offset + 1000]
                ],
                "Quiet": True,
            },
        )
    print(f"Removed {len(items)} generated Bronze objects.")


def load_manifest(client, settings):
    key = f"{settings.bronze_prefix}/_manifest.json"
    for item in list_objects(client, settings.s3_bucket, key):
        if item["Key"] == key:
            body = client.get_object(Bucket=settings.s3_bucket, Key=key)["Body"].read()
            return json.loads(body)
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Create the ~5 GiB historical bronze Parquet state"
    )
    parser.add_argument(
        "--reset", action="store_true", help="replace the generated Bronze fixture"
    )
    args = parser.parse_args()
    settings = Settings.from_env()
    client = s3_client(settings)
    if args.reset:
        delete_history(client, settings)
    previous_manifest = load_manifest(client, settings)
    parts = existing_parts(client, settings)
    unknown = set(parts) - set(TABLE_ORDER)
    if unknown:
        raise RuntimeError(f"Unexpected bronze table prefixes: {sorted(unknown)}")

    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '1GB'")
    totals = {}
    row_counts = {}
    try:
        for table in TABLE_ORDER:
            target = (
                int(settings.bronze_target_bytes * TRANSACTION_BYTE_WEIGHT)
                if table == "transactions"
                else settings.bronze_target_bytes
            )
            transaction_rows = row_counts.get("transactions", 1)
            target_rows = FIXED_TABLE_ROWS.get(table)
            if table == "transaction_products":
                target_rows = 2 * transaction_rows
            elif table == "BuyerSessionAnalytics":
                target_rows = (4 * transaction_rows + 2) // 3
            elif table in {"PaymentProcessingEvents", "risk_analytics"}:
                target_rows = transaction_rows
            elif table == "RiskModelPredictions":
                target_rows = (transaction_rows + 2) // 3
            totals[table], row_counts[table] = generate_table(
                connection,
                client,
                settings,
                table,
                target,
                parts.setdefault(table, []),
                transaction_rows,
                target_rows,
            )
    finally:
        connection.close()

    manifest = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "generated_at": (
            previous_manifest["generated_at"]
            if previous_manifest
            and previous_manifest.get("schema_version") == HISTORY_SCHEMA_VERSION
            else datetime.now(timezone.utc).isoformat()
        ),
        "format": "parquet",
        "compression": "zstd",
        "tables": totals,
        "rows": row_counts,
        "files": {
            table: [
                {
                    "etag": part["etag"],
                    "key": part["key"],
                    "part_number": part["part_number"],
                    "row_count": part["row_count"],
                    "row_start": part["row_start"],
                    "schema_sha256": part["schema_sha256"],
                    "sha256": part["sha256"],
                    "size": part["size"],
                }
                for part in parts.get(table, [])
            ]
            for table in TABLE_ORDER
        },
        "total_parquet_bytes": sum(totals.values()),
        "requested_bytes": settings.bronze_target_bytes,
    }
    key = f"{settings.bronze_prefix}/_manifest.json"
    if manifest != previous_manifest:
        client.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=json.dumps(manifest, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )
    else:
        print("Bronze history is unchanged; manifest write skipped.")
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
