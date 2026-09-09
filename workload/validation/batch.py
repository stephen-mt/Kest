import hashlib
import json

from workload.core.storage import s3_client
from workload.cybermarket.schema import EXPECTED_COLUMNS, FACT_TABLES
from workload.lakehouse.catalog import catalog, load_manifest, record_count
from workload.lakehouse.cdc import CDC_TABLE
from workload.lakehouse.publication import load_current_pointer
from workload.pipelines.batch import GOLD_QUERIES

GOLD_COLUMNS = {
    "daily_market_metrics": [
        "metric_date",
        "platform_key",
        "transaction_count",
        "unique_buyers",
        "unique_vendors",
        "gross_merchandise_value",
        "cross_border_transactions",
        "high_risk_transactions",
        "payment_failed_events",
    ],
    "vendor_risk_summary": [
        "seller_key",
        "transaction_count",
        "unique_buyers",
        "gross_merchandise_value",
        "high_risk_transactions",
        "average_fraud_probability",
        "last_transaction_at",
    ],
    "buyer_360": [
        "buyer_key",
        "transaction_count",
        "session_count",
        "completed_checkouts",
        "lifetime_value",
        "average_session_seconds",
        "last_purchase_at",
        "auth_level",
        "buyer_risk_profile",
    ],
    "product_performance": [
        "product_category",
        "subcategory",
        "listing_age",
        "seller_key",
        "transaction_count",
        "units_sold",
        "gross_revenue",
        "product_availability",
    ],
}


def _batch_manifest(settings, pointer):
    response = s3_client(settings).get_object(
        Bucket=settings.s3_bucket, Key=pointer["manifest_key"]
    )
    payload = response["Body"].read()
    manifest = json.loads(payload)
    if manifest["batch_id"] != pointer["batch_id"]:
        raise AssertionError("Current pointer and batch manifest differ")
    return manifest, hashlib.sha256(payload).hexdigest()


def check_batch(settings):
    pointer, _ = load_current_pointer(settings)
    batch_manifest, batch_manifest_sha = _batch_manifest(settings, pointer)
    silver_namespace = pointer["silver_namespace"]
    gold_namespace = pointer["gold_namespace"]
    iceberg_catalog = catalog(settings)
    namespaces = set(iceberg_catalog.list_namespaces())
    required_namespaces = {(silver_namespace,), (gold_namespace,)}
    if not required_namespaces <= namespaces:
        raise AssertionError(
            f"Current Iceberg versions are missing: {required_namespaces - namespaces}"
        )

    silver_tables = set(iceberg_catalog.list_tables((silver_namespace,)))
    expected_silver = {(silver_namespace, table) for table in EXPECTED_COLUMNS}
    expected_silver.add((silver_namespace, CDC_TABLE))
    if silver_tables != expected_silver:
        raise AssertionError(f"Silver tables differ: {sorted(silver_tables)}")

    gold_tables = set(iceberg_catalog.list_tables((gold_namespace,)))
    expected_gold = {(gold_namespace, table) for table in GOLD_QUERIES}
    if gold_tables != expected_gold:
        raise AssertionError(f"Gold tables differ: {sorted(gold_tables)}")

    bronze_manifest, bronze_manifest_sha = load_manifest(settings)
    if batch_manifest["bronze_manifest_sha256"] != bronze_manifest_sha:
        raise AssertionError("Current batch points to another Bronze manifest")

    source_counts = batch_manifest["source_snapshot"]["rows"]
    silver_counts = {}
    batch_ids = set()
    for table in EXPECTED_COLUMNS:
        iceberg_table = iceberg_catalog.load_table((silver_namespace, table))
        if iceberg_table.schema().column_names != EXPECTED_COLUMNS[table]:
            raise AssertionError(f"Silver {table} columns differ")
        expected = source_counts[table]
        if table in FACT_TABLES:
            expected += bronze_manifest["rows"][table]
        actual = record_count(iceberg_table)
        if actual != expected or actual != batch_manifest["silver_rows"][table]:
            raise AssertionError(f"Silver {table}: expected {expected}, got {actual}")
        if (
            iceberg_table.properties.get("kest.bronze-manifest-sha256")
            != bronze_manifest_sha
        ):
            raise AssertionError(f"Silver {table} points to another Bronze manifest")
        if (
            iceberg_table.properties.get("kest.source-lsn")
            != batch_manifest["source_snapshot"]["lsn"]
        ):
            raise AssertionError(f"Silver {table} source LSN differs")
        batch_ids.add(iceberg_table.properties.get("kest.batch-id"))
        silver_counts[table] = actual

    cdc_table = iceberg_catalog.load_table((silver_namespace, CDC_TABLE))
    cdc_data = cdc_table.scan().to_arrow()
    cdc_expected = batch_manifest["cdc"]["event_count"]
    if cdc_data.num_rows != cdc_expected:
        raise AssertionError(
            f"Silver CDC expected {cdc_expected}, got {cdc_data.num_rows}"
        )
    if len(set(cdc_data["event_id"].to_pylist())) != cdc_data.num_rows:
        raise AssertionError("Silver CDC contains duplicate event IDs")
    if cdc_table.properties.get("kest.cdc-through-lsn") != (
        batch_manifest["cdc"]["through_lsn"] or ""
    ):
        raise AssertionError("Silver CDC checkpoint differs")
    batch_ids.add(cdc_table.properties.get("kest.batch-id"))
    silver_counts[CDC_TABLE] = cdc_data.num_rows

    gold_data = {}
    gold_counts = {}
    for table, columns in GOLD_COLUMNS.items():
        iceberg_table = iceberg_catalog.load_table((gold_namespace, table))
        if iceberg_table.schema().column_names != columns:
            raise AssertionError(f"Gold {table} columns differ")
        if iceberg_table.properties.get("kest.silver-namespace") != silver_namespace:
            raise AssertionError(f"Gold {table} points to another Silver version")
        batch_ids.add(iceberg_table.properties.get("kest.batch-id"))
        data = iceberg_table.scan().to_arrow()
        if not data.num_rows:
            raise AssertionError(f"Gold {table} is empty")
        gold_data[table] = data
        gold_counts[table] = data.num_rows

    if batch_ids != {pointer["batch_id"]}:
        raise AssertionError(
            f"Iceberg tables do not share the current batch: {batch_ids}"
        )
    if gold_counts != batch_manifest["gold_rows"]:
        raise AssertionError("Gold row counts differ from the batch manifest")
    if gold_counts["vendor_risk_summary"] != source_counts["vendors"]:
        raise AssertionError("Gold vendor cardinality differs from source snapshot")
    if gold_counts["buyer_360"] != source_counts["buyers"]:
        raise AssertionError("Gold buyer cardinality differs from source snapshot")
    if gold_counts["product_performance"] != source_counts["products"]:
        raise AssertionError("Gold product cardinality differs from source snapshot")

    transaction_count = silver_counts["transactions"]
    for table in ("daily_market_metrics", "vendor_risk_summary", "buyer_360"):
        total = sum(gold_data[table]["transaction_count"].to_pylist())
        if total != transaction_count:
            raise AssertionError(f"Gold {table} transaction total differs: {total}")
    units = sum(gold_data["product_performance"]["units_sold"].to_pylist())
    if units != silver_counts["transaction_products"]:
        raise AssertionError(
            "Gold product units differ from Silver transaction products"
        )

    return {
        "batch_id": pointer["batch_id"],
        "batch_manifest_sha256": batch_manifest_sha,
        "cdc_events": cdc_data.num_rows,
        "gold_namespace": gold_namespace,
        "gold_rows": gold_counts,
        "silver_namespace": silver_namespace,
        "silver_rows": silver_counts,
        "source_lsn": batch_manifest["source_snapshot"]["lsn"],
    }
