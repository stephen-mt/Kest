import argparse
import json
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import psycopg
import pyarrow as pa
import s3fs
from pyarrow import parquet as pq

from workload.core.config import Settings
from workload.core.storage import s3_client
from workload.cybermarket.schema import (
    EXPECTED_COLUMNS,
    FACT_TABLES,
    JSON_COLUMNS,
    TABLES,
)
from workload.lakehouse.catalog import (
    bronze_files,
    catalog,
    ensure_namespace,
    iceberg_arrow_schema,
    load_manifest,
    parquet_schema,
    record_count,
    remove_namespace,
    table_key,
)
from workload.lakehouse.cdc import CDC_TABLE, committed_event_table
from workload.lakehouse.publication import (
    load_current_pointer,
    publish_pointer,
    write_batch_manifest,
)

GOLD_QUERIES = {
    "daily_market_metrics": """
        SELECT CAST(txn."EventTimestamp" AS DATE) AS metric_date,
               txn."PlatformKey" AS platform_key,
               count(*)::BIGINT AS transaction_count,
               count(DISTINCT txn."AcqLink")::BIGINT AS unique_buyers,
               count(DISTINCT txn."VendorLink")::BIGINT AS unique_vendors,
               sum(try_cast(json_extract_string(txn.transaction_financials, '$.amount') AS DECIMAL(18,2))) AS gross_merchandise_value,
               sum(txn."CrossBorder")::BIGINT AS cross_border_transactions,
               count(*) FILTER (WHERE risk."ML_Risk" = 'high')::BIGINT AS high_risk_transactions,
               count(*) FILTER (
                   WHERE payment.processing_stage IN ('declined', 'failed')
               )::BIGINT AS payment_failed_events
        FROM silver_transactions txn
        LEFT JOIN silver_risk risk ON risk."TxnLink" = txn."EventCode"
        LEFT JOIN silver_payments payment
          ON payment.transaction_ref = txn."EventCode"
        GROUP BY metric_date, platform_key
        ORDER BY metric_date, platform_key
    """,
    "vendor_risk_summary": """
        SELECT vendor."SellerKey" AS seller_key,
               count(txn."EventCode")::BIGINT AS transaction_count,
               count(DISTINCT txn."AcqLink")::BIGINT AS unique_buyers,
               coalesce(sum(try_cast(json_extract_string(txn.transaction_financials, '$.amount') AS DECIMAL(18,2))), 0)::DECIMAL(38,2) AS gross_merchandise_value,
               count(*) FILTER (WHERE risk."ML_Risk" = 'high')::BIGINT AS high_risk_transactions,
               avg(risk."FraudProb")::DOUBLE AS average_fraud_probability,
               CAST(max(txn."EventTimestamp") AS TIMESTAMP WITH TIME ZONE) AS last_transaction_at
        FROM silver_vendors vendor
        LEFT JOIN silver_transactions txn ON txn."VendorLink" = vendor."SellerKey"
        LEFT JOIN silver_risk risk ON risk."TxnLink" = txn."EventCode"
        GROUP BY seller_key
        ORDER BY seller_key
    """,
    "buyer_360": """
        WITH purchase AS (
            SELECT "AcqLink" AS buyer_key,
                   count(*)::BIGINT AS transaction_count,
                   sum(try_cast(json_extract_string(transaction_financials, '$.amount') AS DECIMAL(18,2))) AS lifetime_value,
                   CAST(max("EventTimestamp") AS TIMESTAMP WITH TIME ZONE) AS last_purchase_at
            FROM silver_transactions GROUP BY buyer_key
        ), session AS (
            SELECT acq_ref AS buyer_key,
                   count(*)::BIGINT AS session_count,
                   count(*) FILTER (WHERE checkout_completed)::BIGINT AS completed_checkouts,
                   avg(session_duration_seconds)::DOUBLE AS average_session_seconds
            FROM silver_sessions GROUP BY buyer_key
        )
        SELECT buyer."AcqCode" AS buyer_key,
               coalesce(purchase.transaction_count, 0)::BIGINT AS transaction_count,
               coalesce(session.session_count, 0)::BIGINT AS session_count,
               coalesce(session.completed_checkouts, 0)::BIGINT AS completed_checkouts,
               coalesce(purchase.lifetime_value, 0)::DECIMAL(38,2) AS lifetime_value,
               session.average_session_seconds,
               purchase.last_purchase_at,
               buyer."AuthLevel" AS auth_level,
               buyer.buyer_risk_profile
        FROM silver_buyers buyer
        LEFT JOIN purchase ON purchase.buyer_key = buyer."AcqCode"
        LEFT JOIN session ON session.buyer_key = buyer."AcqCode"
        ORDER BY buyer_key
    """,
    "product_performance": """
        SELECT product."ProdCat" AS product_category,
               product."Subcategory" AS subcategory,
               product."ListingAge" AS listing_age,
               product."SellerPointer" AS seller_key,
               count(item."EventLink")::BIGINT AS transaction_count,
               coalesce(sum(item."QtySold"), 0)::BIGINT AS units_sold,
               coalesce(sum(CAST(item."PriceAmt" AS DECIMAL(18,2)) * item."QtySold"), 0)::DECIMAL(38,2) AS gross_revenue,
               product.product_availability
        FROM silver_products product
        LEFT JOIN silver_transaction_products item
          ON item."ProdCat" = product."ProdCat"
         AND item."Subcategory" = product."Subcategory"
         AND item."ListingAge" = product."ListingAge"
         AND item."SellerPointer" = product."SellerPointer"
        GROUP BY product."ProdCat", product."Subcategory", product."ListingAge",
                 product."SellerPointer", product.product_availability
        ORDER BY seller_key, product_category, subcategory, listing_age
    """,
}

SILVER_ALIASES = {
    "markets": "silver_markets",
    "vendors": "silver_vendors",
    "buyers": "silver_buyers",
    "products": "silver_products",
    "transactions": "silver_transactions",
    "transaction_products": "silver_transaction_products",
    "BuyerSessionAnalytics": "silver_sessions",
    "PaymentProcessingEvents": "silver_payments",
    "risk_analytics": "silver_risk",
    "RiskModelPredictions": "silver_predictions",
}


def _quoted(identifier):
    return '"' + identifier.replace('"', '""') + '"'


def _sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _postgres_snapshot(connection, table, schema):
    columns = EXPECTED_COLUMNS[table]
    selection = ", ".join(_quoted(column) for column in columns)
    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {selection} FROM {_quoted(table)}")
        rows = cursor.fetchall()

    json_column = JSON_COLUMNS.get(table)
    records = []
    for row in rows:
        record = dict(zip(columns, row, strict=True))
        if json_column and record[json_column] is not None:
            record[json_column] = json.dumps(
                record[json_column], separators=(",", ":"), sort_keys=True
            )
        records.append(record)
    return pa.Table.from_pylist(records, schema=schema)


def _source_snapshot(settings, schemas):
    with psycopg.connect(**settings.pg_kwargs()) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_current_wal_lsn()::text, transaction_timestamp()")
            source_lsn, captured_at = cursor.fetchone()
        tables = {
            table: _postgres_snapshot(connection, table, schema)
            for table, schema in schemas.items()
        }
    return tables, {
        "captured_at": captured_at.isoformat(),
        "lsn": source_lsn,
        "rows": {table: data.num_rows for table, data in tables.items()},
    }


def _rewrite_json(connection, source, output, column):
    quoted = _quoted(column)
    connection.execute(f"""
        COPY (
            SELECT * REPLACE (CAST({quoted} AS VARCHAR) AS {quoted})
            FROM read_parquet({_sql_literal(source)})
        ) TO {_sql_literal(output)} (
            FORMAT parquet,
            COMPRESSION zstd,
            COMPRESSION_LEVEL 1,
            ROW_GROUP_SIZE 100000
        )
    """)


def _stage_history_files(
    connection, client, settings, iceberg_table, table, items, temp_dir
):
    uris = []
    json_column = JSON_COLUMNS.get(table)
    for index, item in enumerate(items):
        suffix = f"history-{index:05d}.parquet"
        bucket, key = table_key(iceberg_table, suffix)
        if bucket != settings.s3_bucket:
            raise RuntimeError(f"Iceberg table uses unexpected bucket: {bucket}")
        if json_column:
            source = Path(temp_dir) / f"{table}-{index:05d}-source.parquet"
            output = Path(temp_dir) / f"{table}-{index:05d}-iceberg.parquet"
            client.download_file(settings.s3_bucket, item["Key"], str(source))
            _rewrite_json(connection, source, output, json_column)
            client.upload_file(
                str(output),
                settings.s3_bucket,
                key,
                ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
            )
            source.unlink()
            output.unlink()
        else:
            client.copy_object(
                Bucket=settings.s3_bucket,
                Key=key,
                CopySource={"Bucket": settings.s3_bucket, "Key": item["Key"]},
                ContentType="application/vnd.apache.parquet",
                MetadataDirective="REPLACE",
            )
        uris.append(f"s3://{settings.s3_bucket}/{key}")
    return uris


def _stage_current_file(client, settings, iceberg_table, table, data, temp_dir):
    output = Path(temp_dir) / f"{table}-current.parquet"
    pq.write_table(data, output, compression="zstd", compression_level=1)
    bucket, key = table_key(iceberg_table, "current.parquet")
    if bucket != settings.s3_bucket:
        raise RuntimeError(f"Iceberg table uses unexpected bucket: {bucket}")
    client.upload_file(
        str(output),
        settings.s3_bucket,
        key,
        ExtraArgs={"ContentType": "application/vnd.apache.parquet"},
    )
    return f"s3://{settings.s3_bucket}/{key}"


def _build_silver(
    settings,
    batch_id,
    namespace,
    manifest,
    manifest_sha,
    history,
    schemas,
    source_tables,
    source_metadata,
    cdc_data,
    cdc_checkpoint,
    cdc_commit_count,
):
    iceberg_catalog = catalog(settings)
    ensure_namespace(iceberg_catalog, namespace)
    client = s3_client(settings)

    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute(
        f"SET memory_limit = {_sql_literal(settings.batch_duckdb_memory)}"
    )
    connection.execute("SET preserve_insertion_order = false")
    row_counts = {}
    data_files = {}
    try:
        with tempfile.TemporaryDirectory(prefix="kest-batch-") as temp_dir:
            for table in EXPECTED_COLUMNS:
                schema = schemas[table]
                identifier = (namespace, table)
                iceberg_table = iceberg_catalog.create_table(
                    identifier,
                    schema=schema,
                    properties={
                        "kest.layer": "silver",
                        "kest.batch-id": batch_id,
                        "kest.bronze-manifest-sha256": manifest_sha,
                        "kest.source-captured-at": source_metadata["captured_at"],
                        "kest.source-lsn": source_metadata["lsn"],
                        "write.parquet.compression-codec": "zstd",
                        "write.target-file-size-bytes": str(128 * 1024**2),
                    },
                )
                history_uris = []
                if table in FACT_TABLES:
                    history_uris = _stage_history_files(
                        connection,
                        client,
                        settings,
                        iceberg_table,
                        table,
                        history[table],
                        temp_dir,
                    )
                current = source_tables[table]
                if table in FACT_TABLES:
                    if current.num_rows:
                        history_uris.append(
                            _stage_current_file(
                                client,
                                settings,
                                iceberg_table,
                                table,
                                current,
                                temp_dir,
                            )
                        )
                    with iceberg_table.transaction() as transaction:
                        transaction.add_files(
                            history_uris,
                            snapshot_properties={
                                "kest.source": "bronze-history+postgres-snapshot"
                            },
                        )
                elif current.num_rows:
                    iceberg_table.append(
                        current,
                        snapshot_properties={"kest.source": "postgres-snapshot"},
                    )
                iceberg_table.refresh()
                expected = current.num_rows
                if table in FACT_TABLES:
                    expected += manifest["rows"][table]
                actual = record_count(iceberg_table)
                if actual != expected:
                    raise RuntimeError(
                        f"Silver {table} row count is {actual}; expected {expected}"
                    )
                row_counts[table] = actual
                data_files[table] = [
                    task.file.file_path for task in iceberg_table.scan().plan_files()
                ]
                print(f"Silver {table}: {actual:,} rows")

            cdc_table = iceberg_catalog.create_table(
                (namespace, CDC_TABLE),
                schema=cdc_data.schema,
                properties={
                    "kest.layer": "silver",
                    "kest.batch-id": batch_id,
                    "kest.cdc-through-lsn": cdc_checkpoint or "",
                    "kest.cdc-commit-count": str(cdc_commit_count),
                    "write.parquet.compression-codec": "zstd",
                },
            )
            if cdc_data.num_rows:
                cdc_table.append(
                    cdc_data, snapshot_properties={"kest.source": "committed-raw-cdc"}
                )
            row_counts[CDC_TABLE] = record_count(cdc_table)
            print(f"Silver {CDC_TABLE}: {cdc_data.num_rows:,} rows")
    finally:
        connection.close()
    return row_counts, data_files


def _build_gold(settings, batch_id, gold_namespace, silver_files):
    iceberg_catalog = catalog(settings)
    ensure_namespace(iceberg_catalog, gold_namespace)
    connection = duckdb.connect()
    connection.execute("SET threads = 1")
    connection.execute(
        f"SET memory_limit = {_sql_literal(settings.batch_duckdb_memory)}"
    )
    connection.execute("SET preserve_insertion_order = false")
    connection.execute("SET temp_directory = '/tmp/kest-duckdb-spill'")
    connection.execute("SET TimeZone = 'UTC'")
    filesystem = s3fs.S3FileSystem(
        key=settings.aws_access_key_id,
        secret=settings.aws_secret_access_key,
        client_kwargs={
            "endpoint_url": settings.s3_endpoint,
            "region_name": settings.aws_region,
        },
    )
    connection.register_filesystem(filesystem)
    row_counts = {}
    try:
        for table, alias in SILVER_ALIASES.items():
            paths = ", ".join(_sql_literal(path) for path in silver_files[table])
            connection.execute(
                f"CREATE VIEW {alias} AS SELECT * FROM read_parquet([{paths}])"
            )
        for table, query in GOLD_QUERIES.items():
            result = connection.execute(query).to_arrow_table()
            iceberg_table = iceberg_catalog.create_table(
                (gold_namespace, table),
                schema=result.schema,
                properties={
                    "kest.layer": "gold",
                    "kest.batch-id": batch_id,
                    "kest.silver-namespace": silver_files["_namespace"],
                    "write.parquet.compression-codec": "zstd",
                },
            )
            iceberg_table.append(
                result, snapshot_properties={"kest.source": "silver-batch"}
            )
            actual = record_count(iceberg_table)
            if actual != result.num_rows or actual == 0:
                raise RuntimeError(f"Gold {table} row count is invalid: {actual}")
            row_counts[table] = actual
            print(f"Gold {table}: {actual:,} rows")
    finally:
        connection.close()
    return row_counts


def run():
    settings = Settings.from_env()
    manifest, manifest_sha = load_manifest(settings)
    history = bronze_files(settings)
    if set(history) != TABLES:
        raise RuntimeError(f"Bronze tables differ: {sorted(history)}")
    schemas = {
        table: iceberg_arrow_schema(
            parquet_schema(s3_client(settings), settings.s3_bucket, items[0])
        )
        for table, items in history.items()
    }
    source_tables, source_metadata = _source_snapshot(settings, schemas)
    cdc_data, cdc_checkpoint, cdc_commit_count = committed_event_table(settings)
    _, expected_pointer_etag = load_current_pointer(settings, required=False)
    batch_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    version = batch_id.replace("-", "_").lower()
    silver_version = f"{settings.silver_namespace}_{version}"
    gold_version = f"{settings.gold_namespace}_{version}"
    iceberg_catalog = catalog(settings)
    published = False
    try:
        silver_rows, silver_files = _build_silver(
            settings,
            batch_id,
            silver_version,
            manifest,
            manifest_sha,
            history,
            schemas,
            source_tables,
            source_metadata,
            cdc_data,
            cdc_checkpoint,
            cdc_commit_count,
        )
        silver_files["_namespace"] = silver_version
        gold_rows = _build_gold(settings, batch_id, gold_version, silver_files)

        result = {
            "batch_id": batch_id,
            "bronze_manifest_sha256": manifest_sha,
            "cdc": {
                "commit_count": cdc_commit_count,
                "event_count": cdc_data.num_rows,
                "through_lsn": cdc_checkpoint,
            },
            "gold_namespace": gold_version,
            "gold_rows": gold_rows,
            "schema_version": 2,
            "silver_namespace": silver_version,
            "silver_rows": silver_rows,
            "source_snapshot": source_metadata,
        }
        manifest_key = write_batch_manifest(settings, batch_id, result)
        pointer = {
            "batch_id": batch_id,
            "gold_namespace": gold_version,
            "manifest_key": manifest_key,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "silver_namespace": silver_version,
        }
        publish_pointer(settings, pointer, expected_pointer_etag)
        published = True
    except Exception:
        if not published:
            remove_namespace(iceberg_catalog, gold_version)
            remove_namespace(iceberg_catalog, silver_version)
        raise

    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Build staged Silver and Gold Iceberg tables with DuckDB"
    )
    parser.parse_args()
    run()


if __name__ == "__main__":
    main()
