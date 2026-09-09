import json

import duckdb
import s3fs

from workload.core.config import Settings
from workload.lakehouse.catalog import bronze_files


def _literal(value):
    return "'" + value.replace("'", "''") + "'"


def _view(connection, name, items, bucket):
    paths = ", ".join(_literal(f"s3://{bucket}/{item['Key']}") for item in items)
    connection.execute(f'CREATE VIEW "{name}" AS SELECT * FROM read_parquet([{paths}])')


def main():
    settings = Settings.from_env()
    files = bronze_files(settings)
    connection = duckdb.connect()
    connection.execute("SET threads = 2")
    connection.execute("SET memory_limit = '512MB'")
    connection.execute("SET preserve_insertion_order = false")
    connection.execute("SET temp_directory = '/tmp/kest-duckdb-spill'")
    connection.register_filesystem(
        s3fs.S3FileSystem(
            key=settings.aws_access_key_id,
            secret=settings.aws_secret_access_key,
            client_kwargs={
                "endpoint_url": settings.s3_endpoint,
                "region_name": settings.aws_region,
            },
        )
    )
    for table in (
        "transaction_products",
        "BuyerSessionAnalytics",
        "PaymentProcessingEvents",
        "risk_analytics",
        "RiskModelPredictions",
    ):
        _view(connection, table, files[table], settings.s3_bucket)

    checks = {
        "session_causality": """
            SELECT count(*) FROM "BuyerSessionAnalytics"
            WHERE cart_removals_count > cart_additions_count
               OR (checkout_completed AND NOT checkout_initiated)
               OR (bounce_indicator AND (
                    checkout_initiated OR checkout_completed
                    OR cart_additions_count > 0
               ))
        """,
        "payment_state": """
            SELECT count(*) FROM "PaymentProcessingEvents"
            WHERE (processing_stage = 'failed' AND (
                       amount_processed != 0 OR decline_reason IS NULL
                       OR retry_count < 1
                   ))
               OR (processing_stage = 'settled' AND (
                       abs(amount_processed - amount_requested) > 0.01
                       OR decline_reason IS NOT NULL OR retry_count != 0
                   ))
               OR processing_stage NOT IN ('failed', 'settled')
        """,
        "risk_label": """
            SELECT count(*) FROM risk_analytics
            WHERE "ML_Risk" != CASE
                WHEN round("FraudProb", 3) >= 0.7 THEN 'high'
                WHEN round("FraudProb", 3) >= 0.3 THEN 'medium'
                ELSE 'low'
            END
        """,
        "prediction_label": """
            SELECT count(*) FROM "RiskModelPredictions"
            WHERE risk_category_predicted != CASE
                WHEN round(fraud_probability, 3) >= 0.7 THEN 'high'
                WHEN round(fraud_probability, 3) >= 0.3 THEN 'medium'
                ELSE 'low'
            END
        """,
    }
    results = {}
    try:
        for name, query in checks.items():
            invalid = connection.execute(query).fetchone()[0]
            if invalid:
                raise AssertionError(
                    f"Bronze deep check {name}: {invalid} invalid rows"
                )
            results[name] = 0
        product_domain = connection.execute("""
            SELECT count(*) FROM (
                SELECT DISTINCT "ProdCat", "Subcategory", "ListingAge", "SellerPointer"
                FROM transaction_products
            )
        """).fetchone()[0]
        if product_domain != 4000:
            raise AssertionError(
                f"Bronze sells {product_domain} products; expected all 4000"
            )
        payment_stages = dict(
            connection.execute("""
                SELECT processing_stage, count(*)
                FROM "PaymentProcessingEvents"
                GROUP BY processing_stage
            """).fetchall()
        )
        if not payment_stages.get("failed") or not payment_stages.get("settled"):
            raise AssertionError("Bronze payment history lacks failures or settlements")
    finally:
        connection.close()

    print(
        json.dumps(
            {
                "invalid_rows": results,
                "payment_stages": payment_stages,
                "sold_product_domain": product_domain,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
