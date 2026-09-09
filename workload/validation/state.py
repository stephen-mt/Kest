import argparse
import json

from workload.core.config import Settings
from workload.lakehouse.catalog import catalog
from workload.validation.batch import check_batch
from workload.validation.postgres import check_postgres
from workload.validation.storage import (
    check_cdc,
    check_history,
)

PHASES = ("setup", "history", "cdc", "batch")


def verify(phase):
    settings = Settings.from_env()
    check_postgres(settings, require_slot=phase == "cdc")

    result = {
        "bucket": settings.s3_bucket,
        "phase": phase,
        "postgres_tables": 10,
    }
    if phase == "history":
        total, parquet_count = check_history(settings)
        result.update(
            bronze_gib=round(total / 1024**3, 3),
            parquet_objects=parquet_count,
        )
    if phase == "cdc":
        result.update(cdc_slot_active=False)
        result.update(check_cdc(settings))
    if phase == "batch":
        result.update(check_batch(settings))

    namespaces = catalog(settings).list_namespaces()
    result["iceberg_namespaces"] = len(namespaces)
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Validate a CyberMarket lifecycle phase"
    )
    parser.add_argument("--phase", choices=PHASES, default="setup")
    args = parser.parse_args()
    verify(args.phase)


if __name__ == "__main__":
    main()
