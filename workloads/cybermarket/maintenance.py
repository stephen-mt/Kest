import argparse
import re
from urllib.parse import urlparse

from kest.lakehouse.catalog import catalog
from workloads.cybermarket.config import Settings

LEGACY_VERSION = r"\d{8}t\d{6}z_[0-9a-f]{8}"


def legacy_namespace_pattern(settings):
    layers = "|".join(
        re.escape(name) for name in (settings.silver_namespace, settings.gold_namespace)
    )
    return re.compile(rf"^(?:{layers})_{LEGACY_VERSION}$")


def _verified_location(settings, table):
    location = table.location()
    parsed = urlparse(location)
    expected_prefix = settings.iceberg_prefix.strip("/") + "/"
    if (
        parsed.scheme != "s3"
        or parsed.netloc != settings.s3_bucket
        or not parsed.path.strip("/").startswith(expected_prefix)
    ):
        raise RuntimeError(
            f"Refusing to purge {table.name()}: location is outside "
            f"s3://{settings.s3_bucket}/{settings.iceberg_prefix}/: {location}"
        )
    return location


def legacy_namespace_plan(settings, iceberg_catalog):
    pattern = legacy_namespace_pattern(settings)
    plan = []
    for identifier in sorted(iceberg_catalog.list_namespaces()):
        if len(identifier) != 1 or not pattern.fullmatch(identifier[0]):
            continue
        tables = []
        for table_identifier in sorted(iceberg_catalog.list_tables(identifier)):
            table = iceberg_catalog.load_table(table_identifier)
            tables.append(
                {
                    "identifier": table_identifier,
                    "location": _verified_location(settings, table),
                }
            )
        plan.append({"namespace": identifier, "tables": tables})
    return plan


def clean_legacy_namespaces(settings, purge=False):
    iceberg_catalog = catalog(settings)
    plan = legacy_namespace_plan(settings, iceberg_catalog)
    mode = "PURGE" if purge else "DRY-RUN"
    if not plan:
        print(f"{mode}: no legacy Kest batch namespaces found")
        return []

    for item in plan:
        namespace = item["namespace"][0]
        print(f"{mode}: namespace {namespace}")
        for table in item["tables"]:
            name = ".".join(table["identifier"])
            print(f"{mode}: table {name} -> {table['location']}")

    if purge:
        for item in plan:
            for table in item["tables"]:
                iceberg_catalog.purge_table(table["identifier"])
            iceberg_catalog.drop_namespace(item["namespace"])
            print(f"PURGED: namespace {item['namespace'][0]}")
    return plan


def main():
    parser = argparse.ArgumentParser(
        description="Find or purge legacy versioned Kest Silver/Gold namespaces"
    )
    parser.add_argument(
        "--purge",
        action="store_true",
        help="purge the listed tables and namespaces; default is dry-run",
    )
    arguments = parser.parse_args()
    clean_legacy_namespaces(Settings.from_env(), purge=arguments.purge)


if __name__ == "__main__":
    main()
