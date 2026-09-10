from urllib.parse import urlparse

from pyiceberg.catalog import load_catalog


def catalog(settings):
    return load_catalog(settings.iceberg_catalog)


def table_key(table, suffix):
    location = urlparse(table.location())
    if location.scheme != "s3" or not location.netloc:
        raise RuntimeError(f"Unexpected Iceberg table location: {table.location()}")
    return location.netloc, f"{location.path.strip('/')}/data/{suffix}"


def ensure_namespace(iceberg_catalog, namespace):
    identifier = (namespace,)
    if identifier not in iceberg_catalog.list_namespaces():
        iceberg_catalog.create_namespace(identifier)


def record_count(table, snapshot_id=None):
    scan = (
        table.scan(snapshot_id=snapshot_id) if snapshot_id is not None else table.scan()
    )
    return sum(task.file.record_count for task in scan.plan_files())
