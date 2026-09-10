import hashlib
import json

import pyarrow as pa
from pyiceberg.io.pyarrow import parquet_files_to_data_files

BRONZE_MANIFEST_PROPERTY = "kest.bronze-manifest-sha256"
BRONZE_BOOTSTRAP_PROPERTY = "kest.bronze-bootstrap-state"
CURRENT_FILES_PROPERTY = "kest.current-data-files"
DATA_FINGERPRINT_PROPERTY = "kest.data-sha256"


def arrow_fingerprint(data):
    """Return a deterministic digest for an Arrow table with stable row ordering."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, data.schema) as writer:
        writer.write_table(data)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def load_or_create_table(iceberg_catalog, identifier, schema, properties):
    if iceberg_catalog.table_exists(identifier):
        return iceberg_catalog.load_table(identifier), False
    table = iceberg_catalog.create_table(
        identifier,
        schema=schema,
        properties=properties,
    )
    return table, True


def current_snapshot_id(table):
    table.refresh()
    snapshot = table.current_snapshot()
    if snapshot is None:
        return None
    return snapshot.snapshot_id


def current_data_files(table):
    return [task.file.file_path for task in table.scan().plan_files()]


def _set_properties(transaction, properties):
    transaction.set_properties({key: str(value) for key, value in properties.items()})


def refresh_arrow_table(table, data, properties, snapshot_properties):
    """Replace a current/derived table, skipping the data commit if unchanged."""
    fingerprint = arrow_fingerprint(data)
    changed = table.properties.get(DATA_FINGERPRINT_PROPERTY) != fingerprint
    with table.transaction() as transaction:
        if changed:
            if table.current_snapshot() is None:
                transaction.append(data, snapshot_properties=snapshot_properties)
            else:
                transaction.overwrite(data, snapshot_properties=snapshot_properties)
        _set_properties(
            transaction,
            {**properties, DATA_FINGERPRINT_PROPERTY: fingerprint},
        )
    table.refresh()
    return changed


def _referenced_current_tasks(table):
    raw_paths = table.properties.get(CURRENT_FILES_PROPERTY)
    if raw_paths is None:
        raise RuntimeError(
            f"Stable fact table {table.name()} has no current-file provenance"
        )
    expected = set(json.loads(raw_paths))
    tasks = [
        task
        for task in table.scan().plan_files()
        if str(task.file.file_path) in expected
    ]
    found = {str(task.file.file_path) for task in tasks}
    if found != expected:
        missing = sorted(expected - found)
        raise RuntimeError(
            f"Stable fact table {table.name()} is missing current data files: {missing}"
        )
    return tasks


def refresh_fact_table(
    table,
    manifest_sha,
    current_data,
    properties,
    snapshot_properties,
    stage_history,
    stage_current,
):
    """Bootstrap Bronze once, then replace only the PostgreSQL current files."""
    bootstrap_state = table.properties.get(BRONZE_BOOTSTRAP_PROPERTY)
    recorded_manifest = table.properties.get(BRONZE_MANIFEST_PROPERTY)
    snapshot = table.current_snapshot()

    if bootstrap_state == "complete" and recorded_manifest != manifest_sha:
        raise RuntimeError(
            "Bronze history manifest changed after Silver bootstrap; "
            "incremental Bronze-history reconciliation is not implemented"
        )
    if bootstrap_state != "complete" and snapshot is not None:
        raise RuntimeError(
            f"Stable fact table {table.name()} has an incomplete Bronze bootstrap"
        )

    fingerprint = arrow_fingerprint(current_data)
    common_properties = {
        **properties,
        BRONZE_MANIFEST_PROPERTY: manifest_sha,
        DATA_FINGERPRINT_PROPERTY: fingerprint,
    }

    if bootstrap_state != "complete":
        history_paths = stage_history()
        current_paths = [stage_current()] if current_data.num_rows else []
        with table.transaction() as transaction:
            transaction.add_files(
                history_paths + current_paths,
                snapshot_properties=snapshot_properties,
            )
            _set_properties(
                transaction,
                {
                    **common_properties,
                    BRONZE_BOOTSTRAP_PROPERTY: "complete",
                    CURRENT_FILES_PROPERTY: json.dumps(current_paths),
                },
            )
        table.refresh()
        return True, True

    if table.properties.get(DATA_FINGERPRINT_PROPERTY) == fingerprint:
        with table.transaction() as transaction:
            _set_properties(transaction, common_properties)
        table.refresh()
        return False, False

    previous_current = _referenced_current_tasks(table)
    current_paths = [stage_current()] if current_data.num_rows else []
    new_data_files = list(
        parquet_files_to_data_files(
            io=table.io,
            table_metadata=table.metadata,
            file_paths=iter(current_paths),
        )
    )
    with table.transaction() as transaction:
        with transaction.update_snapshot(
            snapshot_properties=snapshot_properties
        ).overwrite() as overwrite_files:
            for task in previous_current:
                overwrite_files.delete_data_file(task.file)
            for data_file in new_data_files:
                overwrite_files.append_data_file(data_file)
        _set_properties(
            transaction,
            {
                **common_properties,
                BRONZE_BOOTSTRAP_PROPERTY: "complete",
                CURRENT_FILES_PROPERTY: json.dumps(current_paths),
            },
        )
    table.refresh()
    return True, False
