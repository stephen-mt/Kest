import io
import unittest
from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa

from workload.lakehouse.maintenance import (
    clean_legacy_namespaces,
    legacy_namespace_plan,
)
from workload.lakehouse.tables import (
    BRONZE_BOOTSTRAP_PROPERTY,
    BRONZE_MANIFEST_PROPERTY,
    CURRENT_FILES_PROPERTY,
    DATA_FINGERPRINT_PROPERTY,
    refresh_arrow_table,
    refresh_fact_table,
)
from workload.pipelines import batch


class FakeDataFile:
    def __init__(self, path, rows=0):
        self.file_path = path
        self.record_count = rows


class FakeTask:
    def __init__(self, data_file):
        self.file = data_file


class FakeScan:
    def __init__(self, table):
        self.table = table

    def plan_files(self):
        return [FakeTask(data_file) for data_file in self.table.files]


class FakeSnapshot:
    def __init__(self, snapshot_id):
        self.snapshot_id = snapshot_id


class FakeDelete:
    def __init__(self, table):
        self.table = table
        self.deleted = []
        self.appended = []

    def __enter__(self):
        return self

    def delete_data_file(self, data_file):
        self.deleted.append(data_file)

    def append_data_file(self, data_file):
        self.appended.append(data_file)

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.table.files = [
                item for item in self.table.files if item not in self.deleted
            ]
            self.table.files.extend(self.appended)
            self.table.advance_snapshot()


class FakeUpdateSnapshot:
    def __init__(self, table):
        self.table = table

    def delete(self):
        return FakeDelete(self.table)

    def overwrite(self):
        return FakeDelete(self.table)


class FakeTransaction:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def set_properties(self, properties):
        self.table.properties.update(properties)

    def add_files(self, paths, snapshot_properties=None):
        del snapshot_properties
        self.table.files.extend(
            FakeDataFile(path, self.table.path_rows[path]) for path in paths
        )
        self.table.advance_snapshot()

    def overwrite(self, data, snapshot_properties=None):
        del snapshot_properties
        self.table.files = [FakeDataFile("generated.parquet", data.num_rows)]
        self.table.advance_snapshot()

    def append(self, data, snapshot_properties=None):
        del snapshot_properties
        self.table.files.extend([FakeDataFile("generated.parquet", data.num_rows)])
        self.table.advance_snapshot()

    def update_snapshot(self, snapshot_properties=None):
        del snapshot_properties
        return FakeUpdateSnapshot(self.table)


class FakeTable:
    def __init__(self, name=("silver", "transactions")):
        self._name = name
        self.properties = {}
        self.files = []
        self.path_rows = {}
        self.snapshot_number = 0
        self.io = object()
        self.metadata = object()

    def name(self):
        return self._name

    def current_snapshot(self):
        return FakeSnapshot(self.snapshot_number) if self.snapshot_number else None

    def advance_snapshot(self):
        self.snapshot_number += 1

    def transaction(self):
        return FakeTransaction(self)

    def refresh(self):
        return self

    def scan(self):
        return FakeScan(self)


class StableTableTest(unittest.TestCase):
    def test_fact_bootstraps_bronze_once_and_replaces_only_current_files(self):
        table = FakeTable()
        table.path_rows.update(
            {
                "s3://bucket/history.parquet": 100,
                "s3://bucket/current-1.parquet": 2,
                "s3://bucket/current-2.parquet": 3,
            }
        )
        history_calls = 0
        current_paths = iter(
            ["s3://bucket/current-1.parquet", "s3://bucket/current-2.parquet"]
        )

        def stage_history():
            nonlocal history_calls
            history_calls += 1
            return ["s3://bucket/history.parquet"]

        first = pa.table({"id": [1, 2]})
        changed, bootstrapped = refresh_fact_table(
            table,
            "manifest-a",
            first,
            {"kest.batch-id": "batch-1"},
            {"kest.source": "test"},
            stage_history,
            lambda: next(current_paths),
        )
        first_snapshot = table.snapshot_number
        self.assertTrue(changed)
        self.assertTrue(bootstrapped)
        self.assertEqual(history_calls, 1)
        self.assertEqual(sum(item.record_count for item in table.files), 102)

        changed, bootstrapped = refresh_fact_table(
            table,
            "manifest-a",
            first,
            {"kest.batch-id": "batch-2"},
            {"kest.source": "test"},
            stage_history,
            lambda: next(current_paths),
        )
        self.assertFalse(changed)
        self.assertFalse(bootstrapped)
        self.assertEqual(history_calls, 1)
        self.assertEqual(table.snapshot_number, first_snapshot)
        self.assertEqual(sum(item.record_count for item in table.files), 102)

        def data_files(**arguments):
            return [
                FakeDataFile(path, table.path_rows[path])
                for path in arguments["file_paths"]
            ]

        with patch(
            "workload.lakehouse.tables.parquet_files_to_data_files",
            side_effect=data_files,
        ):
            changed, bootstrapped = refresh_fact_table(
                table,
                "manifest-a",
                pa.table({"id": [1, 2, 3]}),
                {"kest.batch-id": "batch-3"},
                {"kest.source": "test"},
                stage_history,
                lambda: next(current_paths),
            )
        self.assertTrue(changed)
        self.assertFalse(bootstrapped)
        self.assertEqual(history_calls, 1)
        self.assertEqual(sum(item.record_count for item in table.files), 103)
        self.assertEqual(
            [item.file_path for item in table.files],
            ["s3://bucket/history.parquet", "s3://bucket/current-2.parquet"],
        )

    def test_fact_rejects_changed_bronze_manifest(self):
        table = FakeTable()
        table.properties.update(
            {
                BRONZE_BOOTSTRAP_PROPERTY: "complete",
                BRONZE_MANIFEST_PROPERTY: "manifest-a",
                CURRENT_FILES_PROPERTY: "[]",
                DATA_FINGERPRINT_PROPERTY: "old",
            }
        )
        table.advance_snapshot()
        with self.assertRaisesRegex(RuntimeError, "manifest changed"):
            refresh_fact_table(
                table,
                "manifest-b",
                pa.table({"id": []}, schema=pa.schema([("id", pa.int64())])),
                {},
                {},
                Mock(),
                Mock(),
            )

    def test_current_table_overwrite_is_idempotent(self):
        table = FakeTable(("gold", "daily_market_metrics"))
        data = pa.table({"count": [10]})
        self.assertTrue(refresh_arrow_table(table, data, {}, {}))
        snapshot = table.snapshot_number
        self.assertFalse(refresh_arrow_table(table, data, {}, {}))
        self.assertEqual(table.snapshot_number, snapshot)
        self.assertTrue(refresh_arrow_table(table, pa.table({"count": [11]}), {}, {}))
        self.assertGreater(table.snapshot_number, snapshot)
        self.assertEqual(sum(item.record_count for item in table.files), 1)


class BatchPublicationTest(unittest.TestCase):
    def settings(self):
        return SimpleNamespace(
            s3_bucket="mini-cybet",
            silver_namespace="silver",
            gold_namespace="gold",
        )

    def run_patches(self, fail_gold=False):
        settings = self.settings()
        history = {name: [{"Key": f"bronze/{name}.parquet"}] for name in batch.TABLES}
        silver_result = (
            {name: 1 for name in (*batch.EXPECTED_COLUMNS, "cdc_events")},
            {name: [f"s3://bucket/{name}.parquet"] for name in batch.EXPECTED_COLUMNS},
            {
                name: index + 1
                for index, name in enumerate((*batch.EXPECTED_COLUMNS, "cdc_events"))
            },
        )
        gold_result = (
            {name: 1 for name in batch.GOLD_QUERIES},
            {name: index + 100 for index, name in enumerate(batch.GOLD_QUERIES)},
        )
        published = Mock()
        stack = [
            patch.object(batch.Settings, "from_env", return_value=settings),
            patch.object(
                batch, "load_manifest", return_value=({"rows": {}}, "manifest")
            ),
            patch.object(batch, "bronze_files", return_value=history),
            patch.object(
                batch, "parquet_schema", return_value=pa.schema([("id", pa.int64())])
            ),
            patch.object(
                batch, "iceberg_arrow_schema", side_effect=lambda schema: schema
            ),
            patch.object(batch, "s3_client", return_value=Mock()),
            patch.object(
                batch,
                "_source_snapshot",
                return_value=({}, {"captured_at": "now", "lsn": "0/1", "rows": {}}),
            ),
            patch.object(
                batch,
                "committed_event_table",
                return_value=(pa.table({"event_id": []}), None, 0),
            ),
            patch.object(batch, "load_current_pointer", return_value=(None, "etag")),
            patch.object(batch, "_build_silver", return_value=silver_result),
            patch.object(
                batch,
                "_build_gold",
                side_effect=RuntimeError("gold failed") if fail_gold else None,
                return_value=gold_result,
            ),
            patch.object(
                batch,
                "write_batch_manifest",
                return_value="iceberg/_kest_batches/batch.json",
            ),
            patch.object(batch, "publish_pointer", published),
        ]
        return stack, published

    def test_two_runs_use_only_stable_namespaces_and_publish_snapshot_ids(self):
        stacks, published = self.run_patches()
        with ExitStack() as context:
            mocks = [context.enter_context(item) for item in stacks]
            silver = mocks[9]
            gold = mocks[10]
            batch.run()
            batch.run()

        self.assertEqual(
            [call.args[2] for call in silver.call_args_list], ["silver", "silver"]
        )
        self.assertEqual(
            [call.args[2] for call in gold.call_args_list], ["gold", "gold"]
        )
        self.assertEqual(published.call_count, 2)
        for call in published.call_args_list:
            pointer = call.args[1]
            self.assertEqual(pointer["silver_namespace"], "silver")
            self.assertEqual(pointer["gold_namespace"], "gold")
            self.assertIn("silver_snapshots", pointer)
            self.assertIn("gold_snapshots", pointer)

    def test_failed_table_work_never_advances_pointer(self):
        stacks, published = self.run_patches(fail_gold=True)
        with ExitStack() as context:
            for item in stacks:
                context.enter_context(item)
            with self.assertRaisesRegex(RuntimeError, "gold failed"):
                batch.run()
        published.assert_not_called()


class FakeMaintenanceTable:
    def __init__(self, identifier, location):
        self.identifier = identifier
        self._location = location

    def name(self):
        return self.identifier

    def location(self):
        return self._location


class FakeMaintenanceCatalog:
    def __init__(self, outside=False):
        self.namespaces = {
            ("bronze",),
            ("silver",),
            ("gold",),
            ("silver_20260910t120000z_deadbeef",),
            ("gold_manual",),
        }
        location = (
            "s3://mini-cybet/elsewhere/table"
            if outside
            else "s3://mini-cybet/iceberg/table"
        )
        identifier = ("silver_20260910t120000z_deadbeef", "transactions")
        self.tables = {identifier: FakeMaintenanceTable(identifier, location)}
        self.purged = []
        self.dropped = []

    def list_namespaces(self):
        return self.namespaces

    def list_tables(self, namespace):
        return [identifier for identifier in self.tables if identifier[:1] == namespace]

    def load_table(self, identifier):
        return self.tables[identifier]

    def purge_table(self, identifier):
        self.purged.append(identifier)

    def drop_namespace(self, identifier):
        self.dropped.append(identifier)


class MaintenanceTest(unittest.TestCase):
    def settings(self):
        return SimpleNamespace(
            silver_namespace="silver",
            gold_namespace="gold",
            iceberg_prefix="iceberg",
            s3_bucket="mini-cybet",
        )

    def test_cleanup_matches_only_old_versioned_namespaces_and_defaults_to_dry_run(
        self,
    ):
        iceberg_catalog = FakeMaintenanceCatalog()
        plan = legacy_namespace_plan(self.settings(), iceberg_catalog)
        self.assertEqual(
            [item["namespace"] for item in plan],
            [("silver_20260910t120000z_deadbeef",)],
        )
        with (
            patch(
                "workload.lakehouse.maintenance.catalog", return_value=iceberg_catalog
            ),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            clean_legacy_namespaces(self.settings())
        self.assertEqual(iceberg_catalog.purged, [])
        self.assertEqual(iceberg_catalog.dropped, [])

    def test_cleanup_requires_safe_locations_before_purge(self):
        iceberg_catalog = FakeMaintenanceCatalog(outside=True)
        with self.assertRaisesRegex(RuntimeError, "outside"):
            legacy_namespace_plan(self.settings(), iceberg_catalog)
        self.assertEqual(iceberg_catalog.purged, [])

    def test_explicit_purge_removes_only_the_reviewed_plan(self):
        iceberg_catalog = FakeMaintenanceCatalog()
        with (
            patch(
                "workload.lakehouse.maintenance.catalog", return_value=iceberg_catalog
            ),
            patch("sys.stdout", new_callable=io.StringIO),
        ):
            clean_legacy_namespaces(self.settings(), purge=True)
        self.assertEqual(
            iceberg_catalog.purged,
            [("silver_20260910t120000z_deadbeef", "transactions")],
        )
        self.assertEqual(
            iceberg_catalog.dropped,
            [("silver_20260910t120000z_deadbeef",)],
        )


if __name__ == "__main__":
    unittest.main()
