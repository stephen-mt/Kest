import gzip
import hashlib
import json
from collections import defaultdict

import psycopg

from workload.core.storage import s3_client
from workload.landing.commits import put_immutable, sha256

DECODER_OPTIONS = (
    "'format-version', '2', "
    "'include-xids', 'true', "
    "'include-timestamp', 'true', "
    "'include-lsn', 'true', "
    "'include-types', 'true', "
    "'include-transaction', 'false'"
)


class LandingWriter:
    def __init__(self, settings):
        self.settings = settings
        self.s3 = s3_client(settings)
        self.connection = psycopg.connect(**settings.pg_kwargs(), autocommit=True)

    def close(self):
        self.connection.close()

    def ensure_slot(self):
        with self.connection.cursor() as cursor:
            cursor.execute(
                "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                (self.settings.cdc_slot,),
            )
            row = cursor.fetchone()
            if row and row[0] != "wal2json":
                raise RuntimeError(
                    f"Slot {self.settings.cdc_slot} uses {row[0]}, expected wal2json"
                )
            if not row:
                cursor.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')",
                    (self.settings.cdc_slot,),
                )
                cursor.fetchone()
                print(f"Created logical replication slot {self.settings.cdc_slot}.")

    def peek(self):
        statement = f"""
            SELECT lsn::text, xid::text, data
            FROM pg_logical_slot_peek_changes(
                %s, NULL, %s, {DECODER_OPTIONS}
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                statement, (self.settings.cdc_slot, self.settings.cdc_batch_size)
            )
            return cursor.fetchall()

    def acknowledge(self, expected_rows):
        statement = f"""
            SELECT lsn::text, xid::text, data
            FROM pg_logical_slot_get_changes(
                %s, NULL, %s, {DECODER_OPTIONS}
            )
        """
        with self.connection.cursor() as cursor:
            cursor.execute(statement, (self.settings.cdc_slot, len(expected_rows)))
            acknowledged = cursor.fetchall()
        if acknowledged != expected_rows:
            raise RuntimeError("WAL changes acknowledged do not match the landed batch")
        return len(acknowledged)

    def land_batch(self, rows):
        batch_material = "\n".join(
            f"{lsn}|{xid}|{raw_data}" for lsn, xid, raw_data in rows
        ).encode()
        batch_id = hashlib.sha256(batch_material).hexdigest()
        groups = defaultdict(list)
        for lsn, xid, raw_data in rows:
            change = json.loads(raw_data)
            table = change.get("table", "_metadata")
            event_id = hashlib.sha256(
                f"{self.settings.pg_database}|{lsn}|{xid}|{raw_data}".encode()
            ).hexdigest()
            groups[table].append(
                {
                    "schema_version": 1,
                    "event_id": event_id,
                    # wal2json's transaction timestamp is stable across retries.
                    "ingested_at": change.get("timestamp"),
                    "source": {
                        "connector": "postgresql-wal2json",
                        "service": "postgres-source",
                        "database": self.settings.pg_database,
                        "lsn": lsn,
                        "xid": xid,
                    },
                    "change": change,
                }
            )

        objects = []
        for table, events in sorted(groups.items()):
            key = (
                f"{self.settings.landing_prefix}/data/{table}/batch-{batch_id}.jsonl.gz"
            )
            payload = gzip.compress(
                b"".join(
                    json.dumps(event, separators=(",", ":"), sort_keys=True).encode()
                    + b"\n"
                    for event in events
                ),
                mtime=0,
            )
            put_immutable(
                self.s3,
                self.settings.s3_bucket,
                key,
                payload,
                ContentType="application/x-ndjson",
                ContentEncoding="gzip",
                Metadata={
                    "first-lsn": rows[0][0],
                    "last-lsn": rows[-1][0],
                    "event-count": str(len(events)),
                    "sha256": sha256(payload),
                },
            )
            objects.append(
                {
                    "event_count": len(events),
                    "key": key,
                    "sha256": sha256(payload),
                    "table": table,
                }
            )
            print(
                f"Landed {len(events):4d} events to s3://{self.settings.s3_bucket}/{key}"
            )

        commit = {
            "batch_id": batch_id,
            "event_count": sum(len(events) for events in groups.values()),
            "objects": objects,
            "schema_version": 2,
            "source": {
                "database": self.settings.pg_database,
                "first_lsn": rows[0][0],
                "first_lsn_int": _lsn_int(rows[0][0]),
                "last_lsn": rows[-1][0],
                "last_lsn_int": _lsn_int(rows[-1][0]),
            },
        }
        commit_payload = json.dumps(
            commit, separators=(",", ":"), sort_keys=True
        ).encode()
        commit_key = f"{self.settings.landing_prefix}/commits/{batch_id}.json"
        put_immutable(
            self.s3,
            self.settings.s3_bucket,
            commit_key,
            commit_payload,
            ContentType="application/json",
            Metadata={"event-count": str(commit["event_count"])},
        )

        acknowledged = self.acknowledge(rows)
        print(f"Acknowledged {acknowledged} WAL changes through LSN {rows[-1][0]}.")
        return sum(len(events) for events in groups.values())


def _lsn_int(value):
    high, low = value.split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)
