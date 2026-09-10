import json

import pyarrow as pa

from workloads.cybermarket.ingestion.commits import load_committed_events

CDC_TABLE = "cdc_events"
CDC_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("table_name", pa.string(), nullable=False),
        pa.field("operation", pa.string()),
        pa.field("source_lsn", pa.string(), nullable=False),
        pa.field("source_xid", pa.string(), nullable=False),
        pa.field("source_timestamp", pa.string()),
        pa.field("ingested_at", pa.string()),
        pa.field("change_json", pa.string(), nullable=False),
    ]
)


def committed_event_table(settings):
    events, commits = load_committed_events(settings)
    rows = []
    for event in events:
        change = event["change"]
        rows.append(
            {
                "event_id": event["event_id"],
                "table_name": change.get("table", "_metadata"),
                "operation": change.get("action"),
                "source_lsn": event["source"]["lsn"],
                "source_xid": event["source"]["xid"],
                "source_timestamp": change.get("timestamp"),
                "ingested_at": event.get("ingested_at"),
                "change_json": json.dumps(
                    change, separators=(",", ":"), sort_keys=True
                ),
            }
        )
    rows.sort(key=lambda row: (lsn_int(row["source_lsn"]), row["event_id"]))
    checkpoint = commits[-1]["source"]["last_lsn"] if commits else None
    return pa.Table.from_pylist(rows, schema=CDC_SCHEMA), checkpoint, len(commits)


def lsn_int(value):
    high, low = value.split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)
