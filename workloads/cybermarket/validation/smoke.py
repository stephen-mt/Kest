import json
from collections import Counter

import psycopg

from workloads.cybermarket.config import Settings
from workloads.cybermarket.ingestion.cdc import run as run_cdc
from workloads.cybermarket.ingestion.commits import load_commits, load_committed_events
from workloads.cybermarket.ingestion.generator import run as run_generator
from workloads.cybermarket.ingestion.writer import LandingWriter
from workloads.cybermarket.source.schema import TABLES


def lsn_int(value):
    high, low = value.split("/", 1)
    return (int(high, 16) << 32) + int(low, 16)


def row_counts(connection):
    with connection.cursor() as cursor:
        result = {}
        for table in TABLES:
            cursor.execute(f'SELECT count(*) FROM "{table}"')
            result[table] = cursor.fetchone()[0]
        return result


def live_id_counts(connection):
    identities = {
        "BuyerSessionAnalytics": ("BSA_id", "BSA-LIVE-%"),
        "transactions": ("EventCode", "EVT-LIVE-%"),
        "PaymentProcessingEvents": ("PPE_id", "PPE-LIVE-%"),
        "RiskModelPredictions": ("RMP_id", "RMP-LIVE-%"),
    }
    with connection.cursor() as cursor:
        result = {}
        for table, (column, pattern) in identities.items():
            cursor.execute(
                f'SELECT count(*) FROM "{table}" WHERE "{column}" LIKE %s',
                (pattern,),
            )
            result[table] = cursor.fetchone()[0]
        return result


def main():
    settings = Settings.from_env()

    writer = LandingWriter(settings)
    try:
        writer.ensure_slot()
    finally:
        writer.close()
    run_cdc(follow=False)
    before_commits = {commit["batch_id"] for commit in load_commits(settings)}

    with psycopg.connect(**settings.pg_kwargs()) as connection:
        before_rows = row_counts(connection)
        before_live_ids = live_id_counts(connection)

    counts = run_generator(duration=1.0)
    expected_events = Counter(
        {
            "buyer_session": 8,
            "purchase": 6,
            "payment_update": 3,
            "risk_prediction": 2,
            "transaction_status_update": 1,
        }
    )
    if counts != expected_events:
        raise AssertionError(f"Unexpected one-second event mix: {counts}")

    landed = run_cdc(follow=False)
    if not 58 <= landed <= 62:
        raise AssertionError(f"Expected 58-62 causal row changes, landed {landed}")

    with psycopg.connect(**settings.pg_kwargs()) as connection:
        after_rows = row_counts(connection)
        after_live_ids = live_id_counts(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (settings.cdc_slot,),
            )
            slot = cursor.fetchone()
            if slot != (False,):
                raise AssertionError(
                    f"CDC slot should be inactive after the test: {slot}"
                )

    expected_deltas = {
        "BuyerSessionAnalytics": 8,
        "transactions": 6,
        "transaction_products": 12,
        "PaymentProcessingEvents": 6,
        "risk_analytics": 6,
        "RiskModelPredictions": 2,
    }
    for table, delta in expected_deltas.items():
        actual = after_rows[table] - before_rows[table]
        if actual != delta:
            raise AssertionError(f"{table}: expected +{delta}, got +{actual}")

    expected_live_ids = {
        "BuyerSessionAnalytics": 8,
        "transactions": 6,
        "PaymentProcessingEvents": 6,
        "RiskModelPredictions": 2,
    }
    for table, delta in expected_live_ids.items():
        actual = after_live_ids[table] - before_live_ids[table]
        if actual != delta:
            raise AssertionError(
                f"{table}: expected +{delta} canonical live IDs, got {actual}"
            )

    all_events, commits = load_committed_events(settings)
    new_commits = [
        commit for commit in commits if commit["batch_id"] not in before_commits
    ]
    if not new_commits:
        raise AssertionError("CDC produced no completion manifest")
    new_event_ids = {
        event["event_id"]
        for event in all_events
        if any(
            event["source"]["lsn"] == commit["source"]["first_lsn"]
            or commit["source"]["first_lsn_int"]
            <= lsn_int(event["source"]["lsn"])
            <= commit["source"]["last_lsn_int"]
            for commit in new_commits
        )
    }
    raw_events = [event for event in all_events if event["event_id"] in new_event_ids]
    if len(raw_events) != landed:
        raise AssertionError(f"Expected {landed} raw events, read {len(raw_events)}")
    if len({event["event_id"] for event in raw_events}) != landed:
        raise AssertionError("Raw CDC event IDs are not unique")
    if any(event["change"].get("table") not in TABLES for event in raw_events):
        raise AssertionError("Landing contains a change outside the selected 10 tables")

    print(
        json.dumps(
            {
                "business_events": sum(counts.values()),
                "landing_commits": len(new_commits),
                "raw_changes": len(raw_events),
                "slot_active": False,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
