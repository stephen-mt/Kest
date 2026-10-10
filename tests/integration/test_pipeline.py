import hashlib

import pytest
from botocore.exceptions import ClientError
from kest_ingestion.config import Settings
from kest_ingestion.db import clickhouse_client, control_connection
from kest_ingestion.ingest import _s3_client
from kest_ingestion.loader import load_batch
from kest_ingestion.publish import MARTS

pytestmark = pytest.mark.integration


def test_ingestion_replay_is_noop() -> None:
    settings = Settings()
    client = clickhouse_client(settings, "pipeline")
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT batch_id::text FROM batch_manifest WHERE source='postgres' AND status='LOADED' "
            "ORDER BY created_at LIMIT 1"
        )
        batch_id = cur.fetchone()[0]
    before = {
        table: client.query(f"SELECT count() FROM raw.{table}").first_row[0]
        for table in ("orders", "events", "campaign_spend")
    }
    assert load_batch(settings, batch_id) is False
    assert load_batch(settings, batch_id) is False
    assert load_batch(settings, batch_id) is False
    after = {table: client.query(f"SELECT count() FROM raw.{table}").first_row[0] for table in before}
    assert before == after


def test_gold_is_queryable_and_manifested() -> None:
    settings = Settings()
    client = clickhouse_client(settings, "da")
    assert all(client.query(f"SELECT count() FROM gold.{mart}").first_row[0] > 0 for mart in MARTS)
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(DISTINCT dataset) FROM publication_manifest")
        assert cur.fetchone()[0] == 3


def test_seaweed_reader_policy_and_round_trip() -> None:
    settings = Settings()
    writer = _s3_client(settings)
    reader = _s3_client(settings, writer=False)
    payload = b"kest-s3-policy-test"
    key = "compatibility/tests/policy.bin"
    writer.put_object(
        Bucket="raw", Key=key, Body=payload, Metadata={"sha256": hashlib.sha256(payload).hexdigest()}
    )
    assert reader.get_object(Bucket="raw", Key=key)["Body"].read() == payload
    with pytest.raises(ClientError):
        reader.put_object(Bucket="raw", Key=f"{key}.denied", Body=b"denied")
