from pathlib import Path

import httpx
import pytest
from kest_ingestion.config import ROOT, Settings
from kest_ingestion.db import control_connection
from kest_ingestion.ingest import _csv_records, ingest_rest
from kest_ingestion.publish import publish_mart

pytestmark = pytest.mark.integration


@pytest.mark.demo
def test_rest_429_retries_without_duplicate_manifest() -> None:
    settings = Settings()
    config = ROOT / "ingestion" / "examples" / "config" / "campaigns.yml"
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(batch_id::text) FROM batch_manifest WHERE source='campaigns'")
        before = cur.fetchone()
    httpx.post(
        f"{settings.source_api_url}/admin/failure/429", params={"count": 2}, timeout=5
    ).raise_for_status()
    result = ingest_rest(settings, config)
    with control_connection(settings) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(batch_id::text) FROM batch_manifest WHERE source='campaigns'")
        after = cur.fetchone()
    assert result is not None and result.reused
    assert before == after


def test_breaking_csv_fails_before_checkpoint() -> None:
    with pytest.raises(ValueError, match="contract failed"):
        _csv_records(Path(ROOT / "ingestion" / "fixtures" / "product_catalog_breaking.csv"))


def test_second_publisher_is_rejected_without_manifest() -> None:
    settings = Settings()
    dataset = "mart_daily_revenue"
    with control_connection(settings, autocommit=True) as lock_conn, lock_conn.cursor() as lock_cur:
        lock_cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (f"publish:{dataset}",))
        with control_connection(settings) as count_conn, count_conn.cursor() as count_cur:
            count_cur.execute("SELECT count(*) FROM publication_manifest WHERE dataset=%s", (dataset,))
            before = count_cur.fetchone()[0]
        with pytest.raises(RuntimeError, match="Another publisher owns the lock"):
            publish_mart(settings, dataset, "00000000-0000-0000-0000-000000000000")
        with control_connection(settings) as count_conn, count_conn.cursor() as count_cur:
            count_cur.execute("SELECT count(*) FROM publication_manifest WHERE dataset=%s", (dataset,))
            after = count_cur.fetchone()[0]
        assert after == before
        lock_cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (f"publish:{dataset}",))
