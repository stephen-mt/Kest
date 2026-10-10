from pathlib import Path

import httpx
import pytest
from kest_ingestion.config import ROOT, Settings
from kest_ingestion.db import control_connection
from kest_ingestion.ingest import _csv_records, ingest_rest

pytestmark = pytest.mark.integration


def test_rest_429_retries_without_duplicate_manifest() -> None:
    settings = Settings()
    config = ROOT / "ingestion" / "config" / "campaigns.yml"
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
