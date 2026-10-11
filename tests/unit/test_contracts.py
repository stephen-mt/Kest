from pathlib import Path

import pytest
from kest_ingestion.config import ROOT, Settings
from kest_ingestion.ingest import _content_identity, _csv_records
from kest_ingestion.models import SourceConfig
from pydantic import ValidationError


def test_content_identity_is_order_stable() -> None:
    first = {"orders": [{"order_id": "2", "amount": 2}, {"order_id": "1", "amount": 1}]}
    second = {"orders": [{"amount": 2, "order_id": "2"}, {"amount": 1, "order_id": "1"}]}
    assert _content_identity("postgres", first) == _content_identity("postgres", second)


def test_breaking_csv_is_rejected() -> None:
    path = ROOT / "ingestion" / "fixtures" / "product_catalog_breaking.csv"
    with pytest.raises(ValueError, match="unit_cost is not numeric"):
        _csv_records(path)


def test_source_config_forbids_private_dsl_fields() -> None:
    with pytest.raises(ValidationError):
        SourceConfig.model_validate(
            {
                "name": "campaigns",
                "base_url": "http://source",
                "endpoint": "/campaigns",
                "table_name": "campaigns",
                "primary_key": "id",
                "made_up_operator": "magic",
            }
        )


def test_runtime_directory_is_outside_tracked_source() -> None:
    assert Settings().runtime_dir == Path(ROOT / ".runtime")


def test_airflow_dags_use_task_sdk_instead_of_shell_wrappers() -> None:
    dag_dir = ROOT / "orchestration" / "dags"
    dag_source = "\n".join(path.read_text() for path in dag_dir.glob("*.py"))
    assert "BashOperator" not in dag_source
    assert "bash_command" not in dag_source
    assert "kest_ingestion.cli" not in dag_source
    assert not (dag_dir / "backup_restore_verify.py").exists()


def test_airflow_public_and_execution_urls_are_separate() -> None:
    compose = (ROOT / "compose.yaml").read_text()
    assert "AIRFLOW__API__BASE_URL: http://127.0.0.1:8080" in compose
    assert (
        "AIRFLOW__CORE__EXECUTION_API_SERVER_URL: "
        "http://airflow-api-server:8080/execution/"
    ) in compose
