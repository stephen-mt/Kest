import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from kest_ingestion.config import Settings
from kest_ingestion.db import clickhouse_client

pytestmark = [pytest.mark.integration, pytest.mark.security]


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO gold.mart_daily_revenue SELECT * FROM gold.mart_daily_revenue LIMIT 1",
        "ALTER TABLE gold.mart_daily_revenue COMMENT COLUMN currency 'forbidden'",
        "DROP TABLE gold.mart_daily_revenue",
    ],
)
def test_da_cannot_mutate_gold(statement: str) -> None:
    client = clickhouse_client(Settings(), "da")
    with pytest.raises(DatabaseError):
        client.command(statement)


def test_pipeline_cannot_publish_gold() -> None:
    client = clickhouse_client(Settings(), "pipeline")
    with pytest.raises(DatabaseError):
        client.command("TRUNCATE TABLE gold.mart_daily_revenue")
