import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from kest_ingestion.cli import dev_init
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


@pytest.mark.parametrize("developer", ["alice", "bob"])
def test_developers_are_isolated_and_cannot_write_gold(developer: str) -> None:
    settings = Settings()
    dev_init(settings, developer)
    values = dict(
        line.split("=", 1)
        for line in (settings.runtime_dir / "developers" / f"{developer}.env").read_text().splitlines()
    )
    client = clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=f"de_{developer}",
        password=values["DBT_DEV_PASSWORD"],
    )
    client.command(f"CREATE TABLE IF NOT EXISTS dev_{developer}.isolation_probe (id UInt8) ENGINE=Memory")
    with pytest.raises(DatabaseError):
        client.command("CREATE TABLE gold.forbidden_probe (id UInt8) ENGINE=Memory")
