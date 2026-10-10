from __future__ import annotations

import clickhouse_connect
import psycopg

from kest_ingestion.config import Settings


def business_connection(settings: Settings):
    return psycopg.connect(settings.business_dsn)


def control_connection(settings: Settings, *, autocommit: bool = False):
    return psycopg.connect(settings.control_dsn, autocommit=autocommit)


def clickhouse_client(settings: Settings, user: str = "pipeline"):
    passwords = {
        "default": settings.clickhouse_admin_password,
        "pipeline": settings.clickhouse_pipeline_password,
        "publisher": settings.clickhouse_publish_password,
        "da": settings.clickhouse_da_password,
    }
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=user,
        password=passwords[user],
    )
