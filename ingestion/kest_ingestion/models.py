from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class SourceConfig(BaseModel):
    """Small validated wrapper around the native dlt REST source fields we use."""

    model_config = ConfigDict(extra="forbid")
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    base_url: str
    endpoint: str
    table_name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    primary_key: str
    updated_at: str = "updated_at"
    data_path: str = "items"
    cursor_param: str = "cursor"
    next_cursor_path: str = "next_cursor"
    page_size: int = Field(default=100, ge=1, le=1000)
    timeout_seconds: int = Field(default=10, ge=1, le=60)
    max_retries: int = Field(default=4, ge=0, le=10)


class BatchObject(BaseModel):
    table_name: str
    object_path: str
    checksum: str
    size_bytes: int
    row_count: int


class BatchResult(BaseModel):
    batch_id: str
    source: str
    object_prefix: str
    checksum: str
    row_count: int
    watermark: dict[str, Any]
    objects: list[BatchObject]
    reused: bool = False


def json_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value
