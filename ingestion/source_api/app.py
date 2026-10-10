from __future__ import annotations

import asyncio
from datetime import datetime
from threading import Lock

import psycopg
from fastapi import FastAPI, HTTPException, Query, Response
from kest_ingestion.config import Settings

app = FastAPI(title="Kest marketing source", version="1.0.0")
settings = Settings()
failure_lock = Lock()
failure_mode = "none"
failure_remaining = 0


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/admin/failure/{mode}")
def configure_failure(mode: str, count: int = Query(default=1, ge=0, le=20)) -> dict[str, object]:
    if mode not in {"none", "429", "timeout", "500"}:
        raise HTTPException(400, "mode must be none, 429, timeout, or 500")
    global failure_mode, failure_remaining
    with failure_lock:
        failure_mode, failure_remaining = mode, count
    return {"mode": mode, "remaining": count}


async def inject_failure(response: Response) -> None:
    global failure_remaining
    with failure_lock:
        mode = failure_mode
        should_fail = failure_remaining > 0
        if should_fail:
            failure_remaining -= 1
    if not should_fail:
        return
    if mode == "timeout":
        await asyncio.sleep(15)
    if mode == "429":
        response.headers["Retry-After"] = "1"
        raise HTTPException(429, "simulated rate limit", headers={"Retry-After": "1"})
    if mode == "500":
        raise HTTPException(500, "simulated upstream error")


async def paged_table(
    table: str,
    order_column: str,
    response: Response,
    cursor: str | None,
    updated_since: datetime | None,
    limit: int,
) -> dict[str, object]:
    await inject_failure(response)
    clauses = []
    params: list[object] = []
    if cursor:
        clauses.append(f"{order_column} > %s")
        params.append(cursor)
    if updated_since:
        clauses.append("updated_at >= %s")
        params.append(updated_since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"SELECT * FROM {table} {where} ORDER BY {order_column} LIMIT %s"  # noqa: S608
    params.append(limit)
    with psycopg.connect(settings.business_dsn, row_factory=psycopg.rows.dict_row) as conn:
        rows = list(conn.execute(query, params).fetchall())
    next_cursor = str(rows[-1][order_column]) if len(rows) == limit else None
    return {"items": rows, "next_cursor": next_cursor}


@app.get("/campaigns")
async def campaigns(
    response: Response,
    cursor: str | None = None,
    updated_since: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
):
    return await paged_table("marketing_campaigns", "campaign_id", response, cursor, updated_since, limit)


@app.get("/campaign-spend")
async def campaign_spend(
    response: Response,
    cursor: str | None = None,
    updated_since: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
):
    return await paged_table("campaign_spend", "spend_id", response, cursor, updated_since, limit)
