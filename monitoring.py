"""
Shared monitoring helpers for the sync workers (review issues C7/C8):

  - heartbeat(worker, ok, detail): upserts a row into pipeline_heartbeats
    (created by backend migrations/010_core_overhaul.sql; created here too
    if missing so workers don't depend on backend deploy order). The
    backend's /api/ops/freshness endpoint reads this table, so a stalled or
    failing worker becomes visible - and alertable - instead of silently
    letting the data go stale.

  - alert(message): posts to a Discord webhook (ALERT_WEBHOOK_URL). Used
    for the failures a human must act on, e.g. the EA session token
    expiring (ea_price_sync stops updating prices entirely until someone
    pastes a fresh EA_X_UT_SID).

Both are best-effort: monitoring must never crash the worker it watches.
"""
import os
import logging
from typing import Optional

import aiohttp
import asyncpg

log = logging.getLogger("monitoring")

ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL")

_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_heartbeats (
    worker      TEXT PRIMARY KEY,
    last_run_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ok          BOOLEAN NOT NULL DEFAULT TRUE,
    detail      TEXT
)
"""


async def heartbeat(
    dsn_or_conn,
    worker: str,
    ok: bool = True,
    detail: Optional[str] = None,
) -> None:
    """Record that `worker` just ran. Accepts a DSN string or an open
    asyncpg connection (so callers can reuse one they already hold)."""
    try:
        own = isinstance(dsn_or_conn, str)
        conn = await asyncpg.connect(dsn_or_conn) if own else dsn_or_conn
        try:
            await conn.execute(_DDL)
            await conn.execute(
                """
                INSERT INTO pipeline_heartbeats (worker, last_run_at, ok, detail)
                VALUES ($1, NOW(), $2, $3)
                ON CONFLICT (worker) DO UPDATE
                    SET last_run_at = NOW(), ok = EXCLUDED.ok, detail = EXCLUDED.detail
                """,
                worker, ok, (detail or "")[:500] or None,
            )
        finally:
            if own:
                await conn.close()
    except Exception as e:
        log.warning("heartbeat(%s) failed: %s", worker, e)


async def alert(message: str) -> bool:
    """Best-effort Discord webhook alert. Returns True if delivered."""
    if not ALERT_WEBHOOK_URL:
        log.warning("ALERT (no webhook configured): %s", message)
        return False
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                ALERT_WEBHOOK_URL,
                json={"content": f"🚨 **auto_sync alert**\n{message}"[:1900]},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                return r.status in (200, 204)
    except Exception as e:
        log.warning("alert delivery failed: %s", e)
        return False
