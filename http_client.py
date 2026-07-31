"""
Shared HTTP client for auto_sync's scraping workers: a smoothed global
token-bucket rate limiter, a persisted global circuit breaker, jittered
exponential backoff, and adaptive batch sizing - all backed by Postgres
rows (crawler_rate_state / crawler_circuit_breaker / crawler_metrics, see
backend/migrations/037_scrape_queue.sql) since each worker is a separate
Railway Cron container with no shared in-process memory.

Consolidates the near-identical `_get_with_retry` implementations
previously duplicated across bin_sales_history_sync.py,
futbin_card_art_backfill.py, and futbin_rarity_backfill.py, extended with:
  - a smoothed requests/sec limiter (replaces "burst up to a fixed
    concurrency semaphore, per process, with no cross-process budget")
  - a global, persisted circuit breaker (trip on ~20 consecutive 429s or
    ~5 consecutive 403s; cooldown survives container restarts so future
    Cron ticks respect it, per the explicit requirement)
  - jittered backoff (avoids every worker retrying in lockstep)
  - adaptive batch sizing (shrink under rising errors/latency, grow back
    after several clean runs)

The circuit breaker is intentionally global, not per-worktype: bin/sales/
metadata workers all hit the same futbin.com IP, so a trip on one must
stop all of them, not just the worker that noticed.
"""
import asyncio
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import aiohttp
import asyncpg

import config

log = logging.getLogger("http_client")

HEADERS = {"User-Agent": config.HTTP_USER_AGENT}
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=config.HTTP_TIMEOUT_SECONDS)

GLOBAL_SCOPE = "global"

# Defensive, same idiom as bin_sales_history_sync.py's own ensure_tables():
# workers don't depend on backend's migration 037 having run first.
_DDL = """
CREATE TABLE IF NOT EXISTS scrape_queue (
    card_id              BIGINT NOT NULL,
    worktype              TEXT NOT NULL CHECK (worktype IN ('bin', 'sales', 'metadata')),
    priority               INT NOT NULL DEFAULT 0,
    next_due_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at        TIMESTAMPTZ,
    last_success_at        TIMESTAMPTZ,
    consecutive_failures   INT NOT NULL DEFAULT 0,
    failure_reason         TEXT,
    failure_expires_at     TIMESTAMPTZ,
    newest_known_sale_at   TIMESTAMPTZ,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (card_id, worktype)
);
CREATE INDEX IF NOT EXISTS idx_scrape_queue_claim
    ON scrape_queue (worktype, priority DESC, next_due_at ASC);
CREATE INDEX IF NOT EXISTS idx_scrape_queue_failure_expiry
    ON scrape_queue (failure_expires_at) WHERE failure_expires_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS crawler_rate_state (
    scope              TEXT PRIMARY KEY,
    tokens_available    DOUBLE PRECISION NOT NULL,
    requests_per_sec    DOUBLE PRECISION NOT NULL,
    burst_capacity      DOUBLE PRECISION NOT NULL,
    last_refill_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawler_circuit_breaker (
    scope               TEXT PRIMARY KEY,
    tripped_at           TIMESTAMPTZ,
    cooldown_until       TIMESTAMPTZ,
    trip_reason          TEXT,
    consecutive_429      INT NOT NULL DEFAULT 0,
    consecutive_403      INT NOT NULL DEFAULT 0,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS crawler_metrics (
    id                   BIGSERIAL PRIMARY KEY,
    worktype              TEXT NOT NULL,
    started_at            TIMESTAMPTZ NOT NULL,
    finished_at           TIMESTAMPTZ,
    batch_size            INT NOT NULL,
    succeeded             INT NOT NULL DEFAULT 0,
    failed_429            INT NOT NULL DEFAULT 0,
    failed_403            INT NOT NULL DEFAULT 0,
    failed_other          INT NOT NULL DEFAULT 0,
    cache_hits            INT NOT NULL DEFAULT 0,
    avg_latency_ms         DOUBLE PRECISION,
    queue_depth_at_start   INT
);
CREATE INDEX IF NOT EXISTS idx_crawler_metrics_worktype_time
    ON crawler_metrics (worktype, started_at DESC);
"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def ensure_crawler_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(_DDL)
    await conn.execute(
        "INSERT INTO crawler_rate_state (scope, tokens_available, requests_per_sec, burst_capacity) "
        "VALUES ($1, $2, $3, $3) ON CONFLICT (scope) DO NOTHING",
        GLOBAL_SCOPE, config.RATE_LIMIT_REQUESTS_PER_SEC, config.RATE_LIMIT_BURST_CAPACITY,
    )
    await conn.execute(
        "INSERT INTO crawler_circuit_breaker (scope) VALUES ($1) ON CONFLICT (scope) DO NOTHING",
        GLOBAL_SCOPE,
    )


class CircuitOpenError(Exception):
    """Raised when the global circuit breaker is tripped. Callers should
    let this propagate out of the current batch, release any locks, log
    why, and exit successfully (a trip is an intentional stop, not a
    crash) - never treat it as a per-card failure to retry."""


async def check_circuit(pool: asyncpg.Pool, scope: str = GLOBAL_SCOPE) -> None:
    row = await pool.fetchrow(
        "SELECT cooldown_until, trip_reason FROM crawler_circuit_breaker WHERE scope = $1", scope
    )
    if row and row["cooldown_until"] and row["cooldown_until"] > _now():
        raise CircuitOpenError(f"tripped ({row['trip_reason']}), cooldown until {row['cooldown_until']}")


async def _record_response(pool: asyncpg.Pool, status: int, scope: str = GLOBAL_SCOPE) -> None:
    """Updates the shared 429/403 streak counters; any 2xx resets both.
    Trips (sets cooldown_until) once a threshold is crossed."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                "SELECT consecutive_429, consecutive_403 FROM crawler_circuit_breaker "
                "WHERE scope = $1 FOR UPDATE",
                scope,
            )
            c429 = row["consecutive_429"] if row else 0
            c403 = row["consecutive_403"] if row else 0

            if 200 <= status < 300:
                c429, c403 = 0, 0
            elif status == 429:
                c429 += 1
            elif status == 403:
                c403 += 1

            trip_reason = None
            if c429 >= config.CIRCUIT_BREAKER_429_THRESHOLD:
                trip_reason = f"{c429} consecutive HTTP 429s"
            elif c403 >= config.CIRCUIT_BREAKER_403_THRESHOLD:
                trip_reason = f"{c403} consecutive HTTP 403s"

            if trip_reason:
                cooldown_until = _now() + timedelta(minutes=config.CIRCUIT_BREAKER_COOLDOWN_MINUTES)
                await conn.execute(
                    """
                    UPDATE crawler_circuit_breaker SET
                        tripped_at = now(), cooldown_until = $2, trip_reason = $3,
                        consecutive_429 = $4, consecutive_403 = $5, updated_at = now()
                    WHERE scope = $1
                    """,
                    scope, cooldown_until, trip_reason, c429, c403,
                )
                log.warning("Circuit breaker tripped (%s) - cooling down until %s", trip_reason, cooldown_until)
            else:
                await conn.execute(
                    """
                    UPDATE crawler_circuit_breaker SET
                        consecutive_429 = $2, consecutive_403 = $3, updated_at = now()
                    WHERE scope = $1
                    """,
                    scope, c429, c403,
                )


async def acquire_token(pool: asyncpg.Pool, scope: str = GLOBAL_SCOPE) -> None:
    """Blocks until a token is available from the shared bucket. This is
    smoothed pacing (continuous refill from elapsed wall-clock time since
    last_refill_at), not burst-then-sleep - every worker process draws
    from the same row, so total request rate across bin/sales/metadata
    workers combined stays at RATE_LIMIT_REQUESTS_PER_SEC, not that rate
    per process."""
    while True:
        async with pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT tokens_available, requests_per_sec, burst_capacity, last_refill_at "
                    "FROM crawler_rate_state WHERE scope = $1 FOR UPDATE",
                    scope,
                )
                if row is None:
                    return  # not initialized yet - fail open rather than block forever
                now = _now()
                elapsed = max((now - row["last_refill_at"]).total_seconds(), 0.0)
                tokens = min(
                    row["burst_capacity"],
                    row["tokens_available"] + elapsed * row["requests_per_sec"],
                )
                if tokens >= 1.0:
                    await conn.execute(
                        "UPDATE crawler_rate_state SET tokens_available = $2, last_refill_at = $3 WHERE scope = $1",
                        scope, tokens - 1.0, now,
                    )
                    return
                await conn.execute(
                    "UPDATE crawler_rate_state SET tokens_available = $2, last_refill_at = $3 WHERE scope = $1",
                    scope, tokens, now,
                )
                wait = (1.0 - tokens) / row["requests_per_sec"] if row["requests_per_sec"] > 0 else 1.0
        await asyncio.sleep(min(max(wait, 0.05), 5.0))


def _jittered(base: float) -> float:
    jitter = base * config.BACKOFF_JITTER_FRACTION
    return max(0.0, base + random.uniform(-jitter, jitter))


async def get_with_retry(
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    url: str,
    diag: Dict[str, Any],
    scope: str = GLOBAL_SCOPE,
) -> "tuple[int, Optional[str]]":
    """Drop-in replacement for the duplicated `_get_with_retry` in
    bin_sales_history_sync.py / futbin_card_art_backfill.py /
    futbin_rarity_backfill.py - same 429-aware retry contract, now backed
    by the shared rate limiter + circuit breaker instead of a purely
    in-process semaphore. Raises CircuitOpenError immediately (no request
    attempted) if the breaker is tripped; callers should let that
    propagate and end the run cleanly rather than catching it per-card."""
    backoff = config.BACKOFF_BASE_SECONDS
    for attempt in range(config.MAX_RETRIES + 1):
        await check_circuit(pool, scope)
        await acquire_token(pool, scope)

        start = time.monotonic()
        try:
            async with session.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
                latency_ms = (time.monotonic() - start) * 1000
                diag.setdefault("_latency_samples", []).append(latency_ms)
                await _record_response(pool, r.status, scope)

                if r.status == 429:
                    diag["http_429_hits"] += 1
                    if attempt < config.MAX_RETRIES:
                        retry_after = r.headers.get("Retry-After")
                        wait = (
                            float(retry_after)
                            if retry_after and retry_after.replace(".", "", 1).isdigit()
                            else _jittered(backoff)
                        )
                        await asyncio.sleep(wait)
                        backoff = min(backoff * 2, config.BACKOFF_MAX_SECONDS)
                        continue
                    return 429, None
                if r.status == 403:
                    diag["http_403_hits"] += 1
                    return 403, None
                if r.status != 200:
                    return r.status, None
                return 200, await r.text()
        except CircuitOpenError:
            raise
        except Exception:
            if attempt < config.MAX_RETRIES:
                await asyncio.sleep(_jittered(backoff))
                backoff = min(backoff * 2, config.BACKOFF_MAX_SECONDS)
                continue
            diag["http_exceptions"] += 1
            return 0, None
    return 0, None


async def start_metrics_run(pool: asyncpg.Pool, worktype: str, batch_size: int, queue_depth_at_start: int) -> int:
    return await pool.fetchval(
        "INSERT INTO crawler_metrics (worktype, started_at, batch_size, queue_depth_at_start) "
        "VALUES ($1, now(), $2, $3) RETURNING id",
        worktype, batch_size, queue_depth_at_start,
    )


async def finish_metrics_run(pool: asyncpg.Pool, run_id: int, diag: Dict[str, Any]) -> None:
    latencies = diag.get("_latency_samples") or []
    avg_latency = sum(latencies) / len(latencies) if latencies else None
    await pool.execute(
        """
        UPDATE crawler_metrics SET
            finished_at = now(),
            succeeded = $2, failed_429 = $3, failed_403 = $4, failed_other = $5,
            cache_hits = $6, avg_latency_ms = $7
        WHERE id = $1
        """,
        run_id,
        diag.get("succeeded", 0), diag.get("http_429_hits", 0), diag.get("http_403_hits", 0),
        diag.get("failed_other", 0), diag.get("cache_hits", 0), avg_latency,
    )


async def compute_batch_size(pool: asyncpg.Pool, worktype: str) -> int:
    """Shrinks the next batch when the most recent run shows a rising
    error rate; grows it back toward BATCH_SIZE_MAX only after several
    consecutive clean runs. Reads crawler_metrics rather than keeping
    in-process state, since each Cron invocation is a fresh container."""
    rows = await pool.fetch(
        """
        SELECT batch_size, succeeded, failed_429, failed_403, failed_other
        FROM crawler_metrics
        WHERE worktype = $1 AND finished_at IS NOT NULL
        ORDER BY started_at DESC
        LIMIT $2
        """,
        worktype, config.BATCH_SIZE_HEALTHY_RUNS_TO_GROW,
    )
    if not rows:
        return config.BATCH_SIZE_DEFAULT

    def _error_rate(r) -> float:
        total = r["succeeded"] + r["failed_429"] + r["failed_403"] + r["failed_other"]
        errors = r["failed_429"] + r["failed_403"] + r["failed_other"]
        return errors / total if total else 0.0

    last = rows[0]
    if _error_rate(last) > config.BATCH_SIZE_ERROR_RATE_THRESHOLD:
        return max(config.BATCH_SIZE_MIN, int(last["batch_size"] * config.BATCH_SIZE_SHRINK_FACTOR))

    if len(rows) >= config.BATCH_SIZE_HEALTHY_RUNS_TO_GROW and all(
        _error_rate(r) <= config.BATCH_SIZE_ERROR_RATE_THRESHOLD for r in rows
    ):
        return min(config.BATCH_SIZE_MAX, int(last["batch_size"] * config.BATCH_SIZE_GROW_FACTOR))

    return last["batch_size"]
