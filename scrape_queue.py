"""
Persistent, prioritized, per-card-per-worktype scheduling (see
backend/migrations/037_scrape_queue.sql). Replaces bin_sales_history_sync.py's
"re-scan the whole Tier A/B candidate table every invocation" design with
incremental claiming: each run pulls a bounded, priority-ordered batch via
`FOR UPDATE SKIP LOCKED` - the row-level equivalent of this codebase's
existing pg_try_advisory_lock idiom - so a run that gets circuit-broken
partway through simply leaves the rest of the queue for the next tick,
with no lost work and no re-scraping of already-fresh cards.
"""
import logging
from typing import Dict, List, Optional

import asyncpg

import config

log = logging.getLogger("scrape_queue")


async def claim_batch(pool: asyncpg.Pool, worktype: str, limit: int) -> List[asyncpg.Record]:
    """Claims up to `limit` due rows for `worktype`, highest-priority and
    oldest-due first, skipping rows a concurrent worker already holds."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT card_id, worktype, priority, newest_known_sale_at
                FROM scrape_queue
                WHERE worktype = $1
                  AND next_due_at <= now()
                  AND (failure_expires_at IS NULL OR failure_expires_at < now())
                ORDER BY priority DESC, next_due_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
                """,
                worktype, limit,
            )
            if rows:
                await conn.executemany(
                    "UPDATE scrape_queue SET last_attempt_at = now(), updated_at = now() "
                    "WHERE card_id = $1 AND worktype = $2",
                    [(r["card_id"], worktype) for r in rows],
                )
            return rows


async def mark_success(
    pool: asyncpg.Pool, card_id: int, worktype: str, next_delay_minutes: int,
    newest_known_sale_at=None,
) -> None:
    await pool.execute(
        """
        UPDATE scrape_queue SET
            last_success_at = now(),
            consecutive_failures = 0,
            failure_reason = NULL,
            failure_expires_at = NULL,
            next_due_at = now() + ($3 || ' minutes')::interval,
            newest_known_sale_at = COALESCE($4, newest_known_sale_at),
            updated_at = now()
        WHERE card_id = $1 AND worktype = $2
        """,
        card_id, worktype, str(next_delay_minutes), newest_known_sale_at,
    )


async def mark_failure(
    pool: asyncpg.Pool, card_id: int, worktype: str, reason: str,
    ttl_minutes: Optional[int] = None, retry_delay_minutes: int = 10,
) -> None:
    """Records a failure. When `ttl_minutes` is set (404 / no-market-page /
    missing-sales-link - the failure classes explicitly called out in the
    brief), the row is excluded from future claims until the TTL expires
    instead of being retried every cycle for something unlikely to change
    soon. Pass `ttl_minutes=None` for a transient failure (e.g. a timeout)
    that should simply be retried after `retry_delay_minutes`."""
    effective_ttl = ttl_minutes if ttl_minutes is not None else 0
    await pool.execute(
        """
        UPDATE scrape_queue SET
            consecutive_failures = consecutive_failures + 1,
            failure_reason = $3,
            failure_expires_at = CASE WHEN $4 > 0 THEN now() + ($4 || ' minutes')::interval ELSE NULL END,
            next_due_at = now() + ($5 || ' minutes')::interval,
            updated_at = now()
        WHERE card_id = $1 AND worktype = $2
        """,
        card_id, worktype, reason, str(effective_ttl), str(retry_delay_minutes),
    )


async def upsert_candidates(pool: asyncpg.Pool, worktype: str, priorities: Dict[int, int]) -> None:
    """Backfills new cards into the queue (or bumps an existing row's
    priority) - called by metadata_worker.py's periodic recompute pass.
    Never resets next_due_at/failure state for a row that already exists;
    only inserts new rows or updates `priority`."""
    if not priorities:
        return
    rows = [(card_id, worktype, pr) for card_id, pr in priorities.items()]
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO scrape_queue (card_id, worktype, priority)
            VALUES ($1, $2, $3)
            ON CONFLICT (card_id, worktype) DO UPDATE
                SET priority = EXCLUDED.priority, updated_at = now()
            """,
            rows,
        )


async def queue_depth(pool: asyncpg.Pool, worktype: str) -> int:
    return await pool.fetchval(
        "SELECT count(*) FROM scrape_queue WHERE worktype = $1 AND next_due_at <= now() "
        "AND (failure_expires_at IS NULL OR failure_expires_at < now())",
        worktype,
    )


async def estimated_hours_to_full_cycle(pool: asyncpg.Pool, worktype: str) -> Optional[float]:
    """queue_depth / observed throughput, for the metrics dashboard - how
    long a full pass over the currently-due population would take at the
    rate the last several runs actually achieved."""
    depth = await queue_depth(pool, worktype)
    row = await pool.fetchrow(
        """
        SELECT sum(succeeded) AS succeeded, sum(extract(epoch from (finished_at - started_at))) AS secs
        FROM (
            SELECT succeeded, started_at, finished_at
            FROM crawler_metrics
            WHERE worktype = $1 AND finished_at IS NOT NULL
            ORDER BY started_at DESC
            LIMIT 10
        ) recent
        """,
        worktype,
    )
    if not row or not row["succeeded"] or not row["secs"]:
        return None
    rate_per_hour = row["succeeded"] / (row["secs"] / 3600.0)
    return depth / rate_per_hour if rate_per_hour > 0 else None
