"""
Incremental BIN-price (+ piggybacked bio-stats) worker - the queue-driven
replacement for bin_sales_history_sync.py's BIN-fetch half. Claims a
bounded, priority-ordered batch from scrape_queue (worktype='bin') via
scrape_queue.claim_batch() instead of re-scanning the whole Tier A/B
candidate table every invocation, and lets the shared circuit breaker /
rate limiter in http_client.py decide how hard to push against futbin
rather than a fixed per-process concurrency semaphore alone.

Bio stats (games played, avg goals, top chem style) stay piggybacked on
this same page fetch rather than becoming their own worker - per explicit
decision, since parse_bio_stats already runs against the identical HTML
parse_lowest_bin uses; splitting it out would double requests against
futbin for the same data, working against the whole point of this
redesign.

Deployed as a Railway Cron Job - same one-shot-per-invocation model as
the script it replaces. A circuit-breaker trip mid-run ends the batch
early (remaining claimed rows stay claimed but unprocessed - their
next_due_at was already bumped by claim_batch, so they's simply picked up
by a later run, not lost).
"""
import asyncio
import logging
import os
import sys
from collections import defaultdict
from typing import Any, Dict

import asyncpg
import aiohttp

import config
import http_client
import scrape_queue as queue
from bin_sales_history_sync import parse_lowest_bin, parse_bio_stats
from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bin_worker")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found!")

WORKTYPE = "bin"
SUCCESS_REFRESH_MINUTES = int(os.getenv("BIN_SUCCESS_REFRESH_MINUTES", "20"))
CONCURRENCY = int(os.getenv("BIN_WORKER_CONCURRENCY", "5"))


async def _scrape_one(pool: asyncpg.Pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore, row, diag: Dict[str, Any]) -> None:
    card_id = row["card_id"]
    async with sem:
        player_url_row = await pool.fetchrow("SELECT player_url FROM fut_players WHERE card_id = $1", card_id)
        player_url = player_url_row["player_url"] if player_url_row else None
        if not player_url or "futbin.com" not in player_url:
            diag["stale_non_futbin_url"] += 1
            await queue.mark_failure(pool, card_id, WORKTYPE, "no_futbin_url", ttl_minutes=config.FAILURE_CACHE_TTL_MINUTES)
            return

        any_price_found = False
        bio_by_platform: Dict[str, Dict[str, Any]] = {}
        for platform in ("ps", "pc"):
            status, html = await http_client.get_with_retry(pool, session, player_url, diag)
            if status != 200 or html is None:
                diag["bin_failed"] += 1
                if status == 404:
                    await queue.mark_failure(pool, card_id, WORKTYPE, "404", ttl_minutes=config.FAILURE_CACHE_TTL_MINUTES)
                continue

            bin_price = parse_lowest_bin(html, platform, diag)
            bio_by_platform[platform] = parse_bio_stats(html, platform)
            if bin_price is not None:
                await pool.execute(
                    "INSERT INTO bin_history (player_id, platform, lowest_bin, captured_at) VALUES ($1, $2, $3, NOW())",
                    card_id, platform, bin_price,
                )
                diag["bin_price_found"] += 1
                any_price_found = True
            else:
                diag["bin_price_null"] += 1

        console_bio = bio_by_platform.get("ps") or {}
        pc_bio = bio_by_platform.get("pc") or {}
        if any(v is not None for v in (
            console_bio.get("games"), console_bio.get("avg_goals"), console_bio.get("top_chem_style"),
            pc_bio.get("games"), pc_bio.get("avg_goals"), pc_bio.get("top_chem_style"),
        )):
            await pool.execute(
                """
                UPDATE fut_players SET
                    games_played_console = COALESCE($1, games_played_console),
                    avg_goals_console = COALESCE($2, avg_goals_console),
                    top_chem_style_console = COALESCE($3, top_chem_style_console),
                    games_played_pc = COALESCE($4, games_played_pc),
                    avg_goals_pc = COALESCE($5, avg_goals_pc),
                    top_chem_style_pc = COALESCE($6, top_chem_style_pc)
                WHERE card_id = $7
                """,
                console_bio.get("games"), console_bio.get("avg_goals"), console_bio.get("top_chem_style"),
                pc_bio.get("games"), pc_bio.get("avg_goals"), pc_bio.get("top_chem_style"),
                card_id,
            )
            diag["bio_stats_updated"] += 1

        if any_price_found:
            diag["succeeded"] += 1
            await queue.mark_success(pool, card_id, WORKTYPE, SUCCESS_REFRESH_MINUTES)
        else:
            diag["failed_other"] += 1
            await queue.mark_failure(pool, card_id, WORKTYPE, "bin_price_null", ttl_minutes=None, retry_delay_minutes=10)


async def run_once() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=CONCURRENCY + 2)
    diag: Dict[str, Any] = defaultdict(int)
    run_id = None
    try:
        async with pool.acquire() as conn:
            await http_client.ensure_crawler_tables(conn)

        depth = await queue.queue_depth(pool, WORKTYPE)
        batch_size = await http_client.compute_batch_size(pool, WORKTYPE)
        run_id = await http_client.start_metrics_run(pool, WORKTYPE, batch_size, depth)

        rows = await queue.claim_batch(pool, WORKTYPE, batch_size)
        log.info("bin_worker claimed %d/%d due rows (batch_size=%d)", len(rows), depth, batch_size)
        if not rows:
            return

        sem = asyncio.Semaphore(CONCURRENCY)
        async with aiohttp.ClientSession() as session:
            try:
                await asyncio.gather(*[_scrape_one(pool, session, sem, r, diag) for r in rows])
            except http_client.CircuitOpenError as e:
                log.warning("Circuit breaker open - stopping run early: %s", e)
                diag["circuit_open"] = 1

        log.info(
            "bin_worker run complete. claimed=%d succeeded=%d bin_price_found=%d bin_price_null=%d "
            "bin_failed=%d bin_platform_scoped_hit=%d bin_platform_fallback_used=%d bio_stats_updated=%d "
            "http_429_hits=%d http_403_hits=%d http_exceptions=%d circuit_open=%s",
            len(rows), diag["succeeded"], diag["bin_price_found"], diag["bin_price_null"], diag["bin_failed"],
            diag["bin_platform_scoped_hit"], diag["bin_platform_fallback_used"], diag["bio_stats_updated"],
            diag["http_429_hits"], diag["http_403_hits"], diag["http_exceptions"], bool(diag.get("circuit_open")),
        )

        run_ok = not diag.get("circuit_open") and diag["succeeded"] > 0
        await heartbeat(
            pool, "bin_worker", ok=run_ok,
            detail=(
                f"claimed={len(rows)} succeeded={diag['succeeded']} bin_found={diag['bin_price_found']} "
                f"http_429={diag['http_429_hits']} circuit_open={bool(diag.get('circuit_open'))}"
            ),
        )
        if diag.get("circuit_open"):
            await alert(f"bin_worker: circuit breaker tripped mid-run after {diag['succeeded']} successes - backing off.")
        elif not run_ok:
            await alert(f"bin_worker: every claimed card failed this run ({len(rows)} attempted).")
    finally:
        if run_id is not None:
            await http_client.finish_metrics_run(pool, run_id, diag)
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_once())
    except Exception as e:
        log.error("run_once() failed: %s", e)
        sys.exit(1)
    sys.exit(0)
