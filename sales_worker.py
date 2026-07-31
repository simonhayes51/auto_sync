"""
Incremental sales-history worker - the queue-driven replacement for
bin_sales_history_sync.py's sales-history half. Claims a bounded,
priority-ordered batch from scrape_queue (worktype='sales'), and pages
futbin's sales-history table only until it reaches a sale at-or-before
the card's own `newest_known_sale_at` cursor (persisted on the
scrape_queue row) - since futbin lists sales newest-first, once a card is
caught up most runs recognize "nothing new" after the very first row
instead of always parsing a fixed-depth page.
"""
import asyncio
import logging
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

import config
import http_client
import scrape_queue as queue
from bin_sales_history_sync import parse_sales_table
from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sales_worker")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found!")

WORKTYPE = "sales"
SUCCESS_REFRESH_MINUTES = int(os.getenv("SALES_SUCCESS_REFRESH_MINUTES", "30"))
CONCURRENCY = int(os.getenv("SALES_WORKER_CONCURRENCY", "5"))

_SALES_LINK_RE = re.compile(r"\bmarket-grid-lates-sale-link\b")


async def _resolve_sales_url(pool: asyncpg.Pool, session: aiohttp.ClientSession, player_url: str, diag: Dict[str, Any]) -> Optional[str]:
    market_url = player_url.rstrip("/") + "/market"
    status, html = await http_client.get_with_retry(pool, session, market_url, diag)
    if status != 200 or html is None:
        diag["sales_market_fetch_failed"] += 1
        return None
    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", class_=_SALES_LINK_RE)
    if not a or not a.get("href"):
        diag["sales_no_history_link"] += 1
        return None
    path = a["href"].split("?")[0]
    return f"https://www.futbin.com{path}?platform=ps"


async def _scrape_one(pool: asyncpg.Pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore, row, diag: Dict[str, Any]) -> None:
    card_id = row["card_id"]
    newest_known_sale_at = row["newest_known_sale_at"]
    async with sem:
        player_url_row = await pool.fetchrow("SELECT player_url FROM fut_players WHERE card_id = $1", card_id)
        player_url = player_url_row["player_url"] if player_url_row else None
        if not player_url or "futbin.com" not in player_url:
            await queue.mark_failure(pool, card_id, WORKTYPE, "no_futbin_url", ttl_minutes=config.FAILURE_CACHE_TTL_MINUTES)
            return

        url = await _resolve_sales_url(pool, session, player_url, diag)
        if not url:
            await queue.mark_failure(pool, card_id, WORKTYPE, "no_sales_link", ttl_minutes=config.FAILURE_CACHE_TTL_MINUTES)
            return

        status, html = await http_client.get_with_retry(pool, session, url, diag)
        if status != 200 or html is None:
            diag["sales_page_fetch_failed"] += 1
            await queue.mark_failure(pool, card_id, WORKTYPE, f"sales_page_status_{status}", ttl_minutes=None, retry_delay_minutes=10)
            return

        sales = parse_sales_table(html, diag)

        new_max_sale_at = newest_known_sale_at
        for s in sales:
            # Early-stop dedup: futbin lists newest-first, so the first
            # sale at-or-before our stored cursor means the rest of this
            # page is already known - stop, don't even attempt the insert.
            if newest_known_sale_at and s["sold_at"] <= newest_known_sale_at:
                diag["cache_hits"] += 1
                break
            try:
                result = await pool.execute(
                    """
                    INSERT INTO sales_history
                        (player_id, listed_price, sold_price, ea_tax, net_price, sold_at, captured_at)
                    VALUES ($1, $2, $3, $4, $5, $6, NOW())
                    ON CONFLICT (player_id, sold_at, sold_price) DO NOTHING
                    """,
                    card_id, s["listed_price"], s["sold_price"], s["ea_tax"], s["net_price"], s["sold_at"],
                )
                rowcount = int(result.rsplit(" ", 1)[-1])
                if rowcount:
                    diag["sales_new"] += 1
                    if new_max_sale_at is None or s["sold_at"] > new_max_sale_at:
                        new_max_sale_at = s["sold_at"]
                else:
                    diag["sales_dupe"] += 1
            except Exception as e:
                diag["sales_failed"] += 1
                log.warning("Sales insert failed for card_id=%s sold_at=%s: %s", card_id, s["sold_at"], e)

        diag["succeeded"] += 1
        await queue.mark_success(pool, card_id, WORKTYPE, SUCCESS_REFRESH_MINUTES, newest_known_sale_at=new_max_sale_at)


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
        log.info("sales_worker claimed %d/%d due rows (batch_size=%d)", len(rows), depth, batch_size)
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
            "sales_worker run complete. claimed=%d succeeded=%d sales_new=%d sales_dupe=%d sales_failed=%d "
            "cache_hits=%d http_429_hits=%d http_403_hits=%d circuit_open=%s",
            len(rows), diag["succeeded"], diag["sales_new"], diag["sales_dupe"], diag["sales_failed"],
            diag["cache_hits"], diag["http_429_hits"], diag["http_403_hits"], bool(diag.get("circuit_open")),
        )

        run_ok = not diag.get("circuit_open") and diag["succeeded"] > 0
        await heartbeat(
            pool, "sales_worker", ok=run_ok,
            detail=(
                f"claimed={len(rows)} succeeded={diag['succeeded']} sales_new={diag['sales_new']} "
                f"http_429={diag['http_429_hits']} circuit_open={bool(diag.get('circuit_open'))}"
            ),
        )
        if diag.get("circuit_open"):
            await alert(f"sales_worker: circuit breaker tripped mid-run after {diag['succeeded']} successes.")
        elif not run_ok:
            await alert(f"sales_worker: every claimed card failed this run ({len(rows)} attempted).")
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
