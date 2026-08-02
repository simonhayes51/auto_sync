#!/usr/bin/env python
"""Tiered FUT.GG price and recent-sales sync.

Reads cards from futgg_players, prioritises cards whose next_price_due_at is
oldest, and stores observations in futgg_bin_history/futgg_sales_history.
Metadata discovery remains the responsibility of futgg_player_sync.py.

Suggested Railway Cron: every 10 minutes. The due intervals below control
actual per-card frequency, so frequent cron invocation does not mean every
card is fetched every 10 minutes.

Default intervals:
  special      60 minutes
  gold_rare   180 minutes
  gold_common 720 minutes
  silver     2880 minutes (48h)
  bronze     4320 minutes (72h)

Environment variables:
  DATABASE_URL                    required
  PLAYWRIGHT_HEADLESS             true
  PLAYWRIGHT_TIMEOUT_MS           45000
  FUTGG_PRICE_BATCH_SIZE          500
  FUTGG_PRICE_CONCURRENCY         2
  FUTGG_PRICE_REQUEST_DELAY       0.25
  FUTGG_SPECIAL_INTERVAL_MIN      60
  FUTGG_GOLD_RARE_INTERVAL_MIN    180
  FUTGG_GOLD_COMMON_INTERVAL_MIN  720
  FUTGG_SILVER_INTERVAL_MIN       2880
  FUTGG_BRONZE_INTERVAL_MIN       4320
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg
from playwright.async_api import async_playwright

from futgg_common import parse_futgg_card
from futgg_player_sync import ensure_schema as ensure_player_schema
from monitoring import alert, heartbeat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futgg_price_sync")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")

HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").strip().lower() in {"1", "true", "yes", "on"}
TIMEOUT_MS = max(5000, int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "45000")))
BATCH_SIZE = max(1, int(os.getenv("FUTGG_PRICE_BATCH_SIZE", "500")))
CONCURRENCY = max(1, int(os.getenv("FUTGG_PRICE_CONCURRENCY", "2")))
REQUEST_DELAY = max(0.0, float(os.getenv("FUTGG_PRICE_REQUEST_DELAY", "0.25")))
OVERLAP_LOCK_KEY = int(os.getenv("FUTGG_PRICE_LOCK_KEY", "7741022"))

INTERVALS = {
    "special": max(10, int(os.getenv("FUTGG_SPECIAL_INTERVAL_MIN", "60"))),
    "gold_rare": max(10, int(os.getenv("FUTGG_GOLD_RARE_INTERVAL_MIN", "180"))),
    "gold_common": max(30, int(os.getenv("FUTGG_GOLD_COMMON_INTERVAL_MIN", "720"))),
    "silver": max(60, int(os.getenv("FUTGG_SILVER_INTERVAL_MIN", "2880"))),
    "bronze": max(60, int(os.getenv("FUTGG_BRONZE_INTERVAL_MIN", "4320"))),
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await ensure_player_schema(conn)
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS futgg_bin_history (
            id BIGSERIAL PRIMARY KEY,
            source_card_id BIGINT NOT NULL REFERENCES futgg_players(source_card_id),
            lowest_bin INTEGER NOT NULL,
            price_range_low INTEGER,
            price_range_high INTEGER,
            source_age_text TEXT,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_bin_history_card_captured_idx "
        "ON futgg_bin_history (source_card_id, captured_at DESC)"
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS futgg_sales_history (
            id BIGSERIAL PRIMARY KEY,
            source_card_id BIGINT NOT NULL REFERENCES futgg_players(source_card_id),
            listed_price INTEGER NOT NULL,
            sold_price INTEGER NOT NULL,
            ea_tax INTEGER NOT NULL,
            net_price INTEGER NOT NULL,
            approximate_sold_at TIMESTAMPTZ NOT NULL,
            source_age_text TEXT NOT NULL,
            source_age_seconds INTEGER NOT NULL,
            source_row_position SMALLINT NOT NULL,
            occurrence_index SMALLINT NOT NULL,
            source_fingerprint TEXT NOT NULL UNIQUE,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_sales_history_card_sold_idx "
        "ON futgg_sales_history (source_card_id, approximate_sold_at DESC)"
    )


async def fetch_due_cards(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT source_card_id, source_url, price_tier
        FROM futgg_players
        WHERE is_active
          AND (next_price_due_at IS NULL OR next_price_due_at <= NOW())
        ORDER BY next_price_due_at ASC NULLS FIRST, price_updated_at ASC NULLS FIRST
        LIMIT $1
        """,
        BATCH_SIZE,
    )


async def record_success(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    card,
    captured_at: datetime,
) -> tuple[int, int, int]:
    bin_rows = sales_new = sales_dupe = 0

    if card.lowest_bin is not None:
        await conn.execute(
            """
            INSERT INTO futgg_bin_history (
                source_card_id, lowest_bin, price_range_low, price_range_high,
                source_age_text, captured_at
            ) VALUES ($1,$2,$3,$4,$5,$6)
            """,
            card.source_card_id,
            card.lowest_bin,
            card.price_range_low,
            card.price_range_high,
            card.lowest_bin_age,
            captured_at,
        )
        bin_rows = 1

    for sale in card.recent_sales:
        tax = int(sale.sold_price * 0.05)
        result = await conn.execute(
            """
            INSERT INTO futgg_sales_history (
                source_card_id, listed_price, sold_price, ea_tax, net_price,
                approximate_sold_at, source_age_text, source_age_seconds,
                source_row_position, occurrence_index, source_fingerprint,
                captured_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (source_fingerprint) DO NOTHING
            """,
            card.source_card_id,
            sale.sold_price,
            sale.sold_price,
            tax,
            sale.sold_price - tax,
            sale.approximate_sold_at,
            sale.age_text,
            sale.age_seconds,
            sale.row_position,
            sale.occurrence_index,
            sale.fingerprint,
            captured_at,
        )
        inserted = int(result.rsplit(" ", 1)[-1])
        if inserted:
            sales_new += 1
        else:
            sales_dupe += 1

    interval = INTERVALS.get(row["price_tier"], INTERVALS["bronze"])
    status = "ok" if card.lowest_bin is not None or card.recent_sales else "no_market_data"
    await conn.execute(
        """
        UPDATE futgg_players
        SET price_updated_at = $2,
            next_price_due_at = $2 + ($3 * INTERVAL '1 minute'),
            last_price_status = $4,
            last_seen_at = NOW()
        WHERE source_card_id = $1
        """,
        card.source_card_id,
        captured_at,
        interval,
        status,
    )
    return bin_rows, sales_new, sales_dupe


async def record_failure(conn: asyncpg.Connection, row: asyncpg.Record, reason: str) -> None:
    interval = min(INTERVALS.get(row["price_tier"], 720), 360)
    await conn.execute(
        """
        UPDATE futgg_players
        SET next_price_due_at = NOW() + ($2 * INTERVAL '1 minute'),
            last_price_status = $3
        WHERE source_card_id = $1
        """,
        row["source_card_id"],
        interval,
        reason[:200],
    )


async def worker_loop(
    worker_id: int,
    page,
    pool: asyncpg.Pool,
    queue: asyncio.Queue,
    stats: dict[str, int],
) -> None:
    while True:
        try:
            row = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        captured_at = datetime.now(timezone.utc)
        try:
            response = await page.goto(
                row["source_url"],
                wait_until="domcontentloaded",
                timeout=TIMEOUT_MS,
            )
            status = response.status if response else 0
            if status != 200:
                raise RuntimeError(f"HTTP {status}")

            await page.locator(".fc-card").first.wait_for(state="attached", timeout=15000)
            try:
                await page.locator("#prices-overview").wait_for(state="attached", timeout=12000)
            except Exception:
                pass
            await page.wait_for_timeout(700)

            card = parse_futgg_card(await page.content(), row["source_url"], captured_at)
            async with pool.acquire() as conn:
                bin_rows, sales_new, sales_dupe = await record_success(conn, row, card, captured_at)

            stats["cards_ok"] += 1
            stats["bin_rows"] += bin_rows
            stats["sales_new"] += sales_new
            stats["sales_dupe"] += sales_dupe
            if card.lowest_bin is None and not card.recent_sales:
                stats["no_market_data"] += 1
            log.info(
                "worker=%d card=%s tier=%s bin=%s sales=%d new=%d",
                worker_id, row["source_card_id"], row["price_tier"],
                card.lowest_bin, len(card.recent_sales), sales_new,
            )
        except Exception as exc:
            stats["cards_failed"] += 1
            log.warning("worker=%d card=%s failed: %s", worker_id, row["source_card_id"], exc)
            async with pool.acquire() as conn:
                await record_failure(conn, row, f"error:{type(exc).__name__}")
        finally:
            queue.task_done()

        if REQUEST_DELAY:
            await asyncio.sleep(random.uniform(REQUEST_DELAY * 0.7, REQUEST_DELAY * 1.3))


async def crawl_once() -> None:
    lock_conn = await asyncpg.connect(DATABASE_URL)
    got_lock = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", OVERLAP_LOCK_KEY)
    if not got_lock:
        log.info("Previous FUT.GG price sync still running; skipping")
        await lock_conn.close()
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=CONCURRENCY + 3)
    playwright = browser = context = None
    stats: dict[str, int] = {
        "selected": 0,
        "cards_ok": 0,
        "cards_failed": 0,
        "bin_rows": 0,
        "sales_new": 0,
        "sales_dupe": 0,
        "no_market_data": 0,
    }
    try:
        async with pool.acquire() as conn:
            await ensure_schema(conn)
            rows = await fetch_due_cards(conn)
        stats["selected"] = len(rows)
        log.info("Selected %d due cards; intervals=%s", len(rows), INTERVALS)
        if not rows:
            return

        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-GB",
            viewport={"width": 1440, "height": 1000},
        )

        queue: asyncio.Queue = asyncio.Queue()
        for row in rows:
            queue.put_nowait(row)

        pages = [await context.new_page() for _ in range(CONCURRENCY)]
        await asyncio.gather(
            *(worker_loop(index + 1, page, pool, queue, stats) for index, page in enumerate(pages))
        )

        ok = stats["cards_ok"] > 0 and stats["cards_failed"] < stats["selected"]
        async with pool.acquire() as conn:
            await heartbeat(
                conn,
                "futgg_price_sync",
                ok=ok,
                detail=" ".join(f"{key}={value}" for key, value in stats.items()),
            )
        if not ok:
            await alert(f"futgg_price_sync unhealthy: {stats}")
        log.info("Run complete: %s", stats)

    finally:
        if context:
            await context.close()
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
        await pool.close()
        await lock_conn.execute("SELECT pg_advisory_unlock($1)", OVERLAP_LOCK_KEY)
        await lock_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())
    except Exception as exc:
        log.exception("futgg_price_sync failed: %s", exc)
        sys.exit(1)
