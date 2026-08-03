#!/usr/bin/env python
"""FUT.GG sales-history sync via the signed JSON endpoint.

WHY THIS REPLACES PAGE SCRAPING
-------------------------------
The bulk feed (futgg_bulk_price_sync.py) now supplies BIN for the whole
catalogue in ~7 seconds, so scraping pages for prices is redundant. What
it does NOT carry is completed sales - and sales are 70% of fair value
and the gate on every signal the engine emits, so they still have to come
from somewhere.

Page scraping was a poor way to get them: a 275 KB render yielding ~50
sales, at ~2.0s per success with ~26% failing outright because the
price-access signing endpoint throttles under concurrency.

The same data is available directly:

    POST /api/fut/price-access/sign/   {"url": "/api/fut/player-prices/26/<eaId>/?platform=ps5"}
      -> {"data": {"url": "...?verify=<token>", "expiresIn": 120}}
    GET  <signed url>                  -> ~25 KB JSON

carrying `completedAuctions` with ~100 sales - twice the depth of the
table scrape - plus exact soldDate timestamps rather than "18 minutes
ago" text that has to be parsed and rounded.

TRANSPORT
---------
Requests are issued by fetch() INSIDE a live fut.gg page. That is the one
transport proven to work (see futgg_bulk_probe.py): same-origin, so no
CORS, with the browser supplying cookies, TLS and sec-fetch-* headers
exactly as the real app does. Crucially the page is loaded ONCE and then
reused - there is no navigation per card, which is the entire cost the
old scraper was paying.

Each batch runs its sign+fetch pairs concurrently inside a single
page.evaluate via Promise.all, so N cards cost one round trip to the
browser rather than N.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from futgg_instrumentation import StageTimers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futgg_sales_sync")

SCRIPT_VERSION = "futgg-sales-sync-v1"

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    return default if raw is None else raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value) if minimum is not None else value


def env_float(name: str, default: float, minimum: float | None = None) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value) if minimum is not None else value


GAME_YEAR = os.getenv("FUTGG_GAME_YEAR", "26")
PLATFORM = os.getenv("FUTGG_SALES_PLATFORM", "ps5")
HEADLESS = env_bool("PLAYWRIGHT_HEADLESS", True)
TIMEOUT_MS = env_int("PLAYWRIGHT_TIMEOUT_MS", 45000, minimum=5000)

#: Cards per browser round trip. Each runs sign+fetch concurrently inside
#: the page. Kept modest because the signing endpoint is exactly what
#: throttles - it reports `challengeRequired`, and hammering it is what
#: gave the old scraper a 26% failure rate.
BATCH_SIZE = env_int("FUTGG_SALES_BATCH_SIZE", 8, minimum=1)
#: Pause between batches, to stay under the signing throttle.
BATCH_DELAY = env_float("FUTGG_SALES_BATCH_DELAY", 0.5, minimum=0.0)
CYCLE_SLEEP = env_float("FUTGG_SALES_CYCLE_SLEEP", 5.0, minimum=0.0)
IDLE_SLEEP = env_float("FUTGG_SALES_IDLE_SLEEP", 30.0, minimum=5.0)
#: How many cards to pull per selection pass.
SELECT_LIMIT = env_int("FUTGG_SALES_SELECT_LIMIT", 200, minimum=1)

#: Sales refresh cadence by rating. Far longer than the price cadence:
#: sales accumulate, so re-reading them often adds duplicates rather than
#: information. The bulk feed already keeps BIN fresh independently.
SALES_INTERVALS = {
    "85_plus": env_int("FUTGG_SALES_85_PLUS_INTERVAL_MIN", 30, minimum=5),
    "80_84": env_int("FUTGG_SALES_80_84_INTERVAL_MIN", 120, minimum=10),
    "75_79": env_int("FUTGG_SALES_75_79_INTERVAL_MIN", 360, minimum=30),
    "under_75": env_int("FUTGG_SALES_UNDER_75_INTERVAL_MIN", 1440, minimum=60),
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


def sales_interval_minutes(rating: int | None) -> int:
    if rating is None:
        return SALES_INTERVALS["under_75"]
    if rating >= 85:
        return SALES_INTERVALS["85_plus"]
    if rating >= 80:
        return SALES_INTERVALS["80_84"]
    if rating >= 75:
        return SALES_INTERVALS["75_79"]
    return SALES_INTERVALS["under_75"]


# The signing + fetch pair, executed inside the page so it is same-origin
# and carries the browser's own cookies/TLS. Promise.all means a batch of
# N cards costs ONE round trip to the browser instead of N.
_FETCH_SCRIPT = """
async ({ ids, gameYear, platform }) => {
    const one = async (eaId) => {
        try {
            const target = `/api/fut/player-prices/${gameYear}/${eaId}/?platform=${platform}`;
            const signRes = await fetch('/api/fut/price-access/sign/', {
                method: 'POST',
                headers: { 'content-type': 'application/json', 'accept': 'application/json' },
                body: JSON.stringify({ url: target }),
                credentials: 'include',
            });
            if (!signRes.ok) return { eaId, error: 'sign_' + signRes.status };
            const signed = await signRes.json();
            const signedUrl = signed && signed.data && signed.data.url;
            if (!signedUrl) return { eaId, error: 'no_signed_url' };
            if (signed.data.challengeRequired) return { eaId, error: 'challenge_required' };

            const dataRes = await fetch(signedUrl, {
                headers: { 'accept': 'application/json' },
                credentials: 'include',
            });
            if (!dataRes.ok) return { eaId, error: 'data_' + dataRes.status };
            const payload = await dataRes.json();
            const d = (payload && payload.data) || {};
            return {
                eaId,
                completedAuctions: d.completedAuctions || [],
                currentPrice: d.currentPrice || null,
            };
        } catch (e) {
            return { eaId, error: String(e) };
        }
    };
    return await Promise.all(ids.map(one));
}
"""


def _parse_sold_at(value: Any, now: datetime) -> datetime | None:
    """completedAuctions carry an exact timestamp, unlike the scraped
    table's relative 'N minutes ago' text. Accept ISO strings and epoch
    seconds/millis, since the exact shape is unverified."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        if seconds > 1e11:  # milliseconds
            seconds /= 1000.0
        try:
            return datetime.fromtimestamp(seconds, timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def build_sale_rows(card_id: int, auctions: list[dict], captured_at: datetime) -> list[tuple]:
    """Map completedAuctions onto futgg_sales_history rows.

    The fingerprint keeps the existing scheme's shape so the unique index
    still dedupes, but is built from the EXACT soldDate rather than a
    rounded approximation - the API gives a real timestamp, so there is no
    need to bucket by minute and guess at collisions.
    """
    rows: list[tuple] = []
    seen: dict[tuple, int] = {}
    for position, auction in enumerate(auctions, start=1):
        if not isinstance(auction, dict):
            continue
        price = auction.get("soldPrice", auction.get("price"))
        sold_at = _parse_sold_at(
            auction.get("soldDate", auction.get("soldAt", auction.get("date"))), captured_at
        )
        if price is None or sold_at is None:
            continue
        try:
            price = int(price)
        except (TypeError, ValueError):
            continue
        if price <= 0:
            continue

        key = (int(sold_at.timestamp()), price)
        occurrence = seen.get(key, 0) + 1
        seen[key] = occurrence
        raw = f"{card_id}|{sold_at.isoformat()}|{price}|{occurrence}"
        fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()

        age_seconds = max(0, int((captured_at - sold_at).total_seconds()))
        tax = int(price * 0.05)
        rows.append(
            (
                card_id, price, price, tax, price - tax, sold_at,
                f"{age_seconds // 60} minutes ago", age_seconds,
                position, occurrence, fingerprint, captured_at,
            )
        )
    return rows


_INSERT = """
INSERT INTO futgg_sales_history (
    source_card_id, listed_price, sold_price, ea_tax, net_price,
    approximate_sold_at, source_age_text, source_age_seconds,
    source_row_position, occurrence_index, source_fingerprint, captured_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
ON CONFLICT (source_fingerprint) DO NOTHING
"""


async def select_due(conn: asyncpg.Connection) -> list[asyncpg.Record]:
    return await conn.fetch(
        """
        SELECT source_card_id, rating, next_sales_due_at
        FROM futgg_players
        WHERE is_active
          AND is_tradeable IS DISTINCT FROM FALSE
          AND (next_sales_due_at IS NULL OR next_sales_due_at <= NOW())
        ORDER BY
            CASE WHEN rating >= 85 THEN 0
                 WHEN rating >= 80 THEN 1
                 WHEN rating >= 75 THEN 2
                 ELSE 3 END,
            next_sales_due_at ASC NULLS FIRST,
            rating DESC NULLS LAST
        LIMIT $1
        """,
        SELECT_LIMIT,
    )


async def ensure_schema(conn: asyncpg.Connection) -> None:
    # Sales get their own due-at column: the price cadence is now driven
    # by the bulk feed and is far shorter, so sharing next_price_due_at
    # would make the two workers fight over the same schedule.
    await conn.execute(
        "ALTER TABLE futgg_players ADD COLUMN IF NOT EXISTS next_sales_due_at TIMESTAMPTZ"
    )
    await conn.execute(
        "ALTER TABLE futgg_players ADD COLUMN IF NOT EXISTS last_sales_status TEXT"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_players_sales_due_idx "
        "ON futgg_players (next_sales_due_at) "
        "WHERE is_active AND is_tradeable IS DISTINCT FROM FALSE"
    )


async def process_batch(page, pool: asyncpg.Pool, rows, stats, timers) -> None:
    ids = [int(r["source_card_id"]) for r in rows]
    by_id = {int(r["source_card_id"]): r for r in rows}
    captured_at = datetime.now(timezone.utc)

    with timers.track("fetch_batch"):
        try:
            results = await page.evaluate(
                _FETCH_SCRIPT,
                {"ids": ids, "gameYear": GAME_YEAR, "platform": PLATFORM},
            )
        except Exception:
            log.warning("batch fetch failed", exc_info=True)
            stats["batch_errors"] += 1
            return

    to_insert: list[tuple] = []
    updates: list[tuple] = []
    for result in results or []:
        card_id = int(result.get("eaId"))
        row = by_id.get(card_id)
        rating = row["rating"] if row is not None else None
        error = result.get("error")
        if error:
            stats["failed"] += 1
            stats.setdefault("errors", {})
            stats["errors"][error] = stats["errors"].get(error, 0) + 1
            # Back off this card briefly rather than retrying immediately;
            # the common error is throttling, which a retry only worsens.
            updates.append((card_id, 5, f"error:{error}"))
            continue

        sale_rows = build_sale_rows(card_id, result.get("completedAuctions") or [], captured_at)
        to_insert.extend(sale_rows)
        stats["ok"] += 1
        stats["sales_seen"] += len(sale_rows)
        updates.append((card_id, sales_interval_minutes(rating), "success"))

    with timers.track("db_write"):
        async with pool.acquire() as conn:
            async with conn.transaction():
                if to_insert:
                    # executemany: one round trip for the whole batch
                    # instead of one per sale. The old scraper issued up
                    # to 50 sequential inserts per card.
                    await conn.executemany(_INSERT, to_insert)
                for card_id, minutes, status in updates:
                    await conn.execute(
                        """
                        UPDATE futgg_players
                        SET next_sales_due_at = NOW() + ($2 * INTERVAL '1 minute'),
                            last_sales_status = $3
                        WHERE source_card_id = $1
                        """,
                        card_id, minutes, status,
                    )


async def run_forever() -> None:
    from playwright.async_api import async_playwright

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4, command_timeout=120)
    async with pool.acquire() as conn:
        await ensure_schema(conn)
        anchor = await conn.fetchval(
            """
            SELECT source_url FROM futgg_players
            WHERE is_active AND source_url IS NOT NULL
            ORDER BY rating DESC NULLS LAST LIMIT 1
            """
        )
    anchor = anchor or "https://www.fut.gg/players/1114-roberto-baggio/26-1114/"

    log.info(
        "%s starting: platform=%s batch=%d select_limit=%d intervals=%s",
        SCRIPT_VERSION, PLATFORM, BATCH_SIZE, SELECT_LIMIT, SALES_INTERVALS,
    )

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            context = await browser.new_context(user_agent=USER_AGENT, locale="en-GB")
            page = await context.new_page()
            # ONE navigation for the entire process lifetime. Everything
            # after this is fetch() from within the already-loaded page.
            await page.goto(anchor, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
            log.info("anchor page loaded: %s", anchor)

            while True:
                timers = StageTimers()
                stats = {"ok": 0, "failed": 0, "sales_seen": 0, "batch_errors": 0}
                started = time.perf_counter()

                async with pool.acquire() as conn:
                    due = await select_due(conn)
                if not due:
                    await asyncio.sleep(IDLE_SLEEP)
                    continue

                for offset in range(0, len(due), BATCH_SIZE):
                    await process_batch(
                        page, pool, due[offset: offset + BATCH_SIZE], stats, timers
                    )
                    if BATCH_DELAY:
                        await asyncio.sleep(BATCH_DELAY)

                elapsed = max(time.perf_counter() - started, 0.001)
                log.info(
                    "cycle: cards_ok=%d failed=%d sales=%d in %.1fs (%.0f cards/min) errors=%s",
                    stats["ok"], stats["failed"], stats["sales_seen"], elapsed,
                    (stats["ok"] / elapsed) * 60, stats.get("errors", {}),
                )
                for line in timers.format_lines():
                    log.info(line)
                await asyncio.sleep(CYCLE_SLEEP)
        finally:
            await browser.close()
            await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        log.info("stopped")
    except Exception:
        log.exception("futgg_sales_sync failed")
    sys.exit(0)
