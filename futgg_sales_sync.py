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

#: Sales refresh cadence by rating.
#
# Deliberately much slower than the price cadence, for two reasons.
#
# Sales are CUMULATIVE: a sale already captured stays captured, so a
# refresh only adds what happened since the last read. The interval
# therefore controls how quickly new sales enter the window, not whether
# history exists at all. Reading hourly instead of half-hourly costs
# nothing but latency on the newest few sales.
#
# And the time-critical signal has moved. Trend and volatility now come
# from futgg_bin_history, which the bulk feed refreshes every 5 minutes
# for all 10,064 cards. Sales are needed for the fair-value ANCHOR
# (median, trimmed mean, dispersion, liquidity), which moves slowly.
#
# Sized against measured capacity: the previous 30/120/360/1440 spread
# demanded ~133 cards/min sustained, which is what produced 144 HTTP 429s
# in the first live run. Halving each band brings it to ~67/min.
SALES_INTERVALS = {
    "85_plus": env_int("FUTGG_SALES_85_PLUS_INTERVAL_MIN", 60, minimum=5),
    "80_84": env_int("FUTGG_SALES_80_84_INTERVAL_MIN", 240, minimum=10),
    "75_79": env_int("FUTGG_SALES_75_79_INTERVAL_MIN", 720, minimum=30),
    "under_75": env_int("FUTGG_SALES_UNDER_75_INTERVAL_MIN", 2880, minimum=60),
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


class AdaptiveThrottle:
    """Self-tuning request rate, driven by observed 429s.

    The first live run attempted 200 cards in 15.4s and got 144 HTTP 429s
    on the data fetch - the transport itself is fast (89ms per batch of
    eight), so the only real constraint is FUT.GG's rate limit, and its
    exact threshold is unknown and liable to change.

    Rather than hard-code a guess, back off hard when throttled and ease
    back in when clean. Backing off on BOTH axes matters: a smaller batch
    reduces instantaneous concurrency, while a longer delay reduces
    sustained rate, and 429s can be triggered by either.

    Note what the required throughput actually is: ~3,272 cards at 85+ on
    a 30-minute cadence is ~109 cards/min, and the long tail adds maybe
    another 25. So roughly 2.2 cards/sec sustained is enough. The first
    run was already achieving 3.6/sec of successes - being slower and
    clean is strictly better than fast and mostly rejected, because
    rejected requests still cost quota and risk a harder block.
    """

    def __init__(self, batch_size: int, delay: float) -> None:
        self.batch_size = batch_size
        self.delay = delay
        self.max_batch = batch_size
        self.min_batch = 1
        self.min_delay = delay
        self.max_delay = max(delay * 40, 10.0)
        self._clean_streak = 0

    def record(self, attempted: int, throttled: int) -> None:
        if throttled:
            self._clean_streak = 0
            # Proportional to how badly we overshot: a batch that was
            # entirely rejected deserves a harder cut than one that lost a
            # single request.
            severity = throttled / max(attempted, 1)
            self.delay = min(self.max_delay, max(self.delay, 0.25) * (1.0 + 2.0 * severity))
            if severity > 0.25:
                self.batch_size = max(self.min_batch, self.batch_size - 1)
            return

        self._clean_streak += 1
        # Ease back in slowly, and only after sustained success - the
        # limiter is likely windowed, so one clean batch proves little.
        if self._clean_streak >= 5:
            self._clean_streak = 0
            self.delay = max(self.min_delay, self.delay * 0.8)
            self.batch_size = min(self.max_batch, self.batch_size + 1)

    def describe(self) -> str:
        return f"batch={self.batch_size} delay={self.delay:.2f}s"


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
    """Pick the next cards to read.

    Cards that have NEVER been read come first, regardless of rating.

    That ordering matters more than it looks. Sorting by rating band first
    starves the tail: the 85+ band alone needs ~55 cards/min at its
    cadence, so if throughput sits anywhere near that, lower-rated cards
    never reach the front of the queue and never get a FIRST read at all -
    they would sit permanently at zero sales, and a card with no sales
    returns insufficient_data forever no matter how fresh its BIN is.

    Backfill-first guarantees every card gets covered once, after which
    NULLs are exhausted and the ordering settles into normal
    band-priority maintenance.
    """
    return await conn.fetch(
        """
        SELECT source_card_id, rating, next_sales_due_at
        FROM futgg_players
        WHERE is_active
          AND is_tradeable IS DISTINCT FROM FALSE
          AND (next_sales_due_at IS NULL OR next_sales_due_at <= NOW())
        ORDER BY
            -- never-read cards first (backfill), best cards within that
            (next_sales_due_at IS NOT NULL),
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


async def report_coverage(conn: asyncpg.Connection) -> None:
    """How much of the catalogue actually has sales yet.

    The single number that says whether the intelligence layer can work:
    a card with fewer than MIN_SALES_FOR_SIGNAL sales produces no signal
    at all, however good its price data is.
    """
    row = await conn.fetchrow(
        """
        SELECT
            count(*) AS total,
            count(*) FILTER (WHERE s.n > 0) AS any_sales,
            count(*) FILTER (WHERE s.n >= 5) AS enough_sales,
            count(*) FILTER (WHERE p.next_sales_due_at IS NULL) AS never_read
        FROM futgg_players p
        LEFT JOIN LATERAL (
            SELECT count(*) AS n FROM futgg_sales_history h
            WHERE h.source_card_id = p.source_card_id
              AND h.approximate_sold_at >= now() - interval '14 days'
        ) s ON TRUE
        WHERE p.is_active AND p.is_tradeable IS DISTINCT FROM FALSE
        """
    )
    total = int(row["total"] or 0)
    if not total:
        return
    log.info(
        "sales coverage: %d/%d have any sales (%.1f%%) | %d/%d have >=5 and can "
        "produce a signal (%.1f%%) | %d never read",
        row["any_sales"], total, 100.0 * row["any_sales"] / total,
        row["enough_sales"], total, 100.0 * row["enough_sales"] / total,
        row["never_read"],
    )


async def ensure_schema(conn: asyncpg.Connection) -> None:
    # futgg_sales_history is owned by futgg_price_sync.ensure_schema, and
    # this worker is meant to REPLACE that one - so on any database where
    # price sync has been retired (or never ran) the table this worker
    # writes to would not exist. Own it here too; CREATE TABLE IF NOT
    # EXISTS keeps the two definitions safely idempotent while both
    # workers coexist during the changeover.
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS futgg_sales_history (
            id BIGSERIAL PRIMARY KEY,
            source_card_id BIGINT NOT NULL
                REFERENCES futgg_players(source_card_id),
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
    # Both the snapshot view and the coverage query filter sales by card
    # and recency; without this they degrade to sequential scans once the
    # table passes a few million rows.
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_sales_history_card_sold_idx "
        "ON futgg_sales_history (source_card_id, approximate_sold_at DESC)"
    )

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


async def process_batch(page, pool: asyncpg.Pool, rows, stats, timers, throttle) -> None:
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
            throttle.record(len(ids), len(ids))
            return

    to_insert: list[tuple] = []
    updates: list[tuple] = []
    throttled = 0
    for result in results or []:
        card_id = int(result.get("eaId"))
        row = by_id.get(card_id)
        rating = row["rating"] if row is not None else None
        error = result.get("error")
        if error:
            stats["failed"] += 1
            stats.setdefault("errors", {})
            stats["errors"][error] = stats["errors"].get(error, 0) + 1
            if "429" in error or error == "challenge_required":
                # Rate-limited, not evaluated. Deliberately leave the
                # schedule untouched so the card stays due and is retried
                # once the throttle has adapted - pushing it out would
                # silently drop cards purely because we asked too fast.
                throttled += 1
            else:
                updates.append((card_id, 5, f"error:{error}"))
            continue

        sale_rows = build_sale_rows(card_id, result.get("completedAuctions") or [], captured_at)
        to_insert.extend(sale_rows)
        stats["ok"] += 1
        stats["sales_seen"] += len(sale_rows)
        updates.append((card_id, sales_interval_minutes(rating), "success"))

    throttle.record(len(ids), throttled)

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

            throttle = AdaptiveThrottle(BATCH_SIZE, BATCH_DELAY)

            while True:
                timers = StageTimers()
                stats = {"ok": 0, "failed": 0, "sales_seen": 0, "batch_errors": 0}
                started = time.perf_counter()

                async with pool.acquire() as conn:
                    due = await select_due(conn)
                    await report_coverage(conn)
                if not due:
                    await asyncio.sleep(IDLE_SLEEP)
                    continue

                offset = 0
                while offset < len(due):
                    # Size is read per iteration, so a mid-cycle backoff
                    # takes effect immediately rather than at the next
                    # cycle.
                    size = throttle.batch_size
                    await process_batch(
                        page, pool, due[offset: offset + size], stats, timers, throttle
                    )
                    offset += size
                    if throttle.delay:
                        await asyncio.sleep(throttle.delay)

                elapsed = max(time.perf_counter() - started, 0.001)
                log.info(
                    "cycle: cards_ok=%d failed=%d sales=%d in %.1fs (%.0f cards/min) "
                    "throttle[%s] errors=%s",
                    stats["ok"], stats["failed"], stats["sales_seen"], elapsed,
                    (stats["ok"] / elapsed) * 60, throttle.describe(),
                    stats.get("errors", {}),
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
