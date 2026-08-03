#!/usr/bin/env python
"""Rating-tiered FUT.GG price and recent-sales sync.

Reads cards from futgg_players, prioritises high-rated cards whose
next_price_due_at is due, and stores observations in futgg_bin_history
and futgg_sales_history.

Metadata discovery remains the responsibility of futgg_player_sync.py.

Suggested Railway Cron:
    */10 * * * *

Default refresh intervals:
    Rating 85+       10 minutes
    Rating 80–84     30 minutes
    Rating 75–79     120 minutes
    Rating 70–74     360 minutes
    Rating under 70  1440 minutes
    Unknown rating   1440 minutes

Hot opportunities can be refreshed sooner than their normal interval.

Environment variables:
    DATABASE_URL                              required
    PLAYWRIGHT_HEADLESS                       true
    PLAYWRIGHT_TIMEOUT_MS                     45000

    FUTGG_PRICE_BATCH_SIZE                    4000
    FUTGG_PRICE_CONCURRENCY                   15
    FUTGG_PRICE_REQUEST_DELAY                 0.10

    FUTGG_RATING_85_PLUS_INTERVAL_MIN         10
    FUTGG_RATING_80_84_INTERVAL_MIN           30
    FUTGG_RATING_75_79_INTERVAL_MIN           120
    FUTGG_RATING_70_74_INTERVAL_MIN           360
    FUTGG_RATING_UNDER_70_INTERVAL_MIN        1440

    FUTGG_CIRCUIT_BREAKER_THRESHOLD           8
    FUTGG_HOT_DISCOUNT_THRESHOLD              0.12
    FUTGG_HOT_INTERVAL_MIN                    10
    FUTGG_HOT_MIN_SALES                       5

Price outcomes stored in futgg_players.last_price_status:
    success
    untradeable
    no_active_market
    price_section_missing
    page_failed
    parse_failed
    rate_limited
    blocked

A confirmed untradeable card receives:
    is_tradeable = FALSE
    next_price_due_at = NULL

It is then excluded from future price-sync runs.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import statistics
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from playwright.async_api import async_playwright

from futgg_common import CircuitBreaker, parse_futgg_card
from futgg_player_sync import ensure_schema as ensure_player_schema
from monitoring import alert, heartbeat


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("futgg_price_sync")


DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")


HEADLESS = (
    os.getenv("PLAYWRIGHT_HEADLESS", "true")
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)

TIMEOUT_MS = max(
    5000,
    int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "45000")),
)

BATCH_SIZE = max(
    1,
    int(os.getenv("FUTGG_PRICE_BATCH_SIZE", "4000")),
)

CONCURRENCY = max(
    1,
    int(os.getenv("FUTGG_PRICE_CONCURRENCY", "15")),
)

REQUEST_DELAY = max(
    0.0,
    float(os.getenv("FUTGG_PRICE_REQUEST_DELAY", "0.10")),
)

OVERLAP_LOCK_KEY = int(
    os.getenv("FUTGG_PRICE_LOCK_KEY", "7741022")
)


CIRCUIT_BREAKER_THRESHOLD = max(
    1,
    int(os.getenv("FUTGG_CIRCUIT_BREAKER_THRESHOLD", "8")),
)

BLOCKED_STATUS_CODES = {403, 429}


RATING_INTERVALS = {
    "85_plus": max(
        5,
        int(os.getenv("FUTGG_RATING_85_PLUS_INTERVAL_MIN", "10")),
    ),
    "80_84": max(
        10,
        int(os.getenv("FUTGG_RATING_80_84_INTERVAL_MIN", "30")),
    ),
    "75_79": max(
        30,
        int(os.getenv("FUTGG_RATING_75_79_INTERVAL_MIN", "120")),
    ),
    "70_74": max(
        60,
        int(os.getenv("FUTGG_RATING_70_74_INTERVAL_MIN", "360")),
    ),
    "under_70": max(
        60,
        int(os.getenv("FUTGG_RATING_UNDER_70_INTERVAL_MIN", "1440")),
    ),
}


HOT_DISCOUNT_THRESHOLD = max(
    0.0,
    float(os.getenv("FUTGG_HOT_DISCOUNT_THRESHOLD", "0.12")),
)

HOT_INTERVAL_MIN = max(
    5,
    int(os.getenv("FUTGG_HOT_INTERVAL_MIN", "10")),
)

HOT_MIN_SALES = max(
    1,
    int(os.getenv("FUTGG_HOT_MIN_SALES", "5")),
)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


def price_interval_minutes(rating: int | None) -> int:
    """Return the standard price-refresh interval for a card rating."""

    if rating is None:
        return RATING_INTERVALS["under_70"]

    if rating >= 85:
        return RATING_INTERVALS["85_plus"]

    if rating >= 80:
        return RATING_INTERVALS["80_84"]

    if rating >= 75:
        return RATING_INTERVALS["75_79"]

    if rating >= 70:
        return RATING_INTERVALS["70_74"]

    return RATING_INTERVALS["under_70"]


def _is_hot_opportunity(card: Any) -> bool:
    """Return True when the live BIN is materially below recent sales.

    This uses the current scrape's own recent-sales data, avoiding an
    additional request.
    """

    if card.lowest_bin is None:
        return False

    if len(card.recent_sales) < HOT_MIN_SALES:
        return False

    valid_prices = [
        sale.sold_price
        for sale in card.recent_sales
        if sale.sold_price is not None and sale.sold_price > 0
    ]

    if len(valid_prices) < HOT_MIN_SALES:
        return False

    median = statistics.median(valid_prices)

    if median <= 0:
        return False

    discount = (median - card.lowest_bin) / median

    return discount >= HOT_DISCOUNT_THRESHOLD


async def ensure_schema(conn: asyncpg.Connection) -> None:
    """Ensure the player and price-history schemas exist."""

    await ensure_player_schema(conn)

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS futgg_bin_history (
            id BIGSERIAL PRIMARY KEY,

            source_card_id BIGINT NOT NULL
                REFERENCES futgg_players(source_card_id),

            lowest_bin INTEGER NOT NULL,
            price_range_low INTEGER,
            price_range_high INTEGER,
            source_age_text TEXT,

            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS futgg_bin_history_card_captured_idx
        ON futgg_bin_history (
            source_card_id,
            captured_at DESC
        )
        """
    )

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

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS futgg_sales_history_card_sold_idx
        ON futgg_sales_history (
            source_card_id,
            approximate_sold_at DESC
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS futgg_players_rating_price_due_idx
        ON futgg_players (
            rating DESC,
            next_price_due_at,
            price_updated_at
        )
        WHERE is_active
          AND is_tradeable IS DISTINCT FROM FALSE
        """
    )


async def fetch_due_cards(
    conn: asyncpg.Connection,
) -> list[asyncpg.Record]:
    """Return due cards, with high-rated cards always selected first."""

    return await conn.fetch(
        """
        SELECT
            source_card_id,
            source_url,
            price_tier,
            rating,
            next_price_due_at,
            price_updated_at
        FROM futgg_players
        WHERE is_active = TRUE
          AND is_tradeable IS DISTINCT FROM FALSE
          AND (
              next_price_due_at IS NULL
              OR next_price_due_at <= NOW()
          )
        ORDER BY
            CASE
                WHEN rating >= 85 THEN 0
                WHEN rating >= 80 THEN 1
                WHEN rating >= 75 THEN 2
                WHEN rating >= 70 THEN 3
                ELSE 4
            END,
            next_price_due_at ASC NULLS FIRST,
            price_updated_at ASC NULLS FIRST,
            rating DESC NULLS LAST,
            source_card_id ASC
        LIMIT $1
        """,
        BATCH_SIZE,
    )


async def record_success(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    card: Any,
    captured_at: datetime,
    cycle_started_at: datetime,
) -> tuple[int, int, int, bool]:
    """Store a successful page parse and schedule the next refresh."""

    bin_rows = 0
    sales_new = 0
    sales_dupe = 0

    if card.lowest_bin is not None:
        await conn.execute(
            """
            INSERT INTO futgg_bin_history (
                source_card_id,
                lowest_bin,
                price_range_low,
                price_range_high,
                source_age_text,
                captured_at
            )
            VALUES ($1, $2, $3, $4, $5, $6)
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
        sold_price = sale.sold_price

        if sold_price is None or sold_price <= 0:
            continue

        tax = int(sold_price * 0.05)
        net_price = sold_price - tax

        result = await conn.execute(
            """
            INSERT INTO futgg_sales_history (
                source_card_id,
                listed_price,
                sold_price,
                ea_tax,
                net_price,
                approximate_sold_at,
                source_age_text,
                source_age_seconds,
                source_row_position,
                occurrence_index,
                source_fingerprint,
                captured_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12
            )
            ON CONFLICT (source_fingerprint) DO NOTHING
            """,
            card.source_card_id,
            sold_price,
            sold_price,
            tax,
            net_price,
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

    status = card.price_outcome

    if status == "untradeable":
        await conn.execute(
            """
            UPDATE futgg_players
            SET
                price_updated_at = $2,
                next_price_due_at = NULL,
                last_price_status = $3,
                is_tradeable = FALSE,
                last_seen_at = NOW()
            WHERE source_card_id = $1
            """,
            card.source_card_id,
            captured_at,
            status,
        )

        return bin_rows, sales_new, sales_dupe, False

    interval = price_interval_minutes(row["rating"])

    if status in {
        "no_active_market",
        "price_section_missing",
    }:
        interval = min(interval * 3, 4320)

    is_hot = (
        status == "success"
        and _is_hot_opportunity(card)
    )

    if is_hot:
        interval = min(interval, HOT_INTERVAL_MIN)

    # Anchor the next due time to the beginning of the entire run.
    #
    # Example:
    # The cron begins at 10:00 and takes six minutes.
    # A rating-85 card completed at 10:06 is still next due at 10:10,
    # rather than 10:16.
    next_due_at = cycle_started_at + timedelta(minutes=interval)

    # Do not leave a card immediately overdue if an unusually slow run
    # already passed the intended next-due time.
    minimum_next_due = captured_at + timedelta(minutes=1)

    if next_due_at < minimum_next_due:
        next_due_at = minimum_next_due

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            price_updated_at = $2,
            next_price_due_at = $3,
            last_price_status = $4,
            is_tradeable = COALESCE($5, is_tradeable),
            last_seen_at = NOW()
        WHERE source_card_id = $1
        """,
        card.source_card_id,
        captured_at,
        next_due_at,
        status,
        True if status == "success" else None,
    )

    return bin_rows, sales_new, sales_dupe, is_hot


async def record_failure(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    status: str = "page_failed",
) -> None:
    """Record a failure and assign an appropriate retry time."""

    normal_interval = price_interval_minutes(row["rating"])

    if status in {"blocked", "rate_limited"}:
        retry_interval = 15
    else:
        retry_interval = min(normal_interval, 5)

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            next_price_due_at =
                NOW() + ($2 * INTERVAL '1 minute'),
            last_price_status = $3
        WHERE source_card_id = $1
        """,
        row["source_card_id"],
        retry_interval,
        status[:200],
    )


async def worker_loop(
    worker_id: int,
    page: Any,
    pool: asyncpg.Pool,
    queue: asyncio.Queue,
    stats: dict[str, int],
    breaker: CircuitBreaker,
    cycle_started_at: datetime,
) -> None:
    """Process due cards until the queue is empty or the breaker trips."""

    while True:
        if breaker.tripped:
            log.warning(
                "worker=%d stopping: circuit breaker tripped (%s)",
                worker_id,
                breaker.trip_reason,
            )
            return

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

            http_status = response.status if response else 0

            if http_status in BLOCKED_STATUS_CODES:
                stats["blocked"] += 1
                stats["cards_failed"] += 1

                failure_status = (
                    "rate_limited"
                    if http_status == 429
                    else "blocked"
                )

                async with pool.acquire() as conn:
                    await record_failure(
                        conn,
                        row,
                        status=failure_status,
                    )

                breaker.record_failure(
                    f"HTTP {http_status}"
                )

                log.warning(
                    "worker=%d card=%s HTTP %d status=%s",
                    worker_id,
                    row["source_card_id"],
                    http_status,
                    failure_status,
                )

                continue

            if http_status != 200:
                raise RuntimeError(
                    f"HTTP {http_status}"
                )

            await page.locator(".fc-card").first.wait_for(
                state="attached",
                timeout=15000,
            )

            try:
                await page.locator("#prices-overview").wait_for(
                    state="attached",
                    timeout=12000,
                )
            except Exception:
                # The parser decides whether this means untradeable,
                # no market data, or a missing section.
                pass

            await page.wait_for_timeout(700)

            card = parse_futgg_card(
                await page.content(),
                row["source_url"],
                captured_at,
            )

            async with pool.acquire() as conn:
                (
                    bin_rows,
                    sales_new,
                    sales_dupe,
                    is_hot,
                ) = await record_success(
                    conn,
                    row,
                    card,
                    captured_at,
                    cycle_started_at,
                )

            breaker.record_success()

            stats["cards_ok"] += 1
            stats["bin_rows"] += bin_rows
            stats["sales_new"] += sales_new
            stats["sales_dupe"] += sales_dupe

            if card.price_outcome == "untradeable":
                stats["untradeable"] += 1

            elif card.price_outcome in {
                "no_active_market",
                "price_section_missing",
            }:
                stats["no_market_data"] += 1

            if is_hot:
                stats["hot_opportunities"] += 1

            log.info(
                (
                    "worker=%d card=%s rating=%s tier=%s "
                    "outcome=%s bin=%s sales=%d new=%d hot=%s"
                ),
                worker_id,
                row["source_card_id"],
                row["rating"],
                row["price_tier"],
                card.price_outcome,
                card.lowest_bin,
                len(card.recent_sales),
                sales_new,
                is_hot,
            )

        except Exception as exc:
            stats["cards_failed"] += 1

            is_parse_failure = isinstance(
                exc,
                (ValueError, KeyError),
            )

            if is_parse_failure:
                stats["parse_failures"] += 1
                failure_status = "parse_failed"
            else:
                stats["navigation_failures"] += 1
                failure_status = "page_failed"

            log.warning(
                "worker=%d card=%s failed: %s",
                worker_id,
                row["source_card_id"],
                exc,
            )

            breaker.record_failure(
                f"{type(exc).__name__}: {exc}"
            )

            async with pool.acquire() as conn:
                await record_failure(
                    conn,
                    row,
                    status=failure_status,
                )

        finally:
            queue.task_done()

        if REQUEST_DELAY:
            await asyncio.sleep(
                random.uniform(
                    REQUEST_DELAY * 0.7,
                    REQUEST_DELAY * 1.3,
                )
            )


async def crawl_once() -> None:
    """Run one due-card price-sync cycle."""

    cycle_started_at = datetime.now(timezone.utc)

    lock_conn = await asyncpg.connect(DATABASE_URL)

    got_lock = await lock_conn.fetchval(
        "SELECT pg_try_advisory_lock($1)",
        OVERLAP_LOCK_KEY,
    )

    if not got_lock:
        log.info(
            "Previous FUT.GG price sync still running; skipping"
        )
        await lock_conn.close()
        return

    pool: asyncpg.Pool | None = None
    playwright = None
    browser = None
    context = None

    stats: dict[str, int] = {
        "selected": 0,
        "cards_ok": 0,
        "cards_failed": 0,
        "bin_rows": 0,
        "sales_new": 0,
        "sales_dupe": 0,
        "no_market_data": 0,
        "untradeable": 0,
        "hot_opportunities": 0,
        "blocked": 0,
        "parse_failures": 0,
        "navigation_failures": 0,
    }

    breaker = CircuitBreaker(
        CIRCUIT_BREAKER_THRESHOLD
    )

    try:
        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=CONCURRENCY + 3,
        )

        async with pool.acquire() as conn:
            await ensure_schema(conn)
            rows = await fetch_due_cards(conn)

        stats["selected"] = len(rows)

        rating_counts = {
            "85_plus": 0,
            "80_84": 0,
            "75_79": 0,
            "70_74": 0,
            "under_70_or_unknown": 0,
        }

        for row in rows:
            rating = row["rating"]

            if rating is not None and rating >= 85:
                rating_counts["85_plus"] += 1
            elif rating is not None and rating >= 80:
                rating_counts["80_84"] += 1
            elif rating is not None and rating >= 75:
                rating_counts["75_79"] += 1
            elif rating is not None and rating >= 70:
                rating_counts["70_74"] += 1
            else:
                rating_counts["under_70_or_unknown"] += 1

        log.info(
            (
                "Selected=%d rating_counts=%s intervals=%s "
                "concurrency=%d batch_size=%d request_delay=%.2f"
            ),
            len(rows),
            rating_counts,
            RATING_INTERVALS,
            CONCURRENCY,
            BATCH_SIZE,
            REQUEST_DELAY,
        )

        if not rows:
            async with pool.acquire() as conn:
                await heartbeat(
                    conn,
                    "futgg_price_sync",
                    ok=True,
                    detail=(
                        "selected=0 cards_ok=0 "
                        "message=no_due_cards"
                    ),
                )

            log.info("No due cards")
            return

        playwright = await async_playwright().start()

        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-background-networking",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
            ],
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-GB",
            viewport={
                "width": 1440,
                "height": 1000,
            },
        )

        # Prices do not need images, fonts, video or other heavy assets.
        # Blocking these reduces bandwidth and page-rendering overhead.
        async def block_unneeded_resources(route: Any) -> None:
            resource_type = route.request.resource_type

            if resource_type in {
                "image",
                "media",
                "font",
            }:
                await route.abort()
            else:
                await route.continue_()

        await context.route(
            "**/*",
            block_unneeded_resources,
        )

        queue: asyncio.Queue = asyncio.Queue()

        for row in rows:
            queue.put_nowait(row)

        pages = [
            await context.new_page()
            for _ in range(CONCURRENCY)
        ]

        try:
            await asyncio.gather(
                *(
                    worker_loop(
                        index + 1,
                        page,
                        pool,
                        queue,
                        stats,
                        breaker,
                        cycle_started_at,
                    )
                    for index, page in enumerate(pages)
                )
            )

        finally:
            for page in pages:
                try:
                    await page.close()
                except Exception:
                    pass

        if breaker.tripped:
            log.warning(
                (
                    "Circuit breaker tripped: %s. "
                    "Remaining queued cards were not processed."
                ),
                breaker.trip_reason,
            )

        completed = (
            stats["cards_ok"]
            + stats["cards_failed"]
        )

        ok = (
            stats["cards_ok"] > 0
            and completed > 0
            and stats["cards_failed"] < stats["selected"]
            and not breaker.tripped
        )

        run_seconds = int(
            (
                datetime.now(timezone.utc)
                - cycle_started_at
            ).total_seconds()
        )

        heartbeat_detail = " ".join(
            f"{key}={value}"
            for key, value in stats.items()
        )

        heartbeat_detail += (
            f" run_seconds={run_seconds}"
            f" concurrency={CONCURRENCY}"
            f" batch_size={BATCH_SIZE}"
        )

        async with pool.acquire() as conn:
            await heartbeat(
                conn,
                "futgg_price_sync",
                ok=ok,
                detail=heartbeat_detail,
            )

        if not ok:
            await alert(
                f"futgg_price_sync unhealthy: "
                f"{stats} run_seconds={run_seconds}"
            )

        log.info(
            "Run complete in %d seconds: %s",
            run_seconds,
            stats,
        )

    finally:
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass

        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass

        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass

        if pool is not None:
            await pool.close()

        try:
            await lock_conn.execute(
                "SELECT pg_advisory_unlock($1)",
                OVERLAP_LOCK_KEY,
            )
        finally:
            await lock_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())

    except Exception as exc:
        log.exception(
            "futgg_price_sync failed: %s",
            exc,
        )
        sys.exit(1)
