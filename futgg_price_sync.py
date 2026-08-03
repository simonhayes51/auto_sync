#!/usr/bin/env python
"""Continuous rating-prioritised FUT.GG price and recent-sales sync.

This is intended to run as a long-lived Railway worker, not as a cron job.

Core behaviour:
- Rating 85+ cards are refreshed every 10 minutes.
- Lower-rated cards use progressively longer refresh intervals.
- 85+ cards always receive priority.
- Lower-rated cards only use spare batch capacity.
- Chromium, browser context and worker pages remain open between batches.
- Successful cards are rescheduled from their actual capture time.
- Cloudflare blocks and rate limits trigger circuit-breaker backoff.
- Untradeable cards are permanently removed from future price selection.

Recommended Railway service:
    python -m scripts.futgg_price_sync

Default refresh intervals:
    Rating 85+       10 minutes
    Rating 80–84     30 minutes
    Rating 75–79     120 minutes
    Rating 70–74     360 minutes
    Rating under 70  1440 minutes
    Unknown rating   1440 minutes

Recommended environment variables:
    DATABASE_URL                              required
    PLAYWRIGHT_HEADLESS                       true
    PLAYWRIGHT_TIMEOUT_MS                     45000

    FUTGG_PRICE_CONCURRENCY                   15
    FUTGG_PRICE_BATCH_SIZE                    500
    FUTGG_PRICE_HIGH_PRIORITY_SHARE           0.90
    FUTGG_PRICE_REQUEST_DELAY                 0.10
    FUTGG_PRICE_IDLE_SLEEP_SECONDS            5
    FUTGG_PRICE_BATCH_SLEEP_SECONDS           1
    FUTGG_PRICE_BLOCK_BACKOFF_SECONDS         900

    FUTGG_RATING_85_PLUS_INTERVAL_MIN         10
    FUTGG_RATING_80_84_INTERVAL_MIN           30
    FUTGG_RATING_75_79_INTERVAL_MIN           120
    FUTGG_RATING_70_74_INTERVAL_MIN           360
    FUTGG_RATING_UNDER_70_INTERVAL_MIN        1440

    FUTGG_CIRCUIT_BREAKER_THRESHOLD           8
    FUTGG_HOT_DISCOUNT_THRESHOLD              0.12
    FUTGG_HOT_INTERVAL_MIN                    10
    FUTGG_HOT_MIN_SALES                       5

Price outcomes:
    success
    untradeable
    no_active_market
    price_section_missing
    page_failed
    parse_failed
    rate_limited
    blocked
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
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from futgg_common import CircuitBreaker, parse_futgg_card
from futgg_player_sync import ensure_schema as ensure_player_schema
from monitoring import alert, heartbeat


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("futgg_price_sync")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)

    if raw is None:
        return default

    return raw.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def env_int(
    name: str,
    default: int,
    minimum: int | None = None,
) -> int:
    value = int(os.getenv(name, str(default)))

    if minimum is not None:
        value = max(minimum, value)

    return value


def env_float(
    name: str,
    default: float,
    minimum: float | None = None,
    maximum: float | None = None,
) -> float:
    value = float(os.getenv(name, str(default)))

    if minimum is not None:
        value = max(minimum, value)

    if maximum is not None:
        value = min(maximum, value)

    return value


DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")


HEADLESS = env_bool(
    "PLAYWRIGHT_HEADLESS",
    True,
)

TIMEOUT_MS = env_int(
    "PLAYWRIGHT_TIMEOUT_MS",
    45000,
    minimum=5000,
)

CONCURRENCY = env_int(
    "FUTGG_PRICE_CONCURRENCY",
    15,
    minimum=1,
)

BATCH_SIZE = env_int(
    "FUTGG_PRICE_BATCH_SIZE",
    500,
    minimum=1,
)

HIGH_PRIORITY_SHARE = env_float(
    "FUTGG_PRICE_HIGH_PRIORITY_SHARE",
    0.90,
    minimum=0.50,
    maximum=1.0,
)

REQUEST_DELAY = env_float(
    "FUTGG_PRICE_REQUEST_DELAY",
    0.10,
    minimum=0.0,
)

IDLE_SLEEP_SECONDS = env_float(
    "FUTGG_PRICE_IDLE_SLEEP_SECONDS",
    5.0,
    minimum=1.0,
)

BATCH_SLEEP_SECONDS = env_float(
    "FUTGG_PRICE_BATCH_SLEEP_SECONDS",
    1.0,
    minimum=0.0,
)

BLOCK_BACKOFF_SECONDS = env_float(
    "FUTGG_PRICE_BLOCK_BACKOFF_SECONDS",
    900.0,
    minimum=60.0,
)

OVERLAP_LOCK_KEY = env_int(
    "FUTGG_PRICE_LOCK_KEY",
    7741022,
)

CIRCUIT_BREAKER_THRESHOLD = env_int(
    "FUTGG_CIRCUIT_BREAKER_THRESHOLD",
    8,
    minimum=1,
)

BLOCKED_STATUS_CODES = {
    403,
    429,
}


RATING_INTERVALS = {
    "85_plus": env_int(
        "FUTGG_RATING_85_PLUS_INTERVAL_MIN",
        10,
        minimum=5,
    ),
    "80_84": env_int(
        "FUTGG_RATING_80_84_INTERVAL_MIN",
        30,
        minimum=10,
    ),
    "75_79": env_int(
        "FUTGG_RATING_75_79_INTERVAL_MIN",
        120,
        minimum=30,
    ),
    "70_74": env_int(
        "FUTGG_RATING_70_74_INTERVAL_MIN",
        360,
        minimum=60,
    ),
    "under_70": env_int(
        "FUTGG_RATING_UNDER_70_INTERVAL_MIN",
        1440,
        minimum=60,
    ),
}


HOT_DISCOUNT_THRESHOLD = env_float(
    "FUTGG_HOT_DISCOUNT_THRESHOLD",
    0.12,
    minimum=0.0,
)

HOT_INTERVAL_MIN = env_int(
    "FUTGG_HOT_INTERVAL_MIN",
    10,
    minimum=5,
)

HOT_MIN_SALES = env_int(
    "FUTGG_HOT_MIN_SALES",
    5,
    minimum=1,
)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


def price_interval_minutes(
    rating: int | None,
) -> int:
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


def is_hot_opportunity(card: Any) -> bool:
    if card.lowest_bin is None:
        return False

    valid_sales = [
        int(sale.sold_price)
        for sale in card.recent_sales
        if sale.sold_price is not None
        and int(sale.sold_price) > 0
    ]

    if len(valid_sales) < HOT_MIN_SALES:
        return False

    median_price = statistics.median(valid_sales)

    if median_price <= 0:
        return False

    discount = (
        median_price - card.lowest_bin
    ) / median_price

    return discount >= HOT_DISCOUNT_THRESHOLD


def new_stats() -> dict[str, int]:
    return {
        "selected": 0,
        "selected_85_plus": 0,
        "selected_lower": 0,
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


async def ensure_schema(
    conn: asyncpg.Connection,
) -> None:
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
        CREATE INDEX IF NOT EXISTS
        futgg_bin_history_card_captured_idx
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
        CREATE INDEX IF NOT EXISTS
        futgg_sales_history_card_sold_idx
        ON futgg_sales_history (
            source_card_id,
            approximate_sold_at DESC
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        futgg_players_high_rating_due_idx
        ON futgg_players (
            next_price_due_at,
            price_updated_at
        )
        WHERE is_active = TRUE
          AND rating >= 85
          AND is_tradeable IS DISTINCT FROM FALSE
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS
        futgg_players_lower_rating_due_idx
        ON futgg_players (
            next_price_due_at,
            rating DESC,
            price_updated_at
        )
        WHERE is_active = TRUE
          AND (
              rating < 85
              OR rating IS NULL
          )
          AND is_tradeable IS DISTINCT FROM FALSE
        """
    )


async def fetch_due_cards(
    conn: asyncpg.Connection,
) -> list[asyncpg.Record]:
    """Select high-rated due cards first, then fill spare capacity.

    A fixed portion of every batch is reserved for rating-85+ cards.

    If fewer high-rated cards are due than the reserved amount, unused
    capacity is filled by lower-rated cards.
    """

    high_reserved = max(
        1,
        int(BATCH_SIZE * HIGH_PRIORITY_SHARE),
    )

    lower_reserved = max(
        0,
        BATCH_SIZE - high_reserved,
    )

    high_rows = await conn.fetch(
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
          AND rating >= 85
          AND is_tradeable IS DISTINCT FROM FALSE
          AND (
              next_price_due_at IS NULL
              OR next_price_due_at <= NOW()
          )
        ORDER BY
            next_price_due_at ASC NULLS FIRST,
            price_updated_at ASC NULLS FIRST,
            rating DESC,
            source_card_id ASC
        LIMIT $1
        """,
        high_reserved,
    )

    remaining_capacity = BATCH_SIZE - len(high_rows)

    if remaining_capacity <= 0:
        return list(high_rows)

    # Always permit at least the remaining capacity to be filled by
    # lower-rated cards when there are not enough high-rated cards due.
    lower_limit = max(
        lower_reserved,
        remaining_capacity,
    )

    lower_limit = min(
        lower_limit,
        remaining_capacity,
    )

    lower_rows = await conn.fetch(
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
          AND (
              rating < 85
              OR rating IS NULL
          )
          AND is_tradeable IS DISTINCT FROM FALSE
          AND (
              next_price_due_at IS NULL
              OR next_price_due_at <= NOW()
          )
        ORDER BY
            CASE
                WHEN rating >= 80 THEN 0
                WHEN rating >= 75 THEN 1
                WHEN rating >= 70 THEN 2
                ELSE 3
            END,
            next_price_due_at ASC NULLS FIRST,
            price_updated_at ASC NULLS FIRST,
            rating DESC NULLS LAST,
            source_card_id ASC
        LIMIT $1
        """,
        lower_limit,
    )

    return [
        *high_rows,
        *lower_rows,
    ]


async def record_success(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    card: Any,
    captured_at: datetime,
) -> tuple[int, int, int, bool]:
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
            VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6
            )
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

        if sold_price is None:
            continue

        sold_price = int(sold_price)

        if sold_price <= 0:
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
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11,
                $12
            )
            ON CONFLICT (source_fingerprint)
            DO NOTHING
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

        inserted = int(
            result.rsplit(" ", 1)[-1]
        )

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

        return (
            bin_rows,
            sales_new,
            sales_dupe,
            False,
        )

    interval_minutes = price_interval_minutes(
        row["rating"]
    )

    if status in {
        "no_active_market",
        "price_section_missing",
    }:
        # Empty-market cards do not need to consume the same capacity as
        # cards which currently have an active and useful market.
        interval_minutes = min(
            interval_minutes * 3,
            4320,
        )

    is_hot = (
        status == "success"
        and is_hot_opportunity(card)
    )

    if is_hot:
        interval_minutes = min(
            interval_minutes,
            HOT_INTERVAL_MIN,
        )

    # Rolling schedule: every card becomes due relative to the moment its
    # latest price observation was captured.
    next_due_at = captured_at + timedelta(
        minutes=interval_minutes
    )

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            price_updated_at = $2,
            next_price_due_at = $3,
            last_price_status = $4,
            is_tradeable = COALESCE(
                $5,
                is_tradeable
            ),
            last_seen_at = NOW()
        WHERE source_card_id = $1
        """,
        card.source_card_id,
        captured_at,
        next_due_at,
        status,
        True if status == "success" else None,
    )

    return (
        bin_rows,
        sales_new,
        sales_dupe,
        is_hot,
    )


async def record_failure(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    status: str,
) -> None:
    normal_interval = price_interval_minutes(
        row["rating"]
    )

    if status in {
        "blocked",
        "rate_limited",
    }:
        retry_minutes = 15
    else:
        retry_minutes = min(
            normal_interval,
            5,
        )

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            next_price_due_at =
                NOW()
                + ($2 * INTERVAL '1 minute'),
            last_price_status = $3
        WHERE source_card_id = $1
        """,
        row["source_card_id"],
        retry_minutes,
        status[:200],
    )


async def process_card(
    worker_id: int,
    page: Page,
    pool: asyncpg.Pool,
    row: asyncpg.Record,
    stats: dict[str, int],
    breaker: CircuitBreaker,
) -> None:
    captured_at = datetime.now(timezone.utc)

    try:
        response = await page.goto(
            row["source_url"],
            wait_until="domcontentloaded",
            timeout=TIMEOUT_MS,
        )

        http_status = (
            response.status
            if response is not None
            else 0
        )

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
                    failure_status,
                )

            breaker.record_failure(
                f"HTTP {http_status}"
            )

            log.warning(
                (
                    "worker=%d card=%s rating=%s "
                    "HTTP=%d status=%s"
                ),
                worker_id,
                row["source_card_id"],
                row["rating"],
                http_status,
                failure_status,
            )

            return

        if http_status != 200:
            raise RuntimeError(
                f"HTTP {http_status}"
            )

        await page.locator(
            ".fc-card"
        ).first.wait_for(
            state="attached",
            timeout=15000,
        )

        try:
            await page.locator(
                "#prices-overview"
            ).wait_for(
                state="attached",
                timeout=12000,
            )
        except Exception:
            # parse_futgg_card determines whether the price section is
            # absent because the card is untradeable, has no market, or
            # the section failed to render.
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
                "outcome=%s bin=%s sales=%d "
                "sales_new=%d hot=%s"
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
            (
                ValueError,
                KeyError,
                TypeError,
            ),
        )

        if is_parse_failure:
            stats["parse_failures"] += 1
            failure_status = "parse_failed"
        else:
            stats["navigation_failures"] += 1
            failure_status = "page_failed"

        log.warning(
            "worker=%d card=%s rating=%s failed: %s",
            worker_id,
            row["source_card_id"],
            row["rating"],
            exc,
        )

        breaker.record_failure(
            f"{type(exc).__name__}: {exc}"
        )

        async with pool.acquire() as conn:
            await record_failure(
                conn,
                row,
                failure_status,
            )

    finally:
        if REQUEST_DELAY > 0:
            await asyncio.sleep(
                random.uniform(
                    REQUEST_DELAY * 0.7,
                    REQUEST_DELAY * 1.3,
                )
            )


async def worker_loop(
    worker_id: int,
    page: Page,
    pool: asyncpg.Pool,
    queue: asyncio.Queue[asyncpg.Record],
    stats: dict[str, int],
    breaker: CircuitBreaker,
) -> None:
    while True:
        if breaker.tripped:
            log.warning(
                (
                    "worker=%d stopping batch: "
                    "circuit breaker tripped (%s)"
                ),
                worker_id,
                breaker.trip_reason,
            )
            return

        try:
            row = queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        try:
            await process_card(
                worker_id,
                page,
                pool,
                row,
                stats,
                breaker,
            )
        finally:
            queue.task_done()


async def block_unneeded_resources(route: Any) -> None:
    """Block assets not required for HTML price parsing."""

    resource_type = route.request.resource_type

    if resource_type in {
        "image",
        "media",
        "font",
    }:
        await route.abort()
        return

    await route.continue_()


async def create_browser(
    playwright: Playwright,
) -> tuple[Browser, BrowserContext, list[Page]]:
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

    await context.route(
        "**/*",
        block_unneeded_resources,
    )

    pages = [
        await context.new_page()
        for _ in range(CONCURRENCY)
    ]

    return (
        browser,
        context,
        pages,
    )


async def close_browser_resources(
    browser: Browser | None,
    context: BrowserContext | None,
    pages: list[Page],
) -> None:
    for page in pages:
        try:
            await page.close()
        except Exception:
            pass

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


async def write_heartbeat(
    pool: asyncpg.Pool,
    stats: dict[str, int],
    run_seconds: int,
    breaker: CircuitBreaker,
) -> None:
    selected = stats["selected"]
    cards_ok = stats["cards_ok"]
    cards_failed = stats["cards_failed"]

    ok = (
        not breaker.tripped
        and (
            selected == 0
            or cards_ok > 0
        )
        and cards_failed < max(
            selected,
            1,
        )
    )

    cards_per_minute = 0.0

    if run_seconds > 0:
        cards_per_minute = (
            cards_ok / run_seconds
        ) * 60

    detail_parts = [
        *[
            f"{key}={value}"
            for key, value in stats.items()
        ],
        f"run_seconds={run_seconds}",
        f"cards_per_minute={cards_per_minute:.1f}",
        f"concurrency={CONCURRENCY}",
        f"batch_size={BATCH_SIZE}",
    ]

    async with pool.acquire() as conn:
        await heartbeat(
            conn,
            "futgg_price_sync",
            ok=ok,
            detail=" ".join(detail_parts),
        )

    if not ok:
        await alert(
            (
                "futgg_price_sync unhealthy: "
                f"{stats} "
                f"run_seconds={run_seconds} "
                f"breaker={breaker.trip_reason}"
            )
        )


async def process_due_batch(
    pool: asyncpg.Pool,
    pages: list[Page],
) -> tuple[dict[str, int], CircuitBreaker]:
    batch_started_at = datetime.now(timezone.utc)
    stats = new_stats()

    async with pool.acquire() as conn:
        rows = await fetch_due_cards(conn)

    stats["selected"] = len(rows)

    for row in rows:
        rating = row["rating"]

        if rating is not None and rating >= 85:
            stats["selected_85_plus"] += 1
        else:
            stats["selected_lower"] += 1

    if not rows:
        return (
            stats,
            CircuitBreaker(
                CIRCUIT_BREAKER_THRESHOLD
            ),
        )

    log.info(
        (
            "Selected=%d high=%d lower=%d "
            "intervals=%s concurrency=%d "
            "batch_size=%d request_delay=%.2f"
        ),
        stats["selected"],
        stats["selected_85_plus"],
        stats["selected_lower"],
        RATING_INTERVALS,
        CONCURRENCY,
        BATCH_SIZE,
        REQUEST_DELAY,
    )

    queue: asyncio.Queue[asyncpg.Record] = (
        asyncio.Queue()
    )

    for row in rows:
        queue.put_nowait(row)

    breaker = CircuitBreaker(
        CIRCUIT_BREAKER_THRESHOLD
    )

    await asyncio.gather(
        *[
            worker_loop(
                worker_id=index + 1,
                page=page,
                pool=pool,
                queue=queue,
                stats=stats,
                breaker=breaker,
            )
            for index, page in enumerate(pages)
        ]
    )

    run_seconds = max(
        1,
        int(
            (
                datetime.now(timezone.utc)
                - batch_started_at
            ).total_seconds()
        ),
    )

    await write_heartbeat(
        pool,
        stats,
        run_seconds,
        breaker,
    )

    cards_per_minute = (
        stats["cards_ok"] / run_seconds
    ) * 60

    log.info(
        (
            "Batch complete in %d seconds: "
            "cards_ok=%d cards_failed=%d "
            "cards_per_minute=%.1f "
            "selected_high=%d selected_lower=%d"
        ),
        run_seconds,
        stats["cards_ok"],
        stats["cards_failed"],
        cards_per_minute,
        stats["selected_85_plus"],
        stats["selected_lower"],
    )

    return (
        stats,
        breaker,
    )


async def run_forever() -> None:
    lock_conn: asyncpg.Connection | None = None
    pool: asyncpg.Pool | None = None
    playwright: Playwright | None = None
    browser: Browser | None = None
    context: BrowserContext | None = None
    pages: list[Page] = []

    try:
        lock_conn = await asyncpg.connect(
            DATABASE_URL
        )

        got_lock = await lock_conn.fetchval(
            "SELECT pg_try_advisory_lock($1)",
            OVERLAP_LOCK_KEY,
        )

        if not got_lock:
            log.error(
                (
                    "Another FUT.GG price worker "
                    "already owns advisory lock %d"
                ),
                OVERLAP_LOCK_KEY,
            )
            return

        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=2,
            max_size=CONCURRENCY + 4,
            command_timeout=60,
        )

        async with pool.acquire() as conn:
            await ensure_schema(conn)

        playwright = await async_playwright().start()

        (
            browser,
            context,
            pages,
        ) = await create_browser(playwright)

        log.info(
            (
                "Continuous FUT.GG price worker started: "
                "concurrency=%d batch_size=%d "
                "high_priority_share=%.2f intervals=%s"
            ),
            CONCURRENCY,
            BATCH_SIZE,
            HIGH_PRIORITY_SHARE,
            RATING_INTERVALS,
        )

        while True:
            try:
                stats, breaker = await process_due_batch(
                    pool,
                    pages,
                )

                if breaker.tripped:
                    log.warning(
                        (
                            "Circuit breaker tripped: %s. "
                            "Backing off for %.0f seconds."
                        ),
                        breaker.trip_reason,
                        BLOCK_BACKOFF_SECONDS,
                    )

                    await close_browser_resources(
                        browser,
                        context,
                        pages,
                    )

                    browser = None
                    context = None
                    pages = []

                    await asyncio.sleep(
                        BLOCK_BACKOFF_SECONDS
                    )

                    (
                        browser,
                        context,
                        pages,
                    ) = await create_browser(
                        playwright
                    )

                    continue

                if stats["selected"] == 0:
                    await asyncio.sleep(
                        IDLE_SLEEP_SECONDS
                    )
                else:
                    await asyncio.sleep(
                        BATCH_SLEEP_SECONDS
                    )

            except asyncio.CancelledError:
                raise

            except Exception as exc:
                log.exception(
                    "Price batch failed: %s",
                    exc,
                )

                try:
                    await alert(
                        f"futgg_price_sync batch crashed: {exc}"
                    )
                except Exception:
                    log.exception(
                        "Unable to send price-sync alert"
                    )

                await close_browser_resources(
                    browser,
                    context,
                    pages,
                )

                browser = None
                context = None
                pages = []

                await asyncio.sleep(30)

                (
                    browser,
                    context,
                    pages,
                ) = await create_browser(
                    playwright
                )

    finally:
        await close_browser_resources(
            browser,
            context,
            pages,
        )

        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass

        if pool is not None:
            await pool.close()

        if lock_conn is not None:
            try:
                await lock_conn.execute(
                    "SELECT pg_advisory_unlock($1)",
                    OVERLAP_LOCK_KEY,
                )
            except Exception:
                pass

            await lock_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())

    except KeyboardInterrupt:
        log.info(
            "FUT.GG price worker stopped"
        )

    except Exception as exc:
        log.exception(
            "futgg_price_sync failed: %s",
            exc,
        )
        sys.exit(1)
