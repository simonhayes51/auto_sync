#!/usr/bin/env python
"""Continuous rating-prioritised FUT.GG price and recent-sales sync.

Designed to run as a permanent Railway worker, not as a cron job.

Core behaviour:
- Rating 85+ cards are always selected before lower-rated cards.
- Successful 85+ cards are refreshed every 10 minutes by default.
- Lower-rated cards use progressively longer refresh intervals.
- Browser/context/pages remain alive between batches.
- Incomplete price renders are retried once.
- Ambiguous no-market results are rejected as technical failures.
- price_section_missing is never stored as a valid market outcome.
- Cloudflare 403/429 responses trigger a circuit-breaker backoff.
- Confirmed untradeable cards are removed from future price selection.
- Failed attempts do not overwrite historical BIN or sales data.

Recommended Railway command:
    python -m scripts.futgg_price_sync

Recommended Railway variables:
    PLAYWRIGHT_HEADLESS=true
    PLAYWRIGHT_TIMEOUT_MS=45000

    FUTGG_PRICE_CONCURRENCY=8
    FUTGG_PRICE_BATCH_SIZE=250
    FUTGG_PRICE_REQUEST_DELAY=0.20
    FUTGG_PRICE_IDLE_SLEEP_SECONDS=5
    FUTGG_PRICE_BATCH_SLEEP_SECONDS=1
    FUTGG_PRICE_BLOCK_BACKOFF_SECONDS=900

    FUTGG_PRICE_SECTION_TIMEOUT_MS=12000
    FUTGG_PRICE_RENDER_SETTLE_MS=700
    FUTGG_PRICE_RETRY_SETTLE_MS=2000
    FUTGG_PRICE_MAX_ATTEMPTS=2

    FUTGG_RATING_85_PLUS_INTERVAL_MIN=10
    FUTGG_RATING_80_84_INTERVAL_MIN=30
    FUTGG_RATING_75_79_INTERVAL_MIN=120
    FUTGG_RATING_70_74_INTERVAL_MIN=360
    FUTGG_RATING_UNDER_70_INTERVAL_MIN=1440

    FUTGG_CIRCUIT_BREAKER_THRESHOLD=8
    FUTGG_HOT_DISCOUNT_THRESHOLD=0.12
    FUTGG_HOT_INTERVAL_MIN=10
    FUTGG_HOT_MIN_SALES=5
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from futgg_common import CircuitBreaker, parse_futgg_card
from futgg_instrumentation import NullTimers, StageTimers
from futgg_player_sync import ensure_schema as ensure_player_schema
from monitoring import alert, heartbeat


SCRIPT_VERSION = "futgg-price-sync-render-validation-v3"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("futgg_price_sync")


class PriceRenderError(RuntimeError):
    """The page loaded, but usable price data did not render."""


class PriceParseError(RuntimeError):
    """The rendered price content could not be parsed safely."""


class NavigationError(RuntimeError):
    """The player page could not be loaded successfully."""


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
    8,
    minimum=1,
)

BATCH_SIZE = env_int(
    "FUTGG_PRICE_BATCH_SIZE",
    250,
    minimum=1,
)

REQUEST_DELAY = env_float(
    "FUTGG_PRICE_REQUEST_DELAY",
    0.20,
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

PRICE_SECTION_TIMEOUT_MS = env_int(
    "FUTGG_PRICE_SECTION_TIMEOUT_MS",
    12000,
    minimum=1000,
)

RENDER_SETTLE_MS = env_int(
    "FUTGG_PRICE_RENDER_SETTLE_MS",
    700,
    minimum=0,
)

RETRY_SETTLE_MS = env_int(
    "FUTGG_PRICE_RETRY_SETTLE_MS",
    2000,
    minimum=0,
)

MAX_ATTEMPTS = env_int(
    "FUTGG_PRICE_MAX_ATTEMPTS",
    2,
    minimum=1,
)

OVERLAP_LOCK_KEY = env_int(
    "FUTGG_PRICE_LOCK_KEY",
    7741022,
)

LOCK_RETRY_SECONDS = env_float(
    "FUTGG_PRICE_LOCK_RETRY_SECONDS",
    15.0,
    minimum=1.0,
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


# Opt-in one-shot network diagnostic. When true, run_forever() performs a
# single instrumented page load against FUTGG_DISCOVER_URL and exits
# WITHOUT touching the price pipeline. When false (the default) the worker
# behaves exactly as it did before this flag existed - futgg_discover is
# not even imported.
DISCOVER_PRICE_NETWORK = env_bool(
    "FUTGG_DISCOVER_PRICE_NETWORK",
    False,
)

DISCOVER_URL = (
    os.getenv("FUTGG_DISCOVER_URL") or ""
).strip()

DISCOVER_REPORT_PATH = (
    os.getenv("FUTGG_DISCOVER_REPORT_PATH") or ""
).strip()

# Per-stage batch timings. On by default: the collection cost is a float
# subtraction and a list append per stage, and there is no way to find the
# next bottleneck without it.
STAGE_TIMINGS_ENABLED = env_bool(
    "FUTGG_PRICE_STAGE_TIMINGS",
    True,
)


EXPLICIT_NO_MARKET_PHRASES = (
    "no active market",
    "no current listings",
    "no listings available",
    "no market data available",
    "there are no active listings",
    "no recent sales",
)


def price_interval_minutes(rating: int | None) -> int:
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


# Shared no-op sink so every instrumented call site can use the same
# `with timers.track(...)` form without a None check. It must genuinely
# discard rather than being an unused StageTimers - a real collector held
# for the process lifetime would accumulate samples from every batch
# forever, which is precisely the leak instrumentation must not introduce.
_NULL_TIMERS = NullTimers()


def new_timers() -> StageTimers | NullTimers:
    """A fresh collector per batch, or the discarding sink when timings
    are disabled."""
    return StageTimers() if STAGE_TIMINGS_ENABLED else _NULL_TIMERS


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
        "confirmed_no_market": 0,
        "untradeable": 0,
        "hot_opportunities": 0,
        "blocked": 0,
        "rate_limited": 0,
        "parse_failures": 0,
        "render_failures": 0,
        "navigation_failures": 0,
        "retries": 0,
    }


async def ensure_schema(
    conn: asyncpg.Connection,
) -> None:
    await ensure_player_schema(conn)

    await conn.execute(
        """
        ALTER TABLE futgg_players
        ADD COLUMN IF NOT EXISTS last_price_attempt_at TIMESTAMPTZ
        """
    )

    await conn.execute(
        """
        ALTER TABLE futgg_players
        ADD COLUMN IF NOT EXISTS last_price_error TEXT
        """
    )

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
    """Fill each batch with due 85+ cards first.

    Lower-rated cards are selected only when fewer than BATCH_SIZE
    high-rated cards are currently due.
    """

    high_rows = await conn.fetch(
        """
        SELECT
            source_card_id,
            source_url,
            price_tier,
            rating,
            next_price_due_at,
            price_updated_at,
            last_price_status
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
        BATCH_SIZE,
    )

    remaining_capacity = BATCH_SIZE - len(high_rows)

    if remaining_capacity <= 0:
        return list(high_rows)

    lower_rows = await conn.fetch(
        """
        SELECT
            source_card_id,
            source_url,
            price_tier,
            rating,
            next_price_due_at,
            price_updated_at,
            last_price_status
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
        remaining_capacity,
    )

    return [
        *high_rows,
        *lower_rows,
    ]


async def mark_attempt_started(
    conn: asyncpg.Connection,
    source_card_id: int,
    attempted_at: datetime,
) -> None:
    await conn.execute(
        """
        UPDATE futgg_players
        SET
            last_price_attempt_at = $2,
            last_price_error = NULL
        WHERE source_card_id = $1
        """,
        source_card_id,
        attempted_at,
    )


async def record_success(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    card: Any,
    captured_at: datetime,
) -> tuple[int, int, int, bool]:
    bin_rows = 0
    sales_new = 0
    sales_dupe = 0

    if card.price_outcome == "price_section_missing":
        raise PriceParseError(
            "price_section_missing cannot be recorded as a valid outcome"
        )

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
                last_price_attempt_at = $2,
                next_price_due_at = NULL,
                last_price_status = $3,
                last_price_error = NULL,
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

    if status == "no_active_market":
        # This status only reaches record_success when explicit text on
        # the rendered page confirms there is genuinely no market.
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

    next_due_at = captured_at + timedelta(
        minutes=interval_minutes
    )

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            price_updated_at = $2,
            last_price_attempt_at = $2,
            next_price_due_at = $3,
            last_price_status = $4,
            last_price_error = NULL,
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
    error_message: str,
) -> None:
    normal_interval = price_interval_minutes(
        row["rating"]
    )

    if status in {
        "blocked",
        "rate_limited",
    }:
        retry_minutes = 15

    elif status == "price_render_failed":
        retry_minutes = min(
            normal_interval,
            2,
        )

    else:
        retry_minutes = min(
            normal_interval,
            5,
        )

    await conn.execute(
        """
        UPDATE futgg_players
        SET
            last_price_attempt_at = NOW(),
            next_price_due_at =
                NOW()
                + ($2 * INTERVAL '1 minute'),
            last_price_status = $3,
            last_price_error = $4
        WHERE source_card_id = $1
        """,
        row["source_card_id"],
        retry_minutes,
        status,
        error_message[:1000],
    )


async def safe_body_text(page: Page) -> str:
    try:
        return (
            await page.locator("body")
            .inner_text(timeout=5000)
        )
    except Exception:
        return ""


async def safe_price_section_text(page: Page) -> str:
    try:
        section = page.locator(
            "#prices-overview"
        )

        if await section.count() == 0:
            return ""

        return await section.inner_text(
            timeout=5000
        )

    except Exception:
        return ""


def contains_explicit_no_market_text(
    text: str,
) -> bool:
    normalised = " ".join(
        text.lower().split()
    )

    return any(
        phrase in normalised
        for phrase in EXPLICIT_NO_MARKET_PHRASES
    )


async def wait_for_base_card(page: Page) -> None:
    try:
        await page.locator(
            ".fc-card"
        ).first.wait_for(
            state="attached",
            timeout=15000,
        )

    except PlaywrightTimeoutError as exc:
        raise PriceRenderError(
            "player card did not render"
        ) from exc


async def wait_for_price_section(page: Page) -> bool:
    """Wait for the price section.

    Returns True when the section attaches. A False result is not
    immediately treated as a market outcome because untradeable cards
    may legitimately have a different page structure.
    """

    try:
        await page.locator(
            "#prices-overview"
        ).wait_for(
            state="attached",
            timeout=PRICE_SECTION_TIMEOUT_MS,
        )

        return True

    except PlaywrightTimeoutError:
        return False


async def validate_parsed_card(
    page: Page,
    card: Any,
    price_section_attached: bool,
) -> None:
    outcome = card.price_outcome

    if outcome == "success":
        if (
            card.lowest_bin is None
            and not card.recent_sales
        ):
            raise PriceParseError(
                "success outcome contained no BIN and no sales"
            )

        return

    if outcome == "untradeable":
        return

    if outcome == "price_section_missing":
        raise PriceRenderError(
            "parser reported price_section_missing"
        )

    if outcome == "no_active_market":
        section_text = await safe_price_section_text(
            page
        )

        body_text = await safe_body_text(
            page
        )

        combined_text = (
            f"{section_text}\n{body_text}"
        )

        if not contains_explicit_no_market_text(
            combined_text
        ):
            raise PriceRenderError(
                "ambiguous no_active_market without explicit page text"
            )

        return

    raise PriceParseError(
        f"unknown price outcome: {outcome!r}"
    )


async def load_and_parse_card(
    page: Page,
    row: asyncpg.Record,
    stats: dict[str, int],
    timers: StageTimers | NullTimers | None = None,
) -> tuple[Any, datetime]:
    """Load, render, parse and validate one card.

    Incomplete price renders receive one or more attempts according to
    FUTGG_PRICE_MAX_ATTEMPTS. Ambiguous market states never reach the
    database as successful outcomes.
    """

    last_error: Exception | None = None
    timers = timers or _NULL_TIMERS

    for attempt in range(1, MAX_ATTEMPTS + 1):
        captured_at = datetime.now(timezone.utc)
        # Attempts after the first are retries. Timing them separately
        # keeps the p50/p95 of the normal path honest - folding retry cost
        # into page_goto would make a healthy first attempt look slow.
        attempt_started = time.perf_counter()

        try:
            with timers.track("page_goto"):
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
                failure_status = (
                    "rate_limited"
                    if http_status == 429
                    else "blocked"
                )

                raise NavigationError(
                    f"{failure_status}: HTTP {http_status}"
                )

            if http_status != 200:
                raise NavigationError(
                    f"unexpected HTTP {http_status}"
                )

            with timers.track("base_card_wait"):
                await wait_for_base_card(page)

            with timers.track("price_section_wait"):
                price_section_attached = (
                    await wait_for_price_section(page)
                )

            settle_ms = (
                RENDER_SETTLE_MS
                if attempt == 1
                else RETRY_SETTLE_MS
            )

            if settle_ms > 0:
                with timers.track("render_settle"):
                    await page.wait_for_timeout(
                        settle_ms
                    )

            with timers.track("page_content"):
                html = await page.content()

            with timers.track("parse_card"):
                card = parse_futgg_card(
                    html,
                    row["source_url"],
                    captured_at,
                )

            with timers.track("validation"):
                await validate_parsed_card(
                    page,
                    card,
                    price_section_attached,
                )

            if attempt > 1:
                timers.record(
                    "retry_attempt_seconds",
                    time.perf_counter() - attempt_started,
                )

            return card, captured_at

        except NavigationError:
            raise

        except (
            PriceRenderError,
            PriceParseError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            last_error = exc

            if attempt >= MAX_ATTEMPTS:
                break

            stats["retries"] += 1

            log.warning(
                (
                    "card=%s rating=%s attempt=%d/%d "
                    "incomplete=%s; retrying"
                ),
                row["source_card_id"],
                row["rating"],
                attempt,
                MAX_ATTEMPTS,
                exc,
            )

            try:
                await page.goto(
                    "about:blank",
                    wait_until="commit",
                    timeout=5000,
                )
            except Exception:
                pass

            await asyncio.sleep(
                random.uniform(0.3, 0.8)
            )

            # Cost of the attempt that just failed, including its waits and
            # the blank-page reset. This is the number that says whether
            # retries are a meaningful drag on throughput.
            timers.record(
                "retry_attempt_seconds",
                time.perf_counter() - attempt_started,
            )

        except PlaywrightTimeoutError as exc:
            raise NavigationError(
                f"navigation timeout: {exc}"
            ) from exc

        except Exception as exc:
            raise NavigationError(
                f"{type(exc).__name__}: {exc}"
            ) from exc

    if isinstance(last_error, PriceParseError):
        raise last_error

    raise PriceRenderError(
        str(last_error or "price data did not render")
    )


async def process_card(
    worker_id: int,
    page: Page,
    pool: asyncpg.Pool,
    row: asyncpg.Record,
    stats: dict[str, int],
    breaker: CircuitBreaker,
    timers: StageTimers | NullTimers | None = None,
) -> None:
    attempt_started_at = datetime.now(
        timezone.utc
    )
    timers = timers or _NULL_TIMERS
    card_started = time.perf_counter()

    # Deliberately outside the try, exactly as before: a failure to record
    # the attempt is an infrastructure problem, not a card outcome, and
    # must not be rewritten into a per-card failure row.
    with timers.track("mark_attempt_started"):
        async with pool.acquire() as conn:
            await mark_attempt_started(
                conn,
                row["source_card_id"],
                attempt_started_at,
            )

    try:
        card, captured_at = await load_and_parse_card(
            page,
            row,
            stats,
            timers,
        )

        with timers.track("record_success_db"):
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

        elif card.price_outcome == "no_active_market":
            stats["confirmed_no_market"] += 1

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

    except NavigationError as exc:
        message = str(exc)

        if "rate_limited" in message:
            failure_status = "rate_limited"
            stats["rate_limited"] += 1
            stats["blocked"] += 1

        elif "blocked" in message:
            failure_status = "blocked"
            stats["blocked"] += 1

        else:
            failure_status = "page_failed"
            stats["navigation_failures"] += 1

        stats["cards_failed"] += 1

        with timers.track("record_failure_db"):
            async with pool.acquire() as conn:
                await record_failure(
                    conn,
                    row,
                    failure_status,
                    message,
                )

        # Only navigation, block and rate-limit failures influence the
        # global circuit breaker. Individual parser/render misses do not.
        breaker.record_failure(
            message
        )

        log.warning(
            (
                "worker=%d card=%s rating=%s "
                "navigation_failed status=%s error=%s"
            ),
            worker_id,
            row["source_card_id"],
            row["rating"],
            failure_status,
            message,
        )

    except PriceRenderError as exc:
        stats["cards_failed"] += 1
        stats["render_failures"] += 1

        async with pool.acquire() as conn:
            await record_failure(
                conn,
                row,
                "price_render_failed",
                str(exc),
            )

        log.warning(
            (
                "worker=%d card=%s rating=%s "
                "render_failed error=%s"
            ),
            worker_id,
            row["source_card_id"],
            row["rating"],
            exc,
        )

    except (
        PriceParseError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        stats["cards_failed"] += 1
        stats["parse_failures"] += 1

        async with pool.acquire() as conn:
            await record_failure(
                conn,
                row,
                "parse_failed",
                str(exc),
            )

        log.warning(
            (
                "worker=%d card=%s rating=%s "
                "parse_failed error=%s"
            ),
            worker_id,
            row["source_card_id"],
            row["rating"],
            exc,
        )

    except Exception as exc:
        stats["cards_failed"] += 1
        stats["navigation_failures"] += 1

        message = (
            f"{type(exc).__name__}: {exc}"
        )

        with timers.track("record_failure_db"):
            async with pool.acquire() as conn:
                await record_failure(
                    conn,
                    row,
                    "page_failed",
                    message,
                )

        breaker.record_failure(
            message
        )

        log.exception(
            (
                "worker=%d card=%s rating=%s "
                "unexpected failure"
            ),
            worker_id,
            row["source_card_id"],
            row["rating"],
        )

    finally:
        if REQUEST_DELAY > 0:
            with timers.track("request_delay"):
                await asyncio.sleep(
                    random.uniform(
                        REQUEST_DELAY * 0.7,
                        REQUEST_DELAY * 1.3,
                    )
                )

        # Whole-card wall time, recorded on every path so failures are
        # represented. card_total will exceed the sum of the stages by the
        # untimed glue between them; a large gap is itself a finding.
        timers.record(
            "card_total",
            time.perf_counter() - card_started,
        )


async def worker_loop(
    worker_id: int,
    page: Page,
    pool: asyncpg.Pool,
    queue: asyncio.Queue[asyncpg.Record],
    stats: dict[str, int],
    breaker: CircuitBreaker,
    timers: StageTimers | NullTimers | None = None,
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
                timers,
            )

        finally:
            queue.task_done()


async def block_unneeded_resources(
    route: Any,
) -> None:
    """Block heavy assets that are not needed for price parsing."""

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
) -> tuple[
    Browser,
    BrowserContext,
    list[Page],
]:
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
        f"script_version={SCRIPT_VERSION}",
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
) -> tuple[
    dict[str, int],
    CircuitBreaker,
]:
    batch_started_at = datetime.now(
        timezone.utc
    )

    stats = new_stats()
    timers = new_timers()

    with timers.track("db_select_due_cards"):
        async with pool.acquire() as conn:
            rows = await fetch_due_cards(conn)

    stats["selected"] = len(rows)

    for row in rows:
        rating = row["rating"]

        if (
            rating is not None
            and rating >= 85
        ):
            stats["selected_85_plus"] += 1
        else:
            stats["selected_lower"] += 1

    breaker = CircuitBreaker(
        CIRCUIT_BREAKER_THRESHOLD
    )

    if not rows:
        return stats, breaker

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

    await asyncio.gather(
        *[
            worker_loop(
                worker_id=index + 1,
                page=page,
                pool=pool,
                queue=queue,
                stats=stats,
                breaker=breaker,
                timers=timers,
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
        stats["cards_ok"]
        / run_seconds
    ) * 60

    log.info(
        (
            "Batch complete in %d seconds: "
            "cards_ok=%d cards_failed=%d "
            "render_failures=%d parse_failures=%d "
            "navigation_failures=%d retries=%d "
            "cards_per_minute=%.1f "
            "selected_high=%d selected_lower=%d"
        ),
        run_seconds,
        stats["cards_ok"],
        stats["cards_failed"],
        stats["render_failures"],
        stats["parse_failures"],
        stats["navigation_failures"],
        stats["retries"],
        cards_per_minute,
        stats["selected_85_plus"],
        stats["selected_lower"],
    )

    log_batch_timings(stats, timers, run_seconds)

    return stats, breaker


def log_batch_timings(
    stats: dict[str, int],
    timers: StageTimers | NullTimers,
    run_seconds: int,
) -> None:
    """Emit the per-stage breakdown once per batch.

    Aggregated deliberately: logging a timing per card would produce
    thousands of lines per batch and cost more than the work being
    measured. Stages are ordered by total time so whichever one dominates
    is the first line read.
    """
    summary = timers.summary()
    if not summary:
        return

    attempted = stats["cards_ok"] + stats["cards_failed"]
    success_pct = (
        (stats["cards_ok"] / attempted * 100.0) if attempted else 0.0
    )
    ok_per_minute = (stats["cards_ok"] / run_seconds) * 60 if run_seconds else 0.0
    attempted_per_minute = (attempted / run_seconds) * 60 if run_seconds else 0.0

    # Share of wall time each stage accounts for, per worker. Wall time is
    # run_seconds * CONCURRENCY because the stages run in parallel across
    # pages - comparing a stage total against run_seconds alone would
    # overstate every stage by roughly the concurrency factor.
    worker_seconds = max(run_seconds * CONCURRENCY, 1)

    log.info("--- stage timings (batch) ---")
    log.info(
        (
            "attempted=%d successful=%d success_pct=%.1f%% "
            "render_failures=%d parse_failures=%d navigation_failures=%d "
            "retries=%d successful_per_min=%.1f attempted_per_min=%.1f "
            "run_seconds=%d concurrency=%d worker_seconds=%d"
        ),
        attempted,
        stats["cards_ok"],
        success_pct,
        stats["render_failures"],
        stats["parse_failures"],
        stats["navigation_failures"],
        stats["retries"],
        ok_per_minute,
        attempted_per_minute,
        run_seconds,
        CONCURRENCY,
        worker_seconds,
    )

    for line in timers.format_lines():
        log.info(line)

    ordered = sorted(
        summary.items(),
        key=lambda item: item[1]["total_seconds"],
        reverse=True,
    )
    share_parts = [
        f"{stage}={s['total_seconds'] / worker_seconds * 100:.1f}%"
        for stage, s in ordered
        if stage != "card_total"
    ]
    log.info("share_of_worker_time: %s", " ".join(share_parts))

    card_total = summary.get("card_total")
    if card_total and card_total["count"]:
        # Only per-card stages reconcile against card_total.
        # db_select_due_cards runs once per BATCH, and
        # retry_attempt_seconds re-counts stages already summed
        # individually - including either would compare quantities that
        # are not the same kind of thing.
        excluded = {
            "card_total",
            "retry_attempt_seconds",
            "db_select_due_cards",
        }
        accounted = sum(
            s["total_seconds"]
            for stage, s in summary.items()
            if stage not in excluded
        )
        total = card_total["total_seconds"]
        residual = total - accounted
        pct = (residual / total * 100.0) if total else 0.0

        if residual >= 0:
            note = "untimed glue between stages"
        else:
            # Stage totals exceeding card_total means something was timed
            # outside the card_total window. Say so plainly rather than
            # printing a negative "glue" figure that reads like a bug in
            # the worker instead of a bug in the instrumentation.
            note = "NEGATIVE: stage totals exceed card_total, check stage nesting"

        log.info(
            (
                "card_total avg=%.1fms p95=%.1fms | per_card_accounted=%.1fs "
                "residual=%.1fs (%.1f%% - %s)"
            ),
            card_total["avg_ms"],
            card_total["p95_ms"],
            accounted,
            residual,
            pct,
            note,
        )
    log.info("--- end stage timings ---")


async def acquire_worker_lock(
    lock_conn: asyncpg.Connection,
) -> None:
    while True:
        got_lock = await lock_conn.fetchval(
            "SELECT pg_try_advisory_lock($1)",
            OVERLAP_LOCK_KEY,
        )

        if got_lock:
            log.info(
                "Acquired advisory lock %d",
                OVERLAP_LOCK_KEY,
            )
            return

        log.warning(
            (
                "Another FUT.GG price worker owns "
                "advisory lock %d; retrying in %.0f seconds"
            ),
            OVERLAP_LOCK_KEY,
            LOCK_RETRY_SECONDS,
        )

        await asyncio.sleep(
            LOCK_RETRY_SECONDS
        )


async def run_discovery_mode() -> None:
    """One-shot network diagnostic, then exit.

    Takes no worker lock, opens no pool and touches no card state - it must
    be safe to run alongside the live worker without perturbing it. The
    import is local so that when the flag is off, futgg_discover is never
    even loaded.
    """
    from futgg_discover import run_discovery

    if not DISCOVER_URL:
        raise RuntimeError(
            "FUTGG_DISCOVER_PRICE_NETWORK is enabled but FUTGG_DISCOVER_URL is not set. "
            "Set it to one full FUT.GG player URL, e.g. "
            "https://www.fut.gg/players/26-1-239085/"
        )

    log.info(
        "DISCOVERY MODE: single instrumented page load, price pipeline untouched."
    )

    await run_discovery(
        DISCOVER_URL,
        headless=HEADLESS,
        user_agent=USER_AGENT,
        timeout_ms=TIMEOUT_MS,
        settle_ms=max(RENDER_SETTLE_MS, 4000),
        report_path=DISCOVER_REPORT_PATH or None,
    )

    log.info("DISCOVERY MODE: complete. Worker exiting without processing cards.")


async def run_forever() -> None:
    if DISCOVER_PRICE_NETWORK:
        await run_discovery_mode()
        return

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

        await acquire_worker_lock(
            lock_conn
        )

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
        ) = await create_browser(
            playwright
        )

        log.info(
            (
                "Continuous FUT.GG price worker started: "
                "version=%s concurrency=%d batch_size=%d "
                "request_delay=%.2f max_attempts=%d "
                "intervals=%s"
            ),
            SCRIPT_VERSION,
            CONCURRENCY,
            BATCH_SIZE,
            REQUEST_DELAY,
            MAX_ATTEMPTS,
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
