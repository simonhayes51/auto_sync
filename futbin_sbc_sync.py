"""
FUTBIN SBC collector.

Scrapes FUTBIN's rendered Squad Building Challenge listing and detail pages,
then writes into:

    market_events
    sbc_details
    sbc_challenges

This collector is intentionally separate from the regular player/BIN workers.

Important behaviour:

1. SBC pages require browser-rendered markup.
2. One browser context and one page are reused for the whole run.
3. HTTP 403 responses are treated as access denial, not transient failures.
4. The collector stops after repeated 403 responses rather than continuing
   to hammer FUTBIN.
5. Detail freshness is tracked with sbc_details.detail_scraped_at. It is not
   inferred from market_events.updated_at because listing upserts update that
   timestamp independently of detail scraping.
6. Heartbeat health reflects whether due detail pages were actually written.
7. Interesting XHR/fetch URLs are logged during the first successful listing
   render to help identify whether FUTBIN exposes SBC data through an internal
   JSON endpoint.

Suggested Railway Cron schedule:

    0 18 * * *

Required environment variables:

    DATABASE_URL

Optional environment variables:

    SBC_REQUEST_DELAY_SECONDS=4
    SBC_DETAIL_STALE_HOURS=20
    SBC_MAX_RETRIES=2
    SBC_MAX_CONSECUTIVE_403=2
    SBC_NAV_TIMEOUT_MS=30000
    SBC_SELECTOR_TIMEOUT_MS=15000
    SBC_HEADLESS=true
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, DefaultDict, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

import asyncpg
from bs4 import BeautifulSoup
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Response,
    async_playwright,
)

from monitoring import alert, heartbeat


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("futbin_sbc_sync")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")

BASE_URL = "https://www.futbin.com"

CATEGORY_PATHS = [
    "/26/squad-building-challenges/Players",
    "/26/squad-building-challenges/Upgrades",
    "/26/squad-building-challenges/Challenges",
    "/26/squad-building-challenges/Icons",
    "/26/squad-building-challenges/Foundations",
    "/26/squad-building-challenges/Swaps",
    "/26/squad-building-challenges",
]

REQUEST_DELAY_SECONDS = float(
    os.getenv("SBC_REQUEST_DELAY_SECONDS", "4")
)

DETAIL_STALE_HOURS = int(
    os.getenv("SBC_DETAIL_STALE_HOURS", "20")
)

MAX_RETRIES = int(
    os.getenv("SBC_MAX_RETRIES", "2")
)

MAX_CONSECUTIVE_403 = int(
    os.getenv("SBC_MAX_CONSECUTIVE_403", "2")
)

NAV_TIMEOUT_MS = int(
    os.getenv("SBC_NAV_TIMEOUT_MS", "30000")
)

SELECTOR_TIMEOUT_MS = int(
    os.getenv("SBC_SELECTOR_TIMEOUT_MS", "15000")
)

HEADLESS = os.getenv("SBC_HEADLESS", "true").strip().lower() not in {
    "0",
    "false",
    "no",
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Confirmed FUTBIN selectors
# ---------------------------------------------------------------------------

CARD_SELECTOR = ".sbc-card-wrapper"

CARD_NAME_SELECTOR = (
    ".og-card-wrapper-top "
    "div.text-ellipsis > div.text-ellipsis"
)

CARD_BADGE_SELECTOR = ".sbc-badge"

CARD_REWARD_SELECTOR = (
    ".sbc-rewards-area "
    ".xxs-font.slim-font.text-ellipsis-2"
)

CARD_DESC_SELECTOR = (
    ".centered.full-height.max-width-100.text-wrap p"
)

CARD_EXPIRES_SELECTOR = (
    ".sbc-info-row "
    ".xxs-column:nth-of-type(1) .bold"
)

CARD_REPEATABLE_SELECTOR = (
    ".sbc-info-row "
    ".xxs-column:nth-of-type(2) "
    "> div:not(.text-faded)"
)

CARD_PROGRESS_SELECTOR = (
    ".sbc-info-row "
    ".xxs-column:nth-of-type(3) .bold"
)

DETAIL_TOTAL_PRICE_SELECTOR = (
    ".info-row-part .s-row.centered.flex-wrap"
)

DETAIL_CHALLENGE_CARD_SELECTOR = ".sbc-box-wrapper"

DETAIL_CHALLENGE_NAME_SELECTOR = (
    ".og-card-wrapper-top .xxs-font.bold"
)

DETAIL_CHALLENGE_REWARD_SELECTOR = (
    ".sbc-box-front-info .xxs-font"
)

DETAIL_CHALLENGE_DESC_SELECTOR = ".sbc-box-front p"

DETAIL_REQUIREMENT_ROW_SELECTOR = (
    ".sbc-requirements "
    ".challenge-box-description-row"
)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

Diagnostics = DefaultDict[str, int]

FetchResult = Tuple[
    Optional[str],  # HTML
    str,            # reason
    Optional[int],  # HTTP status
]


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------

async def _polite_delay() -> None:
    """
    Sleep for a jittered interval.

    The delay is not treated as a way of overcoming a 403. Once the collector
    detects access denial, the circuit breaker stops the run.
    """

    base = max(0.0, REQUEST_DELAY_SECONDS)
    jitter = random.uniform(0.0, base)

    await asyncio.sleep(base + jitter)


def _num(text: Optional[str]) -> Optional[int]:
    if not text:
        return None

    cleaned = text.lower().replace(",", "").strip()

    try:
        if cleaned.endswith("m"):
            return int(float(cleaned[:-1]) * 1_000_000)

        if cleaned.endswith("k"):
            return int(float(cleaned[:-1]) * 1_000)
    except (TypeError, ValueError):
        return None

    match = re.search(r"\d+(?:\.\d+)?", cleaned)

    if not match:
        return None

    try:
        return int(float(match.group(0)))
    except (TypeError, ValueError):
        return None


_EXPIRY_RE = re.compile(
    r"(\d+)\s*(day|hour|hr|d|h)s?",
    re.IGNORECASE,
)


def _parse_expiry(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None

    match = _EXPIRY_RE.search(text)

    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2).lower()

    if unit.startswith("d"):
        delta = timedelta(days=amount)
    else:
        delta = timedelta(hours=amount)

    return datetime.now(timezone.utc) + delta


def _parse_repeatable(text: Optional[str]) -> bool:
    if not text:
        return False

    normalised = text.strip().lower()

    if not normalised:
        return False

    if "non-repeatable" in normalised:
        return False

    if "not repeatable" in normalised:
        return False

    if normalised.startswith("non"):
        return False

    return "repeatable" in normalised


def _category_from_path(path: str) -> str:
    segment = path.rstrip("/").rsplit("/", 1)[-1]

    if segment == "squad-building-challenges":
        return "all"

    return segment.lower()


def _normalise_external_id(url: str) -> Optional[str]:
    path = urlparse(url).path.strip("/")

    return path or None


def _trim_text(text: str, limit: int = 300) -> str:
    collapsed = re.sub(r"\s+", " ", text).strip()

    if len(collapsed) <= limit:
        return collapsed

    return collapsed[:limit] + "..."


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_listing_page(
    html: str,
    category: str,
) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    output: List[Dict[str, Any]] = []

    for card in soup.select(CARD_SELECTOR):
        link_element = card.select_one("a")
        href = link_element.get("href") if link_element else None

        if not href:
            continue

        url = (
            href
            if href.startswith("http")
            else f"{BASE_URL}{href}"
        )

        external_id = _normalise_external_id(url)

        if not external_id:
            continue

        name_element = card.select_one(CARD_NAME_SELECTOR)
        badge_element = card.select_one(CARD_BADGE_SELECTOR)
        reward_element = card.select_one(CARD_REWARD_SELECTOR)
        description_element = card.select_one(CARD_DESC_SELECTOR)
        expires_element = card.select_one(CARD_EXPIRES_SELECTOR)
        repeatable_element = card.select_one(CARD_REPEATABLE_SELECTOR)
        progress_element = card.select_one(CARD_PROGRESS_SELECTOR)

        output.append(
            {
                "external_id": external_id,
                "url": url,
                "title": (
                    name_element.get_text(strip=True)
                    if name_element
                    else "Unknown SBC"
                ),
                "category": category,
                "badge": (
                    badge_element.get_text(strip=True)
                    if badge_element
                    else None
                ),
                "description": (
                    description_element.get_text(strip=True)
                    if description_element
                    else None
                ),
                "group_reward": (
                    reward_element.get_text(strip=True)
                    if reward_element
                    else None
                ),
                "expires_text": (
                    expires_element.get_text(strip=True)
                    if expires_element
                    else None
                ),
                "repeatable_text": (
                    repeatable_element.get_text(strip=True)
                    if repeatable_element
                    else None
                ),
                "progress_text": (
                    progress_element.get_text(strip=True)
                    if progress_element
                    else None
                ),
            }
        )

    return output


def parse_detail_page(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    result: Dict[str, Any] = {
        "total_cost_coins": None,
        "challenges": [],
    }

    price_element = soup.select_one(DETAIL_TOTAL_PRICE_SELECTOR)

    if price_element:
        rows = [
            row.get_text(" ", strip=True)
            for row in price_element.select(":scope > div")
            if row.get_text(" ", strip=True)
        ]

        if rows:
            result["total_cost_coins"] = _num(rows[0])

    for index, challenge_card in enumerate(
        soup.select(DETAIL_CHALLENGE_CARD_SELECTOR)
    ):
        name_element = challenge_card.select_one(
            DETAIL_CHALLENGE_NAME_SELECTOR
        )

        reward_element = challenge_card.select_one(
            DETAIL_CHALLENGE_REWARD_SELECTOR
        )

        description_element = challenge_card.select_one(
            DETAIL_CHALLENGE_DESC_SELECTOR
        )

        requirement_elements = challenge_card.select(
            DETAIL_REQUIREMENT_ROW_SELECTOR
        )

        requirements: Dict[str, str] = {}

        for requirement_element in requirement_elements:
            text = requirement_element.get_text(" ", strip=True)

            if not text:
                continue

            base_key = re.sub(
                r"[^a-z0-9]+",
                "_",
                text.lower(),
            ).strip("_")[:40]

            key = base_key or f"req_{len(requirements)}"

            if key in requirements:
                key = f"{key}_{len(requirements)}"

            requirements[key] = text

        result["challenges"].append(
            {
                "challenge_name": (
                    name_element.get_text(strip=True)
                    if name_element
                    else f"Challenge {index + 1}"
                ),
                "reward": (
                    reward_element.get_text(strip=True)
                    if reward_element
                    else None
                ),
                "description": (
                    description_element.get_text(strip=True)
                    if description_element
                    else None
                ),
                "requirements": requirements,
                "estimated_cost_coins": None,
            }
        )

    return result


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

async def ensure_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_events (
            id            BIGSERIAL PRIMARY KEY,
            kind          TEXT NOT NULL,
            source        TEXT NOT NULL,
            external_id   TEXT NOT NULL,
            title         TEXT NOT NULL,
            description   TEXT,
            starts_at     TIMESTAMPTZ,
            ends_at       TIMESTAMPTZ,
            fingerprint   TEXT[] NOT NULL DEFAULT '{}',
            payload       JSONB NOT NULL DEFAULT '{}',
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (kind, source, external_id)
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_events_fingerprint
        ON market_events
        USING GIN (fingerprint)
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_market_events_kind_starts
        ON market_events (kind, starts_at DESC)
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sbc_details (
            event_id            BIGINT PRIMARY KEY
                                REFERENCES market_events(id)
                                ON DELETE CASCADE,
            set_name            TEXT NOT NULL,
            category            TEXT,
            total_cost_coins    BIGINT,
            repeatable          BOOLEAN NOT NULL DEFAULT false,
            reward_card_id      BIGINT REFERENCES fut_players(card_id),
            reward_description  TEXT,
            expires_at          TIMESTAMPTZ,
            detail_scraped_at   TIMESTAMPTZ
        )
        """
    )

    # Makes the script safe against databases where sbc_details was created
    # before detail_scraped_at was introduced.
    await conn.execute(
        """
        ALTER TABLE sbc_details
        ADD COLUMN IF NOT EXISTS detail_scraped_at TIMESTAMPTZ
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sbc_details_scraped_at
        ON sbc_details (detail_scraped_at)
        """
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sbc_challenges (
            id                    BIGSERIAL PRIMARY KEY,
            event_id              BIGINT NOT NULL
                                  REFERENCES market_events(id)
                                  ON DELETE CASCADE,
            challenge_name        TEXT NOT NULL,
            requirements          JSONB NOT NULL DEFAULT '{}',
            estimated_cost_coins  BIGINT,
            display_order         INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sbc_challenges_event
        ON sbc_challenges (event_id)
        """
    )


# ---------------------------------------------------------------------------
# Fingerprints
# ---------------------------------------------------------------------------

def _build_fingerprint(
    item: Dict[str, Any],
    detail: Dict[str, Any],
    repeatable: bool,
) -> List[str]:
    tags: List[str] = []

    if repeatable:
        tags.append("repeatable")

    category = str(item.get("category") or "").lower()

    if category in {"icons", "icon", "heroes", "hero"}:
        tags.append("icon_hero_reward")

    for challenge in detail.get("challenges", []):
        requirements = challenge.get("requirements") or {}

        requirement_text = " ".join(
            str(value)
            for value in requirements.values()
        ).lower()

        if (
            "totw" in requirement_text
            and "requires_totw" not in tags
        ):
            tags.append("requires_totw")

        if (
            (
                "team of the week" in requirement_text
                or "inform" in requirement_text
                or re.search(r"\bif\b", requirement_text)
            )
            and "requires_if" not in tags
        ):
            tags.append("requires_if")

    total_cost = detail.get("total_cost_coins")

    if total_cost is not None and total_cost >= 500_000:
        tags.append("high_cost")

    return tags


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------

async def _upsert_event(
    conn: asyncpg.Connection,
    item: Dict[str, Any],
) -> int:
    payload = {
        "badge": item.get("badge"),
        "group_reward": item.get("group_reward"),
        "progress_text": item.get("progress_text"),
        "expires_text": item.get("expires_text"),
        "repeatable_text": item.get("repeatable_text"),
        "url": item.get("url"),
    }

    row = await conn.fetchrow(
        """
        INSERT INTO market_events (
            kind,
            source,
            external_id,
            title,
            description,
            payload,
            updated_at
        )
        VALUES (
            'sbc',
            'futbin',
            $1,
            $2,
            $3,
            $4::jsonb,
            now()
        )
        ON CONFLICT (kind, source, external_id)
        DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            payload = EXCLUDED.payload,
            updated_at = now()
        RETURNING id
        """,
        item["external_id"],
        item["title"],
        item.get("description"),
        json.dumps(payload),
    )

    if row is None:
        raise RuntimeError(
            f"Failed to upsert market event {item['external_id']}"
        )

    return int(row["id"])


async def _write_detail(
    conn: asyncpg.Connection,
    event_id: int,
    item: Dict[str, Any],
    detail: Dict[str, Any],
) -> None:
    expires_at = _parse_expiry(item.get("expires_text"))
    repeatable = _parse_repeatable(item.get("repeatable_text"))

    fingerprint = _build_fingerprint(
        item=item,
        detail=detail,
        repeatable=repeatable,
    )

    async with conn.transaction():
        await conn.execute(
            """
            UPDATE market_events
            SET
                fingerprint = $2,
                ends_at = COALESCE($3, ends_at),
                updated_at = now()
            WHERE id = $1
            """,
            event_id,
            fingerprint,
            expires_at,
        )

        await conn.execute(
            """
            INSERT INTO sbc_details (
                event_id,
                set_name,
                category,
                total_cost_coins,
                repeatable,
                reward_card_id,
                reward_description,
                expires_at,
                detail_scraped_at
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
                now()
            )
            ON CONFLICT (event_id)
            DO UPDATE SET
                set_name = EXCLUDED.set_name,
                category = EXCLUDED.category,
                total_cost_coins = EXCLUDED.total_cost_coins,
                repeatable = EXCLUDED.repeatable,
                reward_card_id = EXCLUDED.reward_card_id,
                reward_description = EXCLUDED.reward_description,
                expires_at = EXCLUDED.expires_at,
                detail_scraped_at = now()
            """,
            event_id,
            item["title"],
            item.get("category"),
            detail.get("total_cost_coins"),
            repeatable,
            None,
            item.get("group_reward"),
            expires_at,
        )

        await conn.execute(
            """
            DELETE FROM sbc_challenges
            WHERE event_id = $1
            """,
            event_id,
        )

        for display_order, challenge in enumerate(
            detail.get("challenges", [])
        ):
            await conn.execute(
                """
                INSERT INTO sbc_challenges (
                    event_id,
                    challenge_name,
                    requirements,
                    estimated_cost_coins,
                    display_order
                )
                VALUES (
                    $1,
                    $2,
                    $3::jsonb,
                    $4,
                    $5
                )
                """,
                event_id,
                challenge["challenge_name"],
                json.dumps(challenge.get("requirements") or {}),
                challenge.get("estimated_cost_coins"),
                display_order,
            )


async def _get_due_external_ids(
    conn: asyncpg.Connection,
    event_ids: Dict[str, int],
) -> Set[str]:
    if not event_ids:
        return set()

    rows = await conn.fetch(
        """
        SELECT e.external_id
        FROM market_events e
        LEFT JOIN sbc_details d
            ON d.event_id = e.id
        WHERE
            e.kind = 'sbc'
            AND e.source = 'futbin'
            AND e.id = ANY($1::bigint[])
            AND (
                d.event_id IS NULL
                OR d.detail_scraped_at IS NULL
                OR d.detail_scraped_at
                    < now() - make_interval(hours => $2)
            )
        """,
        list(event_ids.values()),
        DETAIL_STALE_HOURS,
    )

    return {
        str(row["external_id"])
        for row in rows
    }


# ---------------------------------------------------------------------------
# Browser diagnostics
# ---------------------------------------------------------------------------

def _interesting_response_url(url: str) -> bool:
    lowered = url.lower()

    interesting_terms = (
        "sbc",
        "squad-building",
        "challenge",
        "graphql",
        "/api/",
        "ajax",
    )

    return any(term in lowered for term in interesting_terms)


def _install_network_observer(
    page: Page,
    observed_urls: Set[str],
) -> None:
    async def handle_response(response: Response) -> None:
        try:
            request = response.request

            if request.resource_type not in {
                "xhr",
                "fetch",
            }:
                return

            url = response.url

            if not _interesting_response_url(url):
                return

            if url in observed_urls:
                return

            observed_urls.add(url)

            content_type = response.headers.get(
                "content-type",
                "",
            )

            log.info(
                "Observed browser data request: status=%s type=%s "
                "content_type=%s url=%s",
                response.status,
                request.resource_type,
                content_type,
                url,
            )

        except Exception as exc:
            log.debug(
                "Could not inspect browser response: %s",
                exc,
            )

    page.on(
        "response",
        lambda response: asyncio.create_task(
            handle_response(response)
        ),
    )


async def _log_response_diagnostics(
    page: Page,
    response: Optional[Response],
    url: str,
    status: Optional[int],
) -> None:
    if response is None:
        log.warning(
            "No main-document response object for %s",
            url,
        )
        return

    headers = response.headers

    useful_headers = {
        key: value
        for key, value in headers.items()
        if key.lower() in {
            "server",
            "content-type",
            "cf-ray",
            "cf-cache-status",
            "retry-after",
            "location",
        }
    }

    try:
        title = await page.title()
    except Exception:
        title = ""

    body_preview = ""

    try:
        body_text = await page.locator("body").inner_text(
            timeout=3_000
        )
        body_preview = _trim_text(body_text, limit=350)
    except Exception:
        body_preview = ""

    log.warning(
        "Navigation diagnostics: status=%s url=%s title=%r "
        "headers=%s body_preview=%r",
        status,
        url,
        title,
        useful_headers,
        body_preview,
    )


# ---------------------------------------------------------------------------
# Browser navigation
# ---------------------------------------------------------------------------

async def _navigate_with_retry(
    page: Page,
    url: str,
    wait_selector: str,
    diag: Diagnostics,
) -> FetchResult:
    """
    Navigate using the existing browser page.

    Returns:

        html, reason, status

    Rules:

    - 403 is returned immediately and is not retried.
    - 429 may be retried with exponential backoff.
    - browser/navigation exceptions may be retried.
    - selector timeout is not automatically a fetch failure. The returned
      HTML is still parsed so the caller can distinguish empty markup from
      an HTTP failure.
    """

    backoff_seconds = 2.0
    last_reason = "unknown"
    last_status: Optional[int] = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=NAV_TIMEOUT_MS,
            )

            status = (
                response.status
                if response is not None
                else None
            )

            last_status = status

            if status == 403:
                diag["http_403_hits"] += 1
                diag["http_non200"] += 1

                await _log_response_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                return None, "status=403", status

            if status == 429:
                diag["http_429_hits"] += 1
                diag["http_non200"] += 1

                await _log_response_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                last_reason = "status=429"

                if attempt < MAX_RETRIES:
                    diag["nav_retries"] += 1
                    await asyncio.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                return None, last_reason, status

            if status is not None and status >= 400:
                diag["http_non200"] += 1

                await _log_response_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                return None, f"status={status}", status

            try:
                await page.wait_for_selector(
                    wait_selector,
                    state="attached",
                    timeout=SELECTOR_TIMEOUT_MS,
                )
            except Exception as selector_exc:
                diag["selector_timeouts"] += 1

                log.warning(
                    "Expected selector did not appear: url=%s "
                    "selector=%s error=%s",
                    url,
                    wait_selector,
                    type(selector_exc).__name__,
                )

            html = await page.content()

            return html, "ok", status

        except Exception as exc:
            last_reason = (
                f"exception: {type(exc).__name__}: {exc}"
            )[:300]

            if attempt < MAX_RETRIES:
                diag["nav_retries"] += 1

                log.warning(
                    "Navigation attempt %d/%d failed for %s: %s",
                    attempt + 1,
                    MAX_RETRIES + 1,
                    url,
                    last_reason,
                )

                await asyncio.sleep(backoff_seconds)
                backoff_seconds *= 2
                continue

            diag["http_exceptions"] += 1

            return None, last_reason, last_status

    return None, last_reason, last_status


# ---------------------------------------------------------------------------
# Browser creation
# ---------------------------------------------------------------------------

async def _create_browser(
    playwright: Any,
) -> Tuple[Browser, BrowserContext, Page]:
    browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    context = await browser.new_context(
        user_agent=USER_AGENT,
        viewport={
            "width": 1920,
            "height": 1080,
        },
        locale="en-GB",
        timezone_id="Europe/London",
        extra_http_headers={
            "Accept-Language": "en-GB,en;q=0.9",
        },
    )

    page = await context.new_page()

    page.set_default_navigation_timeout(
        NAV_TIMEOUT_MS
    )

    page.set_default_timeout(
        SELECTOR_TIMEOUT_MS
    )

    return browser, context, page


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

async def crawl_once() -> None:
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=4,
        command_timeout=60,
    )

    diag: Diagnostics = defaultdict(int)

    all_items: Dict[str, Dict[str, Any]] = {}
    event_ids: Dict[str, int] = {}
    due_ids: Set[str] = set()

    written = 0
    failed = 0
    category_successes = 0
    access_blocked = False

    observed_data_urls: Set[str] = set()

    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)

        async with async_playwright() as playwright:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            try:
                browser, context, page = await _create_browser(
                    playwright
                )

                _install_network_observer(
                    page=page,
                    observed_urls=observed_data_urls,
                )

                consecutive_403s = 0

                # -----------------------------------------------------------
                # Listings
                # -----------------------------------------------------------

                for path in CATEGORY_PATHS:
                    category = _category_from_path(path)
                    url = f"{BASE_URL}{path}"

                    html, reason, status = await _navigate_with_retry(
                        page=page,
                        url=url,
                        wait_selector=CARD_SELECTOR,
                        diag=diag,
                    )

                    if status == 403:
                        diag["category_fetch_failed"] += 1
                        consecutive_403s += 1

                        log.warning(
                            "Category fetch forbidden: %s (%s); "
                            "consecutive_403=%d",
                            url,
                            reason,
                            consecutive_403s,
                        )

                        if (
                            consecutive_403s
                            >= MAX_CONSECUTIVE_403
                        ):
                            access_blocked = True
                            diag["blocked_circuit_breaker"] += 1

                            log.error(
                                "Stopping category navigation after %d "
                                "consecutive 403 responses",
                                consecutive_403s,
                            )

                            break

                        await _polite_delay()
                        continue

                    if html is None:
                        diag["category_fetch_failed"] += 1
                        consecutive_403s = 0

                        log.warning(
                            "Category fetch failed: %s (%s)",
                            url,
                            reason,
                        )

                        await _polite_delay()
                        continue

                    consecutive_403s = 0

                    items = parse_listing_page(
                        html=html,
                        category=category,
                    )

                    if not items:
                        diag["category_zero_items"] += 1

                        log.warning(
                            "Category %s loaded but zero SBCs parsed: %s",
                            category,
                            url,
                        )
                    else:
                        category_successes += 1

                        log.info(
                            "Category %s: %d SBCs found",
                            category,
                            len(items),
                        )

                    for item in items:
                        # Specific categories are visited before "all".
                        # The first category found therefore wins.
                        all_items.setdefault(
                            item["external_id"],
                            item,
                        )

                    await _polite_delay()

                if observed_data_urls:
                    log.info(
                        "Observed %d potentially relevant XHR/fetch URLs",
                        len(observed_data_urls),
                    )

                    for observed_url in sorted(observed_data_urls):
                        log.info(
                            "Observed data URL: %s",
                            observed_url,
                        )

                if not all_items:
                    detail_message = (
                        "zero SBC sets parsed; "
                        f"category_successes={category_successes} "
                        f"category_fetch_failed="
                        f"{diag['category_fetch_failed']} "
                        f"http_403={diag['http_403_hits']} "
                        f"http_429={diag['http_429_hits']} "
                        f"http_exceptions={diag['http_exceptions']}"
                    )

                    async with pool.acquire() as conn:
                        await heartbeat(
                            conn,
                            "futbin_sbc_sync",
                            ok=False,
                            detail=detail_message,
                        )

                    await alert(
                        "futbin_sbc_sync: zero SBC sets parsed. "
                        + detail_message
                    )

                    return

                # -----------------------------------------------------------
                # Listing writes
                # -----------------------------------------------------------

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        for item in all_items.values():
                            event_id = await _upsert_event(
                                conn=conn,
                                item=item,
                            )

                            event_ids[
                                item["external_id"]
                            ] = event_id

                async with pool.acquire() as conn:
                    due_ids = await _get_due_external_ids(
                        conn=conn,
                        event_ids=event_ids,
                    )

                log.info(
                    "SBC detail: %d of %d sets due for a (re)scrape",
                    len(due_ids),
                    len(all_items),
                )

                # Do not continue hammering detail routes after the browser
                # has already been refused repeatedly during category fetches.
                if access_blocked:
                    failed = len(due_ids)

                    log.error(
                        "Skipping %d due detail pages because the "
                        "403 circuit breaker was triggered",
                        len(due_ids),
                    )

                else:
                    # -------------------------------------------------------
                    # Details
                    # -------------------------------------------------------

                    consecutive_403s = 0

                    for external_id, item in all_items.items():
                        if external_id not in due_ids:
                            continue

                        html, reason, status = await _navigate_with_retry(
                            page=page,
                            url=item["url"],
                            wait_selector=(
                                DETAIL_CHALLENGE_CARD_SELECTOR
                            ),
                            diag=diag,
                        )

                        if status == 403:
                            failed += 1
                            consecutive_403s += 1

                            log.warning(
                                "Detail fetch forbidden: %s (%s); "
                                "consecutive_403=%d",
                                item["url"],
                                reason,
                                consecutive_403s,
                            )

                            if (
                                consecutive_403s
                                >= MAX_CONSECUTIVE_403
                            ):
                                access_blocked = True
                                diag[
                                    "blocked_circuit_breaker"
                                ] += 1

                                remaining_due = sum(
                                    1
                                    for remaining_id
                                    in due_ids
                                    if (
                                        remaining_id
                                        != external_id
                                        and remaining_id
                                        not in {
                                            key
                                            for key, value
                                            in event_ids.items()
                                            if False
                                        }
                                    )
                                )

                                log.error(
                                    "Stopping detail navigation after %d "
                                    "consecutive 403 responses",
                                    consecutive_403s,
                                )

                                # We count the remaining unattempted rows
                                # below from the final written total rather
                                # than trying to track them individually.
                                break

                            await _polite_delay()
                            continue

                        if html is None:
                            failed += 1
                            consecutive_403s = 0

                            log.warning(
                                "Detail fetch failed: %s (%s)",
                                item["url"],
                                reason,
                            )

                            await _polite_delay()
                            continue

                        consecutive_403s = 0

                        detail = parse_detail_page(html)

                        challenge_count = len(
                            detail.get("challenges", [])
                        )

                        if challenge_count == 0:
                            failed += 1
                            diag["detail_zero_challenges"] += 1

                            log.warning(
                                "Detail page loaded but zero challenges "
                                "parsed: %s",
                                item["url"],
                            )

                            await _polite_delay()
                            continue

                        async with pool.acquire() as conn:
                            await _write_detail(
                                conn=conn,
                                event_id=event_ids[external_id],
                                item=item,
                                detail=detail,
                            )

                        written += 1
                        diag["challenges_written"] += (
                            challenge_count
                        )

                        log.info(
                            "Detail written: %s; challenges=%d; "
                            "total_cost=%s",
                            item["title"],
                            challenge_count,
                            detail.get("total_cost_coins"),
                        )

                        await _polite_delay()

            finally:
                if page is not None:
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

        # If the detail circuit breaker stopped the loop, include every
        # unwritten due record in the failure count.
        if due_ids:
            failed = max(
                failed,
                len(due_ids) - written,
            )

        detail_success_rate = (
            written / len(due_ids)
            if due_ids
            else 1.0
        )

        run_ok = (
            len(all_items) > 0
            and category_successes > 0
            and not access_blocked
            and (
                not due_ids
                or detail_success_rate >= 0.80
            )
        )

        detail_message = (
            f"sets_found={len(all_items)} "
            f"sets_due={len(due_ids)} "
            f"sets_written={written} "
            f"sets_failed={failed} "
            f"detail_success_rate={detail_success_rate:.2f} "
            f"challenges_written={diag['challenges_written']} "
            f"category_successes={category_successes} "
            f"category_fetch_failed="
            f"{diag['category_fetch_failed']} "
            f"category_zero_items={diag['category_zero_items']} "
            f"detail_zero_challenges="
            f"{diag['detail_zero_challenges']} "
            f"selector_timeouts={diag['selector_timeouts']} "
            f"http_403={diag['http_403_hits']} "
            f"http_429={diag['http_429_hits']} "
            f"http_non200={diag['http_non200']} "
            f"http_exceptions={diag['http_exceptions']} "
            f"nav_retries={diag['nav_retries']} "
            f"circuit_breakers="
            f"{diag['blocked_circuit_breaker']} "
            f"observed_data_urls={len(observed_data_urls)}"
        )

        async with pool.acquire() as conn:
            await heartbeat(
                conn,
                "futbin_sbc_sync",
                ok=run_ok,
                detail=detail_message,
            )

        if run_ok:
            log.info(
                "Run complete. %s",
                detail_message,
            )
        else:
            log.error(
                "Run failed health checks. %s",
                detail_message,
            )

        if access_blocked:
            await alert(
                "futbin_sbc_sync: FUTBIN returned repeated HTTP 403 "
                "responses and the circuit breaker stopped the crawl. "
                + detail_message
            )

        elif due_ids and written == 0:
            await alert(
                "futbin_sbc_sync: detail pages were due but zero "
                "details were written. "
                + detail_message
            )

        elif detail_success_rate < 0.80:
            await alert(
                "futbin_sbc_sync: fewer than 80% of due SBC detail "
                "pages were written. "
                + detail_message
            )

    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())

    except KeyboardInterrupt:
        log.warning("Interrupted")
        sys.exit(130)

    except Exception as exc:
        log.exception(
            "crawl_once() failed: %s",
            exc,
        )
        sys.exit(1)

    sys.exit(0)
