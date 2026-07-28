"""
FUTBIN SBC sync worker.

Flow
----
1. Load FUTBIN's "ALL SBCs" page once.
2. Parse every available SBC listing.
3. Upsert those listings into market_events.
4. Identify SBC detail records that are missing or stale.
5. Visit only those detail pages.
6. Stop immediately if Cloudflare returns HTTP 403.

Required environment variable
-----------------------------
DATABASE_URL

Optional environment variables
------------------------------
SBC_REQUEST_DELAY_SECONDS=8
SBC_DETAIL_STALE_HOURS=20
SBC_MAX_DETAIL_PAGES=0
SBC_NAV_TIMEOUT_MS=30000
SBC_SELECTOR_TIMEOUT_MS=15000
SBC_MAX_RETRIES=2
SBC_HEADLESS=true

SBC_MAX_DETAIL_PAGES:
    0 = no limit
    1 = useful for testing one detail page
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


# =============================================================================
# Logging
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

log = logging.getLogger("futbin_sbc_sync")


# =============================================================================
# Configuration
# =============================================================================

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

BASE_URL = "https://www.futbin.com"

# Only use the ALL page. The other URLs are filters over the same dataset.
LISTING_URL = f"{BASE_URL}/26/squad-building-challenges"

REQUEST_DELAY_SECONDS = float(
    os.getenv("SBC_REQUEST_DELAY_SECONDS", "8")
)

DETAIL_STALE_HOURS = int(
    os.getenv("SBC_DETAIL_STALE_HOURS", "20")
)

MAX_DETAIL_PAGES = int(
    os.getenv("SBC_MAX_DETAIL_PAGES", "0")
)

NAV_TIMEOUT_MS = int(
    os.getenv("SBC_NAV_TIMEOUT_MS", "30000")
)

SELECTOR_TIMEOUT_MS = int(
    os.getenv("SBC_SELECTOR_TIMEOUT_MS", "15000")
)

MAX_RETRIES = int(
    os.getenv("SBC_MAX_RETRIES", "2")
)

HEADLESS = os.getenv(
    "SBC_HEADLESS",
    "true",
).strip().lower() not in {
    "0",
    "false",
    "no",
}


# =============================================================================
# FUTBIN selectors
# =============================================================================

LISTING_CARD_SELECTOR = ".sbc-card-wrapper"

LISTING_NAME_SELECTOR = (
    ".og-card-wrapper-top "
    "div.text-ellipsis > div.text-ellipsis"
)

LISTING_BADGE_SELECTOR = ".sbc-badge"

LISTING_REWARD_SELECTOR = (
    ".sbc-rewards-area "
    ".xxs-font.slim-font.text-ellipsis-2"
)

LISTING_DESCRIPTION_SELECTOR = (
    ".centered.full-height.max-width-100.text-wrap p"
)

LISTING_EXPIRES_SELECTOR = (
    ".sbc-info-row "
    ".xxs-column:nth-of-type(1) .bold"
)

LISTING_REPEATABLE_SELECTOR = (
    ".sbc-info-row "
    ".xxs-column:nth-of-type(2) "
    "> div:not(.text-faded)"
)

LISTING_PROGRESS_SELECTOR = (
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

DETAIL_CHALLENGE_DESCRIPTION_SELECTOR = (
    ".sbc-box-front p"
)

DETAIL_REQUIREMENT_ROW_SELECTOR = (
    ".sbc-requirements "
    ".challenge-box-description-row"
)


# =============================================================================
# Types
# =============================================================================

Diagnostics = DefaultDict[str, int]

NavigationResult = Tuple[
    Optional[str],  # HTML
    Optional[int],  # HTTP status
    str,            # reason
]


# =============================================================================
# General helpers
# =============================================================================

async def polite_delay() -> None:
    """
    Wait between document navigations.

    The delay is deliberately jittered so runs do not always follow the exact
    same timing pattern.
    """

    base = max(0.0, REQUEST_DELAY_SECONDS)
    jitter = random.uniform(0.0, max(1.0, base * 0.5))

    await asyncio.sleep(base + jitter)


def clean_text(value: Optional[str]) -> Optional[str]:
    if not value:
        return None

    cleaned = re.sub(r"\s+", " ", value).strip()

    return cleaned or None


def text_from_element(element: Any) -> Optional[str]:
    if element is None:
        return None

    return clean_text(
        element.get_text(" ", strip=True)
    )


def absolute_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href

    if not href.startswith("/"):
        href = f"/{href}"

    return f"{BASE_URL}{href}"


def external_id_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    path = parsed.path.strip("/")

    return path or None


def parse_coin_value(text: Optional[str]) -> Optional[int]:
    if not text:
        return None

    normalised = (
        text.lower()
        .replace(",", "")
        .replace("coins", "")
        .strip()
    )

    match = re.search(
        r"(\d+(?:\.\d+)?)\s*([km]?)",
        normalised,
        re.IGNORECASE,
    )

    if not match:
        return None

    try:
        number = float(match.group(1))
    except ValueError:
        return None

    suffix = match.group(2).lower()

    if suffix == "k":
        number *= 1_000
    elif suffix == "m":
        number *= 1_000_000

    return int(number)


_EXPIRY_PATTERN = re.compile(
    r"(\d+)\s*(day|days|d|hour|hours|hr|hrs|h)",
    re.IGNORECASE,
)


def parse_expiry(text: Optional[str]) -> Optional[datetime]:
    if not text:
        return None

    match = _EXPIRY_PATTERN.search(text)

    if not match:
        return None

    amount = int(match.group(1))
    unit = match.group(2).lower()

    if unit.startswith("d"):
        delta = timedelta(days=amount)
    else:
        delta = timedelta(hours=amount)

    return datetime.now(timezone.utc) + delta


def parse_repeatable(text: Optional[str]) -> bool:
    if not text:
        return False

    normalised = text.strip().lower()

    if "non-repeatable" in normalised:
        return False

    if "not repeatable" in normalised:
        return False

    return "repeatable" in normalised


def shortened(value: str, length: int = 350) -> str:
    value = re.sub(r"\s+", " ", value).strip()

    if len(value) <= length:
        return value

    return value[:length] + "..."


# =============================================================================
# HTML parsing
# =============================================================================

def parse_listing_page(html: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")

    results: List[Dict[str, Any]] = []
    seen_external_ids: Set[str] = set()

    for card in soup.select(LISTING_CARD_SELECTOR):
        link = card.select_one("a[href]")

        if link is None:
            continue

        href = link.get("href")

        if not href:
            continue

        url = absolute_url(href)
        external_id = external_id_from_url(url)

        if not external_id:
            continue

        if external_id in seen_external_ids:
            continue

        seen_external_ids.add(external_id)

        name_element = card.select_one(
            LISTING_NAME_SELECTOR
        )

        badge_element = card.select_one(
            LISTING_BADGE_SELECTOR
        )

        reward_element = card.select_one(
            LISTING_REWARD_SELECTOR
        )

        description_element = card.select_one(
            LISTING_DESCRIPTION_SELECTOR
        )

        expires_element = card.select_one(
            LISTING_EXPIRES_SELECTOR
        )

        repeatable_element = card.select_one(
            LISTING_REPEATABLE_SELECTOR
        )

        progress_element = card.select_one(
            LISTING_PROGRESS_SELECTOR
        )

        title = text_from_element(name_element)

        if not title:
            title = "Unknown SBC"

        results.append(
            {
                "external_id": external_id,
                "url": url,
                "title": title,
                "category": "all",
                "badge": text_from_element(badge_element),
                "group_reward": text_from_element(
                    reward_element
                ),
                "description": text_from_element(
                    description_element
                ),
                "expires_text": text_from_element(
                    expires_element
                ),
                "repeatable_text": text_from_element(
                    repeatable_element
                ),
                "progress_text": text_from_element(
                    progress_element
                ),
            }
        )

    return results


def parse_total_cost(soup: BeautifulSoup) -> Optional[int]:
    price_section = soup.select_one(
        DETAIL_TOTAL_PRICE_SELECTOR
    )

    if price_section is None:
        return None

    # FUTBIN markup may expose several prices. Take the first sensible
    # coin-like value in the section.
    candidate_texts: List[str] = []

    for element in price_section.select("*"):
        text = clean_text(
            element.get_text(" ", strip=True)
        )

        if text:
            candidate_texts.append(text)

    candidate_texts.insert(
        0,
        price_section.get_text(" ", strip=True),
    )

    for candidate in candidate_texts:
        value = parse_coin_value(candidate)

        if value is not None:
            return value

    return None


def requirement_key(
    text: str,
    index: int,
    existing: Dict[str, str],
) -> str:
    key = re.sub(
        r"[^a-z0-9]+",
        "_",
        text.lower(),
    ).strip("_")

    key = key[:60]

    if not key:
        key = f"requirement_{index + 1}"

    original_key = key
    duplicate_index = 2

    while key in existing:
        key = f"{original_key}_{duplicate_index}"
        duplicate_index += 1

    return key


def parse_detail_page(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    challenges: List[Dict[str, Any]] = []

    for index, card in enumerate(
        soup.select(DETAIL_CHALLENGE_CARD_SELECTOR)
    ):
        name_element = card.select_one(
            DETAIL_CHALLENGE_NAME_SELECTOR
        )

        reward_element = card.select_one(
            DETAIL_CHALLENGE_REWARD_SELECTOR
        )

        description_element = card.select_one(
            DETAIL_CHALLENGE_DESCRIPTION_SELECTOR
        )

        requirements: Dict[str, str] = {}

        requirement_rows = card.select(
            DETAIL_REQUIREMENT_ROW_SELECTOR
        )

        for requirement_index, row in enumerate(
            requirement_rows
        ):
            requirement_text = clean_text(
                row.get_text(" ", strip=True)
            )

            if not requirement_text:
                continue

            key = requirement_key(
                text=requirement_text,
                index=requirement_index,
                existing=requirements,
            )

            requirements[key] = requirement_text

        challenge_name = text_from_element(
            name_element
        )

        if not challenge_name:
            challenge_name = f"Challenge {index + 1}"

        challenges.append(
            {
                "challenge_name": challenge_name,
                "reward": text_from_element(
                    reward_element
                ),
                "description": text_from_element(
                    description_element
                ),
                "requirements": requirements,
                "estimated_cost_coins": None,
            }
        )

    return {
        "total_cost_coins": parse_total_cost(soup),
        "challenges": challenges,
    }


# =============================================================================
# Database schema
# =============================================================================

async def ensure_tables(
    conn: asyncpg.Connection,
) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_events (
            id BIGSERIAL PRIMARY KEY,
            kind TEXT NOT NULL,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            starts_at TIMESTAMPTZ,
            ends_at TIMESTAMPTZ,
            fingerprint TEXT[] NOT NULL DEFAULT '{}',
            payload JSONB NOT NULL DEFAULT '{}',
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
            event_id BIGINT PRIMARY KEY
                REFERENCES market_events(id)
                ON DELETE CASCADE,
            set_name TEXT NOT NULL,
            category TEXT,
            total_cost_coins BIGINT,
            repeatable BOOLEAN NOT NULL DEFAULT false,
            reward_card_id BIGINT
                REFERENCES fut_players(card_id),
            reward_description TEXT,
            expires_at TIMESTAMPTZ,
            detail_scraped_at TIMESTAMPTZ
        )
        """
    )

    # Existing databases may have been created before this column existed.
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
            id BIGSERIAL PRIMARY KEY,
            event_id BIGINT NOT NULL
                REFERENCES market_events(id)
                ON DELETE CASCADE,
            challenge_name TEXT NOT NULL,
            requirements JSONB NOT NULL DEFAULT '{}',
            estimated_cost_coins BIGINT,
            display_order INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    await conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sbc_challenges_event
        ON sbc_challenges (event_id)
        """
    )


# =============================================================================
# Database reads and writes
# =============================================================================

async def upsert_listing(
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
            f"Could not upsert listing: {item['external_id']}"
        )

    return int(row["id"])


async def get_due_external_ids(
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
            e.id = ANY($1::bigint[])
            AND e.kind = 'sbc'
            AND e.source = 'futbin'
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


def build_fingerprint(
    item: Dict[str, Any],
    detail: Dict[str, Any],
) -> List[str]:
    tags: List[str] = []

    repeatable = parse_repeatable(
        item.get("repeatable_text")
    )

    if repeatable:
        tags.append("repeatable")

    title_and_badge = " ".join(
        [
            str(item.get("title") or ""),
            str(item.get("badge") or ""),
        ]
    ).lower()

    if "icon" in title_and_badge:
        tags.append("icon_reward")

    if "hero" in title_and_badge:
        tags.append("hero_reward")

    all_requirement_text = " ".join(
        str(requirement)
        for challenge in detail.get("challenges", [])
        for requirement in (
            challenge.get("requirements") or {}
        ).values()
    ).lower()

    if (
        "team of the week" in all_requirement_text
        or "totw" in all_requirement_text
    ):
        tags.append("requires_totw")

    if (
        "team of the season" in all_requirement_text
        or "tots" in all_requirement_text
    ):
        tags.append("requires_tots")

    if (
        "team of the year" in all_requirement_text
        or "toty" in all_requirement_text
    ):
        tags.append("requires_toty")

    total_cost = detail.get("total_cost_coins")

    if total_cost is not None:
        if total_cost >= 500_000:
            tags.append("high_cost")
        elif total_cost <= 50_000:
            tags.append("low_cost")

    return list(dict.fromkeys(tags))


async def write_detail(
    conn: asyncpg.Connection,
    event_id: int,
    item: Dict[str, Any],
    detail: Dict[str, Any],
) -> None:
    expires_at = parse_expiry(
        item.get("expires_text")
    )

    repeatable = parse_repeatable(
        item.get("repeatable_text")
    )

    fingerprint = build_fingerprint(
        item=item,
        detail=detail,
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
                NULL,
                $6,
                $7,
                now()
            )
            ON CONFLICT (event_id)
            DO UPDATE SET
                set_name = EXCLUDED.set_name,
                category = EXCLUDED.category,
                total_cost_coins = EXCLUDED.total_cost_coins,
                repeatable = EXCLUDED.repeatable,
                reward_description =
                    EXCLUDED.reward_description,
                expires_at = EXCLUDED.expires_at,
                detail_scraped_at = now()
            """,
            event_id,
            item["title"],
            item.get("category"),
            detail.get("total_cost_coins"),
            repeatable,
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
                json.dumps(
                    challenge.get("requirements") or {}
                ),
                challenge.get("estimated_cost_coins"),
                display_order,
            )


# =============================================================================
# Browser diagnostics
# =============================================================================

def is_possible_application_json(
    response: Response,
) -> bool:
    try:
        parsed = urlparse(response.url)
        content_type = response.headers.get(
            "content-type",
            "",
        ).lower()

        if response.request.resource_type not in {
            "xhr",
            "fetch",
        }:
            return False

        if parsed.netloc not in {
            "futbin.com",
            "www.futbin.com",
        }:
            return False

        if "/cdn-cgi/" in parsed.path.lower():
            return False

        if "json" not in content_type:
            return False

        return True

    except Exception:
        return False


def install_network_observer(
    page: Page,
    observed_urls: Set[str],
) -> None:
    async def inspect_response(
        response: Response,
    ) -> None:
        if not is_possible_application_json(response):
            return

        if response.url in observed_urls:
            return

        observed_urls.add(response.url)

        log.info(
            "Observed FUTBIN JSON request: status=%s url=%s",
            response.status,
            response.url,
        )

    def response_handler(
        response: Response,
    ) -> None:
        asyncio.create_task(
            inspect_response(response)
        )

    page.on("response", response_handler)


async def log_navigation_diagnostics(
    page: Page,
    response: Optional[Response],
    url: str,
    status: Optional[int],
) -> None:
    headers: Dict[str, str] = {}

    if response is not None:
        headers = {
            key: value
            for key, value in response.headers.items()
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

    try:
        body_text = await page.locator(
            "body"
        ).inner_text(timeout=3_000)

        body_preview = shortened(body_text)
    except Exception:
        body_preview = ""

    log.warning(
        "Navigation diagnostics: status=%s url=%s title=%r "
        "headers=%s body_preview=%r",
        status,
        url,
        title,
        headers,
        body_preview,
    )


# =============================================================================
# Browser navigation
# =============================================================================

async def navigate(
    page: Page,
    url: str,
    expected_selector: str,
    diagnostics: Diagnostics,
) -> NavigationResult:
    """
    Navigate to one document.

    HTTP 403 is returned immediately and is never retried.

    HTTP 429 and browser exceptions may be retried with backoff.
    """

    backoff = 3.0
    last_status: Optional[int] = None
    last_reason = "unknown"

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
                diagnostics["http_403"] += 1
                diagnostics["http_non_200"] += 1

                await log_navigation_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                return None, status, "status=403"

            if status == 429:
                diagnostics["http_429"] += 1
                diagnostics["http_non_200"] += 1

                await log_navigation_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                last_reason = "status=429"

                if attempt < MAX_RETRIES:
                    diagnostics["navigation_retries"] += 1

                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue

                return None, status, last_reason

            if status is not None and status >= 400:
                diagnostics["http_non_200"] += 1

                await log_navigation_diagnostics(
                    page=page,
                    response=response,
                    url=url,
                    status=status,
                )

                return None, status, f"status={status}"

            try:
                await page.wait_for_selector(
                    expected_selector,
                    state="attached",
                    timeout=SELECTOR_TIMEOUT_MS,
                )

            except Exception as exc:
                diagnostics["selector_timeouts"] += 1

                log.warning(
                    "Selector not found after navigation: "
                    "url=%s selector=%s error=%s",
                    url,
                    expected_selector,
                    type(exc).__name__,
                )

            html = await page.content()

            return html, status, "ok"

        except Exception as exc:
            last_reason = (
                f"{type(exc).__name__}: {exc}"
            )[:400]

            if attempt < MAX_RETRIES:
                diagnostics["navigation_retries"] += 1

                log.warning(
                    "Navigation failed; retrying: "
                    "attempt=%d/%d url=%s reason=%s",
                    attempt + 1,
                    MAX_RETRIES + 1,
                    url,
                    last_reason,
                )

                await asyncio.sleep(backoff)
                backoff *= 2
                continue

            diagnostics["navigation_exceptions"] += 1

            return (
                None,
                last_status,
                last_reason,
            )

    return None, last_status, last_reason


# =============================================================================
# Browser creation
# =============================================================================

async def create_browser(
    playwright: Any,
) -> Tuple[Browser, BrowserContext, Page]:
    browser = await playwright.chromium.launch(
        headless=HEADLESS,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
    )

    # Do not supply a fake user-agent. Playwright's native browser UA must
    # remain consistent with its browser version and client hints.
    context = await browser.new_context(
        viewport={
            "width": 1920,
            "height": 1080,
        },
        locale="en-GB",
        timezone_id="Europe/London",
    )

    page = await context.new_page()

    page.set_default_navigation_timeout(
        NAV_TIMEOUT_MS
    )

    page.set_default_timeout(
        SELECTOR_TIMEOUT_MS
    )

    return browser, context, page


# =============================================================================
# Main sync
# =============================================================================

async def crawl_once() -> None:
    diagnostics: Diagnostics = defaultdict(int)

    listing_items: Dict[str, Dict[str, Any]] = {}
    event_ids: Dict[str, int] = {}
    due_external_ids: Set[str] = set()
    observed_json_urls: Set[str] = set()

    attempted_details = 0
    written_details = 0
    failed_details = 0
    blocked = False

    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=4,
        command_timeout=60,
    )

    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)

        async with async_playwright() as playwright:
            browser: Optional[Browser] = None
            context: Optional[BrowserContext] = None
            page: Optional[Page] = None

            try:
                browser, context, page = await create_browser(
                    playwright
                )

                install_network_observer(
                    page=page,
                    observed_urls=observed_json_urls,
                )

                # ==========================================================
                # Load ALL SBCs once
                # ==========================================================

                log.info(
                    "Loading ALL SBC listings: %s",
                    LISTING_URL,
                )

                listing_html, listing_status, listing_reason = (
                    await navigate(
                        page=page,
                        url=LISTING_URL,
                        expected_selector=LISTING_CARD_SELECTOR,
                        diagnostics=diagnostics,
                    )
                )

                if listing_html is None:
                    detail_message = (
                        f"listing_failed=true "
                        f"status={listing_status} "
                        f"reason={listing_reason} "
                        f"http_403={diagnostics['http_403']} "
                        f"http_429={diagnostics['http_429']}"
                    )

                    async with pool.acquire() as conn:
                        await heartbeat(
                            conn,
                            "futbin_sbc_sync",
                            ok=False,
                            detail=detail_message,
                        )

                    await alert(
                        "futbin_sbc_sync: ALL SBC listing page failed. "
                        + detail_message
                    )

                    return

                parsed_items = parse_listing_page(
                    listing_html
                )

                for item in parsed_items:
                    listing_items[
                        item["external_id"]
                    ] = item

                log.info(
                    "ALL SBC listing: %d unique SBCs found",
                    len(listing_items),
                )

                if not listing_items:
                    diagnostics["listing_zero_items"] += 1

                    detail_message = (
                        "listing_loaded=true "
                        "sets_found=0 "
                        f"selector_timeouts="
                        f"{diagnostics['selector_timeouts']}"
                    )

                    async with pool.acquire() as conn:
                        await heartbeat(
                            conn,
                            "futbin_sbc_sync",
                            ok=False,
                            detail=detail_message,
                        )

                    await alert(
                        "futbin_sbc_sync: ALL page loaded but no SBC "
                        "cards were parsed. "
                        + detail_message
                    )

                    return

                # ==========================================================
                # Save listing records
                # ==========================================================

                async with pool.acquire() as conn:
                    async with conn.transaction():
                        for item in listing_items.values():
                            event_id = await upsert_listing(
                                conn=conn,
                                item=item,
                            )

                            event_ids[
                                item["external_id"]
                            ] = event_id

                async with pool.acquire() as conn:
                    due_external_ids = (
                        await get_due_external_ids(
                            conn=conn,
                            event_ids=event_ids,
                        )
                    )

                log.info(
                    "SBC details due: %d of %d",
                    len(due_external_ids),
                    len(listing_items),
                )

                if not due_external_ids:
                    log.info(
                        "No SBC detail pages require refreshing"
                    )

                else:
                    await polite_delay()

                # ==========================================================
                # Load only missing or stale details
                # ==========================================================

                for external_id, item in listing_items.items():
                    if external_id not in due_external_ids:
                        continue

                    if (
                        MAX_DETAIL_PAGES > 0
                        and attempted_details >= MAX_DETAIL_PAGES
                    ):
                        log.info(
                            "Stopping after configured detail limit: %d",
                            MAX_DETAIL_PAGES,
                        )
                        break

                    attempted_details += 1

                    log.info(
                        "Loading SBC detail %d: %s",
                        attempted_details,
                        item["title"],
                    )

                    detail_html, detail_status, detail_reason = (
                        await navigate(
                            page=page,
                            url=item["url"],
                            expected_selector=(
                                DETAIL_CHALLENGE_CARD_SELECTOR
                            ),
                            diagnostics=diagnostics,
                        )
                    )

                    if detail_status == 403:
                        failed_details += 1
                        blocked = True

                        log.error(
                            "Cloudflare returned 403 on detail page. "
                            "Stopping the crawl immediately: %s",
                            item["url"],
                        )

                        break

                    if detail_html is None:
                        failed_details += 1

                        log.warning(
                            "Detail fetch failed: title=%s url=%s "
                            "status=%s reason=%s",
                            item["title"],
                            item["url"],
                            detail_status,
                            detail_reason,
                        )

                        await polite_delay()
                        continue

                    detail = parse_detail_page(
                        detail_html
                    )

                    challenges = detail.get(
                        "challenges",
                        [],
                    )

                    if not challenges:
                        failed_details += 1
                        diagnostics[
                            "detail_zero_challenges"
                        ] += 1

                        log.warning(
                            "Detail page loaded but no challenges "
                            "were parsed: title=%s url=%s",
                            item["title"],
                            item["url"],
                        )

                        await polite_delay()
                        continue

                    async with pool.acquire() as conn:
                        await write_detail(
                            conn=conn,
                            event_id=event_ids[external_id],
                            item=item,
                            detail=detail,
                        )

                    written_details += 1

                    diagnostics["challenges_written"] += len(
                        challenges
                    )

                    log.info(
                        "SBC detail saved: title=%s "
                        "challenges=%d total_cost=%s",
                        item["title"],
                        len(challenges),
                        detail.get("total_cost_coins"),
                    )

                    await polite_delay()

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

        # =================================================================
        # Final health calculation
        # =================================================================

        detail_limit_reached = (
            MAX_DETAIL_PAGES > 0
            and attempted_details >= MAX_DETAIL_PAGES
            and attempted_details < len(due_external_ids)
        )

        if due_external_ids:
            attempted_success_rate = (
                written_details / attempted_details
                if attempted_details > 0
                else 0.0
            )
        else:
            attempted_success_rate = 1.0

        run_ok = (
            len(listing_items) > 0
            and not blocked
            and failed_details == 0
            and (
                not due_external_ids
                or written_details > 0
                or detail_limit_reached
            )
        )

        detail_message = (
            f"sets_found={len(listing_items)} "
            f"sets_due={len(due_external_ids)} "
            f"details_attempted={attempted_details} "
            f"details_written={written_details} "
            f"details_failed={failed_details} "
            f"attempted_success_rate="
            f"{attempted_success_rate:.2f} "
            f"detail_limit={MAX_DETAIL_PAGES} "
            f"detail_limit_reached={detail_limit_reached} "
            f"challenges_written="
            f"{diagnostics['challenges_written']} "
            f"detail_zero_challenges="
            f"{diagnostics['detail_zero_challenges']} "
            f"selector_timeouts="
            f"{diagnostics['selector_timeouts']} "
            f"http_403={diagnostics['http_403']} "
            f"http_429={diagnostics['http_429']} "
            f"http_non_200="
            f"{diagnostics['http_non_200']} "
            f"navigation_retries="
            f"{diagnostics['navigation_retries']} "
            f"navigation_exceptions="
            f"{diagnostics['navigation_exceptions']} "
            f"observed_json_urls="
            f"{len(observed_json_urls)} "
            f"blocked={blocked}"
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
                "SBC sync complete. %s",
                detail_message,
            )
        else:
            log.error(
                "SBC sync failed health checks. %s",
                detail_message,
            )

        if blocked:
            await alert(
                "futbin_sbc_sync: Cloudflare returned HTTP 403 "
                "during SBC detail navigation. "
                + detail_message
            )

        elif attempted_details > 0 and written_details == 0:
            await alert(
                "futbin_sbc_sync: SBC detail pages were attempted "
                "but none were written. "
                + detail_message
            )

        elif failed_details > 0:
            await alert(
                "futbin_sbc_sync: one or more SBC detail pages failed. "
                + detail_message
            )

        if observed_json_urls:
            for url in sorted(observed_json_urls):
                log.info(
                    "Observed FUTBIN JSON URL: %s",
                    url,
                )

    finally:
        await pool.close()


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())

    except KeyboardInterrupt:
        log.warning("SBC sync interrupted")
        sys.exit(130)

    except Exception as exc:
        log.exception(
            "SBC sync crashed: %s",
            exc,
        )
        sys.exit(1)

    sys.exit(0)
