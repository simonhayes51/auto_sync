"""
SBC (Squad Building Challenge) collector - an independent process alongside
futbin_full_sync.py/bin_sales_history_sync.py/ea_price_sync.py/
futbin_card_art_backfill.py. Scrapes futbin.com's SBC listing pages + each
set's detail page and writes to the backend's market_events/sbc_details/
sbc_challenges tables (backend migrations 018/019, `-- target: player`),
NOT any table this script owns exclusively - see ensure_tables() below.

*** Selectors below are CONFIRMED against real FUTBIN markup ***
The first version of this file (git history) was a best-effort guess
written with no live network access to futbin.com - explicitly flagged as
not-yet-verified, per its own checklist. That's now resolved: the CSS
selectors below were checked against real saved FUTBIN HTML (both a
listing page and an individual SBC detail page) and cross-validated with
BeautifulSoup independently of Playwright, confirming every field parses
correctly as of that check. This file ports that confirmed scraping
strategy into this project's real schema/monitoring/Cron Job conventions,
rather than the original standalone script's local-JSON-file output.

One real, confirmed finding from that validation that changes this
script's whole approach: SBC pages are NOT plain server-rendered HTML
like futbin's player pages (which bin_sales_history_sync.py and
futbin_card_art_backfill.py fetch with a simple aiohttp GET) - the
listing grid and detail page both need a real browser to render before
this markup exists in the page. This is the only collector in this repo
that needs Playwright + Chromium rather than aiohttp; requirements.txt
already lists playwright (added ahead of this), and nixpacks.toml (new,
alongside this file) installs the Chromium binary at build time so it's
present in the deployed image rather than fetched on every cron
invocation.

Still NOT independently confirmed in THIS environment (no live network
access to futbin.com here) - do one supervised manual run
(`python3 futbin_sbc_sync.py`) and read the log output before adding
this to a real Cron schedule:
  - FUTBIN does redesign occasionally (per the original scraper's own
    notes) - selectors that matched at validation time may have drifted
    by the time this actually runs. If a run logs "zero SBCs parsed",
    that's the signal to re-check.
  - The exact text format of the listing card's "expires"/"repeatable"
    fields - confirmed to exist and be selectable, but this file's
    interpretation of their *content* (_parse_expiry's day/hour regex,
    _parse_repeatable's substring check) was not asserted against the
    literal real strings, only reasoned about.
  - 429/403 behavior under this exact page-count/timing pattern -
    unrelated to whether the selectors are right.
  - Total runtime: ~7 listing pages + ~40-60 detail pages at
    REQUEST_DELAY_SECONDS apart is several minutes per run (matches the
    original scraper's own "5-7 minutes" estimate) - too slow for a
    tight interval. Recommend a Railway Cron Job once daily (e.g. 18:00,
    matching the original scraper's own suggested schedule), not the
    15-30 minute cadence that suits futbin_card_art_backfill.py's much
    cheaper per-card fetches.

reward_card_id (a specific fut_players.card_id for the SBC's reward) is
deliberately left unset here, same as the prior draft - resolving a
reward's text description to a real catalog id needs the same fuzzy
name-matching futbin_full_sync.py already does elsewhere in this repo,
not reinvented here against an unconfirmed text format.
"""
import os
import re
import sys
import json
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import asyncpg
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, BrowserContext

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futbin_sbc_sync")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

BASE_URL = "https://www.futbin.com"
# Specific categories first, "All" last - see the merge loop in crawl_once()
# for why order matters (first-seen category wins per SBC).
CATEGORY_PATHS = [
    "/26/squad-building-challenges/Players",
    "/26/squad-building-challenges/Upgrades",
    "/26/squad-building-challenges/Challenges",
    "/26/squad-building-challenges/Icons",
    "/26/squad-building-challenges/Foundations",
    "/26/squad-building-challenges/Swaps",
    "/squad-building-challenges",
]

REQUEST_DELAY_SECONDS = float(os.getenv("SBC_REQUEST_DELAY_SECONDS", "4"))
# Matched to the recommended once-daily cadence (see module docstring) -
# comfortably shorter than 24h so a run that's a little early or late
# still re-scrapes everything, longer than the gap between runs so a
# same-day re-run (e.g. manual testing) doesn't redundantly re-scrape.
DETAIL_STALE_HOURS = int(os.getenv("SBC_DETAIL_STALE_HOURS", "20"))
NAV_TIMEOUT_MS = 30_000
SELECTOR_TIMEOUT_MS = 15_000
MAX_RETRIES = int(os.getenv("SBC_MAX_RETRIES", "2"))

USER_AGENT = (
    "Mozilla/5.0 (compatible; FutHubSBCBot/1.0; "
    "personal-use SBC price-impact tracker; contact: add-a-real-contact-here)"
)

# --- Confirmed against real FUTBIN markup (see module docstring) ---
CARD_SELECTOR = ".sbc-card-wrapper"
CARD_NAME_SELECTOR = ".og-card-wrapper-top div.text-ellipsis > div.text-ellipsis"
CARD_BADGE_SELECTOR = ".sbc-badge"
CARD_REWARD_SELECTOR = ".sbc-rewards-area .xxs-font.slim-font.text-ellipsis-2"
CARD_DESC_SELECTOR = ".centered.full-height.max-width-100.text-wrap p"
CARD_EXPIRES_SELECTOR = ".sbc-info-row .xxs-column:nth-of-type(1) .bold"
CARD_REPEATABLE_SELECTOR = ".sbc-info-row .xxs-column:nth-of-type(2) > div:not(.text-faded)"
CARD_PROGRESS_SELECTOR = ".sbc-info-row .xxs-column:nth-of-type(3) .bold"

DETAIL_TOTAL_PRICE_SELECTOR = ".info-row-part .s-row.centered.flex-wrap"
DETAIL_CHALLENGE_CARD_SELECTOR = ".sbc-box-wrapper"
DETAIL_CHALLENGE_NAME_SELECTOR = ".og-card-wrapper-top .xxs-font.bold"
DETAIL_CHALLENGE_REWARD_SELECTOR = ".sbc-box-front-info .xxs-font"
DETAIL_CHALLENGE_DESC_SELECTOR = ".sbc-box-front p"
DETAIL_REQUIREMENT_ROW_SELECTOR = ".sbc-requirements .challenge-box-description-row"


def _num(txt: Optional[str]) -> Optional[int]:
    if not txt:
        return None
    t = txt.lower().replace(",", "").strip()
    if t.endswith("m"):
        try:
            return int(float(t[:-1]) * 1_000_000)
        except Exception:
            return None
    if t.endswith("k"):
        try:
            return int(float(t[:-1]) * 1_000)
        except Exception:
            return None
    m = re.search(r"\d+(\.\d+)?", t)
    return int(float(m.group(0))) if m else None


_EXPIRY_RE = re.compile(r"(\d+)\s*(day|hour|hr|d|h)s?", re.I)


def _parse_expiry(text: Optional[str]) -> Optional[datetime]:
    """Best-effort - the selector is confirmed, the exact real string
    content wasn't independently re-asserted here. Returns None (an
    honest "unknown", not a guess) rather than trying to force-match."""
    if not text:
        return None
    m = _EXPIRY_RE.search(text)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    delta = timedelta(days=n) if unit.startswith("d") else timedelta(hours=n)
    return datetime.now(timezone.utc) + delta


def _parse_repeatable(text: Optional[str]) -> bool:
    if not text:
        return False
    return "non" not in text.lower()


def parse_listing_page(html: str, category: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict[str, Any]] = []
    for card in soup.select(CARD_SELECTOR):
        link_el = card.select_one("a")
        href = link_el.get("href") if link_el else None
        if not href:
            continue
        url = href if href.startswith("http") else f"{BASE_URL}{href}"
        # No confirmed numeric SBC id was captured by the original
        # scraper (it only kept the resolved URL) - the URL path itself
        # is a stable, unique-per-set natural key, so used directly
        # rather than guessing an id-extraction regex against an
        # unconfirmed URL structure.
        external_id = urlparse(url).path.strip("/")
        if not external_id:
            continue

        name_el = card.select_one(CARD_NAME_SELECTOR)
        badge_el = card.select_one(CARD_BADGE_SELECTOR)
        reward_el = card.select_one(CARD_REWARD_SELECTOR)
        desc_el = card.select_one(CARD_DESC_SELECTOR)
        expires_el = card.select_one(CARD_EXPIRES_SELECTOR)
        repeat_el = card.select_one(CARD_REPEATABLE_SELECTOR)
        progress_el = card.select_one(CARD_PROGRESS_SELECTOR)

        out.append({
            "external_id": external_id,
            "url": url,
            "title": name_el.get_text(strip=True) if name_el else "Unknown SBC",
            "category": category,
            "badge": badge_el.get_text(strip=True) if badge_el else None,
            "description": desc_el.get_text(strip=True) if desc_el else None,
            "group_reward": reward_el.get_text(strip=True) if reward_el else None,
            "expires_text": expires_el.get_text(strip=True) if expires_el else None,
            "repeatable_text": repeat_el.get_text(strip=True) if repeat_el else None,
            "progress_text": progress_el.get_text(strip=True) if progress_el else None,
        })
    return out


def parse_detail_page(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, Any] = {"total_cost_coins": None, "challenges": []}

    price_el = soup.select_one(DETAIL_TOTAL_PRICE_SELECTOR)
    if price_el:
        # PS/Xbox price and PC price are separate child rows with no
        # separator between them in the DOM - take the first row as the
        # PS/Xbox figure, matching the 'ps' platform this whole project
        # is scoped to elsewhere (fair_value_mv, card_scores, etc).
        rows = [r.get_text(strip=True) for r in price_el.select(":scope > div") if r.get_text(strip=True)]
        if rows:
            result["total_cost_coins"] = _num(rows[0])

    for i, cc in enumerate(soup.select(DETAIL_CHALLENGE_CARD_SELECTOR)):
        name_el = cc.select_one(DETAIL_CHALLENGE_NAME_SELECTOR)
        reward_el = cc.select_one(DETAIL_CHALLENGE_REWARD_SELECTOR)
        desc_el = cc.select_one(DETAIL_CHALLENGE_DESC_SELECTOR)
        req_els = cc.select(DETAIL_REQUIREMENT_ROW_SELECTOR)

        requirements: Dict[str, str] = {}
        for r in req_els:
            txt = r.get_text(" ", strip=True)
            if not txt:
                continue
            # Raw requirement text keyed by a slugified version of
            # itself - structured extraction (min_rating, chem_min, etc)
            # needs a confirmed real requirement phrasing to regex
            # against, which wasn't part of this validation pass.
            key = re.sub(r"[^a-z0-9]+", "_", txt.lower()).strip("_")[:40] or f"req_{len(requirements)}"
            requirements[key] = txt

        result["challenges"].append({
            "challenge_name": name_el.get_text(strip=True) if name_el else f"Challenge {i + 1}",
            "reward": reward_el.get_text(strip=True) if reward_el else None,
            "description": desc_el.get_text(strip=True) if desc_el else None,
            "requirements": requirements,
            # Not present on the detail page per the confirmed
            # selectors (only overall total_cost_coins is) - left
            # unset rather than guessed.
            "estimated_cost_coins": None,
        })

    return result


# ---------------------------------------------------------------------------
# Schema - byte-identical DDL to backend/migrations/018_market_events.sql
# and 019_event_market_impact.sql, so whichever lands first on a fresh
# database wins and the other is a true no-op (same idiom
# bin_sales_history_sync.py's own ensure_tables uses against fut_players).
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
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_market_events_fingerprint ON market_events USING GIN (fingerprint)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_market_events_kind_starts ON market_events (kind, starts_at DESC)")
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sbc_details (
            event_id            BIGINT PRIMARY KEY REFERENCES market_events(id) ON DELETE CASCADE,
            set_name            TEXT NOT NULL,
            category            TEXT,
            total_cost_coins    BIGINT,
            repeatable          BOOLEAN NOT NULL DEFAULT false,
            reward_card_id      BIGINT REFERENCES fut_players(card_id),
            reward_description  TEXT,
            expires_at          TIMESTAMPTZ
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sbc_challenges (
            id                    BIGSERIAL PRIMARY KEY,
            event_id              BIGINT NOT NULL REFERENCES market_events(id) ON DELETE CASCADE,
            challenge_name        TEXT NOT NULL,
            requirements          JSONB NOT NULL DEFAULT '{}',
            estimated_cost_coins  BIGINT,
            display_order         INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_sbc_challenges_event ON sbc_challenges (event_id)")


# ---------------------------------------------------------------------------
# Fingerprints - generated at write time from parsed structure, never
# re-derived downstream (see the v2 plan's SBC collector design).
# ---------------------------------------------------------------------------
def _build_fingerprint(item: Dict[str, Any], detail: Dict[str, Any], repeatable: bool) -> List[str]:
    tags: List[str] = []
    if repeatable:
        tags.append("repeatable")
    category = (item.get("category") or "").lower()
    if category in ("icons", "icon", "heroes", "hero"):
        tags.append("icon_hero_reward")
    for ch in detail.get("challenges", []):
        req_text = " ".join(str(v) for v in ch.get("requirements", {}).values()).lower()
        if "totw" in req_text and "requires_totw" not in tags:
            tags.append("requires_totw")
        if ("if " in req_text or "inform" in req_text) and "requires_if" not in tags:
            tags.append("requires_if")
    if detail.get("total_cost_coins") and detail["total_cost_coins"] >= 500_000:
        tags.append("high_cost")
    return tags


async def _upsert_event(conn: asyncpg.Connection, item: Dict[str, Any]) -> int:
    payload = {
        "badge": item.get("badge"),
        "group_reward": item.get("group_reward"),
        "progress_text": item.get("progress_text"),
    }
    row = await conn.fetchrow(
        """
        INSERT INTO market_events (kind, source, external_id, title, description, payload, updated_at)
        VALUES ('sbc', 'futbin', $1, $2, $3, $4, now())
        ON CONFLICT (kind, source, external_id) DO UPDATE SET
            title = EXCLUDED.title, description = EXCLUDED.description,
            payload = EXCLUDED.payload, updated_at = now()
        RETURNING id
        """,
        item["external_id"], item["title"], item.get("description"), json.dumps(payload),
    )
    return row["id"]


async def _write_detail(conn: asyncpg.Connection, event_id: int, item: Dict[str, Any], detail: Dict[str, Any]) -> None:
    expires_at = _parse_expiry(item.get("expires_text"))
    repeatable = _parse_repeatable(item.get("repeatable_text"))
    fingerprint = _build_fingerprint(item, detail, repeatable)

    await conn.execute(
        "UPDATE market_events SET fingerprint = $2, ends_at = COALESCE($3, ends_at) WHERE id = $1",
        event_id, fingerprint, expires_at,
    )
    await conn.execute(
        """
        INSERT INTO sbc_details (event_id, set_name, category, total_cost_coins, repeatable, reward_card_id, reward_description, expires_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (event_id) DO UPDATE SET
            set_name = EXCLUDED.set_name, category = EXCLUDED.category,
            total_cost_coins = EXCLUDED.total_cost_coins, repeatable = EXCLUDED.repeatable,
            reward_card_id = EXCLUDED.reward_card_id, reward_description = EXCLUDED.reward_description,
            expires_at = EXCLUDED.expires_at
        """,
        event_id, item["title"], item.get("category"),
        detail.get("total_cost_coins"), repeatable,
        None, item.get("group_reward"), expires_at,
    )
    await conn.execute("DELETE FROM sbc_challenges WHERE event_id = $1", event_id)
    for i, ch in enumerate(detail.get("challenges", [])):
        await conn.execute(
            """
            INSERT INTO sbc_challenges (event_id, challenge_name, requirements, estimated_cost_coins, display_order)
            VALUES ($1, $2, $3, $4, $5)
            """,
            event_id, ch["challenge_name"], json.dumps(ch.get("requirements") or {}),
            ch.get("estimated_cost_coins"), i,
        )


# ---------------------------------------------------------------------------
# Fetching - Playwright, not aiohttp (see module docstring for why).
# ---------------------------------------------------------------------------
async def _goto_with_retry(context: BrowserContext, url: str, wait_selector: str, diag: Dict[str, Any]) -> Optional[str]:
    page: Page = await context.new_page()
    try:
        backoff = 2.0
        for attempt in range(MAX_RETRIES + 1):
            try:
                resp = await page.goto(url, wait_until="networkidle", timeout=NAV_TIMEOUT_MS)
                if resp is not None and resp.status == 429:
                    diag["http_429_hits"] += 1
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(backoff)
                        backoff *= 2
                        continue
                    return None
                if resp is not None and resp.status >= 400:
                    diag["http_non200"] += 1
                    return None
                try:
                    await page.wait_for_selector(wait_selector, timeout=SELECTOR_TIMEOUT_MS)
                except Exception:
                    # Page loaded but the expected content never showed up -
                    # let the caller's parser report zero items/challenges
                    # rather than treating this as a hard fetch failure.
                    pass
                return await page.content()
            except Exception as e:
                if attempt < MAX_RETRIES:
                    diag["nav_retries"] += 1
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
                diag["http_exceptions"] += 1
                log.warning("goto failed for %s: %s", url, e)
                return None
        return None
    finally:
        await page.close()


async def crawl_once() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=4)
    diag: Dict[str, Any] = defaultdict(int)
    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)

        all_items: Dict[str, Dict[str, Any]] = {}
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=USER_AGENT)

            for path in CATEGORY_PATHS:
                last_segment = path.rstrip("/").rsplit("/", 1)[-1]
                category = "all" if last_segment == "squad-building-challenges" else last_segment.lower()
                url = f"{BASE_URL}{path}"
                html = await _goto_with_retry(context, url, CARD_SELECTOR, diag)
                if html is None:
                    diag["category_fetch_failed"] += 1
                    log.warning("Category fetch failed: %s", url)
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
                    continue

                items = parse_listing_page(html, category)
                log.info("Category %s: %d SBCs found", category, len(items))
                for it in items:
                    # First-seen category wins - CATEGORY_PATHS lists the
                    # specific categories before "All", so a set only
                    # gets category="all" if it wasn't found under any
                    # specific one (an honest fallback, not expected to
                    # be common).
                    all_items.setdefault(it["external_id"], it)
                await asyncio.sleep(REQUEST_DELAY_SECONDS)

            if not all_items:
                await browser.close()
                async with pool.acquire() as hb_conn:
                    await heartbeat(hb_conn, "futbin_sbc_sync", ok=False, detail="zero SBCs parsed from any listing page")
                await alert(
                    "futbin_sbc_sync: zero SBC sets parsed from any listing page - FUTBIN markup may have "
                    "changed (re-check selectors against a fresh page save) or every category fetch failed."
                )
                return

            event_ids: Dict[str, int] = {}
            async with pool.acquire() as conn:
                for item in all_items.values():
                    event_ids[item["external_id"]] = await _upsert_event(conn, item)

            async with pool.acquire() as conn:
                stale_rows = await conn.fetch(
                    """
                    SELECT e.external_id
                    FROM market_events e
                    LEFT JOIN sbc_details d ON d.event_id = e.id
                    WHERE e.kind = 'sbc' AND e.id = ANY($1::bigint[])
                      AND (d.event_id IS NULL OR e.updated_at < now() - ($2 || ' hours')::interval)
                    """,
                    list(event_ids.values()), str(DETAIL_STALE_HOURS),
                )
            due_ids = {r["external_id"] for r in stale_rows}
            log.info("SBC detail: %d of %d sets due for a (re)scrape", len(due_ids), len(all_items))

            written = failed = 0
            for external_id, item in all_items.items():
                if external_id not in due_ids:
                    continue
                html = await _goto_with_retry(context, item["url"], DETAIL_CHALLENGE_CARD_SELECTOR, diag)
                if html is None:
                    failed += 1
                    await asyncio.sleep(REQUEST_DELAY_SECONDS)
                    continue
                detail = parse_detail_page(html)
                async with pool.acquire() as conn:
                    await _write_detail(conn, event_ids[external_id], item, detail)
                written += 1
                diag["challenges_written"] += len(detail.get("challenges", []))
                await asyncio.sleep(REQUEST_DELAY_SECONDS)

            await browser.close()

        run_ok = diag["category_fetch_failed"] < len(CATEGORY_PATHS)
        detail_msg = (
            f"sets_found={len(all_items)} sets_due={len(due_ids)} sets_written={written} "
            f"sets_failed={failed} challenges_written={diag['challenges_written']} "
            f"category_fetch_failed={diag['category_fetch_failed']} "
            f"http_429={diag['http_429_hits']} http_exc={diag['http_exceptions']}"
        )
        async with pool.acquire() as hb_conn:
            await heartbeat(hb_conn, "futbin_sbc_sync", ok=run_ok, detail=detail_msg)
        log.info("Run complete. %s", detail_msg)

        if written == 0 and len(due_ids) > 0:
            await alert(f"futbin_sbc_sync: {len(due_ids)} sets were due for a detail scrape but 0 succeeded - {detail_msg}")
    finally:
        await pool.close()


# ================== ONE-SHOT ENTRY POINT ==================
# Deployed as a Railway Cron Job (not a permanent worker), same reasoning
# as bin_sales_history_sync.py/futbin_card_art_backfill.py. Recommend
# once daily (e.g. "0 18 * * *") - see module docstring for why this is
# too slow a job for a tight interval.
if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())
    except Exception as e:
        log.error("crawl_once() failed: %s", e)
        sys.exit(1)
    sys.exit(0)
