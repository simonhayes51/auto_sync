"""
SBC (Squad Building Challenge) collector - a new, independent process
alongside futbin_full_sync.py/bin_sales_history_sync.py/ea_price_sync.py.
Scrapes futbin.com's SBC hub + per-set detail pages and writes to the
backend's market_events/sbc_details/sbc_challenges tables (backend
migrations 018/019, `-- target: player`), NOT any table this script owns
exclusively - see ensure_tables() below for why.

Copies bin_sales_history_sync.py's conventions exactly: aiohttp +
BeautifulSoup, the same 429-aware _get_with_retry backoff helper, the
same HEADERS, from monitoring import heartbeat/alert, one-shot
crawl_once() entry point for a Railway Cron Job (not a permanent
worker - no in-process scheduling loop).

*** IMPORTANT - READ BEFORE SCHEDULING THIS ON A REAL CRON ***
This was written with NO live network access to futbin.com (confirmed
403 from the environment's own fetch tooling during development) - the
parsing logic below is a best-effort first draft based on general
knowledge of futbin's typical page conventions (the same platform-
scoped-class idiom bin_sales_history_sync.py already confirmed live for
BIN price and bio stats), not verified against real SBC hub/detail
markup. Every parser below is defensive (tries a primary selector
strategy, logs a diagnostic and returns empty/None rather than raising
or guessing on failure) so a wrong guess degrades to "found nothing this
run" instead of writing garbage - same philosophy as
bin_sales_history_sync.py's sales-history parsing ("no data is an
honest state; a wrong number silently accumulated forever is not").

Before this runs on a real schedule, a human (or a future session with
live network access) MUST:
  1. Confirm the real SBC hub URL for the current game year (SBC_HUB_URL
     below is a best guess - the game-year path segment has changed
     before, e.g. /26/player vs a different scheme).
  2. Confirm the hub is server-rendered HTML BeautifulSoup can parse,
     not a client-side XHR/JSON-driven page (would need a different
     fetch approach entirely - see requirements.txt, playwright is
     already a dependency here but unused by any script today).
  3. Confirm real CSS selectors/classes for: hub list items, the
     set-id-bearing href, challenge blocks, requirement tags, the
     reward card, and any total-cost figure - the ones below are
     reasonable guesses based on futbin's general layout conventions,
     not confirmed.
  4. Confirm external_id (the SBC set id parsed from the URL) is stable
     for the life of a set.
  5. Confirm the expiry/countdown text format for _parse_expiry below.
  6. Confirm 429/bot-protection behavior specific to /sbc pages - a
     different page type than /player/ or /sales/, no guarantee the
     same backoff constants apply.
  7. Run a manual trial against a handful of currently-live, human-
     verified SBCs and cross-check every parsed field before trusting
     any automated write.
  8. Only then: schedule this as a real Railway Cron Job.
"""
import os
import re
import sys
import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futbin_sbc_sync")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

SBC_MAX_RETRIES = int(os.getenv("SBC_MAX_RETRIES", "3"))
SBC_CONCURRENCY = int(os.getenv("SBC_CONCURRENCY", "3"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}

# Best guess, unconfirmed - see checklist item 1 above.
SBC_HUB_URL = os.getenv("SBC_HUB_URL", "https://www.futbin.com/26/sbc")

# Detail pages are only scraped for sets not yet seen, or seen more than
# this long ago (cheap requirements/reward text rarely changes for a
# live set once posted, so re-scraping every run wastes a request).
DETAIL_STALE_HOURS = int(os.getenv("SBC_DETAIL_STALE_HOURS", "6"))
DETAIL_BATCH_LIMIT = int(os.getenv("SBC_DETAIL_BATCH_LIMIT", "20"))


async def _get_with_retry(
    session: aiohttp.ClientSession, url: str, diag: Dict[str, Any]
) -> "tuple[int, Optional[str]]":
    """GET with 429-aware backoff retry - ported verbatim from
    bin_sales_history_sync.py's proven version (the two scripts don't
    share a module for this today either)."""
    backoff = 1.0
    for attempt in range(SBC_MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
                if r.status == 429:
                    diag["http_429_hits"] += 1
                    if attempt < SBC_MAX_RETRIES:
                        retry_after = r.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else backoff
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    return 429, None
                if r.status != 200:
                    return r.status, None
                return 200, await r.text()
        except Exception:
            if attempt < SBC_MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            diag["http_exceptions"] += 1
            return 0, None
    return 0, None


# ---------------------------------------------------------------------------
# Parsing - best-effort, unverified against live HTML. See module
# docstring's checklist before trusting this on a real schedule.
# ---------------------------------------------------------------------------
def _num(txt: str) -> Optional[int]:
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


_SET_ID_RE = re.compile(r"/(?:sbc|squad-building-challenges)/(\d+)")


def _extract_set_id(href: str) -> Optional[str]:
    m = _SET_ID_RE.search(href or "")
    return m.group(1) if m else None


def parse_sbc_hub_list(html: str, diag: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Expects a grid/list of SBC set cards, each linking to its own
    detail page with the set's numeric id somewhere in the href -
    unverified structure, see module docstring."""
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, Any]] = []

    # Primary strategy: any <a> whose href matches the SBC detail URL
    # pattern - deliberately loose (doesn't assume a specific card/grid
    # class name, since that's exactly the kind of detail that can't be
    # confirmed without a live page) so a markup change is less likely
    # to silently return zero results.
    seen_ids = set()
    for a in soup.find_all("a", href=True):
        set_id = _extract_set_id(a["href"])
        if not set_id or set_id in seen_ids:
            continue
        title = a.get_text(strip=True)
        if not title:
            # The set id link is sometimes an image/card wrapper with no
            # direct text - look for a nearby heading instead of
            # discarding the candidate outright.
            heading = a.find(["h3", "h4", "span"], class_=re.compile(r"title|name", re.I))
            title = heading.get_text(strip=True) if heading else None
        if not title:
            diag["hub_item_no_title"] += 1
            continue

        seen_ids.add(set_id)
        items.append({
            "external_id": set_id,
            "title": title,
            "url": a["href"] if a["href"].startswith("http") else f"https://www.futbin.com{a['href']}",
        })

    if not items:
        diag["hub_no_items_found"] += 1
        diag.setdefault("hub_html_len", len(html))
    return items


_EXPIRY_RE = re.compile(r"(\d+)\s*(day|hour|hr|d|h)s?", re.I)


def _parse_expiry(text: Optional[str]) -> Optional[datetime]:
    """Best-effort countdown-text parser (e.g. "3 days left", "12h").
    Unverified real format - see module docstring checklist item 5.
    Returns None rather than a guess if the text doesn't match."""
    if not text:
        return None
    m = _EXPIRY_RE.search(text)
    if not m:
        return None
    n = int(m.group(1))
    unit = m.group(2).lower()
    delta = timedelta(days=n) if unit.startswith("d") else timedelta(hours=n)
    return datetime.now(timezone.utc) + delta


def parse_sbc_detail(html: str, diag: Dict[str, Any]) -> Dict[str, Any]:
    """Expects a set-name heading, a reward preview, a total-cost
    figure, and one block per challenge with requirement tags -
    unverified structure, see module docstring."""
    soup = BeautifulSoup(html, "html.parser")
    result: Dict[str, Any] = {
        "set_name": None, "category": None, "total_cost_coins": None,
        "repeatable": False, "reward_card_id": None, "reward_description": None,
        "expires_at": None, "challenges": [],
    }

    heading = soup.find(["h1", "h2"])
    if heading:
        result["set_name"] = heading.get_text(strip=True)

    cost_el = soup.find(string=re.compile(r"total\s*cost", re.I))
    if cost_el:
        parent_text = cost_el.parent.get_text(" ", strip=True) if cost_el.parent else str(cost_el)
        result["total_cost_coins"] = _num(parent_text)
    if result["total_cost_coins"] is None:
        diag["detail_no_cost_found"] += 1

    if soup.find(string=re.compile(r"repeatable", re.I)):
        result["repeatable"] = True

    expiry_el = soup.find(class_=re.compile(r"expir|countdown|time-left", re.I))
    result["expires_at"] = _parse_expiry(expiry_el.get_text(strip=True) if expiry_el else None)

    reward_el = soup.find(class_=re.compile(r"reward", re.I))
    if reward_el:
        result["reward_description"] = reward_el.get_text(" ", strip=True)[:200]
    else:
        diag["detail_no_reward_found"] += 1
    # reward_card_id (a specific fut_players.card_id) is deliberately left
    # unset here - a reward card's own catalog id needs the same
    # image/link resolution futbin_full_sync.py already does elsewhere in
    # this repo, not reinvented with an unverified selector here. A
    # future pass can backfill this from reward_description via a
    # fut_players name lookup once the description text format is
    # confirmed live.

    for block in soup.find_all(class_=re.compile(r"challenge|squad-item", re.I)):
        name_el = block.find(["h3", "h4", "span"], class_=re.compile(r"title|name", re.I))
        name = name_el.get_text(strip=True) if name_el else None
        if not name:
            continue
        requirements: Dict[str, Any] = {}
        for tag in block.find_all(class_=re.compile(r"requirement|condition", re.I)):
            txt = tag.get_text(" ", strip=True)
            if not txt:
                continue
            # Store the raw requirement text keyed by a slugified version
            # of itself - structured extraction (min_rating, chem_min,
            # etc.) needs confirmed real requirement phrasing, not a
            # guessed regex against unverified text.
            key = re.sub(r"[^a-z0-9]+", "_", txt.lower()).strip("_")[:40] or f"req_{len(requirements)}"
            requirements[key] = txt
        # Scoped to a cost-classed element specifically, not "the first
        # number-like string anywhere in the block" - the requirement
        # tags above also contain numbers (e.g. "Min. Rating: 85"), and
        # an unscoped search would grab one of those instead of the
        # actual cost figure.
        cost_el = block.find(class_=re.compile(r"\bcost\b|price", re.I))
        cost_tag = cost_el.get_text(strip=True) if cost_el else None
        result["challenges"].append({
            "challenge_name": name,
            "requirements": requirements,
            "estimated_cost_coins": _num(cost_tag) if cost_tag else None,
        })

    if not result["challenges"]:
        diag["detail_no_challenges_found"] += 1

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
def _build_fingerprint(detail: Dict[str, Any]) -> List[str]:
    tags: List[str] = []
    if detail.get("repeatable"):
        tags.append("repeatable")
    category = (detail.get("category") or "").lower()
    if category in ("icon", "hero"):
        tags.append("icon_hero_reward")
    for ch in detail.get("challenges", []):
        req_text = " ".join(str(v) for v in ch.get("requirements", {}).values()).lower()
        if "totw" in req_text and "requires_totw" not in tags:
            tags.append("requires_totw")
        if "if " in req_text or "inform" in req_text:
            if "requires_if" not in tags:
                tags.append("requires_if")
    if detail.get("total_cost_coins") and detail["total_cost_coins"] >= 500_000:
        tags.append("high_cost")
    return tags


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------
async def _upsert_event(conn: asyncpg.Connection, item: Dict[str, Any]) -> int:
    row = await conn.fetchrow(
        """
        INSERT INTO market_events (kind, source, external_id, title, updated_at)
        VALUES ('sbc', 'futbin', $1, $2, now())
        ON CONFLICT (kind, source, external_id) DO UPDATE SET
            title = EXCLUDED.title, updated_at = now()
        RETURNING id
        """,
        item["external_id"], item["title"],
    )
    return row["id"]


async def _write_detail(conn: asyncpg.Connection, event_id: int, detail: Dict[str, Any]) -> None:
    fingerprint = _build_fingerprint(detail)
    await conn.execute(
        "UPDATE market_events SET fingerprint = $2, ends_at = COALESCE($3, ends_at) WHERE id = $1",
        event_id, fingerprint, detail.get("expires_at"),
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
        event_id, detail.get("set_name") or "Unknown SBC", detail.get("category"),
        detail.get("total_cost_coins"), detail.get("repeatable", False),
        detail.get("reward_card_id"), detail.get("reward_description"), detail.get("expires_at"),
    )
    await conn.execute("DELETE FROM sbc_challenges WHERE event_id = $1", event_id)
    for i, ch in enumerate(detail.get("challenges", [])):
        import json as _json
        await conn.execute(
            """
            INSERT INTO sbc_challenges (event_id, challenge_name, requirements, estimated_cost_coins, display_order)
            VALUES ($1, $2, $3, $4, $5)
            """,
            event_id, ch["challenge_name"], _json.dumps(ch.get("requirements") or {}),
            ch.get("estimated_cost_coins"), i,
        )


async def _scrape_detail_one(
    pool: asyncpg.Pool, session: aiohttp.ClientSession, sem: asyncio.Semaphore,
    event_id: int, url: str, diag: Dict[str, Any],
) -> None:
    async with sem:
        try:
            status, html = await _get_with_retry(session, url, diag)
            if status != 200 or html is None:
                diag["detail_fetch_failed"] += 1
                return
            detail = parse_sbc_detail(html, diag)
            async with pool.acquire() as conn:
                await _write_detail(conn, event_id, detail)
            diag["sets_written"] += 1
            diag["challenges_written"] += len(detail.get("challenges", []))
        except Exception as e:
            diag["detail_scrape_failed"] += 1
            log.warning("SBC detail scrape failed for event_id=%s url=%s: %s", event_id, url, e)


async def crawl_once() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=SBC_CONCURRENCY + 2)
    diag: Dict[str, Any] = defaultdict(int)
    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)

        async with aiohttp.ClientSession() as session:
            # Tier A: live hub list, every run.
            status, html = await _get_with_retry(session, SBC_HUB_URL, diag)
            if status != 200 or html is None:
                diag["hub_fetch_failed"] += 1
                log.error("SBC hub fetch failed: status=%s url=%s", status, SBC_HUB_URL)
                async with pool.acquire() as hb_conn:
                    await heartbeat(hb_conn, "futbin_sbc_sync", ok=False, detail=f"hub_fetch_failed status={status}")
                await alert(f"futbin_sbc_sync: SBC hub fetch failed (status={status}) - markup change, URL change, or block?")
                return

            hub_items = parse_sbc_hub_list(html, diag)
            log.info("SBC hub: %d sets found", len(hub_items))

            if not hub_items:
                async with pool.acquire() as hb_conn:
                    await heartbeat(hb_conn, "futbin_sbc_sync", ok=False, detail="hub returned zero items - see module docstring checklist")
                await alert("futbin_sbc_sync: hub page fetched OK but zero SBC sets were parsed from it - the parsing logic likely needs updating against real markup (see the module's verification checklist).")
                return

            # Upsert events first (cheap, one row each) so every known set
            # has a stable id before any detail scraping.
            event_ids: Dict[str, int] = {}
            async with pool.acquire() as conn:
                for item in hub_items:
                    event_ids[item["external_id"]] = await _upsert_event(conn, item)

            # Tier B: detail pages for sets never scraped, or stale.
            async with pool.acquire() as conn:
                stale_ids = await conn.fetch(
                    """
                    SELECT e.id, e.external_id
                    FROM market_events e
                    LEFT JOIN sbc_details d ON d.event_id = e.id
                    WHERE e.kind = 'sbc' AND e.id = ANY($1::bigint[])
                      AND (d.event_id IS NULL OR e.updated_at < now() - ($2 || ' hours')::interval)
                    LIMIT $3
                    """,
                    list(event_ids.values()), str(DETAIL_STALE_HOURS), DETAIL_BATCH_LIMIT,
                )

            url_by_event_id = {event_ids[i["external_id"]]: next(h["url"] for h in hub_items if h["external_id"] == i["external_id"]) for i in stale_ids}
            log.info("SBC detail: %d sets due for a (re)scrape", len(url_by_event_id))

            sem = asyncio.Semaphore(SBC_CONCURRENCY)
            await asyncio.gather(*[
                _scrape_detail_one(pool, session, sem, event_id, url, diag)
                for event_id, url in url_by_event_id.items()
            ])

        run_ok = diag["hub_no_items_found"] == 0 and diag["detail_scrape_failed"] < max(1, len(url_by_event_id))
        async with pool.acquire() as hb_conn:
            await heartbeat(
                hb_conn, "futbin_sbc_sync", ok=run_ok,
                detail=(
                    f"hub_sets={len(hub_items)} sets_written={diag['sets_written']} "
                    f"challenges_written={diag['challenges_written']} "
                    f"detail_fetch_failed={diag['detail_fetch_failed']} http_429={diag['http_429_hits']}"
                ),
            )
        log.info(
            "Run complete. hub_sets=%d sets_written=%d challenges_written=%d "
            "detail_fetch_failed=%d detail_scrape_failed=%d http_429_hits=%d http_exceptions=%d",
            len(hub_items), diag["sets_written"], diag["challenges_written"],
            diag["detail_fetch_failed"], diag["detail_scrape_failed"],
            diag["http_429_hits"], diag["http_exceptions"],
        )
    finally:
        await pool.close()


# ================== ONE-SHOT ENTRY POINT ==================
# Deployed as a Railway Cron Job (not a permanent worker), same reasoning
# as bin_sales_history_sync.py - see that script's own entry point
# comment. DO NOT schedule this on a real cron before the verification
# checklist in this file's module docstring has been completed.
if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())
    except Exception as e:
        log.error("crawl_once() failed: %s", e)
        sys.exit(1)
    sys.exit(0)
