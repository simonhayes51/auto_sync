import os
import re
import sys
import time
import asyncio
import asyncpg
import aiohttp
import logging
from datetime import datetime, timedelta, timezone
from html import unescape
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Set

# ------------------ CONFIG ------------------ #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

PRICE_API = "https://www.fut.gg/api/fut/player-prices/25/{}"
META_API  = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
NEW_URLS  = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES   = int(os.getenv("NEW_PAGES", "5"))  # pages per URL
REQUEST_TO  = int(os.getenv("REQUEST_TIMEOUT", "15"))
UA_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/1.2)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

# ------------------ LOGGING (rate-limited) ------------------ #
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))  # identical messages throttled within this window

class RateLimitFilter(logging.Filter):
    _last = {}
    def filter(self, record: logging.LogRecord) -> bool:
        key = getattr(record, "msg", "")
        now = time.monotonic() * 1000
        last = self._last.get(key, 0)
        if now - last < LOG_THROTTLE_MS:
            return False
        self._last[key] = now
        return True

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
for h in logging.getLogger().handlers:
    h.addFilter(RateLimitFilter())
log = logging.getLogger("new_players_sync")

print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={LOG_LEVEL}", flush=True)

# ------------------ DB ------------------ #
async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def preflight_check(conn: asyncpg.Connection) -> None:
    """Ensure UNIQUE constraint or index exists for card_id (needed for ON CONFLICT)."""
    q = """
    SELECT 1
    FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)
    WHERE t.relname = 'fut_players'
      AND c.contype = 'u'
      AND a.attname = 'card_id'
    UNION ALL
    SELECT 1
    FROM pg_indexes
    WHERE tablename = 'fut_players'
      AND indexdef ILIKE '%%UNIQUE%%(card_id%%)'
    LIMIT 1;
    """
    ok = await conn.fetchval(q)
    if not ok:
        raise RuntimeError(
            "UNIQUE constraint on fut_players(card_id) is missing. "
            "Run the provided SQL to add it before starting this worker."
        )

# ------------------ Helpers ------------------ #
RX_HREF = re.compile(r'href=[\'\"]/players/(\d+)(?:/[^\'\"]*)?[\'\"]')
RX_DATA = re.compile(r'data-player-id=[\'\"](\d+)[\'\"]')
RX_ANY  = re.compile(r'/players/(\d+)(?:/|\b)')

def normalize_name(obj, *keys) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s or None
    if isinstance(obj, dict):
        for k in keys or ("name", "shortName", "longName", "fullName"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def extract_price(payload: dict) -> Optional[int]:
    if not isinstance(payload, dict):
        return None
    # Legacy shape
    try:
        ps = (payload.get("ps") or {}).get("LCPrice")
        xb = (payload.get("xbox") or {}).get("LCPrice")
        if ps: return int(ps)
        if xb: return int(xb)
    except Exception:
        pass
    # GraphQL-ish: data.currentPrice.price
    try:
        d = payload.get("data") or {}
        cp = d.get("currentPrice") or {}
        if cp.get("isExtinct") is True:
            return None
        if "price" in cp and cp["price"] is not None:
            return int(cp["price"])
    except Exception:
        pass
    # Top-level currentPrice
    try:
        cp = payload.get("currentPrice") or {}
        if cp.get("isExtinct") is True:
            return None
        if "price" in cp and cp["price"] is not None:
            return int(cp["price"])
    except Exception:
        pass
    # data.prices.ps.lowest pattern (defensive)
    try:
        d = payload.get("data") or {}
        prices = d.get("prices") or {}
        for k in ("ps", "xbox", "pc"):
            v = prices.get(k) or {}
            for key in ("lowest", "LCPrice", "price"):
                if key in v and v[key] is not None:
                    return int(v[key])
    except Exception:
        pass
    return None

# ------------------ HTTP ------------------ #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=REQUEST_TO, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                log.debug("GET %s -> %s", url, resp.status)
                return None
            return await resp.text()
    except Exception as e:
        log.debug("GET %s exception: %s", url, e)
        return None

async def fetch_price(session: aiohttp.ClientSession, numeric_id: str) -> Optional[int]:
    try:
        async with session.get(PRICE_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return None
            payload = await resp.json()
            return extract_price(payload)
    except Exception:
        return None

async def fetch_meta(session: aiohttp.ClientSession, numeric_id: str) -> Dict[str, Optional[str]]:
    try:
        async with session.get(META_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return {"name": None, "position": None, "club": None, "nation": None, "league": None}
            data = await resp.json()

            name = (data.get("name") or
                    normalize_name(data.get("player") or data.get("item"), "name", "fullName"))

            position = data.get("position") or data.get("bestPosition") or data.get("primaryPosition")
            if isinstance(position, dict):
                position = normalize_name(position, "code", "name")

            club   = normalize_name(data.get("club")   or data.get("team"),   "name", "shortName")
            nation = normalize_name(data.get("nation") or data.get("country"), "name", "shortName")
            league = normalize_name(data.get("league"), "name", "shortName")

            meta = data.get("meta") or data.get("item") or {}
            if isinstance(meta.get("position"), dict) and not position:
                position = normalize_name(meta.get("position"), "code", "name")
            club   = club   or normalize_name(meta.get("club")  , "name", "shortName")
            nation = nation or normalize_name(meta.get("nation"), "name", "shortName")
            league = league or normalize_name(meta.get("league"), "name", "shortName")

            return {
                "name": (name or "").strip() or None,
                "position": (position or "").strip() or None,
                "club": (club or "").strip() or None,
                "nation": (nation or "").strip() or None,
                "league": (league or "").strip() or None,
            }
    except Exception:
        return {"name": None, "position": None, "club": None, "nation": None, "league": None}

# ------------------ Discovery ------------------ #
async def discover_new_player_ids(session: aiohttp.ClientSession, pages: int) -> Set[int]:
    found: Set[int] = set()
    for base in NEW_URLS:
        for page in range(1, pages + 1):
            url = base.format(page)
            html = await http_get_text(session, url)
            if not html:
                log.info("🔍 %s: no HTML", url)
                continue
            html = unescape(html)
            before = len(found)
            for rx in (RX_HREF, RX_DATA, RX_ANY):
                for m in rx.finditer(html):
                    try:
                        found.add(int(m.group(1)))
                    except Exception:
                        pass
            added = len(found) - before
            log.info("🔎 %s: +%d ids (total %d)", url, added, len(found))
    return found

# ------------------ Upsert / Enrich ------------------ #
async def upsert_new_players(conn: asyncpg.Connection, numeric_ids: Set[int]) -> List[str]:
    if not numeric_ids:
        return []
    candidates = [f"25-{i}" for i in numeric_ids]
    have_rows = await conn.fetch(
        "SELECT card_id FROM fut_players WHERE card_id = ANY($1::text[])",
        candidates
    )
    have = {r["card_id"] for r in have_rows}
    to_insert = [cid for cid in candidates if cid not in have]
    if not to_insert:
        return []

    rows = [(cid, f"https://www.fut.gg/players/{cid.split('-')[1]}/") for cid in to_insert]
    await conn.executemany(
        """
        INSERT INTO fut_players (card_id, player_url)
        VALUES ($1, $2)
        ON CONFLICT (card_id) DO NOTHING
        """,
        rows
    )
    return to_insert

async def enrich_by_card_ids(conn: asyncpg.Connection, card_ids: List[str]) -> None:
    if not card_ids:
        return
    rows = await conn.fetch(
        "SELECT id, card_id FROM fut_players WHERE card_id = ANY($1::text[])",
        card_ids
    )
    id_by_card = {r["card_id"]: r["id"] for r in rows}
    if not id_by_card:
        return

    async with aiohttp.ClientSession() as session:
        batch = []
        for cid in card_ids:
            numeric_id = cid.split("-")[1]
            price, meta = await asyncio.gather(
                fetch_price(session, numeric_id),
                fetch_meta(session, numeric_id)
            )
            batch.append((
                price,
                meta.get("position"),
                meta.get("club"),
                meta.get("nation"),
                meta.get("league"),
                meta.get("name"),
                datetime.now(timezone.utc),  # write to created_at
                id_by_card[cid]
            ))

        await conn.executemany(
            """
            UPDATE fut_players
            SET price      = COALESCE($1, price),
                position   = COALESCE($2, position),
                club       = COALESCE($3, club),
                nation     = COALESCE($4, nation),
                league     = COALESCE($5, league),
                name       = COALESCE($6, name),
                created_at = $7
            WHERE id = $8
            """,
            batch
        )
    log.info("✅ Enriched %d player(s).", len(card_ids))

async def backfill_missing_meta(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT id, card_id
        FROM fut_players
        WHERE card_id IS NOT NULL
          AND (
            name IS NULL OR name = '' OR
            position IS NULL OR position = '' OR
            club IS NULL OR club = '' OR
            nation IS NULL OR nation = '' OR
            league IS NULL OR league = ''
          )
        LIMIT 1000
        """
    )
    if not rows:
        log.info("ℹ️ No rows need meta backfill.")
        return
    card_ids = [r["card_id"] for r in rows]
    log.info("🧩 Backfilling meta for %d existing players…", len(card_ids))
    await enrich_by_card_ids(conn, card_ids)

# ------------------ One-cycle task ------------------ #
async def run_once() -> None:
    conn = await get_db()
    try:
        await preflight_check(conn)

        async with aiohttp.ClientSession() as session:
            new_ids = await discover_new_player_ids(session, pages=NEW_PAGES)

        added_card_ids = await upsert_new_players(conn, new_ids)
        if added_card_ids:
            log.info("🆕 Inserted %d new players.", len(added_card_ids))
            await enrich_by_card_ids(conn, added_card_ids)
        else:
            log.info("ℹ️ No new players found from discovery.")

        await backfill_missing_meta(conn)

    finally:
        await conn.close()

# ------------------ Daily scheduler @ 19:00 UK ------------------ #
async def scheduler_19_uk() -> None:
    tz = ZoneInfo("Europe/London")
    while True:
        now = datetime.now(tz)
        target = now.replace(hour=19, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        sleep_s = (target - now).total_seconds()
        log.info("🕖 Next new-player check scheduled for %s", target.isoformat())
        await asyncio.sleep(sleep_s)
        for attempt in range(3):
            try:
                log.info("⏰ 19:00 UK tick — running discovery/backfill")
                await run_once()
                break
            except Exception as e:
                wait = 5 * (attempt + 1)
                log.error("❌ Run failed (attempt %d/3): %s. Retrying in %ds", attempt+1, e, wait)
                await asyncio.sleep(wait)

# ------------------ Entrypoint ------------------ #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run once immediately and exit")
    args = ap.parse_args()

    if args.now:
        asyncio.run(run_once())
    else:
        # Run once immediately, then schedule daily at 19:00 UK
        try:
            asyncio.run(run_once())
        finally:
            asyncio.run(scheduler_19_uk())
