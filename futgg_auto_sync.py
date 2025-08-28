import os
import re
import json
import asyncio 
import asyncpg
import aiohttp
import logging
from datetime import datetime, timedelta, timezone
from html import unescape
from zoneinfo import ZoneInfo  # Python 3.9+

# ------------------ CONFIG ------------------ #
DATABASE_URL = os.getenv("DATABASE_URL")

PRICE_API = "https://www.fut.gg/api/fut/player-prices/25/{}"
META_API  = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
NEW_URLS  = [
    "https://www.fut.gg/players/new/?page={}",     # primary
    "https://www.fut.gg/players/?sort=new&page={}" # fallback list view
]

NEW_PAGES     = int(os.getenv("NEW_PAGES", "5"))   # pages to scan per URL
REQUEST_TO    = 15
UA_HEADERS    = {
    "User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/1.1)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

BATCH_SIZE = 100

# ------------------ LOGGING ------------------ #
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("new_players_sync")

# ------------------ DB ------------------ #
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# ------------------ HELPERS ------------------ #
# Robust ID extraction patterns
RX_HREF = re.compile(r'href=[\'\"]/players/(\d+)(?:/[^\'\"]*)?[\'\"]')
RX_DATA = re.compile(r'data-player-id=[\'\"](\d+)[\'\"]')
RX_ANY  = re.compile(r'/players/(\d+)(?:/|\b)')

def normalize_name(obj, *keys):
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

# ------------------ HTTP ------------------ #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url, timeout=REQUEST_TO, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                log.debug(f"GET {url} -> {resp.status}")
                return None
            return await resp.text()
    except Exception as e:
        log.debug(f"GET {url} exception: {e}")
        return None

async def fetch_price(session: aiohttp.ClientSession, numeric_id: str) -> int | None:
    try:
        async with session.get(PRICE_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            price = (data.get("ps") or {}).get("LCPrice") or (data.get("xbox") or {}).get("LCPrice")
            return int(price) if price else None
    except Exception:
        return None

async def fetch_meta(session: aiohttp.ClientSession, numeric_id: str):
    """Return dict(name, position, club, nation, league)."""
    try:
        async with session.get(META_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return {"name": None, "position": None, "club": None, "nation": None, "league": None}
            data = await resp.json()

            name = (data.get("name")
                    or normalize_name(data.get("player") or data.get("item"), "name", "fullName"))

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

# ------------------ DISCOVERY ------------------ #
async def discover_new_player_ids(session: aiohttp.ClientSession, pages: int = NEW_PAGES) -> set[int]:
    """
    Scrape multiple URLs & pages, extract numeric IDs via several patterns.
    Logs how many IDs found per page to help diagnose.
    """
    found: set[int] = set()

    for base in NEW_URLS:
        for page in range(1, pages + 1):
            url = base.format(page)
            html = await http_get_text(session, url)
            if not html:
                log.info(f"🔍 {url}: no HTML")
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
            log.info(f"🔎 {url}: +{added} ids (total {len(found)})")

    return found

# ------------------ UPSERT / ENRICH ------------------ #
async def upsert_new_players(conn: asyncpg.Connection, numeric_ids: set[int]) -> list[str]:
    """Insert any missing players. Returns list of new card_ids ("25-{id}")."""
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

async def enrich_by_card_ids(conn: asyncpg.Connection, card_ids: list[str]):
    """Fetch name/meta/price for the given card_ids."""
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
                datetime.now(timezone.utc),
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

async def backfill_missing_meta(conn: asyncpg.Connection):
    """
    For existing rows with missing name/position/club/nation/league, fetch meta (and price).
    Runs even if zero new players were found.
    """
    rows = await conn.fetch(
        """
        SELECT id, card_id
        FROM fut_players
        WHERE card_id IS NOT NULL
          AND (
            name   IS NULL OR name = '' OR
            position IS NULL OR position = '' OR
            club     IS NULL OR club = '' OR
            nation   IS NULL OR nation = '' OR
            league   IS NULL OR league = ''
          )
        LIMIT 1000
        """
    )
    if not rows:
        log.info("ℹ️ No rows need meta backfill.")
        return

    card_ids = [r["card_id"] for r in rows]
    log.info(f"🧩 Backfilling meta for {len(card_ids)} existing players...")
    await enrich_by_card_ids(conn, card_ids)

# ------------------ ONE-CYCLE TASK ------------------ #
async def run_once():
    conn = await get_db()
    try:
        # 1) Discover & insert any new players
        async with aiohttp.ClientSession() as session:
            new_ids = await discover_new_player_ids(session, pages=NEW_PAGES)
        added_card_ids = await upsert_new_players(conn, new_ids)

        if added_card_ids:
            log.info(f"🆕 Inserted {len(added_card_ids)} new players.")
            await enrich_by_card_ids(conn, added_card_ids)
            log.info("✅ Enriched newly added players.")
        else:
            log.info("ℹ️ No new players found from discovery.")

        # 2) Always try to backfill missing meta on existing rows
        await backfill_missing_meta(conn)

    finally:
        await conn.close()

# ------------------ DAILY SCHEDULER @ 19:00 UK ------------------ #
async def scheduler_19_uk():
    tz = ZoneInfo("Europe/London")
    while True:
        now = datetime.now(tz)
        target = now.replace(hour=19, minute=0, second=0, microsecond=0)
        if now >= target:
            target += timedelta(days=1)
        sleep_s = (target - now).total_seconds()
        log.info(f"🕖 Next new-player check scheduled for {target.isoformat()}")
        await asyncio.sleep(sleep_s)
        try:
            await run_once()
        except Exception as e:
            log.error(f"❌ Run failed: {e}")

if __name__ == "__main__":
    # Run immediately on startup, then follow the 19:00 UK schedule
    try:
        asyncio.run(run_once())
    finally:
        asyncio.run(scheduler_19_uk())
