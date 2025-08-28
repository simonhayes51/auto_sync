import os
import re
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
NEW_URL   = "https://www.fut.gg/players/new/?page={}"  # HTML pages

NEW_PAGES     = 3            # How many /players/new pages to scan
REQUEST_TO    = 12           # HTTP timeout (s)
UA            = {"User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/1.0)"}

# ------------------ LOGGING ------------------ #
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("new_players_sync")

# ------------------ DB ------------------ #
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# ------------------ HELPERS ------------------ #
ID_HREF = re.compile(r'href="\/players\/(\d+)\/?[^"]*"')
ID_DATA = re.compile(r'\/players\/(\d+)')

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

# ------------------ FETCHERS ------------------ #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url, timeout=REQUEST_TO, headers=UA) as resp:
            if resp.status != 200:
                log.debug(f"GET {url} -> {resp.status}")
                return None
            return await resp.text()
    except Exception as e:
        log.debug(f"GET {url} exception: {e}")
        return None

async def fetch_price(session: aiohttp.ClientSession, numeric_id: str) -> int | None:
    try:
        async with session.get(PRICE_API.format(numeric_id), timeout=REQUEST_TO, headers=UA) as resp:
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
        async with session.get(META_API.format(numeric_id), timeout=REQUEST_TO, headers=UA) as resp:
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

# ------------------ NEW PLAYERS DISCOVERY ------------------ #
async def discover_new_player_ids(session: aiohttp.ClientSession, pages: int = NEW_PAGES) -> set[int]:
    """Scrape /players/new/?page=1..pages, collect numeric IDs from <a href="/players/{id}/...">."""
    found: set[int] = set()
    for page in range(1, pages + 1):
        url = NEW_URL.format(page)
        html = await http_get_text(session, url)
        if not html:
            continue
        html = unescape(html)

        for m in ID_HREF.finditer(html):
            try:
                found.add(int(m.group(1)))
            except Exception:
                pass
        for m in ID_DATA.finditer(html):
            try:
                found.add(int(m.group(1)))
            except Exception:
                pass
    return found

# ------------------ UPSERT + ENRICH NEW PLAYERS ------------------ #
async def upsert_new_players(conn: asyncpg.Connection, numeric_ids: set[int]) -> list[str]:
    """Insert any missing players. Returns a list of *card_id* strings ("25-{id}") that were newly inserted."""
    if not numeric_ids:
        return []

    candidates = [f"25-{i}" for i in numeric_ids]
    have_rows = await conn.fetch("SELECT card_id FROM fut_players WHERE card_id = ANY($1::text[]);", candidates)
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

async def enrich_just_added(conn: asyncpg.Connection, just_added_card_ids: list[str]):
    """Fetch name/meta/price for newly inserted card_ids only."""
    if not just_added_card_ids:
        return

    rows = await conn.fetch(
        "SELECT id, card_id FROM fut_players WHERE card_id = ANY($1::text[]);",
        just_added_card_ids
    )
    id_by_card = {r["card_id"]: r["id"] for r in rows}
    if not id_by_card:
        return

    async with aiohttp.ClientSession() as session:
        batch = []
        for cid in just_added_card_ids:
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
                datetime.now(timezone.utc),  # keep using created_at
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

# ------------------ ONE-CYCLE TASK ------------------ #
async def run_once():
    conn = await get_db()
    try:
        async with aiohttp.ClientSession() as session:
            new_ids = await discover_new_player_ids(session, pages=NEW_PAGES)
        added_card_ids = await upsert_new_players(conn, new_ids)
        if added_card_ids:
            log.info(f"🆕 Inserted {len(added_card_ids)} new players.")
            await enrich_just_added(conn, added_card_ids)
            log.info("✅ Enriched newly added players.")
        else:
            log.info("ℹ️ No new players found.")
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
