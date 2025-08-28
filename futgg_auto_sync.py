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
from typing import Optional, Dict, List, Set, Tuple

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

NEW_PAGES   = int(os.getenv("NEW_PAGES", "5"))         # pages per URL
REQUEST_TO  = int(os.getenv("REQUEST_TIMEOUT", "15"))  # seconds

UA_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/1.5)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))
LOG_FULL_PAYLOADS = os.getenv("LOG_FULL_PAYLOADS") == "1"

# ------------------ LOGGING (rate-limited) ------------------ #
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
    db = await conn.fetchval("SELECT current_database()")
    user = await conn.fetchval("SELECT current_user")
    schema = await conn.fetchval("SHOW search_path")
    table_exists = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not table_exists:
        raise RuntimeError(f"Table public.fut_players NOT found. db={db} user={user} search_path={schema}")

    idx = await conn.fetchval("""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname='public' AND tablename='fut_players'
          AND indexname='fut_players_card_id_key'
    """)
    if not idx:
        raise RuntimeError("UNIQUE index fut_players_card_id_key missing on public.fut_players")
    log.info("✅ Preflight OK: db=%s | user=%s | search_path=%s | unique index present", db, user, schema)

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

def pick_first(d: dict, keys: List[str]):
    for k in keys:
        if k in d and d[k] not in (None, "", []):
            return d[k]
    return None

def extract_price_and_flags(payload: dict) -> Tuple[Optional[int], Optional[bool], Optional[bool], Optional[bool], Optional[datetime]]:
    price = None
    is_objective = None
    is_untradeable = None
    is_extinct = None
    updated_at = None

    if not isinstance(payload, dict):
        return price, is_objective, is_untradeable, is_extinct, updated_at

    # Legacy LCPrice (ps/xbox)
    try:
        ps = (payload.get("ps") or {}).get("LCPrice")
        xb = (payload.get("xbox") or {}).get("LCPrice")
        if ps: price = int(ps)
        elif xb: price = int(xb)
    except Exception:
        pass

    # GraphQL-ish currentPrice
    try:
        d = payload.get("data") or payload
        cp = d.get("currentPrice") or {}
        if "isObjective" in cp:   is_objective   = bool(cp.get("isObjective"))
        if "isUntradeable" in cp: is_untradeable = bool(cp.get("isUntradeable"))
        if "isExtinct" in cp:     is_extinct     = bool(cp.get("isExtinct"))
        if cp.get("price") is not None:
            price = int(cp["price"])
        if cp.get("priceUpdatedAt"):
            try:
                updated_at = datetime.fromisoformat(cp["priceUpdatedAt"].replace("Z", "+00:00"))
            except Exception:
                updated_at = None
    except Exception:
        pass

    return price, is_objective, is_untradeable, is_extinct, updated_at

def extract_meta_fields(data: dict) -> Dict[str, Optional[str]]:
    """
    Return: name, rating(int->str), version, image_url, player_slug, position, club, nation, league
    Works defensively across possible shapes.
    """
    if not isinstance(data, dict):
        return {k: None for k in ("name","rating","version","image_url","player_slug","position","club","nation","league")}

    # Name
    name = (data.get("name")
            or normalize_name(data.get("player") or data.get("item"), "name", "fullName"))

    # Rating
    rating_val = pick_first(data, ["rating", "overall", "overallRating", "ovr"])
    try:
        rating = int(rating_val) if rating_val is not None else None
    except Exception:
        rating = None

    # Version / card type
    version = pick_first(data, ["version", "cardType", "rarity", "program", "itemType"])

    # Image URL
    image_url = (pick_first(data, ["imageUrl", "imageURL", "imgUrl", "imgURL"])
                 or (isinstance(data.get("image"), dict) and pick_first(data["image"], ["url", "href"])))

    # Slug
    player_slug = pick_first(data, ["slug", "seoName", "urlName", "pathSegment"])

    # Position / club / nation / league
    position = data.get("position") or data.get("bestPosition") or data.get("primaryPosition")
    if isinstance(position, dict):
        position = normalize_name(position, "code", "name")
    meta = data.get("meta") or data.get("item") or {}

    club   = normalize_name(data.get("club")   or data.get("team"),   "name", "shortName") \
             or normalize_name(meta.get("club"), "name", "shortName")
    nation = normalize_name(data.get("nation") or data.get("country"), "name", "shortName") \
             or normalize_name(meta.get("nation"), "name", "shortName")
    league = normalize_name(data.get("league"), "name", "shortName") \
             or normalize_name(meta.get("league"), "name", "shortName")
    if not position and isinstance(meta.get("position"), dict):
        position = normalize_name(meta.get("position"), "code", "name")
    elif not position:
        position = meta.get("position")

    # Cast rating to str? Your table defined rating INT — we’ll keep INT in DB.
    return {
        "name": (name or "").strip() or None,
        "rating": rating,
        "version": (version or "").strip() or None,
        "image_url": (image_url or "").strip() or None,
        "player_slug": (player_slug or "").strip() or None,
        "position": (position or "").strip() or None,
        "club": (club or "").strip() or None,
        "nation": (nation or "").strip() or None,
        "league": (league or "").strip() or None,
    }

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

async def fetch_price(session: aiohttp.ClientSession, numeric_id: str):
    try:
        async with session.get(PRICE_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return (None, None, None, None, None)
            payload = await resp.json()
            if LOG_FULL_PAYLOADS:
                log.debug("FUT.GG price payload for %s: %s", numeric_id, payload)
            return extract_price_and_flags(payload)
    except Exception:
        return (None, None, None, None, None)

async def fetch_meta(session: aiohttp.ClientSession, numeric_id: str) -> Dict[str, Optional[str]]:
    try:
        async with session.get(META_API.format(numeric_id), timeout=REQUEST_TO, headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return {k: None for k in ("name","rating","version","image_url","player_slug","position","club","nation","league")}
            data = await resp.json()
            if LOG_FULL_PAYLOADS:
                log.debug("FUT.GG meta payload for %s: %s", numeric_id, data)
            return extract_meta_fields(data)
    except Exception:
        return {k: None for k in ("name","rating","version","image_url","player_slug","position","club","nation","league")}

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
        "SELECT card_id FROM public.fut_players WHERE card_id = ANY($1::text[])",
        candidates
    )
    have = {r["card_id"] for r in have_rows}
    to_insert = [cid for cid in candidates if cid not in have]
    if not to_insert:
        return []

    rows: List[Tuple[str, str]] = [(cid, f"https://www.fut.gg/players/{cid.split('-')[1]}/") for cid in to_insert]
    await conn.executemany(
        """
        INSERT INTO public.fut_players (card_id, player_url)
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
        "SELECT id, card_id FROM public.fut_players WHERE card_id = ANY($1::text[])",
        card_ids
    )
    id_by_card = {r["card_id"]: r["id"] for r in rows}
    if not id_by_card:
        return

    async with aiohttp.ClientSession() as session:
        batch: List[Tuple[
            Optional[int], Optional[str], Optional[str], Optional[str], Optional[str],
            Optional[str], Optional[int], Optional[str], Optional[str], Optional[str],
            Optional[str], Optional[bool], Optional[bool], Optional[bool], Optional[datetime],
            datetime, int
        ]] = []
        for cid in card_ids:
            numeric_id = cid.split("-")[1]
            (price, is_obj, is_untrad, is_ext, price_ts), meta = await asyncio.gather(
                fetch_price(session, numeric_id),
                fetch_meta(session, numeric_id)
            )
            # If we obtained a slug, we can refine player_url
            player_slug = meta.get("player_slug")
            pretty_url = f"https://www.fut.gg/players/{numeric_id}/" + (f"{player_slug}/" if player_slug else "")

            batch.append((
                price,
                meta.get("position"),
                meta.get("club"),
                meta.get("nation"),
                meta.get("league"),
                meta.get("name"),
                meta.get("rating"),
                meta.get("version"),
                meta.get("image_url"),
                player_slug,
                pretty_url,
                is_obj,
                is_untrad,
                is_ext,
                price_ts,
                datetime.now(timezone.utc),  # created_at (per your preference)
                id_by_card[cid]
            ))

        await conn.executemany(
            """
            UPDATE public.fut_players
            SET price            = COALESCE($1,  price),
                position         = COALESCE($2,  position),
                club             = COALESCE($3,  club),
                nation           = COALESCE($4,  nation),
                league           = COALESCE($5,  league),
                name             = COALESCE($6,  name),
                rating           = COALESCE($7,  rating),
                version          = COALESCE($8,  version),
                image_url        = COALESCE($9,  image_url),
                player_slug      = COALESCE($10, player_slug),
                player_url       = COALESCE($11, player_url),
                is_objective     = COALESCE($12, is_objective),
                is_untradeable   = COALESCE($13, is_untradeable),
                is_extinct       = COALESCE($14, is_extinct),
                price_updated_at = COALESCE($15, price_updated_at),
                created_at       = $16
            WHERE id = $17
            """,
            batch
        )
    log.info("✅ Enriched %d player(s).", len(card_ids))

async def backfill_missing_meta(conn: asyncpg.Connection) -> None:
    total = 0
    while True:
        rows = await conn.fetch(
            """
            SELECT id, card_id
            FROM public.fut_players
            WHERE card_id IS NOT NULL
              AND (
                name IS NULL OR name = '' OR
                position IS NULL OR position = '' OR
                club IS NULL OR club = '' OR
                nation IS NULL OR nation = '' OR
                league IS NULL OR league = '' OR
                rating IS NULL OR
                version IS NULL OR version = '' OR
                image_url IS NULL OR image_url = '' OR
                player_slug IS NULL OR player_slug = ''
              )
            LIMIT 1000
            """
        )
        if not rows:
            if total == 0:
                log.info("ℹ️ No rows need meta backfill.")
            else:
                log.info("✅ Backfill complete (%d players updated).", total)
            return

        card_ids = [r["card_id"] for r in rows]
        total += len(card_ids)
        log.info("🧩 Backfilling meta for %d players (running total %d)…", len(card_ids), total)
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
        try:
            asyncio.run(run_once())
        finally:
            asyncio.run(scheduler_19_uk())
