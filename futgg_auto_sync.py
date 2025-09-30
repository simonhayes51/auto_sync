# scripts/futgg_meta_sync.py 
import os
import re
import sys
import time
import json
import asyncio
import asyncpg
import aiohttp
import logging
import signal
from datetime import datetime, timedelta
from html import unescape
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Tuple, Any
from aiohttp import web  # health server

# ================== CONFIG ================== #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

SEASON = "26"
META_API = f"https://www.fut.gg/api/fut/player-item-definitions/{SEASON}" + "/{}"
LISTING_URLS = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))
CONCURRENCY        = int(os.getenv("CONCURRENCY", "12"))
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))

RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", "0.1"))
BATCH_DELAY        = float(os.getenv("BATCH_DELAY", "5.0"))
BULK_BATCH_SIZE    = int(os.getenv("BULK_BATCH_SIZE", "50"))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", "3"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/3.1)",
    "Accept": "application/json,text/html",
    "Accept-Language": "en-GB,en;q=0.9",
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))

# Optional columns (detected at runtime)
HAS_NICK = HAS_FIRST = HAS_LAST = HAS_ALT = False

# ================== POSITION MAP ================== # 
POSITION_MAP = {
    0: "GK", 1: "GK", 2: "GK",
    3: "RB", 4: "RB",
    5: "CB", 6: "CB",
    7: "LB", 8: "LB", 9: "LB",
    10: "CDM", 11: "CDM",
    12: "RM", 13: "RM",
    14: "CM", 15: "CM",
    16: "LM", 17: "LM",
    18: "CAM", 19: "CAM", 20: "CAM", 21: "CAM", 22: "CAM",
    23: "RW", 24: "RW",
    25: "ST", 26: "ST",
    27: "LW",
}

# ================== LOGGING ================== #
class _RateLimitFilter(logging.Filter):
    _last = {}
    def filter(self, rec: logging.LogRecord) -> bool:
        key = rec.msg
        now = time.monotonic() * 1000
        last = self._last.get(key, 0)
        if now - last < LOG_THROTTLE_MS:
            return False
        self._last[key] = now
        return True

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
for h in logging.getLogger().handlers:
    h.addFilter(_RateLimitFilter())
log = logging.getLogger("futgg_meta_sync")
print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={LOG_LEVEL}", flush=True)

# ================== DB ================== #
async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def preflight(conn: asyncpg.Connection) -> None:
    """
    - Ensure fut_players exists
    - Ensure unique index on (card_id)
    - Detect optional columns and add them if missing
    """
    global HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT

    tbl = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not tbl:
        raise RuntimeError("Table public.fut_players not found")

    # Ensure the unique index on card_id exists (required by this script)
    has_idx = await conn.fetchval("""
        SELECT 1 FROM pg_indexes
        WHERE schemaname='public' AND tablename='fut_players'
          AND indexname='fut_players_card_id_key'
    """)
    if not has_idx:
        await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS fut_players_card_id_key ON fut_players(card_id)")

    # Auto-add optional columns if missing
    await conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='fut_players' AND column_name='first_name'
            ) THEN
                ALTER TABLE fut_players ADD COLUMN first_name TEXT;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='fut_players' AND column_name='last_name'
            ) THEN
                ALTER TABLE fut_players ADD COLUMN last_name TEXT;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='fut_players' AND column_name='nickname'
            ) THEN
                ALTER TABLE fut_players ADD COLUMN nickname TEXT;
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name='fut_players' AND column_name='altposition'
            ) THEN
                ALTER TABLE fut_players ADD COLUMN altposition TEXT;
            END IF;
        END$$;
    """)

    cols = {
        r["column_name"]
        for r in await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
        """)
    }
    HAS_NICK  = "nickname"   in cols
    HAS_FIRST = "first_name" in cols
    HAS_LAST  = "last_name"  in cols
    HAS_ALT   = "altposition" in cols
    log.info(
        "✅ Preflight OK | columns: nickname=%s first=%s last=%s altposition=%s",
        HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT
    )

# ================== HELPERS ================== #
def build_image_url(card_image_path: Optional[str]) -> Optional[str]:
    if not card_image_path or not isinstance(card_image_path, str):
        return None
    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=100,format=auto,width=500/{card_image_path.strip()}"

def pick_name(nick: Optional[str], first: Optional[str], last: Optional[str]) -> Optional[str]:
    if isinstance(nick, str) and nick.strip():
        return nick.strip()
    if first and last:
        return f"{first.strip()} {last.strip()}"
    return (first or last or None)

def name_from_slug(slug: Optional[str]) -> Optional[str]:
    if not slug or not isinstance(slug, str):
        return None
    parts = slug.split("-", 1)
    human = parts[1] if len(parts) > 1 else parts[0]
    human = human.replace("-", " ").strip()
    return " ".join(w.capitalize() for w in human.split()) if human else None

def _get_nested(d: Any, path: List[str]) -> Any:
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

# ================== HTTP ================== #
async def fetch_meta_with_retry(session: aiohttp.ClientSession, card_id: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            async with session.get(META_API.format(card_id), timeout=REQUEST_TIMEOUT,
                                   headers=UA_HEADERS) as resp:
                if resp.status == 429:
                    wait_time = min(2 ** attempt, 30)
                    log.warning(f"Rate limited {card_id}, wait {wait_time}s")
                    await asyncio.sleep(wait_time)
                    continue
                if resp.status != 200:
                    return {}
                raw = await resp.json(content_type=None)
                return parse_player_data(raw, card_id)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                log.error(f"Meta fetch failed {card_id}: {e}")
    return {}

def parse_player_data(raw: dict, card_id: str) -> dict:
    """
    Parse FUT.GG definition payloads. Handles both {data:{...}} and plain dicts,
    and tolerates nesting under 'player', 'resource', or 'metadata'.
    """
    data = raw.get("data") if isinstance(raw, dict) and "data" in raw else raw
    if isinstance(data, list) and data:
        # pick the variant with the highest overall
        try:
            data = max(data, key=lambda d: int(d.get("overall") or 0))
        except Exception:
            data = data[0]
    if not isinstance(data, dict):
        return {}

    # Try multiple nests for name fields
    first = (_get_nested(data, ["firstName"])
             or _get_nested(data, ["player", "firstName"])
             or _get_nested(data, ["resource", "firstName"])
             or _get_nested(data, ["metadata", "firstName"]))
    last  = (_get_nested(data, ["lastName"])
             or _get_nested(data, ["player", "lastName"])
             or _get_nested(data, ["resource", "lastName"])
             or _get_nested(data, ["metadata", "lastName"]))
    nick  = (_get_nested(data, ["nickname"])
             or _get_nested(data, ["player", "nickname"])
             or _get_nested(data, ["resource", "nickname"])
             or _get_nested(data, ["metadata", "nickname"]))

    name = pick_name(nick, first, last)

    # Position mapping
    pos_raw = data.get("position") or data.get("primaryPositionId")
    position = POSITION_MAP.get(pos_raw) if isinstance(pos_raw, int) else pos_raw

    alt_ids = data.get("alternativePositionIds") or []
    alt_list: List[str] = []
    if isinstance(alt_ids, list):
        for aid in alt_ids:
            if isinstance(aid, int):
                label = POSITION_MAP.get(aid) or str(aid)
                if label and label not in alt_list:
                    alt_list.append(label)
    altposition = ",".join(alt_list) if alt_list else None

    # club/league/nation (tolerant)
    def name_of(x):
        if isinstance(x, dict):
            return x.get("name") or x.get("displayName")
        return None
    club   = name_of(data.get("club"))   or name_of(data.get("uniqueClubSlug"))   or data.get("clubName")
    league = name_of(data.get("league")) or name_of(data.get("uniqueLeagueSlug")) or data.get("leagueName")
    nation = name_of(data.get("nation")) or name_of(data.get("uniqueNationSlug")) or data.get("nationName")

    try:
        rating = int(data.get("overall"))
    except Exception:
        rating = None

    version = data.get("version") or data.get("cardType") or data.get("rarityName")
    if isinstance(version, dict):
        version = version.get("name")

    image_url = build_image_url(data.get("cardImagePath"))

    return {
        "name": name,
        "nickname": nick,
        "first_name": first,
        "last_name": last,
        "rating": rating,
        "version": version,
        "position": position,
        "club": club,
        "league": league,
        "nation": nation,
        "image_url": image_url,
        "altposition": altposition,
        "player_slug": None,
        "player_url": None,
    }

# ================== DISCOVERY ================== #
# Examples we'll match in listing HTML:
#   href="/players/haaland-26-231443" or "/players/231443/erling-haaland/"
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/26-(\d+)', re.I)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:26-)?(\d+)', re.I)

async def http_get_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None

async def discover_cards(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
    """
    Scrape the listing pages for recent/new players, returning {card_id: slug_or_None}.
    """
    out: Dict[str, Optional[str]] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(url: str):
        async with sem:
            html = await http_get_text(session, url)
            if not html:
                return
            before = len(out)
            text = unescape(html)
            for m in RX_HREF_SLUG_CARD.finditer(text):
                out[m.group(2)] = m.group(1)
            for m in RX_HREF_CARD_ONLY.finditer(text):
                out.setdefault(m.group(1), None)
            log.info("🔎 %s: +%d cards (total %d)", url, len(out) - before, len(out))

    await asyncio.gather(*(fetch_page(base.format(p)) for base in LISTING_URLS for p in range(1, pages + 1)))
    return out

# ================== BULK ENRICHMENT ================== #
async def get_all_card_ids(conn: asyncpg.Connection) -> List[str]:
    rows = await conn.fetch("SELECT card_id FROM fut_players")
    return [r["card_id"] for r in rows]

async def bulk_enrich_all(conn: asyncpg.Connection) -> None:
    """
    For all card_ids present in fut_players, call FUT.GG meta API and update columns.
    This function now writes first_name, last_name, nickname when those columns exist.
    """
    ids = await get_all_card_ids(conn)
    if not ids:
        return
    total, processed = len(ids), 0
    write_conn = await asyncpg.connect(DATABASE_URL)

    # Build dynamic UPDATE list
    sets = [
        "position    = COALESCE($1, position)",
        "club        = COALESCE($2, club)",
        "nation      = COALESCE($3, nation)",
        "league      = COALESCE($4, league)",
        "name        = COALESCE($5, name)",
        "rating      = COALESCE($6, rating)",
        "version     = COALESCE($7, version)",
        "image_url   = COALESCE($8, image_url)",
    ]
    args_base = 8
    if HAS_ALT:
        sets.append(f"altposition = COALESCE(${args_base + 1}, altposition)")
        args_base += 1
    if HAS_FIRST:
        sets.append(f"first_name = COALESCE(${args_base + 1}, first_name)")
        args_base += 1
    if HAS_LAST:
        sets.append(f"last_name = COALESCE(${args_base + 1}, last_name)")
        args_base += 1
    if HAS_NICK:
        sets.append(f"nickname = COALESCE(${args_base + 1}, nickname)")
        args_base += 1

    # card_id is final param
    card_id_pos = args_base + 1

    sql = (
        "UPDATE fut_players SET\n  " + ",\n  ".join(sets) +
        ",\n  created_at = COALESCE(created_at, NOW() AT TIME ZONE 'UTC')\n" +
        f"WHERE card_id = ${card_id_pos}"
    )

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, BULK_BATCH_SIZE):
            batch = ids[i:i + BULK_BATCH_SIZE]
            results = await asyncio.gather(*(fetch_meta_with_retry(session, cid) for cid in batch))

            updates: List[Tuple[Any, ...]] = []
            for cid, meta in zip(batch, results):
                if not meta:
                    continue
                args: List[Any] = [
                    meta.get("position"),
                    meta.get("club"),
                    meta.get("nation"),
                    meta.get("league"),
                    meta.get("name"),
                    meta.get("rating"),
                    meta.get("version"),
                    meta.get("image_url"),
                ]
                if HAS_ALT:
                    args.append(meta.get("altposition"))
                if HAS_FIRST:
                    args.append(meta.get("first_name"))
                if HAS_LAST:
                    args.append(meta.get("last_name"))
                if HAS_NICK:
                    args.append(meta.get("nickname"))
                args.append(cid)  # WHERE card_id = $N
                updates.append(tuple(args))

            if updates:
                await write_conn.executemany(sql, updates)
            processed += len(updates)
            log.info(f"💾 Updated {processed}/{total}")
            if i + BULK_BATCH_SIZE < total:
                await asyncio.sleep(BATCH_DELAY)

    await write_conn.close()

# ================== ONE CYCLE ================== #
async def run_once():
    conn = await get_db()
    try:
        await preflight(conn)
        async with aiohttp.ClientSession() as session:
            _ = await discover_cards(session, NEW_PAGES)  # currently unused, keep for future
        await bulk_enrich_all(conn)
    finally:
        await conn.close()

# ================== HEALTH & SCHEDULER ================== #
async def start_health():
    app = web.Application()
    app.add_routes([
        web.get("/",       lambda _: web.Response(text="OK")),
        web.get("/health", lambda _: web.Response(text="OK")),
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8080")))
    await site.start()
    return asyncio.create_task(asyncio.Event().wait())

shutdown_evt = asyncio.Event()
def _sig():
    shutdown_evt.set()

async def sleep_until_19_uk():
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    target = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    log.info("🕖 Next run %s", target.isoformat())
    await asyncio.sleep((target - now).total_seconds())

async def main_loop():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _sig)
        except Exception:
            pass
    health = None
    if os.getenv("PORT"):
        health = await start_health()
    while not shutdown_evt.is_set():
        await run_once()
        await sleep_until_19_uk()
    if health:
        health.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run a single cycle now")
    ap.add_argument("--bulk-only", action="store_true", help="Only run bulk enrich (no discovery)")
    args = ap.parse_args()

    async def _runner():
        if args.bulk_only:
            conn = await get_db()
            await preflight(conn)
            await bulk_enrich_all(conn)
            await conn.close()
        elif args.now:
            await run_once()
        else:
            await main_loop()

    asyncio.run(_runner())
