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
from typing import Optional, Dict, List
from aiohttp import web  # health server

# ================== CONFIG ================== #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

META_API = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
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

# optional columns
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
    global HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT
    tbl = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not tbl:
        raise RuntimeError("Table public.fut_players not found")
    idx = await conn.fetchval("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname='public' AND tablename='fut_players'
          AND indexname='fut_players_card_id_key'
    """)
    if not idx:
        raise RuntimeError("Unique index fut_players_card_id_key on (card_id) is required")

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
    log.info("✅ Preflight OK | columns: nickname=%s first=%s last=%s altposition=%s",
             HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT)

# ================== HELPERS ================== #
def build_image_url(card_image_path: Optional[str]) -> Optional[str]:
    if not card_image_path or not isinstance(card_image_path, str):
        return None
    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=100,format=auto,width=500/{card_image_path.strip()}"

def pick_name(nick, first, last) -> Optional[str]:
    if isinstance(nick, str) and nick.strip():
        return nick.strip()
    if first and last:
        return f"{first.strip()} {last.strip()}"
    return first or last

def name_from_slug(slug: Optional[str]) -> Optional[str]:
    if not slug or not isinstance(slug, str):
        return None
    parts = slug.split("-", 1)
    human = parts[1] if len(parts) > 1 else parts[0]
    human = human.replace("-", " ").strip()
    return " ".join(w.capitalize() for w in human.split()) if human else None

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
                raw = await resp.json()
                return parse_player_data(raw, card_id)
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                log.error(f"Meta fetch failed {card_id}: {e}")
    return {}

def parse_player_data(raw: dict, card_id: str) -> dict:
    data = raw.get("data") if isinstance(raw, dict) and "data" in raw else raw
    if isinstance(data, list) and data:
        data = max(data, key=lambda d: int(d.get("overall") or 0))
    if not isinstance(data, dict):
        return {}

    first, last, nick = data.get("firstName"), data.get("lastName"), data.get("nickname")
    name = pick_name(nick, first, last)

    pos_raw = data.get("position") or data.get("primaryPositionId")
    position = POSITION_MAP.get(pos_raw) if isinstance(pos_raw, int) else pos_raw

    alt_ids = data.get("alternativePositionIds") or []
    alt_list = []
    if isinstance(alt_ids, list):
        for aid in alt_ids:
            if isinstance(aid, int):
                label = POSITION_MAP.get(aid) or str(aid)
                if label and label not in alt_list:
                    alt_list.append(label)
    altposition = ",".join(alt_list) if alt_list else None

    club   = (data.get("club") or {}).get("name") or (data.get("uniqueClubSlug") or {}).get("name")
    league = (data.get("league") or {}).get("name") or (data.get("uniqueLeagueSlug") or {}).get("name")
    nation = (data.get("nation") or {}).get("name") or (data.get("uniqueNationSlug") or {}).get("name")

    try: rating = int(data.get("overall"))
    except: rating = None

    version = data.get("version") or data.get("cardType") or data.get("rarityName")
    if isinstance(version, dict): version = version.get("name")

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
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/25-(\d+)', re.I)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:25-)?(\d+)', re.I)

async def http_get_text(session, url):
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=UA_HEADERS) as resp:
            return await resp.text() if resp.status == 200 else None
    except: return None

async def discover_cards(session, pages: int) -> Dict[str, Optional[str]]:
    out = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(url):
        async with sem:
            html = await http_get_text(session, url)
            if not html: return
            before = len(out)
            for m in RX_HREF_SLUG_CARD.finditer(unescape(html)):
                out[m.group(2)] = m.group(1)
            for m in RX_HREF_CARD_ONLY.finditer(unescape(html)):
                out.setdefault(m.group(1), None)
            log.info("🔎 %s: +%d cards (total %d)", url, len(out)-before, len(out))

    await asyncio.gather(*(fetch_page(base.format(p)) for base in LISTING_URLS for p in range(1, pages+1)))
    return out

# ================== BULK ENRICHMENT ================== #
async def get_all_card_ids(conn): return [r["card_id"] for r in await conn.fetch("SELECT card_id FROM fut_players")]

async def bulk_enrich_all(conn):
    ids = await get_all_card_ids(conn)
    if not ids: return
    total, processed = len(ids), 0
    write_conn = await asyncpg.connect(DATABASE_URL)

    sql = """
        UPDATE fut_players SET
            position    = COALESCE($1, position),
            club        = COALESCE($2, club),
            nation      = COALESCE($3, nation),
            league      = COALESCE($4, league),
            name        = COALESCE($5, name),
            rating      = COALESCE($6, rating),
            version     = COALESCE($7, version),
            image_url   = COALESCE($8, image_url),
    """
    arg_count = 8
    if HAS_ALT:
        sql += "    altposition = COALESCE($9, altposition),\n"
        arg_count += 1
    sql += f"""    created_at  = COALESCE(created_at, NOW() AT TIME ZONE 'UTC')
        WHERE card_id = ${arg_count+1}"""

    async with aiohttp.ClientSession() as session:
        for i in range(0, total, BULK_BATCH_SIZE):
            batch = ids[i:i+BULK_BATCH_SIZE]
            results = await asyncio.gather(*(fetch_meta_with_retry(session, cid) for cid in batch))
            updates=[]
            for cid, meta in zip(batch, results):
                if not meta: continue
                args=[meta["position"],meta["club"],meta["nation"],meta["league"],meta["name"],
                      meta["rating"],meta["version"],meta["image_url"]]
                if HAS_ALT: args.append(meta["altposition"])
                args.append(cid)
                updates.append(tuple(args))
            if updates: await write_conn.executemany(sql, updates)
            processed+=len(updates)
            log.info(f"💾 Updated {processed}/{total}")
            if i+BULK_BATCH_SIZE<total: await asyncio.sleep(BATCH_DELAY)
    await write_conn.close()

# ================== ONE CYCLE ================== #
async def run_once():
    conn = await get_db()
    try:
        await preflight(conn)
        async with aiohttp.ClientSession() as session:
            discovered = await discover_cards(session, NEW_PAGES)
        await bulk_enrich_all(conn)
    finally: await conn.close()

# ================== HEALTH & SCHEDULER ================== #
async def start_health():
    app=web.Application(); app.add_routes([web.get("/",lambda _:web.Response(text="OK")),
                                           web.get("/health",lambda _:web.Response(text="OK"))])
    runner=web.AppRunner(app); await runner.setup()
    site=web.TCPSite(runner,"0.0.0.0",int(os.getenv("PORT","8080"))); await site.start()
    return asyncio.create_task(asyncio.Event().wait())

shutdown_evt=asyncio.Event()
def _sig(): shutdown_evt.set()

async def sleep_until_19_uk():
    tz=ZoneInfo("Europe/London"); now=datetime.now(tz)
    target=now.replace(hour=19,minute=0,second=0,microsecond=0)
    if now>=target: target+=timedelta(days=1)
    log.info("🕖 Next run %s", target.isoformat())
    await asyncio.sleep((target-now).total_seconds())

async def main_loop():
    loop=asyncio.get_running_loop()
    for s in (signal.SIGTERM,signal.SIGINT):
        try: loop.add_signal_handler(s,_sig)
        except: pass
    health=None
    if os.getenv("PORT"): health=await start_health()
    while not shutdown_evt.is_set():
        await run_once(); await sleep_until_19_uk()
    if health: health.cancel()

# ================== CLI ================== #
if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--now",action="store_true")
    ap.add_argument("--bulk-only",action="store_true")
    args=ap.parse_args()
    async def _runner():
        if args.bulk_only:
            conn=await get_db(); await preflight(conn); await bulk_enrich_all(conn); await conn.close()
        elif args.now: await run_once()
        else: await main_loop()
    asyncio.run(_runner())