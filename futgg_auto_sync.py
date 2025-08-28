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
from typing import Optional, Dict, List, Set
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

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))            # pages per listing group
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))     # seconds
CONCURRENCY        = int(os.getenv("CONCURRENCY", "24"))         # HTTP concurrency
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/3.1)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))

# optional columns (auto-detected)
HAS_NICK = False
HAS_FIRST = False
HAS_LAST = False

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
# allow overrides via env
try:
    override = json.loads(os.getenv("POSITION_MAP_JSON", "{}"))
    if isinstance(override, dict) and override:
        POSITION_MAP.update({int(k): str(v) for k, v in override.items()})
except Exception:
    pass

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
    global HAS_NICK, HAS_FIRST, HAS_LAST
    # table exists?
    tbl = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not tbl:
        raise RuntimeError("Table public.fut_players not found")
    # unique index on card_id?
    idx = await conn.fetchval("""
        SELECT indexname FROM pg_indexes
        WHERE schemaname='public' AND tablename='fut_players'
          AND indexname='fut_players_card_id_key'
    """)
    if not idx:
        raise RuntimeError("Unique index fut_players_card_id_key on (card_id) is required")
    # detect optional columns
    cols = {
        r["column_name"]
        for r in await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
        """)
    }
    HAS_NICK  = "nickname"  in cols
    HAS_FIRST = "first_name" in cols
    HAS_LAST  = "last_name"  in cols
    log.info("✅ Preflight OK | columns: nickname=%s first=%s last=%s", HAS_NICK, HAS_FIRST, HAS_LAST)

# ================== HELPERS ================== #
def build_image_url(card_image_path: Optional[str]) -> Optional[str]:
    """
    FIXED: Updated to use correct prefix and no path modification
    "2025/player-item-card/25-158023.775a828c071b30324a22919f78f5ca0434c29764be2742a86ce96d36b2e12dca.webp"
    -> "https://game-assets.fut.gg/cdn-cgi/image/quality=100,format=auto,width=500/2025/player-item-card/25-158023.775a828c071b30324a22919f78f5ca0434c29764be2742a86ce96d36b2e12dca.webp"
    """
    if not card_image_path or not isinstance(card_image_path, str):
        return None
    path = card_image_path.strip()
    if not path:
        return None
    # FIXED: Use quality=100 and don't modify the path
    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=100,format=auto,width=500/{path}"

def pick_name(nick, first, last) -> Optional[str]:
    if isinstance(nick, str) and nick.strip():
        return nick.strip()
    if isinstance(first, str) and first.strip() and isinstance(last, str) and last.strip():
        return f"{first.strip()} {last.strip()}"
    if isinstance(first, str) and first.strip():
        return first.strip()
    if isinstance(last, str) and last.strip():
        return last.strip()
    return None

# ================== HTTP ================== #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None

async def fetch_meta(session: aiohttp.ClientSession, card_id: str) -> dict:
    """
    Returns normalized dict of fields we need.
    Handles list payload by picking the highest 'overall'.
    """
    try:
        async with session.get(META_API.format(card_id), timeout=REQUEST_TIMEOUT,
                               headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                log.warning(f"API returned status {resp.status} for card {card_id}")
                return {}
            raw = await resp.json()
            log.info(f"Got API response for card {card_id}: {type(raw)} with {len(str(raw))} chars")
    except Exception as e:
        log.error(f"API request failed for card {card_id}: {e}")
        return {}

    # choose best item
    data = raw
    if isinstance(raw, list):
        best, best_rating = None, -1
        for it in raw:
            r = it.get("overall") or it.get("rating") or it.get("ovr")
            try: r = int(r)
            except Exception: r = -1
            if r > best_rating:
                best_rating, best = r, it
        data = best or (raw[0] if raw else {})

    if not isinstance(data, dict):
        log.warning(f"Final data is not a dict for card {card_id}: {type(data)}")
        return {}

    # Log available keys for debugging
    available_keys = list(data.keys())
    log.info(f"Available keys for card {card_id}: {available_keys[:10]}")

    first = data.get("firstName")
    last  = data.get("lastName")
    nick  = data.get("nickname")
    name  = pick_name(nick, first, last)
    
    log.info(f"Name extraction for {card_id}: first='{first}', last='{last}', nick='{nick}', final='{name}'")

    # position
    pos_raw = data.get("position") or data.get("positionId") or data.get("primaryPositionId") or (data.get("meta") or {}).get("position")
    if isinstance(pos_raw, int):
        position = POSITION_MAP.get(pos_raw) or str(pos_raw)
    else:
        position = pos_raw if isinstance(pos_raw, str) and pos_raw.strip() else None

    # club/league/nation - FIXED: Check nested objects first
    def _lbl(block):
        if isinstance(block, dict):
            v = block.get("name") or block.get("slug")
            return v.strip() if isinstance(v, str) and v.strip() else None
        if isinstance(block, str):
            return block.strip() or None
        return None

    # FIXED: Try nested objects first, then fallback to original logic
    club   = _lbl(data.get("club")) or _lbl(data.get("uniqueClubSlug")) or _lbl(data.get("team"))
    league = _lbl(data.get("league")) or _lbl(data.get("uniqueLeagueSlug"))
    nation = _lbl(data.get("nation")) or _lbl(data.get("uniqueNationSlug")) or _lbl(data.get("country"))
    
    log.info(f"Club/League/Nation for {card_id}: club='{club}', league='{league}', nation='{nation}'")

    rating = None
    try:
        rv = data.get("overall") or data.get("rating") or data.get("overallRating") or data.get("ovr")
        rating = int(rv) if rv is not None else None
    except Exception:
        rating = None

    version = data.get("version") or data.get("cardType") or data.get("rarity") or data.get("program") or data.get("rarityName")
    if isinstance(version, dict):
        version = version.get("name") or version.get("label") or version.get("code")
    if isinstance(version, str):
        version = version.strip() or None
    else:
        version = None

    image_url = build_image_url(data.get("cardImagePath"))

    result = {
        "name": name,
        "nickname": nick.strip() if isinstance(nick, str) and nick.strip() else None,
        "first_name": first.strip() if isinstance(first, str) and first.strip() else None,
        "last_name":  last.strip()  if isinstance(last,  str) and last.strip()  else None,
        "rating": rating,
        "version": version,
        "position": position,
        "club": club,
        "league": league,
        "nation": nation,
        "image_url": image_url,
    }
    
    # Debug: Count non-null fields
    filled_fields = sum(1 for v in result.values() if v is not None)
    log.info(f"Card {card_id} extracted {filled_fields}/11 fields: {result}")
    
    return result

# ================== DISCOVERY ================== #
# e.g. href="/players/256343-robson-bambu/25-50587991/"
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/25-(\d+)[/\'\"]', re.IGNORECASE)
# fallback: /players/25-50587991/ (no slug)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:25-)?(\d+)[/\'\"]', re.IGNORECASE)

async def discover_cards(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
    """
    Returns dict: { card_id (str) : player_slug (str|None) }
    """
    out: Dict[str, Optional[str]] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(url: str):
        async with sem:
            html = await http_get_text(session, url)
            if not html:
                log.info("🔍 %s: no HTML", url)
                return
            doc = unescape(html)
            before = len(out)

            for m in RX_HREF_SLUG_CARD.finditer(doc):
                slug, cid = m.group(1), m.group(2)
                out[cid] = slug

            # don't overwrite existing slug with None
            for m in RX_HREF_CARD_ONLY.finditer(doc):
                cid = m.group(1)
                out.setdefault(cid, None)

            log.info("🔎 %s: +%d cards (total %d)", url, len(out) - before, len(out))

    tasks = []
    for base in LISTING_URLS:
        for p in range(1, pages + 1):
            tasks.append(fetch_page(base.format(p)))
    await asyncio.gather(*tasks, return_exceptions=True)
    return out

# ================== UPSERT & ENRICH ================== #
async def upsert_new(conn: asyncpg.Connection, discovered: Dict[str, Optional[str]]) -> List[str]:
    """Insert missing card_ids; return the list of newly inserted card_ids."""
    if not discovered:
        return []
    ids = list(discovered.keys())

    have_rows = await conn.fetch(
        "SELECT card_id FROM public.fut_players WHERE card_id = ANY($1::text[])",
        ids
    )
    have = {r["card_id"] for r in have_rows}
    new_ids = [cid for cid in ids if cid not in have]
    if not new_ids:
        return []

    rows = []
    for cid in new_ids:
        slug = discovered.get(cid)
        url = f"https://www.fut.gg/players/{slug}/25-{cid}/" if slug else f"https://www.fut.gg/players/25-{cid}/"
        rows.append((cid, slug, url))

    await conn.executemany(
        """
        INSERT INTO public.fut_players (card_id, player_slug, player_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (card_id) DO NOTHING
        """,
        rows
    )
    return new_ids

async def enrich_new(conn: asyncpg.Connection, card_ids: List[str]) -> None:
    """Fetch meta for the just-inserted card_ids and update by card_id (single writer)."""
    if not card_ids:
        return

    # read existing slug/url for these, to avoid overwriting a known slug with None
    rows = await conn.fetch(
        "SELECT card_id, player_slug, player_url FROM public.fut_players WHERE card_id = ANY($1::text[])",
        card_ids
    )
    info = {r["card_id"]: (r["player_slug"], r["player_url"]) for r in rows}

    write_conn = await asyncpg.connect(DATABASE_URL)
    sem = asyncio.Semaphore(CONCURRENCY)
    queue: asyncio.Queue = asyncio.Queue()
    STOP = object()
    written = 0

    # Build dynamic UPDATE that keys by card_id and only fills NULLs (COALESCE)
    # FIXED: Preserve created_at timestamp
    sql = """
        UPDATE public.fut_players
        SET position    = COALESCE($1, position),
            club        = COALESCE($2, club),
            nation      = COALESCE($3, nation),
            league      = COALESCE($4, league),
            name        = COALESCE($5, name),
            rating      = COALESCE($6, rating),
            version     = COALESCE($7, version),
            image_url   = COALESCE($8, image_url),
            player_slug = COALESCE($9, player_slug),
            player_url  = COALESCE($10, player_url),
            created_at  = COALESCE(created_at, NOW() AT TIME ZONE 'UTC')
    """
    arg_count = 10
    if HAS_NICK:
        sql += ", nickname = COALESCE($11, nickname)"
        arg_count += 1
    if HAS_FIRST:
        sql += ", first_name = COALESCE($%d, first_name)" % (arg_count + 1)
        arg_count += 1
    if HAS_LAST:
        sql += ", last_name = COALESCE($%d, last_name)" % (arg_count + 1)
        arg_count += 1
    sql += " WHERE card_id = $%d" % (arg_count + 1)

    async def write_chunk(chunk: list):
        for attempt in range(3):
            try:
                await write_conn.executemany(sql, chunk)
                return
            except Exception as e:
                if "another operation is in progress" in str(e).lower() and attempt < 2:
                    await asyncio.sleep(0.05 * (attempt + 1))
                    continue
                log.error("❌ DB update failed for chunk of %d: %s", len(chunk), e)
                return

    async def writer():
        nonlocal written
        batch = []
        while True:
            item = await queue.get()
            if item is STOP:
                if batch:
                    await write_chunk(batch)
                    written += len(batch)
                    log.info("💾 Enriched chunk of %d (running total %d)", len(batch), written)
                break
            batch.append(item)
            if len(batch) >= UPDATE_CHUNK_SIZE:
                to_write = batch
                batch = []
                await write_chunk(to_write)
                written += len(to_write)
                log.info("💾 Enriched chunk of %d (running total %d)", len(to_write), written)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, connect=REQUEST_TIMEOUT),
        connector=aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
    ) as session:

        async def produce(cid: str):
            slug_existing, url_existing = info.get(cid, (None, None))
            async with sem:
                meta = await fetch_meta(session, cid)

            # prefer existing slug/url if present
            slug = slug_existing
            url  = url_existing or (f"https://www.fut.gg/players/{slug}/25-{cid}/" if slug else f"https://www.fut.gg/players/25-{cid}/")

            args = [
                meta.get("position"),
                meta.get("club"),
                meta.get("nation"),
                meta.get("league"),
                meta.get("name"),
                meta.get("rating"),
                meta.get("version"),
                meta.get("image_url"),
                slug,
                url,
            ]
            if HAS_NICK:  args.append(meta.get("nickname"))
            if HAS_FIRST: args.append(meta.get("first_name"))
            if HAS_LAST:  args.append(meta.get("last_name"))
            args.append(cid)  # WHERE card_id=...
            await queue.put(tuple(args))

        writer_task = asyncio.create_task(writer())
        await asyncio.gather(*(produce(cid) for cid in card_ids), return_exceptions=True)
        await queue.put(STOP)
        await writer_task

    await write_conn.close()
    log.info("✅ Enrichment done. Wrote %d row(s).", written)

# ---- One-off enrichment for rows missing data (used right after first run) ---- #
async def enrich_missing_once(conn: asyncpg.Connection, limit: int = 2000) -> None:
    # FIXED: Removed card_id regex filter that was preventing enrichment
    rows = await conn.fetch(
        """
        SELECT card_id
        FROM public.fut_players
        WHERE (
            name IS NULL OR name = '' OR
            rating IS NULL OR
            position IS NULL OR position = '' OR
            club IS NULL OR club = '' OR
            league IS NULL OR league = '' OR
            nation IS NULL OR nation = '' OR
            image_url IS NULL OR image_url = '' OR
            player_slug IS NULL OR player_slug = '' OR
            player_url IS NULL OR player_url = ''
        )
        LIMIT $1
        """,
        limit
    )
    if not rows:
        return
    await enrich_new(conn, [r["card_id"] for r in rows])

# ================== ONE CYCLE ================== #
async def run_once() -> None:
    conn = await get_db()
    try:
        await preflight(conn)

        # 1) Discover
        discovered: Dict[str, Optional[str]] = {}
        try:
            async with aiohttp.ClientSession() as session:
                discovered = await discover_cards(session, NEW_PAGES)
        except Exception as e:
            log.error("❌ Discovery failed: %s", e)

        # 2) Upsert
        added = []
        try:
            added = await upsert_new(conn, discovered)
            if added:
                log.info("🆕 Inserted %d new players.", len(added))
            else:
                log.info("ℹ️ No new players to insert.")
        except Exception as e:
            log.error("❌ Upsert failed: %s", e)

        # 3) Enrich only newly inserted, then one-off fill for any existing empties
        try:
            await enrich_new(conn, added)
        except Exception as e:
            log.error("❌ Enrich(new) failed: %s", e)

        try:
            await enrich_missing_once(conn, limit=2000)
        except Exception as e:
            log.error("❌ Enrich-missing failed: %s", e)

    finally:
        await conn.close()

# ================== HEALTH & SCHEDULER ================== #
async def start_health() -> asyncio.Task:
    async def handle(_):
        return web.Response(text="OK")
    app = web.Application()
    app.add_routes([web.get("/", handle), web.get("/health", handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("🌐 Health server listening on :%d", port)
    return asyncio.create_task(asyncio.Event().wait())

shutdown_evt = asyncio.Event()
def _sig():
    log.info("🛑 Received shutdown signal. Finishing current cycle…")
    shutdown_evt.set()

async def sleep_until_19_uk():
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    target = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    log.info("🕖 Next new-player check scheduled for %s", target.isoformat())
    await asyncio.sleep((target - now).total_seconds())

async def main_loop():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _sig)
        except NotImplementedError:
            pass

    health_task = None
    if os.getenv("PORT"):
        health_task = await start_health()

    while not shutdown_evt.is_set():
        log.info("🚦 Cycle start")
        await run_once()
        log.info("✅ Cycle complete")
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run discovery+insert+enrich once and exit")
    args = ap.parse_args()

    async def _runner():
        if args.now:
            await run_once()
        else:
            await main_loop()

    asyncio.run(_runner())
