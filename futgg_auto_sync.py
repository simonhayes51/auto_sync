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
FULL_LISTING_URL = "https://www.fut.gg/players/?page={}"  # full catalog

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))             # pages per listing group (daily)
FULL_PAGES         = int(os.getenv("FULL_PAGES", "335"))          # pages for one-off full build
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))      # seconds
CONCURRENCY        = int(os.getenv("CONCURRENCY", "12"))          # HTTP concurrency (kept modest)
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))

# Rate limiting for bulk processing
RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", "0.1"))   # Delay between requests (100ms)
BATCH_DELAY        = float(os.getenv("BATCH_DELAY", "5.0"))        # Delay between batches (5s)
BULK_BATCH_SIZE    = int(os.getenv("BULK_BATCH_SIZE", "50"))       # Cards per batch
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", "3"))            # Retries per request

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
    Build the complete image URL from the cardImagePath (as returned by API).
    """
    if not card_image_path or not isinstance(card_image_path, str):
        return None
    path = card_image_path.strip()
    if not path:
        return None
    # Using the path as-is (you confirmed this works in your env)
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

async def fetch_meta_with_retry(session: aiohttp.ClientSession, card_id: str) -> dict:
    """
    Fetch metadata with retry logic and rate limiting
    """
    api_headers = {
        "User-Agent": UA_HEADERS["User-Agent"],
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.fut.gg/players/",
        "Origin": "https://www.fut.gg",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            async with session.get(META_API.format(card_id), timeout=REQUEST_TIMEOUT, headers=api_headers) as resp:
                if resp.status == 429:
                    wait_time = min(2 ** attempt, 30)
                    log.warning("Rate limited for card %s, waiting %ss", card_id, wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                elif resp.status != 200:
                    log.warning("META %s → HTTP %s (try %d)", card_id, resp.status, attempt + 1)
                    await asyncio.sleep(0.2 * (attempt + 1))
                    continue
                raw = await resp.json()
                return parse_player_data(raw, card_id)
        except asyncio.TimeoutError:
            log.warning("Timeout on attempt %d for card %s", attempt + 1, card_id)
        except Exception as e:
            log.warning("Exception on attempt %d for card %s: %s", attempt + 1, card_id, e)
    return {}

def parse_player_data(raw: dict, card_id: str) -> dict:
    """
    Parse player data from API response
    """
    data = raw
    if isinstance(raw, dict) and "data" in raw:
        data = raw["data"]

    if isinstance(data, list):
        best, best_rating = None, -1
        for it in data:
            r = it.get("overall") or it.get("rating") or it.get("ovr")
            try: r = int(r)
            except Exception: r = -1
            if r > best_rating:
                best_rating, best = r, it
        data = best or (data[0] if data else {})

    if not isinstance(data, dict):
        return {}

    first = data.get("firstName")
    last  = data.get("lastName")
    nick  = data.get("nickname")
    name  = pick_name(nick, first, last)

    pos_raw = data.get("position") or data.get("positionId") or data.get("primaryPositionId") or (data.get("meta") or {}).get("position")
    if isinstance(pos_raw, int):
        position = POSITION_MAP.get(pos_raw) or str(pos_raw)
    else:
        position = pos_raw if isinstance(pos_raw, str) and pos_raw.strip() else None

    def _lbl(block):
        if isinstance(block, dict):
            v = block.get("name") or block.get("slug")
            return v.strip() if isinstance(v, str) and v.strip() else None
        if isinstance(block, str):
            return block.strip() or None
        return None

    club   = _lbl(data.get("club")) or _lbl(data.get("uniqueClubSlug")) or _lbl(data.get("team"))
    league = _lbl(data.get("league")) or _lbl(data.get("uniqueLeagueSlug"))
    nation = _lbl(data.get("nation")) or _lbl(data.get("uniqueNationSlug")) or _lbl(data.get("country"))

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

    return {
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

# ================== DISCOVERY ================== #
# e.g. href="/players/256343-robson-bambu/25-50587991/"
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/25-(\d+)[/\'\"]', re.IGNORECASE)
# fallback: /players/25-50587991/ (no slug)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:25-)?(\d+)[/\'\"]', re.IGNORECASE)

async def discover_cards(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
    """
    Daily: discover recent cards from /players/new and /players?sort=new
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

async def discover_full_catalog(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
    """
    One-off: discover the full catalog from /players?page=1..pages
    """
    out: Dict[str, Optional[str]] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(p: int):
        url = FULL_LISTING_URL.format(p)
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
            for m in RX_HREF_CARD_ONLY.finditer(doc):
                cid = m.group(1)
                out.setdefault(cid, None)

            log.info("📚 %s: +%d cards (total %d)", url, len(out) - before, len(out))

    tasks = [fetch_page(p) for p in range(1, pages + 1)]
    await asyncio.gather(*tasks, return_exceptions=True)
    return out

# ================== BULK ENRICHMENT ================== #
async def get_all_card_ids(conn: asyncpg.Connection) -> List[str]:
    rows = await conn.fetch("SELECT card_id FROM public.fut_players ORDER BY card_id")
    card_ids = [r["card_id"] for r in rows]
    log.info("📊 Found %d total players in database", len(card_ids))
    return card_ids

async def bulk_enrich_all(conn: asyncpg.Connection) -> None:
    """
    Enrich ALL players in the database with rate limiting
    """
    card_ids = await get_all_card_ids(conn)
    total_cards = len(card_ids)
    if total_cards == 0:
        log.info("No cards found to enrich")
        return

    write_conn = await asyncpg.connect(DATABASE_URL)

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

    processed = failed = 0

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, connect=REQUEST_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
        ) as session:
            for i in range(0, total_cards, BULK_BATCH_SIZE):
                batch = card_ids[i:i + BULK_BATCH_SIZE]
                batch_num = (i // BULK_BATCH_SIZE) + 1
                total_batches = (total_cards + BULK_BATCH_SIZE - 1) // BULK_BATCH_SIZE
                log.info("🔄 Processing batch %d/%d (%d cards)", batch_num, total_batches, len(batch))

                sem = asyncio.Semaphore(CONCURRENCY)

                async def process_card(card_id: str):
                    async with sem:
                        return card_id, await fetch_meta_with_retry(session, card_id)

                results = await asyncio.gather(
                    *[process_card(cid) for cid in batch],
                    return_exceptions=True
                )

                updates, ok, bad = [], 0, 0
                for result in results:
                    if isinstance(result, Exception):
                        bad += 1
                        continue
                    cid, meta = result
                    if not meta:
                        bad += 1
                        continue

                    args = [
                        meta.get("position"),
                        meta.get("club"),
                        meta.get("nation"),
                        meta.get("league"),
                        meta.get("name"),
                        meta.get("rating"),
                        meta.get("version"),
                        meta.get("image_url"),
                        meta.get("player_slug"),  # unchanged (we keep existing)
                        meta.get("player_url"),
                    ]
                    if HAS_NICK:  args.append(meta.get("nickname"))
                    if HAS_FIRST: args.append(meta.get("first_name"))
                    if HAS_LAST:  args.append(meta.get("last_name"))
                    args.append(cid)
                    updates.append(tuple(args))
                    ok += 1

                if updates:
                    try:
                        await write_conn.executemany(sql, updates)
                        log.info("💾 Updated %d records in batch %d", len(updates), batch_num)
                    except Exception as e:
                        log.error("❌ Database update failed for batch %d: %s", batch_num, e)
                        bad += len(updates)
                        ok -= len(updates)

                processed += ok
                failed += bad
                progress = (processed + failed) / total_cards * 100
                log.info("📈 Progress: %d/%d processed (%.1f%%), %d failed", processed, total_cards, progress, failed)

                if i + BULK_BATCH_SIZE < total_cards:
                    log.info("⏳ Waiting %.1fs before next batch…", BATCH_DELAY)
                    await asyncio.sleep(BATCH_DELAY)

    finally:
        await write_conn.close()

    log.info("✅ Bulk enrichment complete: %d updated, %d failed out of %d total", processed, failed, total_cards)

# ================== UPSERT ================== #
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

# ================== RUN MODES ================== #
async def run_once_daily() -> None:
    """Normal daily run: discover 'new' pages, upsert, then bulk enrich."""
    conn = await get_db()
    try:
        await preflight(conn)
        discovered = {}
        try:
            async with aiohttp.ClientSession() as session:
                discovered = await discover_cards(session, NEW_PAGES)
        except Exception as e:
            log.error("❌ Discovery failed: %s", e)

        try:
            added = await upsert_new(conn, discovered)
            if added:
                log.info("🆕 Inserted %d new players.", len(added))
            else:
                log.info("ℹ️ No new players to insert.")
        except Exception as e:
            log.error("❌ Upsert failed: %s", e)

        try:
            log.info("🔄 Starting bulk enrichment of all players…")
            await bulk_enrich_all(conn)
        except Exception as e:
            log.error("❌ Bulk enrichment failed: %s", e)
    finally:
        await conn.close()

async def run_full_build() -> None:
    """One-off full build: scan all catalog pages, upsert everything, then bulk enrich."""
    conn = await get_db()
    try:
        await preflight(conn)
        discovered = {}
        try:
            async with aiohttp.ClientSession() as session:
                log.info("📚 Starting full catalog discovery over %d pages…", FULL_PAGES)
                discovered = await discover_full_catalog(session, FULL_PAGES)
        except Exception as e:
            log.error("❌ Full discovery failed: %s", e)

        try:
            added = await upsert_new(conn, discovered)
            log.info("🆕 Upsert complete. New rows inserted: %d", len(added))
        except Exception as e:
            log.error("❌ Upsert (full) failed: %s", e)

        try:
            log.info("🔄 Starting bulk enrichment of all players…")
            await bulk_enrich_all(conn)
        except Exception as e:
            log.error("❌ Bulk enrichment failed: %s", e)
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
    log.info("🕖 Next bulk enrichment scheduled for %s", target.isoformat())
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
        await run_once_daily()
        log.info("✅ Cycle complete")
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run daily discovery+bulk enrichment once and exit")
    ap.add_argument("--full", action="store_true", help="Run one-off full catalog build (335 pages by default)")
    ap.add_argument("--bulk-only", action="store_true", help="Skip discovery, only do bulk enrichment")
    args = ap.parse_args()

    async def _runner():
        if args.bulk_only:
            conn = await get_db()
            try:
                await preflight(conn)
                await bulk_enrich_all(conn)
            finally:
                await conn.close()
        elif args.full:
            await run_full_build()
        elif args.now:
            await run_once_daily()
        else:
            await main_loop()

    asyncio.run(_runner())