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
from typing import Optional, Dict, List, Tuple
from aiohttp import web  # health server

# ================== CONFIG ================== #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

# FC 26 (you’re using /26/ in the response)
GAME = os.getenv("GAME", "26")
META_API = f"https://www.fut.gg/api/fut/player-item-definitions/{GAME}" + "/{}"

LISTING_URLS = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))
CONCURRENCY        = int(os.getenv("CONCURRENCY", "12"))
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))

# Bulk behaviour
RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", "0.1"))   # 100ms between requests
BATCH_DELAY        = float(os.getenv("BATCH_DELAY", "1.5"))        # delay between batches
BULK_BATCH_SIZE    = int(os.getenv("BULK_BATCH_SIZE", "60"))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", "3"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/4.0)",
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
HAS_ALTPOS = False

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
    global HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALTPOS

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

    HAS_NICK   = "nickname" in cols
    HAS_FIRST  = "first_name" in cols
    HAS_LAST   = "last_name" in cols
    HAS_ALTPOS = "altposition" in cols

    log.info(
        "✅ Preflight OK | columns: nickname=%s first=%s last=%s altposition=%s",
        HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALTPOS
    )

# ================== HELPERS ================== #
def build_image_url(path: Optional[str]) -> Optional[str]:
    if not path or not isinstance(path, str):
        return None
    p = path.strip()
    return p and f"https://game-assets.fut.gg/cdn-cgi/image/quality=100,format=auto,width=500/{p}" or None

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

def slug_to_title(slug: str) -> Optional[str]:
    """
    Converts:
      "112139-al-nassr" -> "Al Nassr"
      "350-major-league-soccer" -> "Major League Soccer"
      "38-argentina" -> "Argentina"
    Best-effort only.
    """
    if not isinstance(slug, str):
        return None
    s = slug.strip()
    if not s:
        return None
    # drop numeric prefix if present
    s = re.sub(r"^\d+-", "", s)
    # replace hyphens with spaces
    s = s.replace("-", " ")
    # title case
    return " ".join(w.capitalize() if w else "" for w in s.split()).strip() or None

def get_nameish(value) -> Optional[str]:
    """
    Accepts:
      - dict with {name: "..."} or {label: "..."}
      - string slug
      - string name
    """
    if isinstance(value, dict):
        for k in ("name", "label", "title"):
            v = value.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        # sometimes nested
        if isinstance(value.get("slug"), str) and value["slug"].strip():
            return slug_to_title(value["slug"])
        return None

    if isinstance(value, str) and value.strip():
        # if it's a slug with digits prefix, convert to title
        if re.match(r"^\d+-[a-z0-9\-]+$", value.strip(), re.I):
            return slug_to_title(value.strip())
        return value.strip()

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

def parse_player_data(raw: dict, card_id: str) -> dict:
    # unwrap {data:{...}}
    data = raw.get("data") if isinstance(raw, dict) and "data" in raw else raw

    # Some responses might still be list-like (rare), handle
    if isinstance(data, list) and data:
        def _overall(x):
            try:
                return int(x.get("overall") or x.get("rating") or 0)
            except Exception:
                return 0
        data = max(data, key=_overall)

    if not isinstance(data, dict):
        return {}

    # Pull names
    first = data.get("firstName")
    last = data.get("lastName")
    nick = data.get("nickname")
    name = pick_name(nick, first, last)

    # rating
    rating = None
    for k in ("overall", "rating", "overallRating", "ovr"):
        if data.get(k) is not None:
            try:
                rating = int(data.get(k))
                break
            except Exception:
                pass

    # version/rarity/program
    version = data.get("rarityName") or data.get("version") or data.get("cardType") or data.get("program")
    version = get_nameish(version)

    # position
    pos_raw = data.get("position")
    if pos_raw is None:
        pos_raw = data.get("positionId") if data.get("positionId") is not None else data.get("primaryPositionId")

    position = None
    if isinstance(pos_raw, int):
        position = POSITION_MAP.get(pos_raw) or str(pos_raw)
    elif isinstance(pos_raw, str) and pos_raw.strip():
        position = pos_raw.strip()

    # alt positions
    altposition = None
    alt_ids = data.get("alternativePositionIds") or []
    if isinstance(alt_ids, list) and alt_ids:
        labels = []
        for aid in alt_ids:
            if isinstance(aid, int):
                lab = POSITION_MAP.get(aid) or str(aid)
                if lab not in labels:
                    labels.append(lab)
        altposition = ",".join(labels) if labels else None

    # club / league / nation
    # FUT.GG sometimes gives these as strings (slugs) and sometimes as objects
    club = (
        get_nameish(data.get("uniqueClubName")) or
        get_nameish(data.get("clubName")) or
        get_nameish(data.get("uniqueClubSlug")) or
        get_nameish(data.get("club")) or
        get_nameish(data.get("team"))
    )

    league = (
        get_nameish(data.get("uniqueLeagueName")) or
        get_nameish(data.get("leagueName")) or
        get_nameish(data.get("uniqueLeagueSlug")) or
        get_nameish(data.get("league"))
    )

    nation = (
        get_nameish(data.get("uniqueNationName")) or
        get_nameish(data.get("nationName")) or
        get_nameish(data.get("uniqueNationSlug")) or
        get_nameish(data.get("nation")) or
        get_nameish(data.get("country"))
    )

    image_url = build_image_url(
        data.get("futggCardImagePath") or data.get("cardImagePath") or data.get("imagePath")
    )

    return {
        "name": name,
        "nickname": nick.strip() if isinstance(nick, str) and nick.strip() else None,
        "first_name": first.strip() if isinstance(first, str) and first.strip() else None,
        "last_name": last.strip() if isinstance(last, str) and last.strip() else None,
        "rating": rating,
        "version": version,
        "position": position,
        "altposition": altposition,
        "club": club,
        "league": league,
        "nation": nation,
        "image_url": image_url,
    }

async def fetch_meta_with_retry(session: aiohttp.ClientSession, card_id: str) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(RATE_LIMIT_DELAY)

            async with session.get(
                META_API.format(card_id),
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": UA_HEADERS["User-Agent"]},
            ) as resp:
                if resp.status == 429:
                    wait_time = min(2 ** attempt, 30)
                    log.warning("Rate limited for card %s, waiting %ss", card_id, wait_time)
                    await asyncio.sleep(wait_time)
                    continue

                if resp.status != 200:
                    log.warning("API returned status %s for card %s", resp.status, card_id)
                    return {}

                raw = await resp.json()
                return parse_player_data(raw, card_id)

        except asyncio.TimeoutError:
            log.warning("Timeout on attempt %d for card %s", attempt + 1, card_id)
        except Exception as e:
            log.error("Error on attempt %d for card %s: %s", attempt + 1, card_id, e)

    return {}

# ================== DISCOVERY ================== #
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/' + re.escape(GAME) + r'-(\d+)[/\'\"]', re.IGNORECASE)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:' + re.escape(GAME) + r'-)?(\d+)[/\'\"]', re.IGNORECASE)

async def discover_cards(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
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

# ================== UPSERT ================== #
async def upsert_new(conn: asyncpg.Connection, discovered: Dict[str, Optional[str]]) -> List[str]:
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
        url = f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/" if slug else f"https://www.fut.gg/players/{GAME}-{cid}/"
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

# ================== BULK ENRICH ================== #
async def get_all_card_ids(conn: asyncpg.Connection) -> List[str]:
    rows = await conn.fetch("SELECT card_id FROM public.fut_players ORDER BY card_id")
    card_ids = [r["card_id"] for r in rows]
    log.info("📊 Found %d total players in database", len(card_ids))
    return card_ids

async def bulk_enrich_all(conn: asyncpg.Connection) -> None:
    card_ids = await get_all_card_ids(conn)
    total_cards = len(card_ids)
    if total_cards == 0:
        log.info("No cards found to enrich")
        return

    processed = 0
    failed = 0

    write_conn = await asyncpg.connect(DATABASE_URL)

    # Dynamic UPDATE (COALESCE)
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
    if HAS_ALTPOS:
        sql += ", altposition = COALESCE($%d, altposition)" % (arg_count + 1)
        arg_count += 1

    sql += " WHERE card_id = $%d" % (arg_count + 1)

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

                async def process_card(cid: str) -> Tuple[str, dict]:
                    async with sem:
                        meta = await fetch_meta_with_retry(session, cid)
                        return cid, meta

                batch_results = await asyncio.gather(
                    *[process_card(cid) for cid in batch],
                    return_exceptions=True
                )

                db_updates = []
                batch_processed = 0
                batch_failed = 0

                for result in batch_results:
                    if isinstance(result, Exception):
                        batch_failed += 1
                        continue

                    cid, meta = result
                    if not meta:
                        batch_failed += 1
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
                        None,  # don't overwrite slug
                        None,  # don't overwrite url
                    ]
                    if HAS_NICK:  args.append(meta.get("nickname"))
                    if HAS_FIRST: args.append(meta.get("first_name"))
                    if HAS_LAST:  args.append(meta.get("last_name"))
                    if HAS_ALTPOS: args.append(meta.get("altposition"))

                    args.append(cid)
                    db_updates.append(tuple(args))
                    batch_processed += 1

                if db_updates:
                    try:
                        await write_conn.executemany(sql, db_updates)
                        log.info("💾 Updated %d records in batch %d", len(db_updates), batch_num)
                    except Exception as e:
                        log.error("❌ Database update failed for batch %d: %s", batch_num, e)
                        batch_failed += len(db_updates)
                        batch_processed -= len(db_updates)

                processed += batch_processed
                failed += batch_failed
                progress = (processed + failed) / total_cards * 100
                log.info("📈 Progress: %d/%d (%.1f%%), %d failed", processed, total_cards, progress, failed)

                if i + BULK_BATCH_SIZE < total_cards:
                    await asyncio.sleep(BATCH_DELAY)

    finally:
        await write_conn.close()

    log.info("✅ Bulk enrichment complete: %d updated, %d failed out of %d", processed, failed, total_cards)

# ================== ONE CYCLE ================== #
async def run_once(do_bulk: bool = True) -> None:
    conn = await get_db()
    try:
        await preflight(conn)

        # discovery
        discovered: Dict[str, Optional[str]] = {}
        async with aiohttp.ClientSession() as session:
            discovered = await discover_cards(session, NEW_PAGES)

        # upsert
        added = await upsert_new(conn, discovered)
        if added:
            log.info("🆕 Inserted %d new players.", len(added))
        else:
            log.info("ℹ️ No new players to insert.")

        # bulk enrichment (your “fill everything” step)
        if do_bulk:
            log.info("🔄 Starting bulk enrichment of all players...")
            await bulk_enrich_all(conn)

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
    log.info("🕖 Next run scheduled for %s", target.isoformat())
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
        # daily cycle: discover new + bulk enrich (safe; COALESCE prevents overwrite)
        await run_once(do_bulk=True)
        log.info("✅ Cycle complete")
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run discovery+bulk enrichment once and exit")
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
        elif args.now:
            await run_once(do_bulk=True)
        else:
            await main_loop()

    asyncio.run(_runner())