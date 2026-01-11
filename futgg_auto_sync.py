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

# FUT.GG "game" code (you showed examples for 26)
GAME = os.getenv("GAME", "26").strip()

META_API = f"https://www.fut.gg/api/fut/player-item-definitions/{GAME}/{{}}"

# Daily discovery sources (newest cards)
NEW_LISTING_URLS = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

# Full one-off build source (all players pages)
FULL_LISTING_URLS = [
    "https://www.fut.gg/players/?page={}",
]

# How many pages to scan
NEW_PAGES  = int(os.getenv("NEW_PAGES", "5"))         # daily scan pages
FULL_PAGES = int(os.getenv("FULL_PAGES", "335"))      # one-off full build pages

REQUEST_TIMEOUT   = int(os.getenv("REQUEST_TIMEOUT", "20"))
CONCURRENCY       = int(os.getenv("CONCURRENCY", "12"))
DISCOVERY_CONC    = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE = int(os.getenv("UPDATE_CHUNK_SIZE", "250"))

# Rate limiting / politeness
RATE_LIMIT_DELAY = float(os.getenv("RATE_LIMIT_DELAY", "0.05"))  # delay per request
BATCH_DELAY      = float(os.getenv("BATCH_DELAY", "1.0"))         # delay between batches
BULK_BATCH_SIZE  = int(os.getenv("BULK_BATCH_SIZE", "80"))        # ids per batch
MAX_RETRIES      = int(os.getenv("MAX_RETRIES", "3"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/4.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/",
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "250"))

# optional columns (auto-detected)
HAS_NICK = False
HAS_FIRST = False
HAS_LAST = False
HAS_ALT = False  # altposition

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
        key = str(rec.msg)
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
print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={LOG_LEVEL} | GAME={GAME}", flush=True)

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
    HAS_NICK  = "nickname" in cols
    HAS_FIRST = "first_name" in cols
    HAS_LAST  = "last_name" in cols
    HAS_ALT   = "altposition" in cols

    # Confirm card_id type (warn only)
    card_id_type = await conn.fetchval("""
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fut_players' AND column_name='card_id'
    """)
    if card_id_type and card_id_type.lower() not in ("bigint", "integer"):
        log.warning("⚠️ card_id column type is %s (expected BIGINT). Script assumes BIGINT/INT.", card_id_type)

    log.info("✅ Preflight OK | columns: nickname=%s first=%s last=%s altposition=%s", HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT)

# ================== HELPERS ================== #
def build_image_url(path: Optional[str]) -> Optional[str]:
    """
    FUT.GG API now often returns:
      - futggCardImagePath: "2026/futgg-player-item-card/26-20801.<hash>.webp"
      - cardImagePath:      "2026/player-item-card/26-20801.<hash>.webp"
    We'll accept any relative path and prefix it.
    """
    if not path or not isinstance(path, str):
        return None
    p = path.strip()
    if not p:
        return None
    # If it's a full URL already, return as-is
    if p.startswith("http://") or p.startswith("https://"):
        return p
    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=90,format=auto,width=500/{p}"

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

def map_position(pos_raw) -> Optional[str]:
    if isinstance(pos_raw, int):
        return POSITION_MAP.get(pos_raw) or str(pos_raw)
    if isinstance(pos_raw, str) and pos_raw.strip():
        return pos_raw.strip()
    return None

def map_alt_positions(ids) -> Optional[str]:
    """
    alternativePositionIds: [12,18,25] -> "RM,CAM,ST"
    stored as comma-separated string in altposition column
    """
    if not isinstance(ids, list) or not ids:
        return None
    out = []
    for pid in ids:
        if isinstance(pid, int):
            out.append(POSITION_MAP.get(pid, str(pid)))
    out = [x for x in out if x]
    return ",".join(out) if out else None

def pick_named_block(block) -> Optional[str]:
    """
    Your requirement: take ONLY the "name" inside objects like:
      uniqueClubSlug: {"slug":"...","name":"Inter Miami CF"}
    """
    if isinstance(block, dict):
        v = block.get("name")
        if isinstance(v, str) and v.strip():
            return v.strip()
        return None
    if isinstance(block, str) and block.strip():
        return block.strip()
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

def parse_player_data(raw: dict) -> dict:
    """
    Normalise FUT.GG player-item-definitions response.
    It may be:
      - {"data": {...}}
      - {...}
      - list[...] (rare)
    """
    data = raw
    if isinstance(raw, dict) and "data" in raw and isinstance(raw["data"], (dict, list)):
        data = raw["data"]

    if isinstance(data, list):
        # pick best "overall"
        best = None
        best_rating = -1
        for it in data:
            if not isinstance(it, dict):
                continue
            rv = it.get("overall") or it.get("rating") or it.get("ovr")
            try:
                r = int(rv)
            except Exception:
                r = -1
            if r > best_rating:
                best_rating = r
                best = it
        data = best or (data[0] if data else {})

    if not isinstance(data, dict):
        return {}

    first = data.get("firstName")
    last = data.get("lastName")
    nick = data.get("nickname")
    name = pick_name(nick, first, last)

    # Position and alt positions
    pos_raw = data.get("position") or data.get("positionId") or data.get("primaryPositionId")
    position = map_position(pos_raw)

    alt_ids = data.get("alternativePositionIds") or []
    altposition = map_alt_positions(alt_ids)

    # Club/League/Nation (name only, as requested)
    club   = pick_named_block(data.get("uniqueClubSlug")) or pick_named_block(data.get("club"))
    league = pick_named_block(data.get("uniqueLeagueSlug")) or pick_named_block(data.get("league"))
    nation = pick_named_block(data.get("uniqueNationSlug")) or pick_named_block(data.get("nation")) or pick_named_block(data.get("country"))

    # Rating
    rating = None
    try:
        rv = data.get("overall") or data.get("rating") or data.get("overallRating") or data.get("ovr")
        rating = int(rv) if rv is not None else None
    except Exception:
        rating = None

    # Version / rarity-like label
    version = data.get("version") or data.get("program") or data.get("cardType") or data.get("rarityName")
    if isinstance(version, dict):
        version = version.get("name")
    if isinstance(version, str):
        version = version.strip() or None
    else:
        version = None

    # Image
    img = data.get("futggCardImagePath") or data.get("cardImagePath") or data.get("imagePath")
    image_url = build_image_url(img)

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

async def fetch_meta_with_retry(session: aiohttp.ClientSession, card_id: int) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            async with session.get(
                META_API.format(card_id),
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": UA_HEADERS["User-Agent"]}
            ) as resp:
                if resp.status == 429:
                    wait_time = min(2 ** attempt, 30)
                    log.warning("⏳ 429 rate limit for %s, waiting %ss", card_id, wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                if resp.status != 200:
                    return {}
                raw = await resp.json()
                return parse_player_data(raw)
        except asyncio.TimeoutError:
            if attempt == MAX_RETRIES - 1:
                return {}
        except Exception:
            if attempt == MAX_RETRIES - 1:
                return {}
    return {}

# ================== DISCOVERY ================== #
# We build regex based on GAME, e.g. /players/<slug>/26-20801/
RX_HREF_SLUG_CARD = re.compile(rf'href=[\'\"]/players/([0-9a-z\-]+)/{re.escape(GAME)}-(\d+)[/\'\"]', re.IGNORECASE)
RX_HREF_CARD_ONLY = re.compile(rf'href=[\'\"]/players/(?:{re.escape(GAME)}-)?(\d+)[/\'\"]', re.IGNORECASE)

async def discover_cards(session: aiohttp.ClientSession, pages: int, bases: List[str]) -> Dict[int, Optional[str]]:
    """
    Returns dict: { card_id (int) : player_slug (str|None) }
    """
    out: Dict[int, Optional[str]] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(url: str):
        async with sem:
            html = await http_get_text(session, url)
            if not html:
                return
            doc = unescape(html)
            before = len(out)

            for m in RX_HREF_SLUG_CARD.finditer(doc):
                slug, cid = m.group(1), int(m.group(2))
                out[cid] = slug

            for m in RX_HREF_CARD_ONLY.finditer(doc):
                cid = int(m.group(1))
                out.setdefault(cid, None)

            log.info("🔎 %s: +%d cards (total %d)", url, len(out) - before, len(out))

    tasks = []
    for base in bases:
        for p in range(1, pages + 1):
            tasks.append(fetch_page(base.format(p)))
    await asyncio.gather(*tasks, return_exceptions=True)
    return out

# ================== UPSERT ================== #
async def upsert_new(conn: asyncpg.Connection, discovered: Dict[int, Optional[str]]) -> List[int]:
    """
    Insert missing card_ids; return list of newly inserted card_ids.
    card_id is BIGINT, so we use bigint[] in query.
    """
    if not discovered:
        return []

    ids = list(discovered.keys())  # List[int]

    have_rows = await conn.fetch(
        "SELECT card_id FROM public.fut_players WHERE card_id = ANY($1::bigint[])",
        ids
    )
    have = {int(r["card_id"]) for r in have_rows}

    new_ids = [cid for cid in ids if cid not in have]
    if not new_ids:
        return []

    rows: List[Tuple[int, Optional[str], str]] = []
    for cid in new_ids:
        slug = discovered.get(cid)
        if slug:
            url = f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/"
        else:
            url = f"https://www.fut.gg/players/{GAME}-{cid}/"
        rows.append((cid, slug, url))

    await conn.executemany(
        f"""
        INSERT INTO public.fut_players (card_id, player_slug, player_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (card_id) DO NOTHING
        """,
        rows
    )
    return new_ids

# ================== ENRICHMENT ================== #
async def get_missing_card_ids(conn: asyncpg.Connection, limit: int = 5000) -> List[int]:
    """
    Only enrich rows where *any* core fields are missing/null.
    """
    rows = await conn.fetch(
        """
        SELECT card_id
        FROM public.fut_players
        WHERE
          (name IS NULL OR name = '' OR
           rating IS NULL OR
           position IS NULL OR position = '' OR
           club IS NULL OR club = '' OR
           league IS NULL OR league = '' OR
           nation IS NULL OR nation = '' OR
           image_url IS NULL OR image_url = '' OR
           player_slug IS NULL OR player_slug = '' OR
           player_url IS NULL OR player_url = '')
        ORDER BY card_id
        LIMIT $1
        """,
        limit
    )
    return [int(r["card_id"]) for r in rows]

async def enrich_card_ids(conn: asyncpg.Connection, card_ids: List[int]) -> Tuple[int, int]:
    """
    Enrich specific card_ids, update by card_id.
    Returns: (updated, failed)
    """
    if not card_ids:
        return (0, 0)

    # Load existing slug/url so we never blank them
    existing = await conn.fetch(
        "SELECT card_id, player_slug, player_url FROM public.fut_players WHERE card_id = ANY($1::bigint[])",
        card_ids
    )
    info = {int(r["card_id"]): (r["player_slug"], r["player_url"]) for r in existing}

    write_conn = await asyncpg.connect(DATABASE_URL)

    # Build UPDATE with COALESCE so we only fill blanks
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
    if HAS_ALT:
        sql += ", altposition = COALESCE($11, altposition)"
        arg_count += 1
    if HAS_NICK:
        sql += f", nickname = COALESCE(${arg_count + 1}, nickname)"
        arg_count += 1
    if HAS_FIRST:
        sql += f", first_name = COALESCE(${arg_count + 1}, first_name)"
        arg_count += 1
    if HAS_LAST:
        sql += f", last_name = COALESCE(${arg_count + 1}, last_name)"
        arg_count += 1

    sql += f" WHERE card_id = ${arg_count + 1}"

    updated = 0
    failed = 0

    try:
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None, connect=REQUEST_TIMEOUT),
            connector=aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
        ) as session:

            total = len(card_ids)
            for i in range(0, total, BULK_BATCH_SIZE):
                batch = card_ids[i:i + BULK_BATCH_SIZE]
                batch_num = (i // BULK_BATCH_SIZE) + 1
                total_batches = (total + BULK_BATCH_SIZE - 1) // BULK_BATCH_SIZE
                log.info("🧩 Enrich batch %d/%d (%d cards)", batch_num, total_batches, len(batch))

                sem = asyncio.Semaphore(CONCURRENCY)

                async def work(cid: int):
                    async with sem:
                        meta = await fetch_meta_with_retry(session, cid)
                    return cid, meta

                results = await asyncio.gather(*(work(cid) for cid in batch), return_exceptions=True)

                updates = []
                for r in results:
                    if isinstance(r, Exception):
                        failed += 1
                        continue
                    cid, meta = r
                    if not meta:
                        failed += 1
                        continue

                    slug_existing, url_existing = info.get(cid, (None, None))
                    slug = slug_existing
                    url = url_existing
                    if not url:
                        if slug:
                            url = f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/"
                        else:
                            url = f"https://www.fut.gg/players/{GAME}-{cid}/"

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
                    if HAS_ALT:
                        args.append(meta.get("altposition"))
                    if HAS_NICK:
                        args.append(meta.get("nickname"))
                    if HAS_FIRST:
                        args.append(meta.get("first_name"))
                    if HAS_LAST:
                        args.append(meta.get("last_name"))

                    args.append(cid)  # WHERE card_id = ...
                    updates.append(tuple(args))

                if updates:
                    try:
                        await write_conn.executemany(sql, updates)
                        updated += len(updates)
                        log.info("💾 Wrote %d updates (total updated %d, failed %d)", len(updates), updated, failed)
                    except Exception as e:
                        log.error("❌ DB update failed for this batch: %s", e)
                        failed += len(updates)

                if i + BULK_BATCH_SIZE < total:
                    await asyncio.sleep(BATCH_DELAY)

    finally:
        await write_conn.close()

    return (updated, failed)

# ================== ONE CYCLE ================== #
async def run_once(mode: str) -> None:
    """
    mode:
      - "daily": scan NEW_PAGES from NEW_LISTING_URLS, insert new, then enrich missing/null rows
      - "full":  scan FULL_PAGES from FULL_LISTING_URLS, insert lots, then enrich missing/null rows
    """
    conn = await get_db()
    try:
        await preflight(conn)

        if mode == "full":
            bases = FULL_LISTING_URLS
            pages = FULL_PAGES
            log.info("📚 FULL BUILD: scanning %d pages", pages)
        else:
            bases = NEW_LISTING_URLS
            pages = NEW_PAGES
            log.info("🆕 DAILY: scanning %d pages", pages)

        discovered: Dict[int, Optional[str]] = {}
        async with aiohttp.ClientSession() as session:
            discovered = await discover_cards(session, pages, bases)

        added = await upsert_new(conn, discovered)
        if added:
            log.info("🆕 Inserted %d new players.", len(added))
        else:
            log.info("ℹ️ No new players to insert.")

        # Enrich missing/null rows (this will include newly inserted)
        # Loop until no missing remain (or until cap)
        total_updated = 0
        total_failed = 0

        while True:
            missing = await get_missing_card_ids(conn, limit=5000)
            log.info("🧾 missing_meta=%d", len(missing))
            if not missing:
                break

            updated, failed = await enrich_card_ids(conn, missing)
            total_updated += updated
            total_failed += failed

            # If we're making no progress, stop to avoid endless looping
            if updated == 0 and failed > 0:
                log.warning("⚠️ No progress in last pass (updated=0). Stopping enrichment loop.")
                break

        log.info("✅ Done. Total updated=%d | total failed=%d", total_updated, total_failed)

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
    log.info("🕖 Next daily run scheduled for %s", target.isoformat())
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
        log.info("🚦 Cycle start (daily)")
        await run_once(mode="daily")
        log.info("✅ Cycle complete (daily)")
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run DAILY discovery + enrich once and exit")
    ap.add_argument("--full", action="store_true", help="Run FULL build (FULL_PAGES) + enrich once and exit")
    args = ap.parse_args()

    async def _runner():
        if args.full:
            await run_once(mode="full")
        elif args.now:
            await run_once(mode="daily")
        else:
            await main_loop()

    asyncio.run(_runner())