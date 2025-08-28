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
from typing import Optional, Dict, List, Set, Tuple
from aiohttp import web  # health server

# ------------------ CONFIG ------------------ #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

META_API  = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
NEW_URLS  = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))            # discovery pages per URL
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))     # seconds (connect/read)
BACKFILL_LIMIT     = int(os.getenv("BACKFILL_LIMIT", "300"))     # rows per backfill SELECT
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))  # rows per DB executemany chunk

# Concurrency knobs
CONCURRENCY = int(os.getenv("CONCURRENCY", "24"))                     # parallel enrich fetches
DISCOVERY_CONCURRENCY = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))  # parallel page fetches

UA_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/2.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))
LOG_FULL_PAYLOADS = os.getenv("LOG_FULL_PAYLOADS") == "1"

# Detect optional columns
TABLE_HAS_NICKNAME = False
TABLE_HAS_FIRST = False
TABLE_HAS_LAST  = False

# ------------------ POSITION MAP ------------------ #
POSITION_MAP_DEFAULT = {
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
    POSITION_MAP = {int(k): str(v) for k, v in json.loads(os.getenv("POSITION_MAP_JSON", "{}")).items()} or POSITION_MAP_DEFAULT
except Exception:
    POSITION_MAP = POSITION_MAP_DEFAULT

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
log = logging.getLogger("futgg_meta_sync")
print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={LOG_LEVEL}", flush=True)

# ------------------ DB ------------------ #
async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def preflight_check(conn: asyncpg.Connection) -> None:
    global TABLE_HAS_NICKNAME, TABLE_HAS_FIRST, TABLE_HAS_LAST
    db = await conn.fetchval("SELECT current_database()")
    user = await conn.fetchval("SELECT current_user")
    schema = await conn.fetchval("SHOW search_path")
    table_exists = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not table_exists:
        raise RuntimeError(f"Table public.fut_players NOT found. db={db} user={user} search_path={schema}")

    # Unique index on card_id expected
    idx = await conn.fetchval("""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname='public' AND tablename='fut_players'
          AND indexname='fut_players_card_id_key'
    """)
    if not idx:
        raise RuntimeError("UNIQUE index fut_players_card_id_key missing on public.fut_players")

    # Detect optional columns
    q = """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fut_players'
    """
    cols = {r["column_name"] for r in await conn.fetch(q)}
    TABLE_HAS_NICKNAME = "nickname" in cols
    TABLE_HAS_FIRST    = "first_name" in cols
    TABLE_HAS_LAST     = "last_name" in cols

    log.info("✅ Preflight OK: db=%s | user=%s | search_path=%s | columns: nickname=%s first=%s last=%s",
             db, user, schema, TABLE_HAS_NICKNAME, TABLE_HAS_FIRST, TABLE_HAS_LAST)

# ------------------ Utils ------------------ #
def normalize_name(obj, *keys) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s or None
    if isinstance(obj, dict):
        for k in keys or ("name", "shortName", "longName", "fullName", "label", "code"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None

def build_image_url(card_image_path: Optional[str]) -> Optional[str]:
    """
    Convert "2025/player-item-card/25-50587991.<hash>.webp"
      -> "https://game-assets.fut.gg/cdn-cgi/image/quality=90,format=auto,width=500/2025/futgg-player-item-card/25-50587991.<hash>.webp"
    """
    if not card_image_path or not isinstance(card_image_path, str):
        return None
    path = card_image_path.strip()
    if not path:
        return None
    path = path.replace("player-item-card", "futgg-player-item-card")
    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=90,format=auto,width=500/{path}"

def _read_slug_name(obj) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s or None
    if isinstance(obj, dict):
        name = obj.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()
        slug = obj.get("slug")
        if isinstance(slug, str) and slug.strip():
            return slug.strip()
    return None

# ------------------ META PARSING ------------------ #
def extract_meta_fields(data: dict) -> Dict[str, Optional[str]]:
    """
    Normalizes metadata from item-definition:
    - name: prefer nickname; else first + last; else fallbacks
    - first_name, last_name, nickname
    - rating: overall
    - version: card/rarity/program
    - position: numeric -> code
    - club/league/nation: unique*Slug.name preferred
    - image_url: from cardImagePath -> fut.gg CDN
    - player_slug: not in this payload (comes from discovery href)
    """
    if not isinstance(data, dict):
        return {k: None for k in ("name","first_name","last_name","nickname","rating","version","image_url","position","club","nation","league")}

    nick = data.get("nickname")
    first = data.get("firstName")
    last  = data.get("lastName")

    if isinstance(nick, str) and nick.strip():
        name = nick.strip()
    elif isinstance(first, str) and first.strip() and isinstance(last, str) and last.strip():
        name = f"{first.strip()} {last.strip()}"
    else:
        name = (normalize_name(data, "name") or
                normalize_name(data.get("player") or data.get("item"), "name", "fullName") or
                normalize_name(data, "playerName", "longName", "fullName") or
                normalize_name(data, "searchableName"))

    rating_val = (data.get("overall") or data.get("rating") or data.get("overallRating") or data.get("ovr"))
    try:
        rating = int(rating_val) if rating_val is not None else None
    except Exception:
        rating = None

    version = (data.get("version") or data.get("cardType") or data.get("rarity")
               or data.get("program") or data.get("itemType") or data.get("rarityName"))
    if isinstance(version, dict):
        version = normalize_name(version)
    elif isinstance(version, str):
        version = version.strip() or None

    # position (numeric -> code)
    pos_raw = data.get("position") or data.get("positionId") or data.get("primaryPositionId") or (data.get("meta") or {}).get("position")
    if isinstance(pos_raw, int):
        position = POSITION_MAP.get(pos_raw) or str(pos_raw)
    else:
        position = normalize_name(pos_raw) if isinstance(pos_raw, dict) else (pos_raw.strip() if isinstance(pos_raw, str) else None)

    club   = _read_slug_name(data.get("uniqueClubSlug"))   or _read_slug_name(data.get("club")   or data.get("team")) \
             or normalize_name((data.get("meta") or {}).get("club"))   or normalize_name(data.get("clubName"))
    league = _read_slug_name(data.get("uniqueLeagueSlug")) or _read_slug_name(data.get("league")) \
             or normalize_name((data.get("meta") or {}).get("league")) or normalize_name(data.get("leagueName"))
    nation = _read_slug_name(data.get("uniqueNationSlug")) or _read_slug_name(data.get("nation") or data.get("country")) \
             or normalize_name((data.get("meta") or {}).get("nation")) or normalize_name(data.get("nationName"))

    image_url = build_image_url(data.get("cardImagePath"))

    return {
        "name": name,
        "first_name": (first.strip() if isinstance(first, str) and first.strip() else None),
        "last_name":  (last.strip()  if isinstance(last,  str) and last.strip()  else None),
        "nickname":   (nick.strip()  if isinstance(nick,  str) and nick.strip()  else None),
        "rating": rating,
        "version": version,
        "image_url": image_url,
        "position": position,
        "club": club,
        "league": league,
        "nation": nation,
    }

# ------------------ HTTP ------------------ #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None

async def fetch_meta(session: aiohttp.ClientSession, card_id_numeric: str) -> Dict[str, Optional[str]]:
    """
    Handles dict or list payloads; for list chooses the highest 'overall' rated item.
    """
    try:
        async with session.get(META_API.format(card_id_numeric), timeout=REQUEST_TIMEOUT,
                               headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
            if resp.status != 200:
                return {k: None for k in ("name","first_name","last_name","nickname","rating","version","image_url","position","club","league","nation")}
            raw = await resp.json()
            data = raw
            if isinstance(raw, list):
                best = None
                best_rating = -1
                for item in raw:
                    r = item.get("overall") or item.get("rating") or item.get("overallRating") or item.get("ovr")
                    try: r = int(r)
                    except Exception: r = -1
                    if r > best_rating:
                        best_rating, best = r, item
                data = best or (raw[0] if raw else {})
            if LOG_FULL_PAYLOADS:
                log.debug("META %s: %s", card_id_numeric, data)
            return extract_meta_fields(data)
    except Exception:
        return {k: None for k in ("name","first_name","last_name","nickname","rating","version","image_url","position","club","league","nation")}

# ------------------ Discovery (concurrent) ------------------ #
# Capture both slug and card id from hrefs like /players/256343-robson-bambu/25-50587991/
RX_HREF_SLUG_CARD = re.compile(r'href=[\'\"]/players/([0-9a-z\-]+)/25-(\d+)[/\'\"]', re.IGNORECASE)
# Also match generic cases just in case (won't have slug)
RX_HREF_CARD_ONLY = re.compile(r'href=[\'\"]/players/(?:\d+/)?(?:25-)?(\d+)[/\'\"]', re.IGNORECASE)

async def discover_new_cards(session: aiohttp.ClientSession, pages: int) -> Dict[str, Optional[str]]:
    """
    Returns dict: card_id -> player_slug (slug may be None if not present in markup)
    """
    found: Dict[str, Optional[str]] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONCURRENCY)

    async def fetch_page(url: str):
        async with sem:
            html = await http_get_text(session, url)
            if not html:
                log.info("🔍 %s: no HTML", url)
                return
            doc = unescape(html)

            before = len(found)
            for m in RX_HREF_SLUG_CARD.finditer(doc):
                slug = m.group(1)
                card = m.group(2)
                found[card] = slug

            # fallback: card only links (don't overwrite existing slug)
            for m in RX_HREF_CARD_ONLY.finditer(doc):
                card = m.group(1)
                found.setdefault(card, None)

            added = len(found) - before
            log.info("🔎 %s: +%d cards (total %d)", url, added, len(found))

    tasks = []
    for base in NEW_URLS:
        for page in range(1, pages + 1):
            tasks.append(fetch_page(base.format(page)))
    await asyncio.gather(*tasks, return_exceptions=True)
    return found

# ------------------ Upsert / Enrich ------------------ #
async def upsert_new_players(conn: asyncpg.Connection, discovered: Dict[str, Optional[str]]) -> List[str]:
    """
    Insert any missing card_ids. Returns list of inserted card_ids (as strings).
    """
    if not discovered:
        return []
    card_ids = list(discovered.keys())

    have_rows = await conn.fetch(
        "SELECT card_id FROM public.fut_players WHERE card_id = ANY($1::text[])",
        card_ids
    )
    have = {r["card_id"] for r in have_rows}
    to_insert = [cid for cid in card_ids if cid not in have]
    if not to_insert:
        return []

    rows = []
    for cid in to_insert:
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
    return to_insert

async def enrich_by_card_ids(conn: asyncpg.Connection, card_ids: List[str]) -> None:
    if not card_ids:
        return

    # Map card_id -> (id, player_slug, player_url)
    rows = await conn.fetch(
        "SELECT id, card_id, player_slug, player_url FROM public.fut_players WHERE card_id = ANY($1::text[])",
        card_ids
    )
    info = {r["card_id"]: (r["id"], r["player_slug"], r["player_url"]) for r in rows}
    if not info:
        return

    # Dedicated write connection
    write_conn = await asyncpg.connect(DATABASE_URL)

    sem = asyncio.Semaphore(CONCURRENCY)
    queue: asyncio.Queue = asyncio.Queue()
    STOP = object()

    written_total = 0

    # SQL: fill missing fields, don't overwrite non-null (COALESCE)
    # Also: if slug/url were null at insert time, allow setting them now.
    base_update = """
        UPDATE public.fut_players
        SET position         = COALESCE($1,  position),
            club             = COALESCE($2,  club),
            nation           = COALESCE($3,  nation),
            league           = COALESCE($4,  league),
            name             = COALESCE($5,  name),
            rating           = COALESCE($6,  rating),
            version          = COALESCE($7,  version),
            image_url        = COALESCE($8,  image_url),
            player_slug      = COALESCE($9,  player_slug),
            player_url       = COALESCE($10, player_url),
            created_at       = NOW() AT TIME ZONE 'UTC'
    """
    # optional pieces
    if TABLE_HAS_NICKNAME:
        base_update += ", nickname = COALESCE($11, nickname)"
    if TABLE_HAS_FIRST:
        base_update += ", first_name = COALESCE($12, first_name)"
    if TABLE_HAS_LAST:
        base_update += ", last_name = COALESCE($13, last_name)"

    # where clause depends on arg count
    # We'll append id as the final positional param
    base_update += " WHERE id = ${}\n"

    # prepare an SQL string with the right ${n} for WHERE id
    def make_sql(arg_count: int) -> str:
        return base_update.format(arg_count + 1)

    async def _try_write_chunk(sql: str, chunk: list):
        for attempt in range(3):
            try:
                await write_conn.executemany(sql, chunk)
                return
            except Exception as e:
                msg = str(e).lower()
                if "another operation is in progress" in msg and attempt < 2:
                    await asyncio.sleep(0.05 * (attempt + 1))
                    continue
                log.error("❌ DB update failed for chunk of %d: %s", len(chunk), e)
                return

    async def writer(arg_count: int):
        nonlocal written_total
        sql = make_sql(arg_count)
        batch = []
        while True:
            item = await queue.get()
            if item is STOP:
                if batch:
                    await _try_write_chunk(sql, batch)
                    written_total += len(batch)
                    log.info("💾 Enriched chunk of %d (running total %d)", len(batch), written_total)
                break
            batch.append(item)
            if len(batch) >= UPDATE_CHUNK_SIZE:
                to_write = batch
                batch = []
                await _try_write_chunk(sql, to_write)
                written_total += len(to_write)
                log.info("💾 Enriched chunk of %d (running total %d)", len(to_write), written_total)

    # Decide arg count/order once
    arg_fields = [
        "position","club","nation","league",
        "name","rating","version","image_url",
        "player_slug","player_url"
    ]
    if TABLE_HAS_NICKNAME: arg_fields.append("nickname")
    if TABLE_HAS_FIRST:    arg_fields.append("first_name")
    if TABLE_HAS_LAST:     arg_fields.append("last_name")
    ARG_COUNT = len(arg_fields)

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=None, connect=REQUEST_TIMEOUT),
        connector=aiohttp.TCPConnector(limit=CONCURRENCY * 2, ttl_dns_cache=300)
    ) as session:

        async def enrich_one(cid: str):
            # existing db info
            rec = info.get(cid)
            if not rec:
                return
            row_id, existing_slug, existing_url = rec

            # fetch meta
            async with sem:
                try:
                    meta = await fetch_meta(session, cid)
                except Exception:
                    return

            # Build/repair slug+url if we somehow didn't have them
            slug = existing_slug
            if not slug:
                # no reliable way from meta; keep None
                pass
            pretty_url = existing_url
            if not pretty_url:
                pretty_url = f"https://www.fut.gg/players/{slug}/25-{cid}/" if slug else f"https://www.fut.gg/players/25-{cid}/"

            # Build args in the exact order expected
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
                pretty_url,
            ]
            if TABLE_HAS_NICKNAME: args.append(meta.get("nickname"))
            if TABLE_HAS_FIRST:    args.append(meta.get("first_name"))
            if TABLE_HAS_LAST:     args.append(meta.get("last_name"))
            args.append(row_id)  # WHERE id = $n

            await queue.put(tuple(args))

        # launch writer and producers
        writer_task = asyncio.create_task(writer(ARG_COUNT))
        await asyncio.gather(*(enrich_one(cid) for cid in card_ids), return_exceptions=True)
        await queue.put(STOP)
        await writer_task

    await write_conn.close()
    log.info("✅ Enrichment done. Wrote %d row(s).", written_total)

# ------------------ Backfill ------------------ #
async def backfill_missing_meta(conn: asyncpg.Connection) -> None:
    total = 0
    while True:
        try:
            rows = await conn.fetch(
                """
                SELECT id, card_id
                FROM public.fut_players
                WHERE card_id ~ '^[0-9]+$'
                  AND (
                    name IS NULL OR name = '' OR
                    position IS NULL OR position = '' OR
                    club IS NULL OR club = '' OR
                    nation IS NULL OR nation = '' OR
                    league IS NULL OR league = '' OR
                    rating IS NULL OR
                    version IS NULL OR version = '' OR
                    image_url IS NULL OR image_url = '' OR
                    player_slug IS NULL OR player_slug = '' OR
                    player_url IS NULL OR player_url = ''
                    -- nickname/first/last are optional
                  )
                LIMIT $1
                """,
                BACKFILL_LIMIT
            )
        except Exception as e:
            log.error("❌ Backfill query failed: %s", e)
            return

        if not rows:
            if total == 0:
                log.info("ℹ️ No rows need meta backfill.")
            else:
                log.info("✅ Backfill complete (%d players updated).", total)
            return

        card_ids = [r["card_id"] for r in rows]
        total += len(card_ids)
        log.info("🧩 Backfilling meta for %d players (running total %d)…", len(card_ids), total)
        try:
            await enrich_by_card_ids(conn, card_ids)
        except Exception as e:
            log.error("❌ Enrich step failed during backfill: %s", e)
            await asyncio.sleep(1)

# ------------------ One-cycle task ------------------ #
async def run_once() -> None:
    conn = await get_db()
    try:
        await preflight_check(conn)

        # Discover new (card_id -> slug)
        discovered: Dict[str, Optional[str]] = {}
        try:
            async with aiohttp.ClientSession() as session:
                discovered = await discover_new_cards(session, pages=NEW_PAGES)
        except Exception as e:
            log.error("❌ Discovery failed: %s", e)

        # Upsert any new rows
        added_card_ids: List[str] = []
        try:
            added_card_ids = await upsert_new_players(conn, discovered)
            if added_card_ids:
                log.info("🆕 Inserted %d new players.", len(added_card_ids))
            else:
                log.info("ℹ️ No new players found from discovery.")
        except Exception as e:
            log.error("❌ Upsert failed: %s", e)

        # Enrich new rows (nice-to-have) then backfill everything missing
        if added_card_ids:
            try:
                await enrich_by_card_ids(conn, added_card_ids)
            except Exception as e:
                log.error("❌ Enrich (new) failed: %s", e)

        try:
            await backfill_missing_meta(conn)
        except Exception as e:
            log.error("❌ Backfill loop failed: %s", e)

    finally:
        try:
            await conn.close()
        except Exception:
            pass

# ------------------ Health server ------------------ #
async def start_health_server() -> asyncio.Task:
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
    return asyncio.create_task(asyncio.Event().wait())  # keep alive

# ------------------ Main loop: run now, then daily @ 19:00 UK ------------------ #
shutdown = asyncio.Event()

def _signal_handler():
    log.info("🛑 Received shutdown signal. Finishing current cycle…")
    shutdown.set()

async def sleep_until_19_uk() -> None:
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    target = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if now >= target:
        target +=  timedelta(days=1)
    sleep_s = (target - now).total_seconds()
    log.info("🕖 Next new-player check scheduled for %s", target.isoformat())
    await asyncio.sleep(sleep_s)

async def main_loop() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    health_task = None
    if os.getenv("PORT"):
        health_task = await start_health_server()

    while not shutdown.is_set():
        try:
            log.info("🚦 Cycle start")
            await run_once()
            log.info("✅ Cycle complete")
        except Exception as e:
            log.error("❌ Unhandled error in cycle: %s", e)
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ------------------ CLI ------------------ #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run discovery+enrich+backfill once, then exit")
    ap.add_argument("--backfill-only", action="store_true", help="Run only the backfill to completion, then exit")
    args = ap.parse_args()

    async def _run():
        if args.backfill_only:
            conn = await get_db()
            try:
                await preflight_check(conn)
                await backfill_missing_meta(conn)
            finally:
                await conn.close()
            return
        if args.now:
            await run_once()
            return
        await main_loop()

    try:
        asyncio.run(_run())
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise