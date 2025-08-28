import os
import re
import sys
import time
import asyncio
import asyncpg
import aiohttp
import logging
import signal
from datetime import datetime
from html import unescape
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List, Set, Tuple
from aiohttp import web  # health server

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

NEW_PAGES   = int(os.getenv("NEW_PAGES", "5"))
REQUEST_TO  = int(os.getenv("REQUEST_TIMEOUT", "15"))

UA_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/1.9)",
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
log = logging.getLogger("futgg_auto_sync")
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

# Numeric ID extraction: "25-231443", "231443", or any string ending with digits
_RE_NUMERIC_ID_TAIL = re.compile(r'(^|-)(\d+)$')
_RE_ANY_DIGITS_AT_END = re.compile(r'(\d+)$')
def extract_numeric_id(card_id: Optional[str]) -> Optional[str]:
    if not card_id:
        return None
    m = _RE_NUMERIC_ID_TAIL.search(card_id)
    if m:
        return m.group(2)
    m2 = _RE_ANY_DIGITS_AT_END.search(card_id)
    return m2.group(1) if m2 else None

def extract_price_and_flags(payload: dict) -> Tuple[Optional[int], Optional[bool], Optional[bool], Optional[bool], Optional[str]]:
    """
    Returns: (price, is_objective, is_untradeable, is_extinct, price_updated_at_text)
    """
    price = None
    is_objective = None
    is_untradeable = None
    is_extinct = None
    updated_at_text = None

    if not isinstance(payload, dict):
        return price, is_objective, is_untradeable, is_extinct, updated_at_text

    # Legacy LCPrice
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
            updated_at_text = str(cp["priceUpdatedAt"])
    except Exception:
        pass

    return price, is_objective, is_untradeable, is_extinct, updated_at_text

def extract_meta_fields(data: dict) -> Dict[str, Optional[str]]:
    if not isinstance(data, dict):
        return {k: None for k in ("name","rating","version","image_url","player_slug","position","club","nation","league")}

    name = (data.get("name")
            or normalize_name(data.get("player") or data.get("item"), "name", "fullName"))

    rating_val = pick_first(data, ["rating", "overall", "overallRating", "ovr"])
    try:
        rating = int(rating_val) if rating_val is not None else None
    except Exception:
        rating = None

    version = pick_first(data, ["version", "cardType", "rarity", "program", "itemType"])
    image_url = (pick_first(data, ["imageUrl", "imageURL", "imgUrl", "imgURL"])
                 or (isinstance(data.get("image"), dict) and pick_first(data["image"], ["url", "href"])))
    player_slug = pick_first(data, ["slug", "seoName", "urlName", "pathSegment"])

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

    rows = [(cid, f"https://www.fut.gg/players/{extract_numeric_id(cid)}/") for cid in to_insert]
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
            Optional[str], Optional[bool], Optional[bool], Optional[bool], Optional[str],
            int
        ]] = []
        skipped_bad = 0
        skipped_missing = 0

        for cid in card_ids:
            if cid not in id_by_card:
                skipped_missing += 1
                continue

            numeric_id = extract_numeric_id(cid)
            if not numeric_id:
                skipped_bad += 1
                continue

            try:
                (price, is_obj, is_untrad, is_ext, price_ts_text), meta = await asyncio.gather(
                    fetch_price(session, numeric_id),
                    fetch_meta(session, numeric_id)
                )
            except Exception as e:
                log.error("❌ fetch failed for %s: %s", cid, e)
                continue

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
                price_ts_text,   # TEXT → cast in SQL
                id_by_card[cid]
            ))

        if not batch:
            log.info("ℹ️ Nothing to enrich for given batch. (Skipped %d malformed, %d missing rows)", skipped_bad, skipped_missing)
            return

        try:
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
                    price_updated_at = COALESCE($15::timestamptz, price_updated_at),
                    created_at       = NOW() AT TIME ZONE 'UTC'
                WHERE id = $16
                """,
                batch
            )
        except Exception as e:
            log.error("❌ DB update failed for batch of %d: %s", len(batch), e)
            return

    log.info("✅ Enriched %d player(s). Skipped %d malformed card_id, %d missing rows.", len(batch), skipped_bad, skipped_missing)

async def backfill_missing_meta(conn: asyncpg.Connection) -> None:
    total = 0
    while True:
        try:
            rows = await conn.fetch(
                """
                SELECT id, card_id
                FROM public.fut_players
                WHERE card_id IS NOT NULL
                  AND card_id ~ '(^|-)\\d+$'   -- must end with digits, optionally after a dash
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

        # 1) Discover
        new_ids: Set[int] = set()
        try:
            async with aiohttp.ClientSession() as session:
                new_ids = await discover_new_player_ids(session, pages=NEW_PAGES)
        except Exception as e:
            log.error("❌ Discovery failed: %s", e)

        # 2) Upsert
        added_card_ids: List[str] = []
        try:
            added_card_ids = await upsert_new_players(conn, new_ids)
            if added_card_ids:
                log.info("🆕 Inserted %d new players.", len(added_card_ids))
            else:
                log.info("ℹ️ No new players found from discovery.")
        except Exception as e:
            log.error("❌ Upsert failed: %s", e)

        # 3) Enrich just-added
        try:
            if added_card_ids:
                await enrich_by_card_ids(conn, added_card_ids)
        except Exception as e:
            log.error("❌ Enrich (new) failed: %s", e)

        # 4) Backfill all missing meta
        try:
            await backfill_missing_meta(conn)
        except Exception as e:
            log.error("❌ Backfill loop failed: %s", e)

    finally:
        try:
            await conn.close()
        except Exception:
            pass

# ------------------ Health server (for Web deployments) ------------------ #
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

async def main() -> None:
    # handle SIGTERM/SIGINT
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    # Start health server if running as Web (PORT set)
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

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr, flush=True)
        raise