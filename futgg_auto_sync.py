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

GAME = os.getenv("GAME", "26").strip()  # FC26
print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={os.getenv('LOG_LEVEL','INFO').upper()} | GAME={GAME}", flush=True)

META_API = f"https://www.fut.gg/api/fut/player-item-definitions/{GAME}/{{}}"

# listing sources for daily/new discovery
LISTING_URLS = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

# full scrape base (one-off)
FULL_BASE_URL = "https://www.fut.gg/players/?page={}"
FULL_PAGES_DEFAULT = int(os.getenv("FULL_PAGES", "335"))

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "20"))
CONCURRENCY        = int(os.getenv("CONCURRENCY", "18"))
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "150"))

# polite throttles
RATE_LIMIT_DELAY   = float(os.getenv("RATE_LIMIT_DELAY", "0.05"))   # per API request delay
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", "3"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/4.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/"
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "250"))

# optional columns (auto-detected)
HAS_NICK = False
HAS_FIRST = False
HAS_LAST = False
HAS_ALT_POS = False

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

# ================== DB ================== #
async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def ensure_card_id_unique(conn: asyncpg.Connection) -> None:
    """
    Ensure a unique index exists on card_id so ON CONFLICT(card_id) works.
    Safe even if you already have a UNIQUE constraint.
    """
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS fut_players_card_id_key
        ON public.fut_players(card_id);
    """)

async def has_unique_on_card_id(conn: asyncpg.Connection) -> bool:
    """
    Detect ANY unique constraint or unique index that covers exactly card_id.
    """
    # unique constraint on card_id?
    c = await conn.fetchval("""
        SELECT 1
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname='public'
          AND rel.relname='fut_players'
          AND con.contype='u'
          AND con.conkey = ARRAY[
              (SELECT attnum FROM pg_attribute
               WHERE attrelid=rel.oid AND attname='card_id' AND NOT attisdropped)
          ]
        LIMIT 1;
    """)
    if c:
        return True

    # unique index on card_id?
    i = await conn.fetchval("""
        SELECT 1
        FROM pg_class rel
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        JOIN pg_index idx ON idx.indrelid = rel.oid
        WHERE nsp.nspname='public'
          AND rel.relname='fut_players'
          AND idx.indisunique = true
          AND idx.indkey = ARRAY[
              (SELECT attnum FROM pg_attribute
               WHERE attrelid=rel.oid AND attname='card_id' AND NOT attisdropped)
          ]
        LIMIT 1;
    """)
    return bool(i)

async def preflight(conn: asyncpg.Connection) -> None:
    global HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT_POS

    tbl = await conn.fetchval("SELECT to_regclass('public.fut_players')")
    if not tbl:
        raise RuntimeError("Table public.fut_players not found")

    cols = {
        r["column_name"]
        for r in await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
        """)
    }
    HAS_NICK    = "nickname" in cols
    HAS_FIRST   = "first_name" in cols
    HAS_LAST    = "last_name" in cols
    HAS_ALT_POS = "altposition" in cols or "alt_position" in cols

    # Ensure unique exists (so ON CONFLICT works)
    if not await has_unique_on_card_id(conn):
        # create index if missing
        await ensure_card_id_unique(conn)
        if not await has_unique_on_card_id(conn):
            raise RuntimeError("A UNIQUE constraint/index on fut_players(card_id) is required")

    log.info("✅ Preflight OK | columns: nickname=%s first=%s last=%s altposition=%s",
             HAS_NICK, HAS_FIRST, HAS_LAST, HAS_ALT_POS)

# ================== HELPERS ================== #
def build_image_url_from_api(data: dict) -> Optional[str]:
    """
    Prefer futggCardImagePath if present (already futgg path).
    Otherwise fall back to cardImagePath/simpleCardImagePath/imagePath.
    """
    if not isinstance(data, dict):
        return None

    path = (
        data.get("futggCardImagePath")
        or data.get("cardImagePath")
        or data.get("simpleCardImagePath")
        or data.get("imagePath")
    )
    if not isinstance(path, str) or not path.strip():
        return None
    path = path.strip().lstrip("/")  # safety

    return f"https://game-assets.fut.gg/cdn-cgi/image/quality=90,format=auto,width=500/{path}"

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

def map_position(pos_id: Optional[int]) -> Optional[str]:
    if isinstance(pos_id, int):
        return POSITION_MAP.get(pos_id) or str(pos_id)
    return None

def map_alt_positions(ids: list) -> Optional[str]:
    if not isinstance(ids, list) or not ids:
        return None
    mapped = []
    for x in ids:
        if isinstance(x, int):
            mapped.append(POSITION_MAP.get(x) or str(x))
    mapped = [m for m in mapped if m]
    return ",".join(mapped) if mapped else None

def safe_text(v) -> Optional[str]:
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None

# Listing alt parsing: "Name - 91 - Gold Rare"
def parse_alt_text(alt_text: str) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    if not alt_text:
        return None, None, None
    parts = [p.strip() for p in alt_text.split(" - ")]
    if len(parts) >= 2 and parts[1].isdigit():
        name = parts[0].strip() or None
        rating = int(parts[1])
        rarity = " - ".join(parts[2:]).strip() if len(parts) > 2 else None
        rarity = rarity or None
        return name, rating, rarity
    return alt_text.strip() or None, None, None

def fallback_name_from_slug(slug: Optional[str], card_id: int) -> str:
    if slug and isinstance(slug, str):
        # "190045-johan-cruyff" -> "johan cruyff"
        tail = slug.split("-", 1)[-1] if "-" in slug else slug
        tail = tail.replace("-", " ").strip()
        if tail:
            return tail.title()
    return f"Unknown {card_id}"

# ================== HTTP ================== #
async def http_get_text(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url, timeout=REQUEST_TIMEOUT, headers=UA_HEADERS) as resp:
            if resp.status != 200:
                return None
            return await resp.text()
    except Exception:
        return None

async def fetch_meta_with_retry(session: aiohttp.ClientSession, card_id: int) -> dict:
    for attempt in range(MAX_RETRIES):
        try:
            await asyncio.sleep(RATE_LIMIT_DELAY)
            async with session.get(META_API.format(card_id), timeout=REQUEST_TIMEOUT,
                                   headers={"User-Agent": UA_HEADERS["User-Agent"]}) as resp:
                if resp.status == 429:
                    wait_time = min(2 ** attempt, 20)
                    log.warning("Rate limited for card %s, waiting %ss", card_id, wait_time)
                    await asyncio.sleep(wait_time)
                    continue
                if resp.status != 200:
                    return {}
                raw = await resp.json()
                return parse_api_payload(raw)
        except asyncio.TimeoutError:
            if attempt == MAX_RETRIES - 1:
                return {}
        except Exception:
            if attempt == MAX_RETRIES - 1:
                return {}
    return {}

def pretty_slug(value: Optional[str]) -> Optional[str]:
    """
    Converts:
      '2-aston-villa' -> 'Aston Villa'
      '73-paris-sg'   -> 'Paris Saint Germain' (override)
      'major-league-soccer' -> 'Major League Soccer'
      'argentina' -> 'Argentina'
    """
    if not isinstance(value, str):
        return None

    s = value.strip()
    if not s:
        return None

    # Drop numeric prefix: "123-foo-bar" -> "foo-bar"
    s = re.sub(r"^\d+-", "", s)

    # Replace separators with spaces
    s = s.replace("_", " ").replace("-", " ").strip()
    if not s:
        return None

    # Common overrides (add more as you find them)
    overrides = {
        "paris sg": "Paris Saint Germain",
        "psg": "Paris Saint Germain",
    }
    key = s.lower()
    if key in overrides:
        return overrides[key]

    # Title case (keeps simple)
    return " ".join(w[:1].upper() + w[1:].lower() for w in s.split())

def parse_api_payload(raw: dict) -> dict:
    """
    FUT.GG API returns {"data": {...}} for this endpoint.
    We normalize the fields we need.
    """
    data = raw
    if isinstance(raw, dict) and "data" in raw and isinstance(raw["data"], dict):
        data = raw["data"]

    if not isinstance(data, dict):
        return {}

    first = data.get("firstName")
    last  = data.get("lastName")
    nick  = data.get("nickname")
    name  = pick_name(nick, first, last)

    pos_id = data.get("position")
    position = map_position(pos_id)

    altpos = map_alt_positions(data.get("alternativePositionIds", []))

    # club/league/nation: FUT.GG often gives "uniqueClubSlug":"112139-al-nassr"
    # Sometimes you'll get dicts in other endpoints, so we support both.
    def _lbl(block):
        if isinstance(block, dict):
            v = block.get("name") or block.get("slug")
            v = safe_text(v)
            return pretty_slug(v) if v else None
        if isinstance(block, str):
            v = safe_text(block)
            return pretty_slug(v) if v else None
        return None

    club   = _lbl(data.get("uniqueClubSlug")) or _lbl(data.get("club")) or _lbl(data.get("team"))
    league = _lbl(data.get("uniqueLeagueSlug")) or _lbl(data.get("league"))
    nation = _lbl(data.get("nation")) or _lbl(data.get("uniqueNationSlug")) or _lbl(data.get("country"))

    rating = None
    try:
        rv = data.get("overall") or data.get("rating") or data.get("overallRating") or data.get("ovr")
        rating = int(rv) if rv is not None else None
    except Exception:
        rating = None

    # "version" here = promo/program-ish label if available
    version = (
        data.get("program")
        or data.get("rarityName")
        or data.get("cardType")
        or data.get("version")
        or data.get("rarity")
    )
    if isinstance(version, dict):
        version = version.get("name") or version.get("label") or version.get("code")
    version = safe_text(version)

    image_url = build_image_url_from_api(data)

    return {
        "name": safe_text(name),
        "nickname": safe_text(nick),
        "first_name": safe_text(first),
        "last_name": safe_text(last),
        "rating": rating,
        "version": version,
        "position": safe_text(position),
        "altposition": safe_text(altpos),
        "club": safe_text(club),
        "league": safe_text(league),
        "nation": safe_text(nation),
        "image_url": safe_text(image_url),
    }

# ================== DISCOVERY (HTML) ================== #
# Matches anchors like: /players/265263-rachel-williams/26-50596911/
RX_CARD_ANCHOR = re.compile(
    rf'href=["\']/players/([0-9a-z\-]+)/{re.escape(GAME)}-(\d+)/["\']',
    re.IGNORECASE
)

# We'll also capture the nearest img alt inside the same anchor block
RX_ANCHOR_BLOCK = re.compile(
    rf'(<a[^>]+href=["\']/players/[0-9a-z\-]+/{re.escape(GAME)}-\d+/["\'][^>]*>.*?</a>)',
    re.IGNORECASE | re.DOTALL
)

RX_ALT = re.compile(r'<img[^>]+alt=["\']([^"\']+)["\']', re.IGNORECASE)

async def discover_cards_with_alt(session: aiohttp.ClientSession, urls: List[str]) -> Dict[int, dict]:
    """
    Return {card_id:int -> {slug, url, name?, rating?, rarity?}}
    """
    out: Dict[int, dict] = {}
    sem = asyncio.Semaphore(DISCOVERY_CONC)

    async def fetch_page(url: str):
        async with sem:
            html = await http_get_text(session, url)
            if not html:
                return
            doc = unescape(html)
            before = len(out)

            # block parse (better chance of grabbing alt)
            for block_m in RX_ANCHOR_BLOCK.finditer(doc):
                block = block_m.group(1)
                m = RX_CARD_ANCHOR.search(block)
                if not m:
                    continue
                slug = m.group(1)
                cid_s = m.group(2)
                try:
                    cid = int(cid_s)
                except Exception:
                    continue

                alt_m = RX_ALT.search(block)
                alt_text = alt_m.group(1).strip() if alt_m else ""
                alt_name, alt_rating, alt_rarity = parse_alt_text(alt_text)

                out[cid] = {
                    "card_id": cid,
                    "player_slug": slug,
                    "player_url": f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/",
                    "alt_name": alt_name,
                    "alt_rating": alt_rating,
                    "alt_rarity": alt_rarity,
                }

            log.info("🔎 %s: +%d cards (total %d)", url, len(out) - before, len(out))

    await asyncio.gather(*(fetch_page(u) for u in urls), return_exceptions=True)
    return out

async def discover_daily(session: aiohttp.ClientSession, pages: int) -> Dict[int, dict]:
    urls = []
    for base in LISTING_URLS:
        for p in range(1, pages + 1):
            urls.append(base.format(p))
    return await discover_cards_with_alt(session, urls)

async def discover_full(session: aiohttp.ClientSession, pages: int) -> Dict[int, dict]:
    urls = [FULL_BASE_URL.format(p) for p in range(1, pages + 1)]
    # fetch in windows so you don’t blow memory / connections
    chunk = 25
    all_out: Dict[int, dict] = {}
    for i in range(0, len(urls), chunk):
        part = urls[i:i+chunk]
        found = await discover_cards_with_alt(session, part)
        all_out.update(found)
    return all_out

# ================== UPSERT ================== #
async def upsert_discovered(conn: asyncpg.Connection, discovered: Dict[int, dict]) -> List[int]:
    """
    Insert new rows (and keep existing ones).
    Because your table may have NOT NULL name, we must provide name on insert.
    We use alt-name -> slug derived -> "Unknown <id>".
    Also store rarity from listing alt into `rarity` column (if it exists).
    """
    if not discovered:
        return []

    ids = list(discovered.keys())

    # existing ids (card_id is bigint/int)
    have_rows = await conn.fetch(
        "SELECT card_id FROM public.fut_players WHERE card_id = ANY($1::bigint[])",
        ids
    )
    have = {int(r["card_id"]) for r in have_rows}
    new_ids = [cid for cid in ids if cid not in have]
    if not new_ids:
        return []

    # detect if rarity column exists
    cols = {
        r["column_name"]
        for r in await conn.fetch("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
        """)
    }
    has_rarity_col = "rarity" in cols

    rows = []
    for cid in new_ids:
        d = discovered[cid]
        slug = d.get("player_slug")
        url = d.get("player_url") or f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/"
        name = d.get("alt_name") or fallback_name_from_slug(slug, cid)  # NOT NULL safe
        rating = d.get("alt_rating")
        rarity = d.get("alt_rarity")

        if has_rarity_col:
            rows.append((cid, slug, url, name, rating, rarity))
        else:
            rows.append((cid, slug, url, name, rating))

    if has_rarity_col:
        await conn.executemany(
            f"""
            INSERT INTO public.fut_players (card_id, player_slug, player_url, name, rating, rarity)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (card_id) DO NOTHING
            """,
            rows
        )
    else:
        await conn.executemany(
            f"""
            INSERT INTO public.fut_players (card_id, player_slug, player_url, name, rating)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (card_id) DO NOTHING
            """,
            rows
        )

    return new_ids

# ================== ENRICH (API) ================== #
async def enrich_card_ids(conn: asyncpg.Connection, card_ids: List[int]) -> int:
    """
    Enrich given card_ids from API, updating by card_id.
    Returns count attempted-to-write (not guaranteed successful per row).
    """
    if not card_ids:
        return 0

    # read existing slug/url so we don't overwrite with nulls
    rows = await conn.fetch(
        "SELECT card_id, player_slug, player_url FROM public.fut_players WHERE card_id = ANY($1::bigint[])",
        card_ids
    )
    existing = {int(r["card_id"]): (r["player_slug"], r["player_url"]) for r in rows}

    # single writer connection
    write_conn = await asyncpg.connect(DATABASE_URL)

    # Build UPDATE statement (only fill if new value present, otherwise keep existing)
    # We intentionally DO NOT overwrite `rarity` here — rarity is from listing alt.
    set_parts = [
        "position    = COALESCE($1, position)",
        "club        = COALESCE($2, club)",
        "nation      = COALESCE($3, nation)",
        "league      = COALESCE($4, league)",
        "name        = COALESCE($5, name)",
        "rating      = COALESCE($6, rating)",
        "version     = COALESCE($7, version)",
        "image_url   = COALESCE($8, image_url)",
        "player_slug = COALESCE($9, player_slug)",
        "player_url  = COALESCE($10, player_url)",
        "created_at  = COALESCE(created_at, NOW() AT TIME ZONE 'UTC')",
    ]
    args_count = 10

    if HAS_ALT_POS:
        set_parts.append(f"altposition = COALESCE(${args_count+1}, altposition)")
        args_count += 1
    if HAS_NICK:
        set_parts.append(f"nickname = COALESCE(${args_count+1}, nickname)")
        args_count += 1
    if HAS_FIRST:
        set_parts.append(f"first_name = COALESCE(${args_count+1}, first_name)")
        args_count += 1
    if HAS_LAST:
        set_parts.append(f"last_name = COALESCE(${args_count+1}, last_name)")
        args_count += 1

    sql = f"""
        UPDATE public.fut_players
        SET {", ".join(set_parts)}
        WHERE card_id = ${args_count+1}
    """

    sem = asyncio.Semaphore(CONCURRENCY)
    q: asyncio.Queue = asyncio.Queue()
    STOP = object()

    async def write_chunk(chunk: List[tuple]):
        try:
            await write_conn.executemany(sql, chunk)
        except Exception as e:
            log.error("❌ DB update failed for chunk of %d: %s", len(chunk), e)

    async def writer():
        batch: List[tuple] = []
        written = 0
        while True:
            item = await q.get()
            if item is STOP:
                if batch:
                    await write_chunk(batch)
                    written += len(batch)
                    log.info("💾 Enriched chunk of %d (running total %d)", len(batch), written)
                return written
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

        async def produce(cid: int):
            slug_existing, url_existing = existing.get(cid, (None, None))
            async with sem:
                meta = await fetch_meta_with_retry(session, cid)

            slug = slug_existing
            url = url_existing or (f"https://www.fut.gg/players/{slug}/{GAME}-{cid}/" if slug else f"https://www.fut.gg/players/{GAME}-{cid}/")

            # args in same order as sql builder
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
            if HAS_ALT_POS:
                args.append(meta.get("altposition"))
            if HAS_NICK:
                args.append(meta.get("nickname"))
            if HAS_FIRST:
                args.append(meta.get("first_name"))
            if HAS_LAST:
                args.append(meta.get("last_name"))

            args.append(cid)
            await q.put(tuple(args))

        writer_task = asyncio.create_task(writer())
        await asyncio.gather(*(produce(cid) for cid in card_ids), return_exceptions=True)
        await q.put(STOP)
        written = await writer_task

    await write_conn.close()
    log.info("✅ Enrichment done. Wrote %d row(s).", written)
    return written

async def enrich_missing(conn: asyncpg.Connection, limit: int = 5000) -> int:
    """
    Backfill any rows missing key API fields.
    """
    # note: card_id is bigint in your schema, so keep it numeric
    rows = await conn.fetch(
        """
        SELECT card_id
        FROM public.fut_players
        WHERE
            position IS NULL OR position = '' OR
            club IS NULL OR club = '' OR
            league IS NULL OR league = '' OR
            nation IS NULL OR nation = '' OR
            image_url IS NULL OR image_url = '' OR
            version IS NULL OR version = ''
        LIMIT $1
        """,
        limit
    )
    if not rows:
        return 0
    ids = [int(r["card_id"]) for r in rows]
    log.info("🧩 Enrich-missing: %d rows", len(ids))
    return await enrich_card_ids(conn, ids)

# ================== RUN MODES ================== #
async def run_full_build(conn: asyncpg.Connection, pages: int) -> None:
    log.info("🧱 FULL BUILD: scanning %d pages", pages)
    async with aiohttp.ClientSession() as session:
        discovered = await discover_full(session, pages)

    log.info("📦 FULL BUILD: discovered %d cards", len(discovered))
    added = await upsert_discovered(conn, discovered)
    log.info("🆕 FULL BUILD: inserted %d new rows", len(added))

    # Enrich new ones first (fast win)
    if added:
        await enrich_card_ids(conn, added)

    # Then backfill everything missing until done (in waves)
    while True:
        wrote = await enrich_missing(conn, limit=8000)
        if wrote <= 0:
            break

    log.info("✅ FULL BUILD complete")

async def run_daily_cycle(conn: asyncpg.Connection) -> None:
    log.info("🆕 DAILY: scanning %d pages", NEW_PAGES)
    async with aiohttp.ClientSession() as session:
        discovered = await discover_daily(session, NEW_PAGES)

    log.info("📦 DAILY: discovered %d cards", len(discovered))
    added = await upsert_discovered(conn, discovered)
    if added:
        log.info("🆕 DAILY: inserted %d new rows", len(added))
        await enrich_card_ids(conn, added)
    else:
        log.info("ℹ️ DAILY: no new players to insert")

    # always backfill any missing bits (small wave)
    await enrich_missing(conn, limit=3000)

async def run_once(mode: str, full_pages: int = FULL_PAGES_DEFAULT) -> None:
    conn = await get_db()
    try:
        await preflight(conn)
        if mode == "full":
            await run_full_build(conn, pages=full_pages)
        else:
            await run_daily_cycle(conn)
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
    log.info("🕖 Next daily check scheduled for %s", target.isoformat())
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
        await run_once(mode="daily")
        log.info("✅ Cycle complete")
        await sleep_until_19_uk()

    if health_task:
        health_task.cancel()

# ================== CLI ================== #
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run DAILY once and exit")
    ap.add_argument("--full", action="store_true", help="Run FULL build (all pages) once and exit")
    ap.add_argument("--full-pages", type=int, default=FULL_PAGES_DEFAULT, help="Pages to scan in FULL build")
    args = ap.parse_args()

    async def _runner():
        if args.full:
            await run_once(mode="full", full_pages=args.full_pages)
        elif args.now:
            await run_once(mode="daily")
        else:
            await main_loop()

    asyncio.run(_runner())