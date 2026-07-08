"""
Crawls futbin.com's paginated player listing pages and upserts fut_players
directly - no per-card requests needed, since each listing row already
shows the PS/Xbox price and embeds our own fut.gg/EA card_id in the player
image URL (confirmed by inspecting real page markup), giving an exact
match with no fuzzy name/rating matching required. Each row also carries
position, club/nation/league (name + logo image), rating, price, foot,
skill moves, weak foot, the 6 main stats (pace/shooting/passing/dribbling/
defending/physicality), popularity, height/weight/body type, and
accelerate type - confirmed by inspecting a full real row's markup - so
this both refreshes prices for cards we already have and fills in cards
we're missing (our own discovery pipeline has been broken since fut.gg
started blocking scrapers).

Uses INSERT ... ON CONFLICT (card_id) DO UPDATE - no need to clear the
table first, card_id's existing unique constraint already prevents
duplicates.

futbin has ~848 listing pages total (confirmed via its own pagination UI).
Defaults to the full 1-848 range now that this is proven safe at scale -
set FUTBIN_PAGE_START/FUTBIN_PAGE_END to override with a smaller range
for local testing.

Deployed as the Railway `worker` process (see Procfile). Since Railway
workers are expected to run indefinitely rather than exit, this runs the
full crawl once daily at a fixed time (19:00 UK, matching the old
futgg_auto_sync.py's schedule) and sleeps between runs - see main_loop()
at the bottom. Pass --now to run a single crawl immediately and exit,
which is useful for local testing.
"""
import os
import re
import sys
import signal
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from aiohttp import web  # health server
from bs4 import BeautifulSoup

from monitoring import heartbeat, alert

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

GAME = os.getenv("FUTBIN_GAME", "26")
PAGE_START = int(os.getenv("FUTBIN_PAGE_START", "1"))
PAGE_END = int(os.getenv("FUTBIN_PAGE_END", "848"))  # full listing range
REQUEST_DELAY = float(os.getenv("FUTBIN_REQUEST_DELAY", "1.5"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}

# futbin's cutout image filename is /players/p{id}.png for special cards
# but /players/{id}.png (no "p") for base gold/silver/bronze cards - the "p"
# is optional here so base cards aren't silently skipped for having no
# card_id (they were: every base/common row failed this match, so this
# script never wrote or refreshed them, leaving them on whatever stale
# image_url/player_url an old fut.gg-based crawl had set).
IMG_ID_RE = re.compile(r"/players/p?(\d+)\.")
FUTBIN_HREF_RE = re.compile(r"/player/(\d+)/([^/\"'?]+)")
FOOT_RE = re.compile(r"foot-(right|left)", re.I)
LEADING_INT_RE = re.compile(r"^\s*(\d+)")
HEIGHT_RE = re.compile(r"(\d+)\s*cm", re.I)
WEIGHT_RE = re.compile(r"(\d+)\s*kg", re.I)

# Candidate columns we can populate from a listing row, mapped to their
# extracted value's Python type. Only columns that actually exist in the
# live table get used - detected at startup, nothing hardcoded blindly.
CANDIDATE_COLUMNS = [
    "name", "rating", "version", "position", "altposition",
    "club", "nation", "league", "club_image", "nation_image", "league_image",
    "image_url", "player_url",
    "foot", "skill_moves", "weak_foot",
    "pace", "shooting", "passing", "dribbling", "defending", "physicality",
    "popularity", "height_cm", "weight_kg", "body_type", "accelerate_type",
    "futbin_rating",
    "price", "price_num", "price_updated_at",
]

# New optional columns this version adds - created automatically (as NULL-
# able extras, so this is always safe) if the live table doesn't have them
# yet, rather than requiring a separate manual migration step.
NEW_COLUMNS_DDL = {
    "club_image": "TEXT",
    "nation_image": "TEXT",
    "league_image": "TEXT",
    "foot": "TEXT",
    "skill_moves": "SMALLINT",
    "weak_foot": "SMALLINT",
    "pace": "SMALLINT",
    "shooting": "SMALLINT",
    "passing": "SMALLINT",
    "dribbling": "SMALLINT",
    "defending": "SMALLINT",
    "physicality": "SMALLINT",
    "popularity": "INTEGER",
    "height_cm": "SMALLINT",
    "weight_kg": "SMALLINT",
    "body_type": "TEXT",
    "accelerate_type": "TEXT",
    "futbin_rating": "NUMERIC(4,1)",
}


def _num(txt: str) -> int:
    if not txt:
        return 0
    t = txt.lower().replace(",", "").strip()
    if t.endswith("m"):
        try:
            return int(float(t[:-1]) * 1_000_000)
        except Exception:
            return 0
    if t.endswith("k"):
        try:
            return int(float(t[:-1]) * 1_000)
        except Exception:
            return 0
    m = re.search(r"\d+(\.\d+)?", t)
    return int(float(m.group(0))) if m else 0


def _stat_value(td):
    if not td:
        return None
    div = td.find("div", class_=re.compile(r"\btable-key-stats\b"))
    if not div:
        return None
    txt = div.get_text(strip=True)
    return int(txt) if txt.isdigit() else None


def _star_count(td):
    if not td:
        return None
    m = LEADING_INT_RE.match(td.get_text(strip=True))
    return int(m.group(1)) if m else None


def parse_row(row):
    name_tag = row.find("a", class_="table-player-name")
    name = name_tag.get_text(strip=True) if name_tag else None

    futbin_id = slug = None
    if name_tag and name_tag.get("href"):
        m = FUTBIN_HREF_RE.search(name_tag["href"])
        if m:
            futbin_id, slug = m.group(1), m.group(2)

    card_id = None
    image_url = None
    for img in row.find_all("img"):
        src = img.get("src", "")
        m = IMG_ID_RE.search(src)
        if m:
            card_id = int(m.group(1))
            image_url = src
            break

    rating_tag = row.find("div", class_="rating-square")
    rating_txt = rating_tag.get_text(strip=True) if rating_tag else ""
    rating = int(rating_txt) if rating_txt.isdigit() else None

    rev_tag = row.find("div", class_="table-player-revision")
    version = rev_tag.get_text(strip=True) if rev_tag else None

    pos_td = row.find("td", class_="table-pos")
    position = altposition = None
    if pos_td:
        main_div = pos_td.find("div", class_="table-pos-main")
        if main_div:
            span = main_div.find("span")
            position = span.get_text(strip=True) if span else None
        alt_div = pos_td.find("div", class_=re.compile(r"\bxs-font\b"))
        if alt_div:
            altposition = alt_div.get_text(strip=True) or None

    def _name_and_image_of(cls):
        a = row.find("a", class_=cls)
        if a:
            img = a.find("img")
            if img:
                return img.get("title"), img.get("src")
        return None, None

    club, club_image = _name_and_image_of("table-player-club")
    nation, nation_image = _name_and_image_of("table-player-nation")
    league, league_image = _name_and_image_of("table-player-league")

    ps_td = row.find("td", class_=re.compile(r"\bplatform-ps-only\b"))
    ps_price = None
    if ps_td:
        price_div = ps_td.find("div", class_=re.compile(r"\bprice\b"))
        if price_div:
            ps_price = _num(price_div.get_text(strip=True))

    foot = None
    foot_td = row.find("td", class_="table-foot")
    if foot_td:
        img = foot_td.find("img")
        if img:
            m = FOOT_RE.search(img.get("src", ""))
            if m:
                foot = m.group(1).capitalize()

    skill_moves = _star_count(row.find("td", class_="table-skills"))
    weak_foot = _star_count(row.find("td", class_="table-weak-foot"))

    pace = _stat_value(row.find("td", class_="table-pace"))
    shooting = _stat_value(row.find("td", class_="table-shooting"))
    passing = _stat_value(row.find("td", class_="table-passing"))
    dribbling = _stat_value(row.find("td", class_="table-dribbling"))
    defending = _stat_value(row.find("td", class_="table-defending"))
    physicality = _stat_value(row.find("td", class_="table-physicality"))

    popularity = None
    pop_td = row.find("td", class_="table-popularity")
    if pop_td:
        txt = pop_td.get_text(strip=True)
        popularity = int(txt) if txt.isdigit() else None

    height_cm = weight_kg = body_type = accelerate_type = None
    height_td = row.find("td", class_="table-height")
    if height_td:
        centered_divs = height_td.find_all("div", class_="text-center")
        if len(centered_divs) > 0:
            m = HEIGHT_RE.search(centered_divs[0].get_text(strip=True))
            if m:
                height_cm = int(m.group(1))
        if len(centered_divs) > 1:
            body_a = centered_divs[1].find("a")
            if body_a:
                body_type = body_a.get_text(strip=True) or None
            m = WEIGHT_RE.search(centered_divs[1].get_text(" ", strip=True))
            if m:
                weight_kg = int(m.group(1))
        accel_a = height_td.find("a", href=re.compile(r"[?&]accelerate="))
        if accel_a:
            accelerate_type = accel_a.get_text(strip=True) or None

    futbin_rating = None
    rating_span = row.find("span", class_=re.compile(r"\bfutbin-rating-tag\b"))
    if rating_span:
        txt = rating_span.get_text(strip=True)
        try:
            futbin_rating = float(txt)
        except ValueError:
            pass

    # We don't have fut.gg's own URL scheme for cards sourced from futbin,
    # but player_url is NOT NULL in the schema - futbin's own page is a
    # genuinely valid, dereferenceable URL for this exact card, not a
    # meaningless placeholder.
    player_url = f"https://www.futbin.com/{GAME}/player/{futbin_id}/{slug}" if futbin_id and slug else None

    return {
        "futbin_id": futbin_id, "slug": slug, "card_id": card_id,
        "name": name, "rating": rating, "version": version,
        "position": position, "altposition": altposition,
        "club": club, "nation": nation, "league": league,
        "club_image": club_image, "nation_image": nation_image, "league_image": league_image,
        "image_url": image_url, "player_url": player_url, "ps_price": ps_price,
        "foot": foot, "skill_moves": skill_moves, "weak_foot": weak_foot,
        "pace": pace, "shooting": shooting, "passing": passing,
        "dribbling": dribbling, "defending": defending, "physicality": physicality,
        "popularity": popularity, "height_cm": height_cm, "weight_kg": weight_kg,
        "body_type": body_type, "accelerate_type": accelerate_type,
        "futbin_rating": futbin_rating,
    }


async def fetch_page(session: aiohttp.ClientSession, page: int):
    url = f"https://www.futbin.com/{GAME}/players?page={page}"
    async with session.get(url, headers=HEADERS, timeout=30) as resp:
        if resp.status != 200:
            print(f"⚠️ page {page} → status {resp.status}", flush=True)
            return []
        html = await resp.text()

    soup = BeautifulSoup(html, "html.parser")
    rows = soup.find_all("tr", class_="player-row")
    return [parse_row(r) for r in rows]


async def ensure_new_columns(conn: asyncpg.Connection) -> None:
    """Add this version's extra stat/image columns if the live table
    predates them. All nullable with no default, so always safe to add."""
    for col, ddl in NEW_COLUMNS_DDL.items():
        await conn.execute(f"ALTER TABLE fut_players ADD COLUMN IF NOT EXISTS {col} {ddl}")


async def detect_columns(conn: asyncpg.Connection) -> set:
    rows = await conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fut_players'
        """
    )
    return {r["column_name"] for r in rows}


async def find_unhandled_not_null_columns(conn: asyncpg.Connection, cols_we_write: set) -> list:
    """Any NOT NULL column with no default that we don't populate would
    fail exactly like player_url just did - catch all of them upfront
    instead of discovering them one failed insert at a time."""
    rows = await conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fut_players'
          AND is_nullable = 'NO' AND column_default IS NULL
          AND column_name <> 'card_id'
        """
    )
    required = {r["column_name"] for r in rows}
    return sorted(required - cols_we_write)


def build_upsert_sql(available: set):
    """Only touch columns that exist. price_updated_at is always NOW(),
    never a bound parameter. card_id is the conflict target and always
    included as the last bound parameter."""
    cols = [c for c in CANDIDATE_COLUMNS if c in available and c != "price_updated_at"]
    has_updated_at = "price_updated_at" in available

    insert_cols = ["card_id"] + cols + (["price_updated_at"] if has_updated_at else [])
    placeholders = [f"${i+1}" for i in range(len(cols) + 1)]  # +1 for card_id
    insert_values = ["$1"] + placeholders[1:] + (["NOW()"] if has_updated_at else [])

    update_parts = [f"{c} = EXCLUDED.{c}" for c in cols]
    if has_updated_at:
        update_parts.append("price_updated_at = NOW()")

    sql = f"""
        INSERT INTO fut_players ({', '.join(insert_cols)})
        VALUES ({', '.join(insert_values)})
        ON CONFLICT (card_id) DO UPDATE SET {', '.join(update_parts)}
    """
    return sql, cols


def row_args(row: dict, cols: list):
    """Build the bound-parameter list in the same order as `cols`, card_id first."""
    args = [row["card_id"]]
    for c in cols:
        if c == "price":
            args.append(str(row["ps_price"]) if row["ps_price"] is not None else None)
        elif c == "price_num":
            args.append(row["ps_price"])
        elif c == "rating":
            args.append(row["rating"])
        else:
            args.append(row.get(c))
    return args


async def ensure_identity_log(conn: asyncpg.Connection) -> None:
    """A card_id whose stored name/rating/version changes between two
    crawls is a strong signal it used to mean a DIFFERENT card (a resolved
    card_id collision - see the card_id_collisions handling above - is the
    known way this happens, but this catches it regardless of cause).
    That matters because sales_history/bin_history are keyed on this same
    numeric card_id and are never auto-deleted: any rows scraped before the
    identity changed are now silently misattributed to whatever card this
    id currently means (confirmed live: a 97 Star Performer's real-money
    price sat next to a ~2.3k median sourced from sales that actually
    belonged to the card this id used to be).

    This only LOGS the change - it deliberately does not delete anything.
    Auto-deleting sales_history on a heuristic match risks destroying
    real history on a false positive (e.g. futbin renaming a version
    label); a human should review before purging using this log as the
    cutoff timestamp."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS card_identity_changes (
            id BIGSERIAL PRIMARY KEY,
            card_id BIGINT NOT NULL,
            old_name TEXT, old_rating INTEGER, old_version TEXT,
            new_name TEXT, new_rating INTEGER, new_version TEXT,
            detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_card_identity_changes_card ON card_identity_changes(card_id, detected_at)"
    )


def _identity_changed(prior: dict, row: dict) -> bool:
    if prior is None:
        return False
    # Rating can legitimately tick up/down via in-form upgrades on some
    # card types - name and version are the reliable signals that this
    # card_id now refers to a genuinely different card.
    #
    # Confirmed live: an exact, case-sensitive comparison here produced
    # false positives for the well-known "normal" vs "Normal" version
    # casing split (see _GOLD_RARE_WHERE's comment elsewhere in this repo -
    # two different crawl eras wrote different casing for the same
    # ordinary-gold version). Combined with the caller skipping the write
    # on a detected change, that meant ordinary gold cards whose stored
    # casing didn't match today's crawl silently stopped getting price
    # updates - a false "collision" on nothing but casing, not a real
    # identity change. Normalize case/whitespace before comparing so this
    # only fires on an actual different name or version.
    def _norm(v):
        v = (v or "").strip().lower()
        return v or None

    return _norm(row.get("name")) != _norm(prior.get("name")) or _norm(row.get("version")) != _norm(
        prior.get("version")
    )


async def crawl_once():
    """Run one full crawl of PAGE_START..PAGE_END and upsert everything found.

    Opens and closes its own DB connection rather than holding one open for
    the process's whole lifetime, since main_loop() only calls this once a
    day - a connection held open across a ~24h idle gap is exactly the kind
    of thing that gets silently dropped by the DB/host and would otherwise
    fail confusingly on the next run.
    """
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await ensure_new_columns(conn)
        await ensure_identity_log(conn)
        existing_identity = {
            r["card_id"]: {"name": r["name"], "rating": r["rating"], "version": r["version"]}
            for r in await conn.fetch("SELECT card_id, name, rating, version FROM fut_players")
        }
        available = await detect_columns(conn)
        if "card_id" not in available:
            raise RuntimeError("❌ fut_players has no card_id column - can't upsert")
        upsert_sql, cols = build_upsert_sql(available)
        print(f"📋 Writing columns: card_id, {', '.join(cols)}", flush=True)

        missing_not_null = await find_unhandled_not_null_columns(conn, set(cols) | {"card_id"})
        if missing_not_null:
            raise RuntimeError(
                f"❌ fut_players has NOT NULL column(s) with no default that we don't populate: "
                f"{missing_not_null} - inserts for new cards will fail on every row. "
                f"Add handling for these before running the real crawl."
            )

        total_rows = written = skipped_no_card_id = skipped_no_name = card_id_collisions = identity_changes = 0

        # card_id is extracted purely from a CDN image filename
        # (IMG_ID_RE), with no cross-check against anything else. On a
        # promo release day, if two distinct new cards briefly share the
        # same placeholder/"coming soon" artwork before their real cutouts
        # are uploaded, both rows extract the SAME numeric card_id even
        # though they're genuinely different cards (e.g. reported live: a
        # 97 Star Performer and a 97 TOTS of the same player, one visibly
        # replacing the other after a re-crawl). Since the upsert is
        # ON CONFLICT (card_id) DO UPDATE, that collision would silently
        # overwrite one card's entire row with the other's data - not a
        # skipped row, an actively destroyed one.
        #
        # futbin_id (parsed from the player-name link's own href, entirely
        # independent of any image) is the cross-check: two rows can only
        # legitimately share a card_id if they're the same real card, which
        # means the same futbin_id too. Track (card_id -> futbin_id) seen
        # so far this run; a second row claiming an already-seen card_id
        # under a DIFFERENT futbin_id is a genuine collision - skip writing
        # it (don't stomp whatever this run already wrote for that
        # card_id) and log it loudly rather than silently losing a card.
        seen_card_ids: dict = {}

        async with aiohttp.ClientSession() as session:
            for page in range(PAGE_START, PAGE_END + 1):
                rows = await fetch_page(session, page)
                total_rows += len(rows)

                for r in rows:
                    if r["card_id"] is None:
                        skipped_no_card_id += 1
                        continue
                    if "name" in cols and not r.get("name"):
                        # name is NOT NULL in the schema for every card_id
                        # we've seen so far - skip anything without one
                        # rather than risk an insert failure mid-batch.
                        skipped_no_name += 1
                        continue

                    cid = r["card_id"]
                    fbid = r.get("futbin_id")
                    prior = seen_card_ids.get(cid)
                    if prior is not None and fbid is not None and prior["futbin_id"] is not None and prior["futbin_id"] != fbid:
                        card_id_collisions += 1
                        print(
                            f"⚠️ card_id collision: {cid} claimed by BOTH "
                            f"futbin_id={prior['futbin_id']} ({prior['name']!r} {prior['rating']} {prior['version']}) "
                            f"AND futbin_id={fbid} ({r.get('name')!r} {r.get('rating')} {r.get('version')}) - "
                            f"keeping the first, skipping this one so it doesn't overwrite it. "
                            f"Likely a shared placeholder image on a brand-new promo card; should self-resolve "
                            f"once futbin uploads distinct artwork for each.",
                            flush=True,
                        )
                        continue
                    if prior is None:
                        seen_card_ids[cid] = {
                            "futbin_id": fbid, "name": r.get("name"),
                            "rating": r.get("rating"), "version": r.get("version"),
                        }

                    old_identity = existing_identity.get(cid)
                    if _identity_changed(old_identity, r):
                        identity_changes += 1
                        print(
                            f"⚠️ card_id {cid} identity change BLOCKED: keeping existing "
                            f"{old_identity['name']!r} {old_identity['rating']} {old_identity['version']!r}, "
                            f"NOT overwriting with {r.get('name')!r} {r.get('rating')} {r.get('version')!r} - "
                            f"this is the same 'shared placeholder image' pattern as same-run collisions, just "
                            f"against an ALREADY-ESTABLISHED card from a previous crawl instead of a same-day one "
                            f"(confirmed live: a brand-new special silently replaced an established base gold, "
                            f"which this same identity-change signal was only logging, not preventing, before). "
                            f"Logged to card_identity_changes for review - if this turns out to be a genuine "
                            f"rename/correction rather than a collision, it needs a manual fix, not an automatic "
                            f"overwrite.",
                            flush=True,
                        )
                        try:
                            await conn.execute(
                                """
                                INSERT INTO card_identity_changes
                                    (card_id, old_name, old_rating, old_version, new_name, new_rating, new_version)
                                VALUES ($1,$2,$3,$4,$5,$6,$7)
                                """,
                                cid, old_identity["name"], old_identity["rating"], old_identity["version"],
                                r.get("name"), r.get("rating"), r.get("version"),
                            )
                        except Exception as e:
                            print(f"❌ failed to log identity change for card_id={cid}: {e}", flush=True)
                        continue

                    try:
                        await conn.execute(upsert_sql, *row_args(r, cols))
                        written += 1
                    except Exception as e:
                        print(f"❌ upsert failed for card_id={r['card_id']} ({r.get('name')}): {e}", flush=True)

                print(
                    f"📦 page {page}/{PAGE_END}: {len(rows)} rows "
                    f"(running totals: written={written} no_card_id={skipped_no_card_id} "
                    f"no_name={skipped_no_name} collisions={card_id_collisions} "
                    f"identity_changes={identity_changes})",
                    flush=True,
                )
                if rows:
                    print(f"   sample row: {rows[0]}", flush=True)

                await asyncio.sleep(REQUEST_DELAY)

        if card_id_collisions:
            print(
                f"⚠️ {card_id_collisions} card_id collision(s) detected this crawl - "
                f"see warnings above for which cards were affected.",
                flush=True,
            )
        if identity_changes:
            print(
                f"⚠️ {identity_changes} card_id identity change(s) detected this crawl - "
                f"see warnings above; query card_identity_changes for the full list and use "
                f"detected_at as a cutoff before purging any contaminated sales_history/bin_history.",
                flush=True,
            )

        print(
            f"✅ Done. pages={PAGE_START}-{PAGE_END} total_rows={total_rows} "
            f"written={written} no_card_id={skipped_no_card_id} no_name={skipped_no_name} "
            f"collisions={card_id_collisions} identity_changes={identity_changes}",
            flush=True,
        )
        # A crawl that parsed zero rows across every page almost certainly
        # means futbin changed markup (or is blocking us) - page a human.
        crawl_ok = total_rows > 0
        await heartbeat(
            conn,
            "futbin_full_sync",
            ok=crawl_ok,
            detail=(
                f"pages={PAGE_START}-{PAGE_END} rows={total_rows} written={written}"
                + (f" identity_changes={identity_changes}" if identity_changes else "")
                + (f" collisions={card_id_collisions}" if card_id_collisions else "")
            ),
        )
        if not crawl_ok:
            await alert(
                "futbin_full_sync: full crawl parsed **0 rows** - futbin markup change or "
                "block? Catalog + daily prices are no longer refreshing."
            )
        elif card_id_collisions:
            # Not a pipeline outage, but real data loss (one card's row was
            # about to overwrite another's) - worth a page, not just a log
            # line nobody will read until someone reports a missing card.
            await alert(
                f"futbin_full_sync: {card_id_collisions} card_id collision(s) this crawl - "
                "two different cards resolved to the same card_id (likely a shared placeholder "
                "image on a brand-new promo release). See worker logs for which cards; affected "
                "card(s) were skipped rather than overwritten, so check if any are still missing "
                "from fut_players once futbin uploads distinct artwork."
            )
    finally:
        await conn.close()


# ================== HEALTH & SCHEDULER ================== #
# Railway's `worker` process type expects a long-running process, not a
# one-shot script that exits - a worker that exits immediately (or once a
# day) looks crashed/restart-looping from Railway's perspective. This loop
# runs a full crawl once daily and sleeps in between, following the same
# sleep_until_19_uk()/health-server pattern futgg_auto_sync.py used.
async def start_health():
    app = web.Application()
    app.add_routes([
        web.get("/", lambda _: web.Response(text="OK")),
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
    print(f"🕖 Next crawl at {target.isoformat()}", flush=True)
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
        try:
            await crawl_once()
        except Exception as e:
            print(f"❌ crawl_once() failed, will retry at next scheduled time: {e}", flush=True)
        await sleep_until_19_uk()
    if health:
        health.cancel()


if __name__ == "__main__":
    if "--now" in sys.argv:
        # Single crawl, then exit - for local testing (mirrors futgg_auto_sync.py's --now flag).
        asyncio.run(crawl_once())
    else:
        asyncio.run(main_loop())
