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
Defaults to a small test range - set FUTBIN_PAGE_START/FUTBIN_PAGE_END to
cover more, or the full 1-848 once this is proven safe at scale.
"""
import os
import re
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

GAME = os.getenv("FUTBIN_GAME", "26")
PAGE_START = int(os.getenv("FUTBIN_PAGE_START", "1"))
PAGE_END = int(os.getenv("FUTBIN_PAGE_END", "3"))  # small default for safe testing
REQUEST_DELAY = float(os.getenv("FUTBIN_REQUEST_DELAY", "1.5"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}

IMG_ID_RE = re.compile(r"/players/p(\d+)\.")
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


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await ensure_new_columns(conn)
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

        total_rows = written = skipped_no_card_id = skipped_no_name = 0

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
                    try:
                        await conn.execute(upsert_sql, *row_args(r, cols))
                        written += 1
                    except Exception as e:
                        print(f"❌ upsert failed for card_id={r['card_id']} ({r.get('name')}): {e}", flush=True)

                print(
                    f"📦 page {page}/{PAGE_END}: {len(rows)} rows "
                    f"(running totals: written={written} no_card_id={skipped_no_card_id} no_name={skipped_no_name})",
                    flush=True,
                )
                if rows:
                    print(f"   sample row: {rows[0]}", flush=True)

                await asyncio.sleep(REQUEST_DELAY)

        print(
            f"✅ Done. pages={PAGE_START}-{PAGE_END} total_rows={total_rows} "
            f"written={written} no_card_id={skipped_no_card_id} no_name={skipped_no_name}",
            flush=True,
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
