"""
Crawls futbin.com's paginated player listing pages and upserts fut_players
directly - no per-card requests needed, since each listing row already
shows the PS/Xbox price and embeds our own fut.gg/EA card_id in the player
image URL (confirmed by inspecting real page markup), giving an exact
match with no fuzzy name/rating matching required. Each row also carries
position, club, nation, league, and an image URL, so this both refreshes
prices for cards we already have and fills in cards we're missing (our own
discovery pipeline has been broken since fut.gg started blocking scrapers).

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

# Candidate columns we can populate from a listing row, mapped to their
# extracted value's Python type. Only columns that actually exist in the
# live table get used - detected at startup, nothing hardcoded blindly.
CANDIDATE_COLUMNS = [
    "name", "rating", "version", "position", "altposition",
    "club", "nation", "league", "image_url",
    "price", "price_num", "price_updated_at",
]


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

    def _title_of(cls):
        a = row.find("a", class_=cls)
        if a:
            img = a.find("img")
            if img:
                return img.get("title")
        return None

    club = _title_of("table-player-club")
    nation = _title_of("table-player-nation")
    league = _title_of("table-player-league")

    ps_td = row.find("td", class_=re.compile(r"\bplatform-ps-only\b"))
    ps_price = None
    if ps_td:
        price_div = ps_td.find("div", class_=re.compile(r"\bprice\b"))
        if price_div:
            ps_price = _num(price_div.get_text(strip=True))

    return {
        "futbin_id": futbin_id, "slug": slug, "card_id": card_id,
        "name": name, "rating": rating, "version": version,
        "position": position, "altposition": altposition,
        "club": club, "nation": nation, "league": league,
        "image_url": image_url, "ps_price": ps_price,
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


async def detect_columns(conn: asyncpg.Connection) -> set:
    rows = await conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='fut_players'
        """
    )
    return {r["column_name"] for r in rows}


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
        available = await detect_columns(conn)
        if "card_id" not in available:
            raise RuntimeError("❌ fut_players has no card_id column - can't upsert")
        upsert_sql, cols = build_upsert_sql(available)
        print(f"📋 Writing columns: card_id, {', '.join(cols)}", flush=True)

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
