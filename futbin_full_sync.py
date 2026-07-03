"""
Crawls futbin.com's paginated player listing pages and updates fut_players
prices directly - no per-card requests needed, since each listing row
already shows the PS/Xbox price and embeds our own fut.gg/EA card_id in
the player image URL (confirmed by inspecting real page markup), giving
an exact match with no fuzzy name/rating matching required.

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
    for img in row.find_all("img"):
        m = IMG_ID_RE.search(img.get("src", ""))
        if m:
            card_id = int(m.group(1))
            break

    rating_tag = row.find("div", class_="rating-square")
    rating_txt = rating_tag.get_text(strip=True) if rating_tag else ""
    rating = int(rating_txt) if rating_txt.isdigit() else None

    rev_tag = row.find("div", class_="table-player-revision")
    version = rev_tag.get_text(strip=True) if rev_tag else None

    ps_td = row.find("td", class_=re.compile(r"\bplatform-ps-only\b"))
    ps_price = None
    if ps_td:
        price_div = ps_td.find("div", class_=re.compile(r"\bprice\b"))
        if price_div:
            ps_price = _num(price_div.get_text(strip=True))

    return {
        "futbin_id": futbin_id,
        "slug": slug,
        "card_id": card_id,
        "name": name,
        "rating": rating,
        "version": version,
        "ps_price": ps_price,
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


async def detect_price_columns(conn: asyncpg.Connection):
    cols = {
        r["column_name"]
        for r in await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
            """
        )
    }
    return {
        "has_price": "price" in cols,
        "has_price_num": "price_num" in cols,
        "has_price_updated_at": "price_updated_at" in cols,
    }


async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        cols = await detect_price_columns(conn)
        set_parts, arg_kinds, idx = [], [], 1
        if cols["has_price"]:
            set_parts.append(f"price = ${idx}"); arg_kinds.append("text"); idx += 1
        if cols["has_price_num"]:
            set_parts.append(f"price_num = ${idx}"); arg_kinds.append("num"); idx += 1
        if cols["has_price_updated_at"]:
            set_parts.append("price_updated_at = NOW()")
        if not set_parts:
            raise RuntimeError("❌ fut_players has none of price/price_num/price_updated_at columns")
        update_sql = f"UPDATE fut_players SET {', '.join(set_parts)} WHERE card_id = ${idx}"

        existing_ids = {
            int(r["card_id"]) for r in await conn.fetch("SELECT card_id FROM fut_players WHERE card_id IS NOT NULL")
        }
        print(f"📊 {len(existing_ids)} cards already in our DB", flush=True)

        total_rows = matched = updated = no_price = unmatched = no_card_id = 0

        async with aiohttp.ClientSession() as session:
            for page in range(PAGE_START, PAGE_END + 1):
                rows = await fetch_page(session, page)
                total_rows += len(rows)

                for r in rows:
                    if r["card_id"] is None:
                        no_card_id += 1
                        continue
                    if r["card_id"] not in existing_ids:
                        unmatched += 1
                        continue
                    matched += 1
                    if r["ps_price"] is None:
                        no_price += 1
                        continue
                    args = [str(r["ps_price"]) if k == "text" else int(r["ps_price"]) for k in arg_kinds]
                    args.append(r["card_id"])
                    await conn.execute(update_sql, *args)
                    updated += 1

                print(
                    f"📦 page {page}/{PAGE_END}: {len(rows)} rows "
                    f"(running totals: matched={matched} updated={updated} unmatched={unmatched} no_card_id={no_card_id})",
                    flush=True,
                )
                if rows:
                    print(f"   sample row: {rows[0]}", flush=True)

                await asyncio.sleep(REQUEST_DELAY)

        print(
            f"✅ Done. pages={PAGE_START}-{PAGE_END} total_rows={total_rows} "
            f"matched={matched} updated={updated} no_price={no_price} "
            f"unmatched(not in our DB)={unmatched} no_card_id_found={no_card_id}",
            flush=True,
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
