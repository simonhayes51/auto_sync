# scripts/sync_futgg_players.py
import os
import re
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import Optional, Tuple, List, Dict

# ---- Config -------------------------------------------------
FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
})

# Example image chunk contains ".../26-247333" or ".../25-247333"
IMG_SEASON_ID_RE = re.compile(r"/(?P<season>\d{2})-(?P<id>\d+)\b")


# ----------------- Helpers -----------------
def parse_alt_text(alt_text: str):
    """
    FUT.GG <img alt="Erling Haaland - 91 - Gold Rare">
    Returns (name, rating, version)
    """
    if not alt_text:
        return "", None, ""
    parts = [p.strip() for p in alt_text.split("-")]
    if len(parts) >= 2 and parts[1].isdigit():
        name = parts[0]
        rating = int(parts[1])
        version = "-".join(parts[2:]).strip() if len(parts) > 2 else ""
        return name, rating, version
    # fallback
    return alt_text.strip(), None, ""


def parse_img_season_id(url: str) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = IMG_SEASON_ID_RE.search(url)
    if not m:
        return None, None
    return m.group("season"), m.group("id")


def pick_from_srcset(img_tag) -> Optional[str]:
    """Prefer highest-res candidate from srcset; else src/data-src."""
    if not img_tag:
        return None
    srcset = (img_tag.get("srcset") or "").strip()
    if srcset:
        try:
            return srcset.split(",")[-1].split(" ")[0].strip()
        except Exception:
            pass
    return img_tag.get("src") or img_tag.get("data-src")


def href_card_id(href: str) -> Optional[str]:
    """
    /players/231443/erling-haaland/  -> "231443"
    /players/231443/ -> "231443"
    """
    if not href:
        return None
    parts = [p for p in href.split("/") if p]
    if len(parts) >= 2 and parts[0] == "players" and parts[1].isdigit():
        return parts[1]
    return None


def fetch_player_detail(player_url: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Load player detail page and return (image_url, image_alt) for the main card.
    We try <meta property="og:image"> first (URL only), then a prominent <img>.
    """
    try:
        r = SESSION.get(player_url, timeout=20)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")

        # 1) og:image (URL only)
        og = soup.select_one('meta[property="og:image"]')
        og_url = og.get("content") if og and og.get("content") else None

        # 2) main image tag (get URL + alt if possible)
        img = soup.select_one('img[alt][src*="-"]') or soup.find("img", src=True)
        img_url = None
        img_alt = None
        if img:
            img_url = pick_from_srcset(img) or img.get("src") or img.get("data-src")
            img_alt = img.get("alt")

        # Prefer explicit <img> (has alt); fallback to og:image for URL
        final_url = img_url or og_url
        final_alt = img_alt

        return final_url, final_alt
    except Exception:
        return None, None


# ----------------- Scraper -----------------
def fetch_players_from_page(page_number: int) -> List[Dict]:
    """
    Scrape the generic /players/ list page, but only keep FC26 cards.
    We follow each tile's link once to ensure we store a 26-image URL.
    """
    url = FUTGG_BASE_URL.format(page_number)
    print(f"🌐 Fetching: {url}")

    try:
        resp = SESSION.get(url, timeout=20)
    except Exception as e:
        print(f"⚠️ Request failed: {e}")
        return []

    if resp.status_code != 200:
        print(f"⚠️ Failed to fetch page {page_number}: status={resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Broad: anchors under /players/ with an <img alt=...> (your original approach)
    cards = soup.select('a[href^="/players/"]')

    players = []
    for a in cards:
        try:
            img = a.select_one("img[alt]") or a.find("img")
            alt_text = (img.get("alt", "").strip() if img else "") or ""
            tile_img_url = pick_from_srcset(img) if img else ""

            # player href + card id
            href = a.get("href") or ""
            player_url = f"https://www.fut.gg{href}" if href.startswith("/") else href
            parts = [p for p in href.split("/") if p]
            player_slug = parts[2] if len(parts) >= 3 else None
            card_id_from_href = href_card_id(href)

            # Load the player page to get the canonical image and confirm season
            detail_img_url, detail_img_alt = fetch_player_detail(player_url)
            if not detail_img_url:
                # if detail failed, skip (keeps DB clean)
                continue

            season, _ = parse_img_season_id(detail_img_url)
            if season != "26":
                # Not FC26 — skip it
                continue

            # prefer alt from detail page; fallback to tile alt
            alt_for_parse = (detail_img_alt or alt_text).strip()
            name, rating, version = parse_alt_text(alt_for_parse)
            if not rating:
                # if alt didn’t include rating, we can still keep name; but to match your original, skip
                continue

            # If we couldn't parse card_id from the image URL, fallback to href id
            _, card_id_from_img = parse_img_season_id(detail_img_url)
            card_id = card_id_from_img or card_id_from_href
            if not card_id:
                continue

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": detail_img_url,          # guaranteed 26-<id>.webp
                "created_at": datetime.now(timezone.utc),
                "player_slug": player_slug,
                "player_url": player_url,             # seasonless canonical path
                "card_id": card_id,
            })
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players


# ----------------- DB -----------------
async def ensure_unique_index(conn: asyncpg.Connection):
    """
    Ensure a unique index exists on (name, rating, player_url) so ON CONFLICT works.
    """
    ddl = """
    CREATE UNIQUE INDEX IF NOT EXISTS fut_players_name_rating_url_uniq
      ON fut_players(name, rating, player_url);
    """
    await conn.execute(ddl)


async def sync_players():
    print(f"\n🚀 Starting FULL RESYNC at {datetime.now(timezone.utc)} UTC")
    all_players = []
    page = 1
    empty_streak = 0

    # paginate until two consecutive empty pages (guards against a stray empty page)
    while True:
        players = fetch_players_from_page(page)
        if not players:
            empty_streak += 1
            if empty_streak >= 2:
                print("✅ Pagination complete.")
                break
        else:
            empty_streak = 0
            all_players.extend(players)
            print(f"📦 Page {page}: +{len(players)} (total {len(all_players)})")
        page += 1
        await asyncio.sleep(0.4)  # be nice to the site

    print(f"🔍 Total players fetched: {len(all_players)}")

    if not all_players:
        print("⚠️ No players fetched; aborting DB write.")
        return

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    try:
        # Make sure our ON CONFLICT target exists
        await ensure_unique_index(conn)

        # Upsert by (name, rating, player_url)
        stmt = """
        INSERT INTO fut_players
            (name, rating, version, image_url, created_at, player_slug, player_url, card_id)
        VALUES
            ($1,   $2,     $3,      $4,        $5,         $6,          $7,         $8)
        ON CONFLICT (name, rating, player_url) DO UPDATE
        SET version    = EXCLUDED.version,
            image_url  = EXCLUDED.image_url,
            created_at = EXCLUDED.created_at,
            player_slug= EXCLUDED.player_slug,
            card_id    = EXCLUDED.card_id;
        """

        # Batch insert for speed
        await conn.executemany(stmt, [
            (
                p["name"], p["rating"], p["version"], p["image_url"],
                p["created_at"], p["player_slug"], p["player_url"], p["card_id"]
            )
            for p in all_players
        ])
        print(f"🎯 Upserted {len(all_players)} players (FC26 only).")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(sync_players())
