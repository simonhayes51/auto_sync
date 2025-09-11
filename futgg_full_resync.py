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
SEASON = "26"
FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
DEF_API = "https://www.fut.gg/api/fut/player-item-definitions/{season}/{card_id}/"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Safari/537.36"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

# Match ".../26-247333.webp" (or 25-....) and capture season + id
IMG_SEASON_ID_RE = re.compile(r"/(?P<season>\d{2})-(?P<id>\d+)\b")


# ---- Helpers ------------------------------------------------
def parse_alt_text(alt_text: str) -> Tuple[str, Optional[int], str]:
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
    return alt_text.strip(), None, ""


def pick_from_srcset(img_tag) -> Optional[str]:
    """Prefer the highest-res candidate from srcset; else src/data-src."""
    if not img_tag:
        return None
    srcset = (img_tag.get("srcset") or "").strip()
    if srcset:
        try:
            return srcset.split(",")[-1].split(" ")[0].strip()
        except Exception:
            pass
    return img_tag.get("src") or img_tag.get("data-src")


def parse_img_season_id(url: str) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None
    m = IMG_SEASON_ID_RE.search(url)
    if not m:
        return None, None
    return m.group("season"), m.group("id")


def href_card_id(href: str) -> Optional[str]:
    """
    /players/231443/erling-haaland/  -> "231443"
    /players/231443/ -> "231443"
    """
    if not href:
        return None
    parts = [p for p in href.split("/") if p]
    # Expect: ['players', '<id>', '<slug?>']
    if len(parts) >= 2 and parts[0] == "players" and parts[1].isdigit():
        return parts[1]
    return None


def get_detail_image_url(player_url: str) -> Optional[str]:
    """
    Fetch player detail page; prefer <meta property="og:image"> then a prominent <img>.
    """
    try:
        r = SESSION.get(player_url, timeout=20)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        og = soup.select_one('meta[property="og:image"]')
        if og and og.get("content"):
            return og["content"]
        img = soup.select_one('img[alt][src*="-"]')
        if img:
            from_srcset = pick_from_srcset(img)
            if from_srcset:
                return from_srcset
            return img.get("src") or img.get("data-src")
    except Exception:
        return None
    return None


def is_fc26_by_def_api(card_id: str) -> bool:
    """
    Check FC26 definitions API. If 200, we treat this card_id as FC26.
    """
    try:
        url = DEF_API.format(season=SEASON, card_id=card_id)
        r = SESSION.get(url, timeout=15)
        return r.status_code == 200
    except Exception:
        return False


def fetch_players_from_page(page_number: int) -> List[Dict]:
    """
    Scrape the generic /players/ list page, but only return FC26 cards.
    Strategy:
      - iterate anchors under /players/
      - get card_id from href (authoritative)
      - verify FC26 via definitions API
      - ensure image URL is season 26 (fix via detail page if needed)
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

    # Generic anchors (seasonless)
    cards = soup.select('a[href^="/players/"]')

    players: List[Dict] = []
    for a in cards:
        try:
            href = a.get("href") or ""
            cid = href_card_id(href)
            if not cid:
                continue

            # Must be FC26:
            if not is_fc26_by_def_api(cid):
                continue

            img = a.select_one("img[alt]") or a.find("img")
            img_url = pick_from_srcset(img) if img else ""

            # If grid shows FC25 or unknown season, fix via detail page:
            season_in_img, _ = parse_img_season_id(img_url) if img_url else (None, None)
            player_url = f"https://www.fut.gg{href}" if href.startswith("/") else href

            if season_in_img != SEASON:
                fixed = get_detail_image_url(player_url)
                if fixed:
                    img_url = fixed
                    season_in_img, _ = parse_img_season_id(img_url)

            # If still not a 26 image, we keep the row (since FC26 is confirmed by API),
            # but we prefer to only store 26 images. If not 26, skip to be strict:
            if season_in_img != SEASON:
                # Strict mode: skip if we couldn't obtain a 26 image
                continue

            alt_text = (img.get("alt", "").strip() if img else "") or ""
            name, rating, version = parse_alt_text(alt_text)
            # If rating missing in alt, leave as None-safe skip
            if not rating:
                # Some tiles might not have rating in alt; still skip to keep DB clean
                continue

            # Optional slug
            parts = [p for p in href.split("/") if p]
            player_slug = parts[2] if len(parts) >= 3 else None

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,                # validated 26-<id>.webp
                "created_at": datetime.now(timezone.utc),
                "player_slug": player_slug,
                "player_url": player_url,            # seasonless URL (site’s canonical)
                "card_id": cid,
            })
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players


# ---- DB -----------------------------------------------------
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
    all_players: List[Dict] = []
    page = 1
    empty_streak = 0

    # paginate until two consecutive empty pages
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
        await asyncio.sleep(0.5)  # be nice to the site

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
        await ensure_unique_index(conn)
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
        await conn.executemany(stmt, [
            (
                p["name"], p["rating"], p["version"], p["image_url"],
                p["created_at"], p["player_slug"], p["player_url"], p["card_id"]
            )
            for p in all_players
        ])
        print(f"🎯 Upserted {len(all_players)} players (FC26 only, verified by API).")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(sync_players())
