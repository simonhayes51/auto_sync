# scripts/sync_futgg_players.py
import os
import re
import time
import math
import random
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from typing import List, Dict, Optional, Tuple

# ---------------- Config ----------------
FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

# Requests session with friendlier headers
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": DEFAULT_UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/",
})

# Example image chunk contains ".../26-247333" or ".../25-247333"
CARD_ID_RE = re.compile(r"\b(\d{2})-(\d+)\b")


# ---------------- Parsing helpers ----------------
def parse_alt_text(alt_text: str) -> Tuple[str, Optional[int], str]:
    """
    FUT.GG <img alt="Erling Haaland - 91 - Gold Rare">
    Returns (name, rating, version)
    """
    if not alt_text:
        return "", None, ""
    parts = [p.strip() for p in alt_text.split("-")]
    if len(parts) >= 2 and parts[1].strip().isdigit():
        name = parts[0].strip()
        rating = int(parts[1].strip())
        version = "-".join(parts[2:]).strip() if len(parts) > 2 else ""
        return name, rating, version
    return alt_text.strip(), None, ""


def extract_card_id(img_url: str) -> Optional[str]:
    """
    Pull the numeric card_id from image URL parts like .../26-247333...
    Returns just the id (e.g. "247333")
    """
    if not img_url:
        return None
    m = CARD_ID_RE.search(img_url)
    if m:
        return m.group(2)  # numeric id
    # fallback: digits in filename
    tail = (img_url.split("/")[-1] or "").strip('"')
    digits = "".join(ch for ch in tail if ch.isdigit())
    return digits or None


def looks_like_block(html: str) -> bool:
    h = (html or "").lower()
    return (
        "just a moment" in h or
        "attention required" in h or
        "verify you are a human" in h or
        "cloudflare" in h or
        "cf-" in h
    )


def get_last_page_from_pager(soup: BeautifulSoup) -> Optional[int]:
    """
    Inspect pager links like ?page=123 and return the highest number.
    """
    nums = []
    for a in soup.select('a[href*="?page="]'):
        txt = (a.get_text() or "").strip()
        # some pagers use « 1 2 3 … 200 »
        if txt.isdigit():
            try:
                nums.append(int(txt))
            except ValueError:
                pass
        # also catch explicit page=X in href
        href = a.get("href", "")
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            nums.append(int(m.group(1)))
    if nums:
        return max(nums)
    return None


def parse_player_cards(soup: BeautifulSoup) -> List[Dict]:
    """
    Return a list of player dicts found on the page.
    """
    players = []
    # anchor with /players/ and an img[alt]
    for a in soup.select('a[href^="/players/"]'):
        img = a.select_one("img[alt]")
        if not img:
            continue

        alt_text = img.get("alt", "").strip()
        name, rating, version = parse_alt_text(alt_text)
        if not rating:
            continue

        href = a.get("href") or ""
        player_url = f"https://www.fut.gg{href}" if href.startswith("/") else href

        # slug: /players/<id>/<slug>/
        parts = [p for p in href.split("/") if p]
        player_slug = parts[2] if len(parts) >= 3 else None

        img_url = img.get("src") or img.get("data-src") or ""
        card_id = extract_card_id(img_url)
        if not card_id:
            continue

        players.append({
            "name": name,
            "rating": rating,
            "version": version,
            "image_url": img_url,
            "created_at": datetime.now(timezone.utc),
            "player_slug": player_slug,
            "player_url": player_url,
            "card_id": card_id,
        })
    return players


# ---------------- HTTP fetch with retry/backoff ----------------
def fetch_html_with_retry(url: str, attempts: int = 3, base_delay: float = 2.0) -> Optional[str]:
    """
    Fetch a URL with simple exponential backoff and block-page detection.
    Returns HTML text or None.
    """
    for i in range(1, attempts + 1):
        try:
            resp = SESSION.get(url, timeout=25)
            status_ok = (200 <= resp.status_code < 300)
            if status_ok and not looks_like_block(resp.text):
                return resp.text
            else:
                print(f"⛔ {url} → status={resp.status_code}, blocked={looks_like_block(resp.text)} (try {i}/{attempts})")
        except Exception as e:
            print(f"⚠️ {url} → exception {e} (try {i}/{attempts})")

        # backoff with jitter
        delay = base_delay * math.pow(1.6, i - 1) + random.random() * 0.75
        time.sleep(delay)

    return None


# ---------------- DB helpers ----------------
async def ensure_unique_index(conn: asyncpg.Connection):
    """
    Ensure a unique index exists on (name, rating, player_url) so ON CONFLICT works.
    """
    ddl = """
    CREATE UNIQUE INDEX IF NOT EXISTS fut_players_name_rating_url_uniq
      ON fut_players(name, rating, player_url);
    """
    await conn.execute(ddl)


async def upsert_players(conn: asyncpg.Connection, players: List[Dict]):
    if not players:
        return
    stmt = """
    INSERT INTO fut_players
        (name, rating, version, image_url, created_at, player_slug, player_url, card_id)
    VALUES
        ($1,   $2,     $3,      $4,        $5,         $6,          $7,         $8)
    ON CONFLICT (name, rating, player_url) DO UPDATE
    SET version     = EXCLUDED.version,
        image_url   = EXCLUDED.image_url,
        created_at  = EXCLUDED.created_at,
        player_slug = EXCLUDED.player_slug,
        card_id     = EXCLUDED.card_id;
    """
    await conn.executemany(stmt, [
        (
            p["name"], p["rating"], p["version"], p["image_url"],
            p["created_at"], p["player_slug"], p["player_url"], p["card_id"]
        ) for p in players
    ])


# ---------------- Main sync ----------------
async def sync_players():
    print(f"\n🚀 Starting FULL RESYNC at {datetime.now(timezone.utc)} UTC")

    # 1) Probe page 1 to discover the last page
    first_url = FUTGG_BASE_URL.format(1)
    html = fetch_html_with_retry(first_url, attempts=4)
    if not html:
        print("❌ Could not fetch the first page; aborting.")
        return

    soup = BeautifulSoup(html, "html.parser")
    last_page = get_last_page_from_pager(soup)
    if not last_page:
        # fallback if pager not visible for some reason — use large cap
        last_page = 3000
        print(f"🟨 Could not determine last page from pager; falling back to hard cap {last_page}")
    else:
        print(f"📄 Detected last page from pager: {last_page}")

    # 2) Parse page 1 immediately
    all_players: List[Dict] = []
    seen_urls = set()
    p1_players = parse_player_cards(soup)
    for p in p1_players:
        if p["player_url"] not in seen_urls:
            seen_urls.add(p["player_url"])
            all_players.append(p)
    print(f"📦 Page 1: +{len(p1_players)} (total {len(all_players)})")

    # 3) Iterate remaining pages
    # Safety: if pager is wrong/hidden sometimes, we still stop after a run of no-new pages.
    MAX_CONSEC_NO_NEW = 8
    consec_no_new = 0

    for page in range(2, last_page + 1):
        url = FUTGG_BASE_URL.format(page)
        print(f"🌐 Fetching: {url}")
        html = fetch_html_with_retry(url, attempts=3)

        if not html:
            print(f"⛔ Skipping page {page} (failed after retries).")
            consec_no_new += 1
            if consec_no_new >= MAX_CONSEC_NO_NEW:
                print("✅ Stopping due to consecutive no-new/failed pages (safety).")
                break
            continue

        if looks_like_block(html):
            print(f"⛔ Page {page} looked blocked; will not advance consec_no_new reset.")
            consec_no_new += 1
            if consec_no_new >= MAX_CONSEC_NO_NEW:
                print("✅ Stopping due to consecutive no-new/blocked pages (safety).")
                break
            continue

        soup = BeautifulSoup(html, "html.parser")
        players = parse_player_cards(soup)

        new_count = 0
        for p in players:
            if p["player_url"] in seen_urls:
                continue
            seen_urls.add(p["player_url"])
            all_players.append(p)
            new_count += 1

        print(f"📦 Page {page}: +{new_count} (total {len(all_players)})")

        if new_count == 0:
            consec_no_new += 1
        else:
            consec_no_new = 0

        # polite delay with jitter
        await asyncio.sleep(0.7 + random.random() * 0.6)

        if consec_no_new >= MAX_CONSEC_NO_NEW:
            print("✅ No new players across several pages — stopping early (safety).")
            break

    print(f"🔍 Total players fetched: {len(all_players)}")

    if not all_players:
        print("⚠️ No players fetched; aborting DB write.")
        return

    # 4) DB write
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    try:
        await ensure_unique_index(conn)
        await upsert_players(conn, all_players)
        print(f"🎯 Upserted {len(all_players)} players.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(sync_players())
