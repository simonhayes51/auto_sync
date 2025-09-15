# scripts/futgg_new_players_only.py
import os
import re
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ---- Config -------------------------------------------------
FUTGG_BASE_URL = "https://www.fut.gg/players/new/?page={}"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Safari/537.36"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

# Example image chunk contains ".../26-247333" or ".../25-247333"
CARD_ID_RE = re.compile(r"\b(\d{2})-(\d+)\b")

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

def extract_card_id(img_url: str):
    """
    Pull the numeric card_id from image URL parts like .../26-247333...
    Returns just the id (e.g. "247333")
    """
    if not img_url:
        return None
    m = CARD_ID_RE.search(img_url)
    if m:
        return m.group(2)  # numeric id
    # fallback: take digits from the filename tail
    tail = (img_url.split("/")[-1] or "").strip('"')
    digits = "".join(ch for ch in tail if ch.isdigit())
    return digits or None

def fetch_players_from_page(page_number: int):
    """
    Scrape one fut.gg list page.
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

    # Be resilient to Tailwind class churn: anchor with /players/ path and an <img alt=...>
    cards = soup.select('a[href^="/players/"]')

    players = []
    for a in cards:
        try:
            img = a.select_one("img[alt]")
            if not img:
                continue

            alt_text = img.get("alt", "").strip()
            img_url = img.get("src") or img.get("data-src") or ""
            name, rating, version = parse_alt_text(alt_text)
            if not rating:
                continue

            href = a.get("href") or ""
            player_url = f"https://www.fut.gg{href}" if href.startswith("/") else href

            # slug: /players/<id>/<slug>/
            parts = [p for p in href.split("/") if p]
            player_slug = parts[2] if len(parts) >= 3 else None

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
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players

async def sync_new_players_only():
    print(f"\n🚀 Starting NEW PLAYERS SYNC at {datetime.now(timezone.utc)} UTC")
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
        await asyncio.sleep(0.6)  # be nice to the site

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
        # Get existing card_ids to filter out duplicates
        existing_card_ids = set()
        existing_rows = await conn.fetch("SELECT card_id FROM fut_players WHERE card_id IS NOT NULL")
        for row in existing_rows:
            existing_card_ids.add(row['card_id'])
        
        print(f"📊 Found {len(existing_card_ids)} existing cards in database")
        
        # Filter out players that already exist
        new_players = []
        skipped_count = 0
        
        for player in all_players:
            if player['card_id'] not in existing_card_ids:
                new_players.append(player)
            else:
                skipped_count += 1
        
        print(f"🆕 {len(new_players)} new players to add, {skipped_count} duplicates skipped")
        
        if not new_players:
            print("✅ No new players to add")
            return
        
        # Simple INSERT for new players only
        stmt = """
        INSERT INTO fut_players
            (name, rating, version, image_url, created_at, player_slug, player_url, card_id)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8)
        """

        # Insert only new players
        inserted_count = 0
        for player in new_players:
            try:
                await conn.execute(stmt,
                    player["name"], player["rating"], player["version"], player["image_url"],
                    player["created_at"], player["player_slug"], player["player_url"], player["card_id"]
                )
                inserted_count += 1
                print(f"✅ Added: {player['name']} (Rating: {player['rating']}, ID: {player['card_id']})")
            except Exception as e:
                print(f"⚠️ Failed to insert {player['name']}: {e}")
        
        print(f"🎯 Successfully inserted {inserted_count} new players")
        
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(sync_new_players_only())
