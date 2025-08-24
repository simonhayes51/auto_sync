import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

print(f"🔌 DEBUG: Using DATABASE_URL = {DATABASE_URL}")

def parse_alt_text(alt_text):
    """Extract player name, rating, and version."""
    try:
        parts = [p.strip() for p in alt_text.split("-")]
        if len(parts) >= 3:
            name = parts[0]
            rating = int(parts[1]) if parts[1].isdigit() else None
            version = "-".join(parts[2:])
            return name, rating, version
        return alt_text, None, "N/A"
    except:
        return alt_text, None, "N/A"

def extract_card_id(img_url: str) -> str:
    """Extract the numeric player ID from the image URL."""
    try:
        # Example:
        # https://game-assets.fut.gg/.../25-117667614.somehash.webp
        filename = img_url.split("/")[-1]
        parts = filename.split("-")
        if len(parts) >= 2:
            return parts[1].split(".")[0]  # Get just the numeric ID
        return None
    except:
        return None

def fetch_players_from_page(page_number):
    """Fetch FUT.GG players from a single page."""
    url = FUTGG_BASE_URL.format(page_number)
    print(f"🌐 Fetching: {url}")

    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch page {page_number}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("a.group\\/player")

    players = []
    for card in cards:
        try:
            img_tag = card.select_one("img")
            if not img_tag:
                continue

            alt_text = img_tag.get("alt", "").strip()
            img_url = img_tag.get("src", "")
            name, rating, version = parse_alt_text(alt_text)
            card_id = extract_card_id(img_url)

            if not rating or not card_id:
                continue

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,
                "card_id": card_id,  # Store just the numeric ID
                "updated_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players

async def sync_players():
    """Sync FUT.GG player data into Railway PostgreSQL."""
    print(f"\n🚀 Starting full player data sync at {datetime.now(timezone.utc)} UTC")

    all_players = []
    page = 1

    while True:
        players = fetch_players_from_page(page)
        if not players:
            print(f"✅ No players found on page {page}, stopping pagination.")
            break
        all_players.extend(players)
        print(f"📦 Page {page}: {len(players)} players fetched.")
        page += 1
        await asyncio.sleep(0.5)

    print(f"🔍 Total players fetched: {len(all_players)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    for p in all_players:
        try:
            await conn.execute("""
                INSERT INTO fut_players (name, rating, version, image_url, card_id, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (name, rating)
                DO UPDATE SET version=$3, image_url=$4, card_id=$5, created_at=$6
            """, p["name"], p["rating"], p["version"], p["image_url"], p["card_id"], p["updated_at"])
        except Exception as e:
            print(f"⚠️ Failed to save {p['name']}: {e}")

    await conn.close()
    print("🎯 Player data sync complete — database updated.")

async def scheduler():
    """Run auto-sync once at startup."""
    await sync_players()

if __name__ == "__main__":
    asyncio.run(scheduler())