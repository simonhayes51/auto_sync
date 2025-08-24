import os
import re
import json
import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

BASE_URL = "https://www.fut.gg/players/?page={}"

# --------------------------
# Extract card_id from image_url
# --------------------------
def extract_card_id(image_url: str) -> str:
    match = re.search(r'/(25-\d+)/', image_url)
    return match.group(1) if match else None

# --------------------------
# Fetch a single FUT.GG page
# --------------------------
async def fetch_page(session, page_number: int):
    url = BASE_URL.format(page_number)
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"⚠️ Failed to fetch page {page_number} — status {response.status}")
                return None
            return await response.text()
    except Exception as e:
        print(f"⚠️ Error fetching page {page_number}: {e}")
        return None

# --------------------------
# Parse players from HTML
# --------------------------
def parse_players(html: str):
    soup = BeautifulSoup(html, "html.parser")
    player_cards = soup.select("a.player-card")
    players = []

    for card in player_cards:
        try:
            player_url = card.get("href")
            if not player_url or not player_url.startswith("/players/"):
                continue

            # Extract slug from URL
            player_slug = player_url.split("/")[2] if len(player_url.split("/")) > 2 else None
            if not player_slug:
                continue

            # Extract player id from slug
            player_id = player_slug.split("-")[0] if "-" in player_slug else None
            if not player_id:
                continue

            # Extract name & rating
            name_tag = card.select_one(".name")
            rating_tag = card.select_one(".rating")
            version_tag = card.select_one(".card-variant")

            name = name_tag.text.strip() if name_tag else "Unknown"
            rating = int(rating_tag.text.strip()) if rating_tag else 0
            version = version_tag.text.strip() if version_tag else "Base"

            # Extract image URL
            image_tag = card.select_one("img")
            image_url = image_tag.get("src") if image_tag else None

            # Get card_id from image_url
            card_id = extract_card_id(image_url) if image_url else None

            # Build player dict
            players.append({
                "id": int(player_id),
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": image_url,
                "player_slug": player_slug,
                "player_url": f"https://www.fut.gg{player_url}",
                "card_id": card_id,
                "created_at": datetime.now(timezone.utc)
            })

        except Exception as e:
            print(f"⚠️ Failed to parse player card: {e}")
            continue

    return players

# --------------------------
# Save players into database
# --------------------------
async def save_players_to_db(conn, players):
    query = """
        INSERT INTO fut_players
        (id, name, rating, version, image_url, player_slug, player_url, card_id, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            rating = EXCLUDED.rating,
            version = EXCLUDED.version,
            image_url = EXCLUDED.image_url,
            player_slug = EXCLUDED.player_slug,
            player_url = EXCLUDED.player_url,
            card_id = EXCLUDED.card_id,
            created_at = EXCLUDED.created_at
    """

    for player in players:
        try:
            await conn.execute(
                query,
                player["id"],
                player["name"],
                player["rating"],
                player["version"],
                player["image_url"],
                player["player_slug"],
                player["player_url"],
                player["card_id"],
                player["created_at"]
            )
            print(f"✅ Saved {player['name']} ({player['rating']})")
        except Exception as e:
            print(f"⚠️ Failed to save {player['name']}: {e}")

# --------------------------
# Main Auto-Sync Function
# --------------------------
async def sync_players():
    print(f"🚀 Starting full auto-sync at {datetime.now(timezone.utc)} UTC")
    conn = await asyncpg.connect(DATABASE_URL)

    async with aiohttp.ClientSession() as session:
        page = 1
        total_players = 0

        while True:
            html = await fetch_page(session, page)
            if not html:
                break

            players = parse_players(html)
            if not players:
                print(f"✅ No more players found — stopping at page {page}")
                break

            await save_players_to_db(conn, players)
            total_players += len(players)
            print(f"✅ Synced page {page} ({len(players)} players)")
            page += 1

        print(f"\n🎯 Full sync complete — {total_players} players updated ✅")

    await conn.close()

# --------------------------
# Run script manually
# --------------------------
if __name__ == "__main__":
    asyncio.run(sync_players())