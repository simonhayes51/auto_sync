import os
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.fut.gg/players/"

# -----------------------
# Extract Card ID from image URL
# -----------------------
def extract_card_id(image_url: str):
    try:
        # Example: .../futgg-player-item-card/"25-100855415"
        return image_url.split("/")[-1].replace('"', '').replace('25-', '')
    except:
        return None

# -----------------------
# Scrape one player page
# -----------------------
async def fetch_player_details(session, player_id):
    url = f"{BASE_URL}{player_id}/"
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"⚠️ Skipping {player_id} — status {response.status}")
                return None

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # Extract name
            name_tag = soup.select_one("h1.text-3xl")
            name = name_tag.get_text(strip=True) if name_tag else "Unknown"

            # Extract rating
            rating_tag = soup.select_one("div.text-4xl.font-bold")
            rating = int(rating_tag.get_text(strip=True)) if rating_tag else None

            # Extract version
            version_tag = soup.select_one("div.player-card-variation")
            version = version_tag.get_text(strip=True) if version_tag else "Base"

            # Extract image URL
            image_tag = soup.select_one("img.player-card-image")
            image_url = image_tag["src"] if image_tag else None

            # Slug = last part of player URL
            player_slug = url.split("/")[-2] if "/" in url else None

            # Card ID = parsed from image URL
            card_id = extract_card_id(image_url) if image_url else None

            # Return structured dict
            return {
                "id": player_id,
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": image_url,
                "player_slug": player_slug,
                "player_url": url,
                "card_id": card_id,
                "created_at": datetime.utcnow(),
            }

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None

# -----------------------
# Save player to DB
# -----------------------
async def save_player(conn, player):
    try:
        await conn.execute("""
            INSERT INTO fut_players
            (id, name, rating, version, image_url, created_at, player_slug, player_url, card_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id)
            DO UPDATE SET
                name = EXCLUDED.name,
                rating = EXCLUDED.rating,
                version = EXCLUDED.version,
                image_url = EXCLUDED.image_url,
                created_at = EXCLUDED.created_at,
                player_slug = EXCLUDED.player_slug,
                player_url = EXCLUDED.player_url,
                card_id = EXCLUDED.card_id
        """, player["id"], player["name"], player["rating"], player["version"],
           player["image_url"], player["created_at"], player["player_slug"], player["player_url"], player["card_id"])
        print(f"✅ Saved {player['name']} ({player['rating']})")
    except Exception as e:
        print(f"⚠️ Failed to save {player['name']}: {e}")

# -----------------------
# Full sync function
# -----------------------
async def run_full_resync():
    print(f"🚀 Starting FULL FUT.GG player resync at {datetime.utcnow()} UTC")

    conn = await asyncpg.connect(DATABASE_URL)

    async with aiohttp.ClientSession() as session:
        # Loop over 400+ pages — FUT.GG has thousands of players
        for page in range(1, 400):
            url = f"{BASE_URL}?page={page}"
            print(f"⏳ Fetching page {page}: {url}")

            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Failed to fetch {url} — status {response.status}")
                        continue

                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")
                    players = soup.select("a.player-item-link")

                    if not players:
                        print(f"✅ Finished scraping at page {page} — no more players found.")
                        break

                    # Process each player
                    for p in players:
                        player_id = p.get("href").split("/")[-2]
                        player_data = await fetch_player_details(session, player_id)
                        if player_data:
                            await save_player(conn, player_data)

            except Exception as e:
                print(f"❌ Error scraping page {page}: {e}")

    await conn.close()
    print(f"🎯 Full player resync complete at {datetime.utcnow()} UTC")

# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":
    asyncio.run(run_full_resync())