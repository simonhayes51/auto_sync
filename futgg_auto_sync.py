import asyncio
import aiohttp
import asyncpg
import os
from datetime import datetime
from bs4 import BeautifulSoup

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.fut.gg/players/?page={}"

# Total pages to scrape (currently 334, can change later if needed)
TOTAL_PAGES = 334

# ------------------------------
# Connect to PostgreSQL database
# ------------------------------
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            rating INTEGER NOT NULL,
            version TEXT,
            image_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    await conn.close()

# ------------------------------
# Save or update player in DB
# ------------------------------
async def save_player(conn, player):
    try:
        await conn.execute("""
            INSERT INTO players (id, name, rating, version, image_url, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (id)
            DO UPDATE SET 
                name = EXCLUDED.name,
                rating = EXCLUDED.rating,
                version = EXCLUDED.version,
                image_url = EXCLUDED.image_url;
        """, player["id"], player["name"], player["rating"], player["version"], player["image_url"], player["created_at"])
    except Exception as e:
        print(f"⚠️ Failed to insert {player['name']} ({player['id']}): {e}")

# ------------------------------
# Scrape players from a single page
# ------------------------------
async def scrape_page(session, page):
    url = BASE_URL.format(page)
    async with session.get(url) as response:
        if response.status != 200:
            print(f"❌ Failed to fetch page {page} (Status {response.status})")
            return []

        html = await response.text()
        soup = BeautifulSoup(html, "html.parser")
        players = []

        for card in soup.select("a.player-card"):
            try:
                player_id = card["href"].split("/")[-2]
                name = card.select_one(".player-card-name").get_text(strip=True)
                rating = int(card.select_one(".player-card-rating").get_text(strip=True))
                version = card.select_one(".player-card-version").get_text(strip=True) if card.select_one(".player-card-version") else "Base"
                image_url = card.select_one("img")["src"]
                players.append({
                    "id": player_id,
                    "name": name,
                    "rating": rating,
                    "version": version,
                    "image_url": image_url,
                    "created_at": datetime.utcnow()
                })
            except Exception:
                continue

        return players

# ------------------------------
# Main sync job
# ------------------------------
async def sync_players():
    await init_db()
    conn = await asyncpg.connect(DATABASE_URL)

    async with aiohttp.ClientSession() as session:
        for page in range(1, TOTAL_PAGES + 1):
            players = await scrape_page(session, page)
            for player in players:
                await save_player(conn, player)
            print(f"✅ Synced page {page} ({len(players)} players)")

    await conn.close()
    print("🎯 All players synced successfully!")

# ------------------------------
# Run sync every 10 minutes
# ------------------------------
async def auto_sync():
    while True:
        print(f"\n🚀 Starting auto-sync at {datetime.utcnow()} UTC")
        await sync_players()
        print("⏳ Waiting 10 minutes before next sync...\n")
        await asyncio.sleep(600)

if __name__ == "__main__":
    asyncio.run(auto_sync())
