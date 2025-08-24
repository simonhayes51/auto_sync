import os
import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_BASE = "https://www.fut.gg/players/?page="

async def fetch_page(session, url):
    """Fetch HTML content for a given URL."""
    async with session.get(url) as response:
        if response.status != 200:
            print(f"⚠️ Failed to fetch {url} (Status: {response.status})")
            return None
        return await response.text()

async def parse_players(html):
    """Parse players from FUT.GG page."""
    soup = BeautifulSoup(html, "html.parser")
    players = []
    for player in soup.select("a.link-block"):
        try:
            name = player.select_one("div.player-name").get_text(strip=True)
            rating = player.select_one("div.player-rating").get_text(strip=True)
            pid = player["href"].split("/")[-2]
            players.append({
                "id": pid,
                "name": name,
                "rating": rating
            })
        except Exception:
            continue
    return players

async def save_to_db(pool, players):
    """Insert or update player data in PostgreSQL."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            for player in players:
                await conn.execute("""
                    INSERT INTO players (id, name, rating)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (id) DO UPDATE
                    SET name = EXCLUDED.name, rating = EXCLUDED.rating
                """, player["id"], player["name"], player["rating"])
    print(f"✅ Synced {len(players)} players.")

async def sync_players():
    """Main sync loop."""
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        page = 1
        while True:
            print(f"🌍 Fetching FUT.GG page {page}...")
            html = await fetch_page(session, FUTGG_BASE + str(page))
            if not html:
                break
            players = await parse_players(html)
            if not players:
                print("🎉 No more players found.")
                break
            await save_to_db(pool, players)
            page += 1
    await pool.close()

async def main():
    """Runs sync every 10 minutes."""
    while True:
        print("🔄 Starting full sync...")
        try:
            await sync_players()
        except Exception as e:
            print(f"❌ Sync error: {e}")
        print("⏳ Waiting 10 minutes before next sync...")
        await asyncio.sleep(600)

if __name__ == "__main__":
    asyncio.run(main())
