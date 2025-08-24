import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
import os
import logging

DATABASE_URL = os.getenv("DATABASE_URL")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("futgg-sync")

BASE_URL = "https://www.fut.gg/players/?page={}"

async def fetch_page(session, page):
    url = BASE_URL.format(page)
    async with session.get(url) as resp:
        if resp.status != 200:
            logger.warning(f"Failed to fetch page {page}, status {resp.status}")
            return None
        return await resp.text()

async def parse_players(html):
    soup = BeautifulSoup(html, "html.parser")
    players = []
    for card in soup.select("a.player-card"):
        try:
            name = card.select_one("span.name").text.strip()
            rating = card.select_one("span.rating").text.strip()
            image_url = card.select_one("img")["src"]
            players.append((name, rating, image_url))
        except:
            continue
    return players

async def save_to_db(conn, players):
    await conn.executemany(
        """
        INSERT INTO players (name, rating, image_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (name, rating) DO UPDATE
        SET image_url = EXCLUDED.image_url
        """,
        players
    )

async def sync_players():
    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        for page in range(1, 5):  # Limit pages initially
            html = await fetch_page(session, page)
            if html:
                players = await parse_players(html)
                if players:
                    await save_to_db(conn, players)
                    logger.info(f"✅ Synced {len(players)} players from page {page}")
    await conn.close()

async def main():
    while True:
        logger.info("🔄 Starting FUT.GG sync...")
        try:
            await sync_players()
            logger.info("✅ Sync completed successfully.")
        except Exception as e:
            logger.error(f"❌ Sync failed: {e}")
        await asyncio.sleep(600)  # Wait 10 minutes

if __name__ == "__main__":
    asyncio.run(main())
