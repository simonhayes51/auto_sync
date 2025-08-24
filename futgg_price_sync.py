import os
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Database URL
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0 Safari/537.36"
    )
}

CONCURRENT_REQUESTS = 25  # How many player pages we fetch in parallel


async def fetch_price(session, player_url):
    """Scrape console price from a FUT.GG player page."""
    try:
        async with session.get(player_url, headers=HEADERS, timeout=15) as response:
            if response.status != 200:
                print(f"⚠️ Failed to fetch {player_url} — Status {response.status}")
                return None

            html = await response.text()
            soup = BeautifulSoup(html, "html.parser")

            # Locate the price container
            price_container = soup.select_one(
                "div.flex.flex-row.items-center.gap-1.font-bold.text-lg"
            )
            if not price_container:
                print(f"⚠️ No price found for {player_url}")
                return None

            # Clean and convert price text
            price_text = price_container.text.strip().replace(",", "").replace(" FUT", "")
            return int(price_text) if price_text.isdigit() else None
    except Exception as e:
        print(f"⚠️ Error fetching {player_url}: {e}")
        return None


async def update_prices():
    """Fetch all player URLs from DB and update their prices in parallel."""
    print(f"\n⏳ Starting price sync at {datetime.utcnow()}")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # Get all players that have URLs
    players = await conn.fetch("""
        SELECT id, player_url FROM fut_players
        WHERE player_url IS NOT NULL
    """)

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = []

        async def process_player(player):
            """Scrape and update one player's price."""
            async with semaphore:
                player_id = player["id"]
                player_url = player["player_url"]
                price = await fetch_price(session, player_url)
                if price is not None:
                    try:
                        await conn.execute(
                            "UPDATE fut_players SET price=$1, created_at=$2 WHERE id=$3",
                            price, datetime.utcnow(), player_id
                        )
                        print(f"✅ Updated {player_id} → {price} coins")
                    except Exception as e:
                        print(f"⚠️ Failed DB update for {player_url}: {e}")

        for player in players:
            tasks.append(asyncio.create_task(process_player(player)))

        await asyncio.gather(*tasks)

    await conn.close()
    print(f"🎯 Price sync complete — {len(players)} players processed.")


async def scheduler():
    """Run price sync every 5 minutes."""
    while True:
        await update_prices()
        print("⏳ Waiting 5 minutes before next sync...")
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(scheduler())