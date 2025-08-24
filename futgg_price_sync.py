import os
import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

CONCURRENCY_LIMIT = 20  # Fetch 20 player prices at once

async def fetch_price(session, player_url: str):
    """Scrape FUT.GG for the player's coin price."""
    try:
        async with session.get(player_url, headers=HEADERS, timeout=12) as resp:
            if resp.status != 200:
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # Find the coin span
            coin_span = soup.find("span", class_="price-coin")
            if not coin_span:
                return None

            # Grab the parent div containing the price text
            parent_div = coin_span.find_parent("div")
            if not parent_div:
                return None

            # Extract the raw number from inside the div
            raw_text = parent_div.get_text(strip=True)
            price = "".join([c for c in raw_text if c.isdigit()])

            return int(price) if price.isdigit() else None

    except Exception:
        return None


async def process_player(semaphore, session, conn, player):
    """Fetch and update a single player's price in the database."""
    async with semaphore:
        player_id = player["id"]
        url = player["player_url"]

        if not url:
            return False

        price = await fetch_price(session, url)
        if price is None:
            print(f"⚠️ No price found: {url}")
            return False

        try:
            await conn.execute("""
                UPDATE fut_players
                SET price = $1, created_at = $2
                WHERE id = $3
            """, price, datetime.now(timezone.utc), player_id)
            print(f"✅ {url} → {price:,} coins")
            return True
        except Exception as e:
            print(f"⚠️ Failed to update {url}: {e}")
            return False


async def populate_prices():
    """Bulk price fetcher with parallel scraping."""
    print(f"\n🚀 Starting price population at {datetime.now(timezone.utc)} UTC")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("""
        SELECT id, player_url
        FROM fut_players
        WHERE price IS NULL OR price = 0
    """)

    total_players = len(players)
    print(f"📦 Found {total_players} players missing prices.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    async with aiohttp.ClientSession() as session:
        tasks = [
            process_player(semaphore, session, conn, player)
            for player in players
        ]
        results = await asyncio.gather(*tasks)

    await conn.close()

    updated = sum(1 for r in results if r)
    skipped = total_players - updated

    print(f"\n🎯 Price sync complete — {updated} prices updated, {skipped} skipped.")


if __name__ == "__main__":
    asyncio.run(populate_prices())