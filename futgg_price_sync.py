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

CONCURRENCY_LIMIT = 15

async def fetch_price(session, url: str):
    """Scrape FUT.GG for player prices based on the coin span + sibling text."""
    try:
        async with session.get(url, headers=HEADERS, timeout=15) as resp:
            if resp.status != 200:
                print(f"⚠️ Failed to fetch {url} — status {resp.status}")
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # Locate the price span
            span = soup.find("span", class_=lambda c: c and "price-coin" in c)
            if not span:
                return None  # Could be SBC/untradeable/reward card

            # Get the parent <div> and the text right after the <span>
            price_div = span.find_parent("div")
            if not price_div:
                return None

            # Extract the price directly after </span>
            siblings = list(price_div.children)
            for i, sib in enumerate(siblings):
                if sib == span and i + 1 < len(siblings):
                    raw_price = siblings[i + 1]
                    price = str(raw_price).strip().replace(",", "")
                    if price.isdigit():
                        return int(price)
                    return None

            return None
    except Exception as e:
        print(f"⚠️ Error scraping {url}: {e}")
        return None


async def process_player(semaphore, session, conn, player):
    """Fetch and update price for a single player."""
    async with semaphore:
        url = player["player_url"]
        player_id = player["id"]

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
            print(f"✅ Updated {url} → {price:,} coins")
            return True
        except Exception as e:
            print(f"⚠️ Failed to update DB for {url}: {e}")
            return False


async def populate_prices():
    """Bulk price fetcher with async scraping."""
    print(f"\n🚀 Starting price sync at {datetime.now(timezone.utc)} UTC")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("""
        SELECT id, player_url FROM fut_players
        WHERE price IS NULL OR price = 0
    """)

    total_players = len(players)
    print(f"📦 Found {total_players} players missing prices.")

    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    async with aiohttp.ClientSession() as session:
        tasks = [process_player(semaphore, session, conn, p) for p in players]
        results = await asyncio.gather(*tasks)

    await conn.close()
    updated = sum(1 for r in results if r)
    skipped = total_players - updated
    print(f"\n🎯 Price sync complete — {updated} prices updated, {skipped} skipped.")


if __name__ == "__main__":
    asyncio.run(populate_prices())