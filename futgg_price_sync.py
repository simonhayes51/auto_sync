import os
import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import re

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

CONCURRENCY_LIMIT = 20

async def fetch_price(session, url: str):
    """Scrape FUT.GG for the player's coin price with multiple fallbacks."""
    try:
        async with session.get(url, headers=HEADERS, timeout=12) as resp:
            if resp.status != 200:
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # 1️⃣ First attempt: search within "font-bold text-2xl" container
            price_div = soup.select_one("div.font-bold.text-2xl.flex.flex-row.items-center.gap-1.justify-self-end")
            if price_div:
                text = price_div.get_text(strip=True)
                price = re.sub(r"[^\d]", "", text)
                if price.isdigit():
                    return int(price)

            # 2️⃣ Second attempt: find first span.price-coin → grab parent div text
            coin_span = soup.find("span", class_=lambda c: c and "price-coin" in c)
            if coin_span:
                parent_div = coin_span.find_parent("div")
                if parent_div:
                    text = parent_div.get_text(strip=True)
                    price = re.sub(r"[^\d]", "", text)
                    if price.isdigit():
                        return int(price)

            # 3️⃣ Last fallback: scan entire HTML for first number next to "price-coin"
            raw_html = soup.prettify()
            match = re.search(r'price-coin[^<]*</span>\s*([\d,]+)', raw_html)
            if match:
                price = match.group(1).replace(",", "")
                if price.isdigit():
                    return int(price)

            # If nothing found, likely SBC/untradable/reward
            return None

    except Exception:
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
            print(f"✅ {url} → {price:,} coins")
            return True
        except Exception as e:
            print(f"⚠️ Failed to update {url}: {e}")
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