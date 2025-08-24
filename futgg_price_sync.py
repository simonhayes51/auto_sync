import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import os

DATABASE_URL = os.getenv("DATABASE_URL")
PRICE_URL = "https://www.fut.gg/players/"

async def fetch_html(session, url):
    async with session.get(url) as resp:
        return await resp.text()

async def fetch_and_update_prices():
    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        page = 1
        total_updated = 0

        while True:
            url = f"{PRICE_URL}?page={page}"
            html = await fetch_html(session, url)
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("a.group\\/player")

            if not cards:
                break

            for card in cards:
                try:
                    pid = card.get("href", "").split("/")[-2]  # Extract player ID
                    price_container = card.select_one("div.flex.flex-col.items-end")

                    if not price_container:
                        continue

                    spans = price_container.find_all("span")
                    if len(spans) < 2:
                        continue

                    ps_price = spans[0].text.replace(",", "").replace(" ", "").replace("₿", "").strip()
                    xbox_price = spans[1].text.replace(",", "").replace(" ", "").replace("₿", "").strip()

                    ps_price = int(ps_price) if ps_price.isdigit() else None
                    xbox_price = int(xbox_price) if xbox_price.isdigit() else None

                    await conn.execute("""
                        UPDATE fut_players
                        SET ps_price=$1, xbox_price=$2, updated_at=$3
                        WHERE id=$4
                    """, ps_price, xbox_price, datetime.now(timezone.utc), pid)

                    total_updated += 1
                except Exception as e:
                    print(f"⚠️ Failed to update price: {e}")
                    continue

            page += 1

        await conn.close()
        print(f"💰 Updated prices for {total_updated} players.")

async def scheduler():
    while True:
        await fetch_and_update_prices()
        await asyncio.sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    asyncio.run(scheduler())