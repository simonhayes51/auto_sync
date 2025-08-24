import os
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_BASE_URL = "https://www.fut.gg/players/"

# ==============================
# FETCH PRICE FROM FUT.GG
# ==============================
async def fetch_price(session, slug, variation_id=None):
    try:
        # Build the correct URL
        if variation_id:
            url = f"{FUTGG_BASE_URL}{slug}/25-{variation_id}/"
        else:
            url = f"{FUTGG_BASE_URL}{slug}/"

        async with session.get(url) as response:
            if response.status == 404:
                print(f"⚠️ Player missing on FUT.GG → {url}")
                return None
            elif response.status != 200:
                print(f"⚠️ Failed {slug} ({variation_id}) → {response.status}")
                return None

            html = await response.text()

            # Extract price via the price container
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            price_tag = soup.select_one("div.font-bold.text-2xl.flex.flex-row.items-center.gap-1.justify-self-end")
            if not price_tag:
                print(f"⚠️ No price found for {slug}")
                return None

            price_text = price_tag.text.strip().replace(",", "").replace(" ", "")
            return int(price_text)
    except Exception as e:
        print(f"❌ Error fetching price for {slug}: {e}")
        return None

# ==============================
# SYNC PRICES TO DATABASE
# ==============================
async def sync_prices():
    print(f"🚀 Starting FUT.GG price sync at {datetime.now(timezone.utc)} UTC")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("SELECT id, slug, variation_id FROM fut_players")

    async with aiohttp.ClientSession() as session:
        for i, player in enumerate(players, start=1):
            slug = player["slug"]
            variation_id = player["variation_id"]

            price = await fetch_price(session, slug, variation_id)
            if price is not None:
                try:
                    await conn.execute("""
                        UPDATE fut_players
                        SET price = $1, updated_at = $2
                        WHERE id = $3
                    """, price, datetime.now(timezone.utc), player["id"])
                    print(f"✅ [{i}/{len(players)}] Updated {slug} → {price} coins")
                except Exception as e:
                    print(f"⚠️ DB update failed for {slug}: {e}")

            await asyncio.sleep(0.5)  # Rate limit FUT.GG

    await conn.close()
    print("🎯 Price sync complete.")

# ==============================
# DAILY SCHEDULER @ 18:02 UTC
# ==============================
async def scheduler():
    while True:
        now = datetime.now(timezone.utc)
        # FUT updates player DB at 6PM London → 17:00 UTC in winter, 18:00 UTC in summer
        if now.hour == 17 and now.minute == 2:
            await sync_prices()
            await asyncio.sleep(60)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(scheduler())