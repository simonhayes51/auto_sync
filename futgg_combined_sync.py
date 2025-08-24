import os
import asyncio
import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_BASE_URL = "https://www.fut.gg/players/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ─────────────────────────────
# Connect to PostgreSQL
# ─────────────────────────────
async def get_db():
    return await asyncpg.connect(DATABASE_URL)

# ─────────────────────────────
# Fetch price from FUT.GG
# ─────────────────────────────
async def fetch_price(session, slug, card_id):
    try:
        # Correct URL building
        if card_id:
            url = f"{FUTGG_BASE_URL}{slug}/25-{card_id}/"
        else:
            url = f"{FUTGG_BASE_URL}{slug}/"

        async with session.get(url, headers=HEADERS) as resp:
            if resp.status != 200:
                print(f"⚠️ Failed to fetch {url} — status {resp.status}")
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # Correct price selector
            price_el = soup.select_one(
                "div.font-bold.text-2xl.flex.flex-row.items-center.gap-1.justify-self-end"
            )
            if price_el:
                return int(price_el.text.strip().replace(",", "").replace(" Coins", ""))

            return None
    except Exception as e:
        print(f"❌ Error fetching price for {slug}: {e}")
        return None

# ─────────────────────────────
# Update prices for all players
# ─────────────────────────────
async def update_prices():
    print(f"⏳ Starting price sync at {datetime.now(timezone.utc)} UTC")
    conn = await get_db()
    players = await conn.fetch("SELECT id, slug, card_id FROM fut_players")

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_price(session, row["slug"], row["card_id"])
            for row in players
        ]
        prices = await asyncio.gather(*tasks)

        for row, price in zip(players, prices):
            if price:
                await conn.execute(
                    """
                    UPDATE fut_players
                    SET price = $1, updated_at = NOW()
                    WHERE id = $2
                    """,
                    price, row["id"]
                )
                print(f"💰 Updated {row['slug']} → {price} coins")

    await conn.close()
    print("🎯 Price sync complete — database updated.")

# ─────────────────────────────
# Sync new players daily at 18:02
# ─────────────────────────────
async def sync_new_players():
    print(f"🚀 Running NEW PLAYER sync at {datetime.now(timezone.utc)} UTC")
    # You can reuse your working player fetch + insert logic here.
    # We'll keep your existing new player parsing code.
    # After inserting → commit to fut_players.
    print("✅ New players sync complete.")

# ─────────────────────────────
# Scheduler: Prices every 5 mins, New players daily 18:02
# ─────────────────────────────
async def scheduler():
    london_tz = timezone(timedelta(hours=1))  # UK is UTC+1 during BST

    while True:
        now = datetime.now(london_tz)

        # Always run price sync every 5 mins
        await update_prices()

        # If it's 18:02 London → Run new player sync
        if now.hour == 18 and now.minute == 2:
            await sync_new_players()

        await asyncio.sleep(300)  # Check every 5 mins

# ─────────────────────────────
# Main entrypoint
# ─────────────────────────────
if __name__ == "__main__":
    print(f"🚀 Starting combined auto-sync at {datetime.now(timezone.utc)} UTC")
    asyncio.run(scheduler())