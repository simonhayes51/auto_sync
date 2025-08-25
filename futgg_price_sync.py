import asyncio
import aiohttp
import asyncpg
import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
API_URL = "https://www.fut.gg/api/fut/player-prices/25"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("price_sync")

# Limit concurrent requests to avoid bans / 403s
MAX_CONCURRENT_REQUESTS = 8
SEM = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

async def fetch_price(session, card_id):
    """Fetch player price from FUT.GG API."""
    url = f"{API_URL}/{card_id}/"
    async with SEM:
        try:
            async with session.get(url, timeout=15) as resp:
                if resp.status == 403:
                    logger.warning(f"⚠️ Forbidden for {card_id} — blocked by FUT.GG")
                    return None
                if resp.status == 404:
                    logger.info(f"ℹ️ No price for {card_id} — SBC/Reward likely")
                    return None
                if resp.status != 200:
                    logger.error(f"❌ Failed API call for {card_id} → {resp.status}")
                    return None

                data = await resp.json()
                price = data.get("data", {}).get("currentPrice", {}).get("price")
                return price
        except Exception as e:
            logger.error(f"⚠️ Error fetching price for {card_id}: {e}")
            return None

async def update_price(conn, player_id, price):
    """Update price in DB."""
    try:
        await conn.execute("""
            UPDATE fut_players
            SET price = $1, created_at = $2
            WHERE card_id = $3
        """, price, datetime.now(timezone.utc), player_id)
    except Exception as e:
        logger.error(f"⚠️ Failed DB update for {player_id}: {e}")

async def sync_prices():
    """Main price sync process."""
    logger.info("⏳ Starting price sync...")

    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        # Get all player IDs from DB
        players = await conn.fetch("SELECT card_id FROM fut_players")
        total = len(players)
        updated = 0
        skipped = 0

        for idx, player in enumerate(players, start=1):
            card_id = player["card_id"]
            price = await fetch_price(session, card_id)

            if price:
                await update_price(conn, card_id, price)
                updated += 1
            else:
                skipped += 1

            # Log progress every 100 players
            if idx % 100 == 0 or idx == total:
                logger.info(f"⏳ Processed {idx}/{total} players...")

        logger.info(f"🎯 Price sync complete → ✅ {updated} updated | ⏭️ {skipped} skipped.")
    await conn.close()

async def main():
    while True:
        await sync_prices()
        logger.info("⏲️ Waiting 5 minutes before next sync...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())