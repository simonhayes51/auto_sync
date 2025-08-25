import aiohttp
import asyncio
import asyncpg
import logging
import os
import random
import re

DATABASE_URL = os.getenv("DATABASE_URL")
API_URL = "https://www.fut.gg/api/fut/player-prices/25/{}"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.248 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
]

# Logging setup
logger = logging.getLogger("fut-price-sync")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

price_regex = re.compile(r'"price":(\d+)')

async def fetch_price(session, card_id):
    """Fetch player price by scraping raw API response"""
    url = API_URL.format(card_id)

    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*"
        }

        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.warning(f"⚠️ {card_id} → HTTP {resp.status}")
                return None

            text = await resp.text()
            match = price_regex.search(text)
            if match:
                price = int(match.group(1))
                logger.info(f"✅ {card_id} → {price}")
                return price
            else:
                logger.info(f"ℹ️ {card_id} → No price found (SBC/Reward)")
                return None

    except Exception as e:
        logger.error(f"❌ Error fetching {card_id}: {e}")
        return None

async def update_prices():
    """Sync prices for all players in DB"""
    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("SELECT id, card_id FROM fut_players")
    logger.info(f"📦 Starting price sync for {len(players)} players...")

    updated = 0
    skipped = 0

    async with aiohttp.ClientSession() as session:
        for player in players:
            card_id = player["card_id"]
            price = await fetch_price(session, card_id)

            if price is not None:
                await conn.execute(
                    "UPDATE fut_players SET price = $1 WHERE id = $2",
                    price, player["id"]
                )
                updated += 1
            else:
                skipped += 1

            # Random delay to avoid detection
            await asyncio.sleep(random.uniform(0.15, 0.3))

    logger.info(f"🎯 Price sync complete → ✅ {updated} updated | ⏭️ {skipped} skipped")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(update_prices())