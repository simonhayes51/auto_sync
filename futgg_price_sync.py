import aiohttp
import asyncio
import asyncpg
import logging
import random
import os

# ------------------------------
# CONFIG
# ------------------------------
API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3
CONCURRENCY = 5  # Parallel requests
DATABASE_URL = os.getenv("DATABASE_URL")  # Railway DB

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.248 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
]

# ------------------------------
# LOGGING
# ------------------------------
logger = logging.getLogger("futgg-price-sync")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


# ------------------------------
# FETCH PLAYER PRICE FROM FUT.GG
# ------------------------------
async def fetch_price(session, card_id):
    url = f"{API_URL}/{card_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Referer": f"https://www.fut.gg/players/{card_id}/",
            }

            async with session.get(url, headers=headers, timeout=15) as resp:
                # Handle rate-limits or temporary blocks
                if resp.status in [403, 429, 500, 502, 503]:
                    logger.warning(f"⚠️ {card_id} → Blocked ({resp.status}) → retrying {attempt}/{MAX_RETRIES}")
                    await asyncio.sleep(random.uniform(1, 3))
                    continue

                # If the player doesn't exist
                if resp.status == 404:
                    logger.info(f"⏭️ {card_id} → Not found (404)")
                    return None

                # Successful response
                data = await resp.json()
                price_block = data.get("data", {}).get("currentPrice", {})

                if isinstance(price_block, dict) and "price" in price_block:
                    price = price_block["price"]
                    logger.info(f"✅ {card_id} → {price}")
                    return price
                else:
                    logger.warning(f"⚠️ {card_id} → No price found in response")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout fetching {card_id} (attempt {attempt})")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ Error fetching {card_id}: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"⏭️ Skipping {card_id} → All retries failed")
    return None


# ------------------------------
# UPDATE DATABASE PRICE
# ------------------------------
async def update_price(pool, card_id, price):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE fut_players
            SET price = $1
            WHERE card_id = $2
            """,
            price,
            card_id
        )


# ------------------------------
# PROCESS SINGLE PLAYER
# ------------------------------
async def process_player(session, pool, card_id, stats):
    price = await fetch_price(session, card_id)

    if price is not None:
        await update_price(pool, card_id, price)
        stats["updated"] += 1
    else:
        stats["skipped"] += 1


# ------------------------------
# MAIN SCRIPT
# ------------------------------
async def main():
    pool = await asyncpg.create_pool(DATABASE_URL)
    logger.info("📦 Connected to database.")

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT card_id FROM fut_players")
        card_ids = [row["card_id"] for row in rows]

    logger.info(f"📌 Starting price sync for {len(card_ids)} players...")

    stats = {"updated": 0, "skipped": 0}

    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            process_player(session, pool, card_id, stats)
            for card_id in card_ids
        ]
        await asyncio.gather(*tasks)

    logger.info("🎯 Price sync complete.")
    logger.info(f"✅ Updated: {stats['updated']}")
    logger.info(f"⏭️ Skipped: {stats['skipped']}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())