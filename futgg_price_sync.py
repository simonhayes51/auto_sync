import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set — please configure it.")

# Batch size avoids overloading the API
BATCH_SIZE = 50

async def fetch_prices(session, year, ids):
    url = f"https://www.fut.gg/api/fut/player-prices/{year}/" + ",".join(ids) + "/"
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                return await resp.json()
    except Exception as e:
        print(f"Error fetching prices for ids {ids}: {e}")
    return {}

async def sync_prices():
    async with asyncpg.create_pool(DATABASE_URL) as pool, aiohttp.ClientSession() as session:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, card_id FROM fut_players WHERE card_id IS NOT NULL")
            tasks = []
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                ids = [row["card_id"] for row in batch]
                tasks.append((batch, fetch_prices(session, 25, ids)))
            
            results = await asyncio.gather(*[t[1] for t in tasks])
            for (batch, data) in zip([t[0] for t in tasks], results):
                for row in batch:
                    price = data.get(str(row["card_id"]))
                    if price is not None:
                        await conn.execute(
                            "UPDATE fut_players SET price=$1, created_at=$2 WHERE id=$3",
                            price, datetime.now(timezone.utc), row["id"]
                        )
                        print(f"Updated card_id {row['card_id']} → price {price}")
                    else:
                        print(f"No price for card_id {row['card_id']} (likely SBC/Reward)")

async def main():
    await sync_prices()

if __name__ == "__main__":
    asyncio.run(main())