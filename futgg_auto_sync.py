import os
import asyncio
import asyncpg
import aiohttp
import logging
from datetime import datetime, timezone

# ------------------ CONFIG ------------------ #
DATABASE_URL = os.getenv("DATABASE_URL")
API_BASE = "https://www.fut.gg/api/fut/player-prices/25/{}"
BATCH_SIZE = 100   # DB updates per commit
LOG_INTERVAL = 50  # Log every 50 players

# ------------------ LOGGING ------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("price_sync")

# ------------------ DB CONNECTION ------------------ #
asnc def get_db():
    try:
        return await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        log.error(f"❌ DB connection failed: {e}")
        raise

# ------------------ PRICE FETCH ------------------ #
async def fetch_price(session, player):
    card_id = player.get("card_id")
    if not card_id:
        return None

    # Extract the numeric part from `card_id` e.g. 25-117667614 → 117667614
    numeric_id = card_id.split("-")[1] if "-" in card_id else card_id
    url = API_BASE.format(numeric_id)

    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 403:
                return None
            if resp.status != 200:
                log.warning(f"⚠️ API failed [{resp.status}] for {numeric_id}")
                return None

            data = await resp.json()
            price = data.get("ps", {}).get("LCPrice") or data.get("xbox", {}).get("LCPrice")
            return int(price) if price else None
    except Exception:
        return None

# ------------------ MAIN SYNC ------------------ #
async def price_sync():
    log.info("🚀 Starting price sync...")

    conn = await get_db()
    players = await conn.fetch("SELECT id, name, card_id, player_url FROM fut_players")
    log.info(f"📦 Found {len(players)} players in DB.")

    updated, skipped = 0, 0

    async with aiohttp.ClientSession() as session:
        batch = []
        for i, player in enumerate(players, start=1):
            price = await fetch_price(session, dict(player))

            if price is None:
                skipped += 1
            else:
                updated += 1
                batch.append((price, datetime.now(timezone.utc), player["id"]))

            # Commit batch updates
            if len(batch) >= BATCH_SIZE:
                await conn.executemany(
                    "UPDATE fut_players SET price=$1, created_at=$2 WHERE id=$3",
                    batch
                )
                batch.clear()

            # Log progress every LOG_INTERVAL players
            if i % LOG_INTERVAL == 0:
                log.info(f"⏳ Processed {i}/{len(players)} players...")

        # Final commit for remaining updates
        if batch:
            await conn.executemany(
                "UPDATE fut_players SET price=$1, created_at=$2 WHERE id=$3",
                batch
            )

    await conn.close()
    log.info(f"🎯 Price sync complete → ✅ {updated} updated | ⏭️ {skipped} skipped.")

# ------------------ SCHEDULER ------------------ #
async def scheduler():
    while True:
        try:
            await price_sync()
        except Exception as e:
            log.error(f"❌ Sync failed: {e}")
        await asyncio.sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    asyncio.run(scheduler())
