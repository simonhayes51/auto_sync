import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found. Set it in Railway → Variables.")

API_URL_TEMPLATE = "https://www.fut.gg/api/fut/player-prices/25/{}/"

# -------------------------------
# Extract numeric player ID
# -------------------------------
def extract_player_id(card_id: str) -> str:
    """
    Extracts the numeric player ID from the stored card_id.
    Example:
        Input: 25-117667614.abcd1234hash.webp
        Output: 117667614
    """
    try:
        return card_id.split("-")[1].split(".")[0]
    except:
        return None

# -------------------------------
# Fetch player price from FUT.GG API
# -------------------------------
async def fetch_price(session, player_id: str, url: str):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"⚠️ Failed API call for {player_id} → {response.status}")
                return None

            data = await response.json()

            # Expected structure:
            # { "prices": { "ps": 18000, "xbox": 17500, ... } }
            if "prices" not in data or not data["prices"]:
                return None

            # Prefer PS price, fallback to Xbox if missing
            price = data["prices"].get("ps") or data["prices"].get("xbox")
            return price
    except Exception as e:
        print(f"⚠️ API error for {player_id}: {e}")
        return None

# -------------------------------
# Update database with player price
# -------------------------------
async def update_price(conn, player_id: int, price: int):
    try:
        await conn.execute(
            """
            UPDATE fut_players
            SET price = $1, created_at = $2
            WHERE id = $3
            """,
            price,
            datetime.now(timezone.utc),
            player_id
        )
    except Exception as e:
        print(f"⚠️ Failed to update DB for {player_id}: {e}")

# -------------------------------
# Main price sync loop
# -------------------------------
async def price_sync():
    print(f"🚀 Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    # Get all players with valid card IDs
    rows = await conn.fetch("SELECT id, card_id FROM fut_players WHERE card_id IS NOT NULL")
    print(f"📦 Found {len(rows)} players to update")

    async with aiohttp.ClientSession() as session:
        for row in rows:
            card_id = row["card_id"]
            player_id_numeric = extract_player_id(card_id)

            if not player_id_numeric:
                print(f"⚠️ Skipping player {row['id']} — invalid card_id: {card_id}")
                continue

            url = API_URL_TEMPLATE.format(player_id_numeric)
            price = await fetch_price(session, player_id_numeric, url)

            if price is None:
                print(f"ℹ️ No price for card_id {card_id} (likely SBC/Reward)")
                continue

            await update_price(conn, row["id"], price)
            print(f"✅ Updated player {row['id']} — Price: {price:,}")

            await asyncio.sleep(0.25)  # avoid hammering the API

    await conn.close()
    print("🎯 Price sync complete — database updated.")

# -------------------------------
# Run price sync every 5 minutes
# -------------------------------
async def scheduler():
    while True:
        try:
            await price_sync()
        except Exception as e:
            print(f"❌ Unexpected error in price sync: {e}")
        await asyncio.sleep(300)  # 5 minutes

if __name__ == "__main__":
    asyncio.run(scheduler())