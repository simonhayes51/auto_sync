import os
import asyncio
import asyncpg
import requests
from datetime import datetime, timezone

# === CONFIG ===
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found. Set it in Railway.")

FUTGG_API_URL = "https://www.fut.gg/api/fut/player-prices/25/{}"
SYNC_INTERVAL = 300  # 5 minutes

# Fake browser headers to bypass bot detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg"
}


async def fetch_price(card_id: str):
    """
    Fetch player price from FUT.GG API using the player's card_id.
    Returns None if SBC/Reward or unavailable.
    """
    url = FUTGG_API_URL.format(card_id)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)

        # Handle forbidden / blocked responses
        if response.status_code == 403:
            print(f"⚠️ Forbidden → {url}")
            return None

        if response.status_code != 200:
            print(f"⚠️ API failed for {url} → {response.status_code}")
            return None

        data = response.json()
        price = data.get("data", {}).get("currentPrice", {}).get("price")

        # If price is missing, it's likely SBC or Reward card
        if price is None:
            print(f"ℹ️ No price available (SBC/Reward): {url}")
            return None

        return price
    except Exception as e:
        print(f"❌ Exception fetching price for {card_id}: {e}")
        return None


async def sync_prices():
    """
    Sync all FUT.GG prices into the Railway PostgreSQL database.
    """
    print(f"\n🚀 Starting price sync at {datetime.now(timezone.utc)}")

    # Connect to DB
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    # Fetch all player IDs from DB
    try:
        players = await conn.fetch("SELECT id, name, card_id FROM fut_players")
    except Exception as e:
        print(f"❌ Failed to fetch players: {e}")
        await conn.close()
        return

    updated_count = 0
    skipped_count = 0

    for idx, player in enumerate(players, start=1):
        player_id = player["id"]
        name = player["name"]
        card_id = player["card_id"]

        if not card_id:
            skipped_count += 1
            continue

        # Fetch price from FUT.GG API
        price = await fetch_price(card_id)

        # Update DB if price found
        if price is not None:
            try:
                await conn.execute("""
                    UPDATE fut_players
                    SET price = $1, created_at = $2
                    WHERE id = $3
                """, price, datetime.now(timezone.utc), player_id)
                updated_count += 1
                print(f"✅ Updated {name} → {price:,} coins")
            except Exception as e:
                print(f"⚠️ Failed to update {name}: {e}")
        else:
            skipped_count += 1

        # Small pause to avoid hammering FUT.GG
        await asyncio.sleep(0.2)

        # Progress log every 100 players
        if idx % 100 == 0:
            print(f"⏳ Processed {idx}/{len(players)} players...")

    await conn.close()
    print(f"\n🎯 Price sync complete → ✅ {updated_count} updated | ⏭️ {skipped_count} skipped.")


async def scheduler():
    """
    Run price sync every 5 minutes.
    """
    while True:
        try:
            await sync_prices()
        except Exception as e:
            print(f"❌ Sync error: {e}")
        await asyncio.sleep(SYNC_INTERVAL)


if __name__ == "__main__":
    asyncio.run(scheduler())