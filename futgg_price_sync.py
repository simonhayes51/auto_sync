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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg"
}


async def fetch_price(card_id: str):
    """
    Fetch player price from FUT.GG API using the card_id directly.
    """
    url = FUTGG_API_URL.format(card_id)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code == 403:
            print(f"⚠️ Forbidden for {card_id} → API blocked")
            return None

        if response.status_code != 200:
            print(f"⚠️ API failed for {card_id} → {response.status_code}")
            return None

        data = response.json()
        price = data.get("data", {}).get("currentPrice", {}).get("price")

        if price is None:
            print(f"ℹ️ No price for card_id {card_id} → Likely SBC/Reward card")
            return None

        return price
    except Exception as e:
        print(f"❌ Exception fetching price for {card_id}: {e}")
        return None


async def run_price_sync_once():
    """
    Fetch all players and update prices one time only.
    """
    print("\n🚀 Starting one-time FUT.GG price sync...")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

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

        # Fetch price using card_id directly
        price = await fetch_price(str(card_id))

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

        # Sleep slightly to avoid rate-limiting issues
        await asyncio.sleep(0.15)

        if idx % 100 == 0:
            print(f"⏳ Processed {idx}/{len(players)} players...")

    await conn.close()
    print(f"\n🎯 One-time price sync complete → ✅ {updated_count} updated | ⏭️ {skipped_count} skipped.")


if __name__ == "__main__":
    asyncio.run(run_price_sync_once())