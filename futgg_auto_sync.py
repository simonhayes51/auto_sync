import os
import asyncio
import asyncpg
import requests
from datetime import datetime, timezone

# === SETTINGS ===
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set. Please configure Railway variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.117 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg"
}

API_URL = "https://www.fut.gg/api/fut/player-prices/25/{}"
REQUEST_DELAY = 0.7  # Safe delay per request

# === FETCH PLAYER PRICES ===
async def fetch_price(session, card_id):
    url = API_URL.format(card_id)
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)

        if response.status_code == 403:
            print(f"⛔ API blocked for card_id {card_id} → Skipping.")
            return None
        elif response.status_code == 404:
            print(f"⚠️ Card {card_id} not found → Likely SBC/Reward.")
            return None
        elif response.status_code != 200:
            print(f"⚠️ Unexpected response {response.status_code} for {card_id}")
            return None

        data = response.json()
        if not data or "prices" not in data:
            return None

        # Always fetch console price (PS/Xbox avg)
        ps_price = data["prices"].get("ps", {}).get("LCPrice")
        xbox_price = data["prices"].get("xbox", {}).get("LCPrice")

        # Prefer PS price, fallback to Xbox, else skip
        price = ps_price or xbox_price
        return price

    except Exception as e:
        print(f"❌ Error fetching price for {card_id}: {e}")
        return None

# === MAIN PRICE SYNC ===
async def sync_prices():
    print(f"\n🚀 Starting price sync at {datetime.now(timezone.utc)} UTC")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    # Fetch all players with a card_id
    players = await conn.fetch("SELECT id, name, card_id FROM fut_players WHERE card_id IS NOT NULL")
    print(f"📦 Found {len(players)} players to update prices for.")

    updated = 0
    skipped = 0

    for player in players:
        card_id = player["card_id"]

        # Strip out the filename extension if present
        if "." in card_id:
            card_id = card_id.split(".")[0]

        price = await fetch_price(conn, card_id)
        await asyncio.sleep(REQUEST_DELAY)

        if price is None:
            skipped += 1
            continue

        try:
            await conn.execute("""
                UPDATE fut_players
                SET price = $1, created_at = $2
                WHERE id = $3
            """, price, datetime.now(timezone.utc), player["id"])

            updated += 1
            print(f"✅ Updated {player['name']} → {price} coins")
        except Exception as e:
            print(f"⚠️ Failed to update {player['name']}: {e}")

    await conn.close()
    print(f"\n🎯 Price sync complete → {updated} updated, {skipped} skipped.")

# === ENTRY POINT ===
if __name__ == "__main__":
    asyncio.run(sync_prices())