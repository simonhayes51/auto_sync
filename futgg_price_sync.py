import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Get DB connection
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

# -------------------------------
# Fetch player price from FUT.GG
# -------------------------------
def fetch_price(player_url):
    """Fetch the player's current price from their FUT.GG page."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find correct price container that includes the price-coin span
        price_container = None
        price_blocks = soup.find_all("div", class_="flex items-center justify-center")

        for block in price_blocks:
            if block.find("span", class_="price-coin"):
                price_container = block
                break

        if not price_container:
            print(f"⚠️ No price found for {player_url}")
            return None

        price_text = price_container.get_text(strip=True)
        if not price_text:
            print(f"⚠️ Empty price for {player_url}")
            return None

        # Remove commas, convert to integer
        return int(price_text.replace(",", ""))
    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None

# -------------------------------
# Update prices in the database
# -------------------------------
async def update_prices():
    """Fetch all players and update their prices in the database."""
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")
        print(f"📦 Found {len(players)} players to update.")

        for player in players:
            player_id = player["id"]
            player_url = player["player_url"]

            if not player_url:
                continue

            price = fetch_price(player_url)
            if price is None:
                continue

            try:
                await conn.execute(
                    "UPDATE fut_players SET price=$1, created_at=$2 WHERE id=$3",
                    price,
                    datetime.now(timezone.utc),
                    player_id
                )
                print(f"✅ Updated player {player_id} → {price} coins")
            except Exception as e:
                print(f"⚠️ Failed to update player {player_id}: {e}")

        print("🎯 Price sync complete — database updated.")
    finally:
        await conn.close()

# -------------------------------
# Scheduler to run every 5 minutes
# -------------------------------
async def scheduler():
    while True:
        await update_prices()
        await asyncio.sleep(300)  # 5 minutes

if __name__ == "__main__":
    asyncio.run(scheduler())