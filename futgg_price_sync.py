import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Load environment variable for DB
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}

async def fetch_price(player_url):
    """Fetch a player's price from FUT.GG."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Target the div containing the price
        price_div = soup.select_one("div.font-bold.text-2xl")
        if not price_div:
            print(f"⚠️ No price container found: {player_url}")
            return None

        # Extract the text after the coin icon
        price_text = price_div.get_text(strip=True).replace(",", "")
        if price_text.isdigit():
            return int(price_text)
        else:
            print(f"⚠️ Price parsing failed for {player_url} → raw text: {price_text}")
            return None

    except Exception as e:
        print(f"❌ Error fetching {player_url}: {e}")
        return None

async def update_prices():
    """Update player prices in the database."""
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")
        print(f"🔍 Found {len(players)} players to update.")

        for player in players:
            player_id = player["id"]
            player_url = player["player_url"]

            price = await fetch_price(player_url)
            if price is not None:
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
                    print(f"✅ Updated {player_url} → {price} coins")
                except Exception as e:
                    print(f"⚠️ Failed to update DB for {player_url}: {e}")
            else:
                print(f"ℹ️ No price found: {player_url}")

    finally:
        await conn.close()
        print("🎯 Price sync complete.")

async def scheduler():
    """Run price sync every 5 minutes."""
    while True:
        await update_prices()
        print("⏳ Waiting 5 minutes before next sync...\n")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(scheduler())