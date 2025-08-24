import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
}

def fetch_price(player_url):
    """Fetches the player's price from FUT.GG"""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        span = soup.find("span", class_="price-coin")

        if not span:
            print(f"⚠️ No coin span found for {player_url}")
            return None

        # Grab the text node directly after the <span>
        if span.next_sibling:
            price_text = span.next_sibling.strip().replace(",", "")
            if price_text.isdigit():
                return int(price_text)

        print(f"⚠️ No price found for {player_url}")
        return None

    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None

async def update_prices():
    """Updates player prices in the database."""
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return

    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")
        print(f"📦 Found {len(players)} players to update.")

        updated_count = 0
        for player in players:
            player_id = player["id"]
            player_url = player["player_url"]

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
                updated_count += 1
            except Exception as e:
                print(f"⚠️ Failed to update player {player_id}: {e}")

        print(f"🎯 Price sync complete — {updated_count} prices updated.")
    finally:
        await conn.close()

async def scheduler():
    while True:
        await update_prices()
        await asyncio.sleep(300)  # Run every 5 minutes

if __name__ == "__main__":
    asyncio.run(scheduler())