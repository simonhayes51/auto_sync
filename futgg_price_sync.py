import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0 Safari/537.36"
}


def fetch_price(player_url):
    """Fetch a player's current price from FUT.GG."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Locate the full price container
        container = soup.find("div", class_="flex items-center justify-center")
        if not container:
            print(f"⚠️ No price container found for {player_url}")
            return None

        # Extract all text, remove commas, verify it's numeric
        text = container.get_text(strip=True).replace(",", "")
        if text.isdigit():
            return int(text)

        print(f"⚠️ No price found for {player_url}")
        return None

    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None


async def update_prices():
    """Fetch prices for all players and update them in the database."""
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        players = await conn.fetch("SELECT id, player_url FROM fut_players")

        print(f"🔍 Found {len(players)} players to update prices for...")

        for player in players:
            player_id = player["id"]
            player_url = player["player_url"]

            if not player_url:
                print(f"⚠️ Skipping player {player_id} — no player_url")
                continue

            price = fetch_price(player_url)
            if price is not None:
                try:
                    await conn.execute(
                        """
                        UPDATE fut_players
                        SET price = $1, created_at = $2
                        WHERE id = $3
                        """,
                        price,
                        datetime.now(timezone.utc).replace(tzinfo=None),  # FIXED
                        player_id,
                    )
                    print(f"✅ Updated price for {player_url} → {price}")
                except Exception as e:
                    print(f"⚠️ Failed to update DB for {player_url}: {e}")
            else:
                print(f"⚠️ No price found for {player_url}")

            await asyncio.sleep(0.3)  # Be gentle with FUT.GG

        await conn.close()
        print("🎯 Price sync complete — database updated.")

    except Exception as e:
        print(f"❌ Fatal DB error: {e}")


async def scheduler():
    """Run the price sync every 5 minutes."""
    while True:
        print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")
        await update_prices()
        await asyncio.sleep(300)  # Wait 5 minutes


if __name__ == "__main__":
    asyncio.run(scheduler())