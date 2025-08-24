import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ----------------------------
# CONFIGURATION
# ----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
}


# ----------------------------
# FETCH PRICE FROM FUT.GG
# ----------------------------
def fetch_price(player_url):
    """Fetch a player's price directly from FUT.GG page."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find price container <div class="flex items-center justify-center">
        price_container = soup.find("div", class_="flex items-center justify-center")
        if not price_container:
            print(f"⚠️ No price container found for {player_url}")
            return None

        # Get the entire text inside this div
        raw_text = price_container.get_text(strip=True)
        price_text = raw_text.replace(",", "").replace(" ", "")

        # Validate numeric price
        if price_text.isdigit():
            return int(price_text)

        print(f"⚠️ No numeric price found for {player_url} → got '{raw_text}'")
        return None

    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None


# ----------------------------
# UPDATE PRICES IN DB
# ----------------------------
async def update_prices():
    """Fetch all players from DB and update their prices."""
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    try:
        # Fetch player URLs
        players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")
        print(f"📦 Found {len(players)} players with URLs to check...")

        updated_count = 0
        missing_count = 0

        for player in players:
            player_id = player["id"]
            url = player["player_url"]

            if not url:
                continue

            price = fetch_price(url)

            if price is not None:
                try:
                    await conn.execute("""
                        UPDATE fut_players
                        SET price = $1, created_at = $2
                        WHERE id = $3
                    """, price, datetime.now(timezone.utc), player_id)
                    updated_count += 1
                except Exception as e:
                    print(f"⚠️ Failed to update {url}: {e}")
            else:
                missing_count += 1

            await asyncio.sleep(0.2)  # Be gentle to FUT.GG servers

        print(f"🎯 Price sync complete — {updated_count} updated, {missing_count} missing.")

    except Exception as e:
        print(f"❌ Error during price update: {e}")

    finally:
        await conn.close()


# ----------------------------
# SCHEDULER - RUN EVERY 5 MINUTES
# ----------------------------
async def scheduler():
    """Run the price updater every 5 minutes forever."""
    while True:
        await update_prices()
        await asyncio.sleep(300)  # 5 minutes


if __name__ == "__main__":
    asyncio.run(scheduler())