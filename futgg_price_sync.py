import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timezone

# ==============================
# CONFIGURATION
# ==============================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

# ==============================
# FETCH PRICE FUNCTION
# ==============================
def fetch_price(player_url: str):
    """Fetch the player's current price from FUT.GG."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Target the price container
        price_container = soup.find(
            "div",
            class_="font-bold text-2xl flex flex-row items-center gap-1 justify-self-end"
        )

        if not price_container:
            # Likely SBC/Objective/Reward card with no market price
            print(f"ℹ️ No price available (SBC/Reward): {player_url}")
            return None

        # Extract number only — e.g. "30,750" → 30750
        text = price_container.get_text(strip=True)
        price = re.sub(r"[^\d]", "", text)

        return int(price) if price.isdigit() else None

    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None

# ==============================
# MAIN PRICE SYNC FUNCTION
# ==============================
async def update_prices():
    """Fetch prices from FUT.GG and update the Railway DB."""
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)} UTC")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players")
    except Exception as e:
        print(f"❌ Failed to fetch players from DB: {e}")
        await conn.close()
        return

    updated = 0
    skipped = 0

    for player in players:
        player_id = player["id"]
        url = player["player_url"]

        if not url:
            skipped += 1
            continue

        price = fetch_price(url)
        if price is None:
            skipped += 1
            continue

        try:
            await conn.execute(
                """
                UPDATE fut_players
                SET price = $1, created_at = $2
                WHERE id = $3
                """,
                price, datetime.now(timezone.utc), player_id
            )
            updated += 1
            print(f"✅ Updated {url} → {price:,} coins")

        except Exception as e:
            print(f"⚠️ Failed to update DB for {url}: {e}")

        # Respect FUT.GG rate limits
        await asyncio.sleep(0.5)

    await conn.close()
    print(f"🎯 Price sync complete — {updated} updated, {skipped} skipped.")

# ==============================
# SCHEDULER — RUN EVERY 5 MINUTES
# ==============================
async def scheduler():
    """Run the price sync every 5 minutes."""
    while True:
        try:
            await update_prices()
        except Exception as e:
            print(f"❌ Sync error: {e}")
        await asyncio.sleep(300)  # 5 minutes

# ==============================
# ENTRY POINT
# ==============================
if __name__ == "__main__":
    asyncio.run(scheduler())