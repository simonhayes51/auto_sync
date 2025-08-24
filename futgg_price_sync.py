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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36"
}

def fetch_price(player_url: str):
    """Fetch the player's price from FUT.GG."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find the price container exactly
        price_container = soup.find(
            "div",
            class_="font-bold text-2xl flex flex-row items-center gap-1 justify-self-end"
        )
        if not price_container:
            return None  # SBC or Reward card, skip it

        # Find the span first
        coin_span = price_container.find("span", class_="price-coin")
        if not coin_span:
            return None  # No price span = no market price

        # Get the **next sibling text** after the span → that's the price
        price_text = coin_span.next_sibling
        if not price_text:
            return None

        # Clean formatting: remove commas, spaces, etc.
        price_text = price_text.strip().replace(",", "")

        return int(price_text) if price_text.isdigit() else None

    except Exception as e:
        print(f"❌ Error fetching price from {player_url}: {e}")
        return None

async def update_prices():
    """Fetch prices and update the DB."""
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

    updated, skipped = 0, 0

    for player in players:
        player_id = player["id"]
        url = player["player_url"]

        if not url:
            skipped += 1
            continue

        price = fetch_price(url)
        if price is None:
            print(f"ℹ️ No price available (SBC/Reward): {url}")
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

        await asyncio.sleep(0.5)  # Rate-limit friendly

    await conn.close()
    print(f"🎯 Price sync complete — {updated} updated, {skipped} skipped.")

async def scheduler():
    """Run the price sync every 5 minutes."""
    while True:
        try:
            await update_prices()
        except Exception as e:
            print(f"❌ Sync error: {e}")
        await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(scheduler())