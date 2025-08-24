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
    """Fetch price from a FUT.GG player page."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=12)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Locate price container exactly
        price_container = soup.find(
            "div",
            class_="font-bold text-2xl flex flex-row items-center gap-1 justify-self-end"
        )
        if not price_container:
            return None  # SBC/Reward cards have no market price

        # Get <span> and extract the text immediately after it → price
        coin_span = price_container.find("span", class_="price-coin")
        if not coin_span:
            return None

        price_text = coin_span.next_sibling
        if not price_text:
            return None

        # Clean up commas and spaces
        price_text = price_text.strip().replace(",", "")
        return int(price_text) if price_text.isdigit() else None

    except Exception as e:
        print(f"❌ Error scraping {player_url}: {e}")
        return None

async def populate_prices():
    """One-time bulk price fetcher — fills missing prices."""
    print(f"\n🚀 Starting bulk price population at {datetime.now(timezone.utc)} UTC")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    try:
        # Grab all players where price is NULL
        players = await conn.fetch("""
            SELECT id, player_url
            FROM fut_players
            WHERE price IS NULL OR price = 0
        """)
    except Exception as e:
        print(f"❌ Failed to fetch players: {e}")
        await conn.close()
        return

    print(f"📦 Found {len(players)} players missing prices.")
    updated, skipped = 0, 0

    for player in players:
        player_id = player["id"]
        url = player["player_url"]

        if not url:
            skipped += 1
            continue

        price = fetch_price(url)
        if price is None:
            print(f"ℹ️ Skipping (SBC/Reward or unavailable): {url}")
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
            print(f"✅ {url} → {price:,} coins")

        except Exception as e:
            print(f"⚠️ Failed to update {url}: {e}")

        await asyncio.sleep(0.4)  # Gentle rate-limit protection

    await conn.close()
    print(f"\n🎯 Bulk price population complete — {updated} prices added, {skipped} skipped.")

if __name__ == "__main__":
    asyncio.run(populate_prices())