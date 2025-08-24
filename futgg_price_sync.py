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
    """Fetch price from FUT.GG player page."""
    try:
        response = requests.get(player_url, headers=HEADERS, timeout=12)
        if response.status_code != 200:
            print(f"⚠️ Failed to fetch {player_url} — status {response.status_code}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Find ALL possible price containers that match FUT.GG patterns
        containers = soup.find_all("div", class_="font-bold text-2xl flex flex-row items-center gap-1 justify-self-end")

        price_text = None
        for container in containers:
            coin_span = container.find("span", class_="price-coin")
            if coin_span:
                # First text node after <span>
                text = coin_span.next_sibling
                if text and text.strip().replace(",", "").isdigit():
                    price_text = text.strip().replace(",", "")
                    break

        # Fallback → scan entire HTML for first price-coin span
        if not price_text:
            coin_span = soup.find("span", class_="price-coin")
            if coin_span and coin_span.next_sibling:
                text = coin_span.next_sibling.strip().replace(",", "")
                if text.isdigit():
                    price_text = text

        return int(price_text) if price_text and price_text.isdigit() else None

    except Exception as e:
        print(f"❌ Error scraping {player_url}: {e}")
        return None

async def populate_prices():
    """One-time bulk price fetcher — fills missing prices."""
    print(f"\n🚀 Starting bulk price population at {datetime.now(timezone.utc)} UTC")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("""
        SELECT id, player_url
        FROM fut_players
        WHERE price IS NULL OR price = 0
    """)

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
            print(f"⚠️ No price found: {url}")
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

        await asyncio.sleep(0.3)

    await conn.close()
    print(f"\n🎯 Bulk price population complete — {updated} prices added, {skipped} skipped.")

if __name__ == "__main__":
    asyncio.run(populate_prices())