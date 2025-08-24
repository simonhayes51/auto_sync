import os
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# Database connection from Railway environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set. Configure it in Railway → Variables.")

# Price fetch interval (5 minutes)
SYNC_INTERVAL = 300

async def fetch_price(session, player_url):
    """
    Fetch the player's price from FUT.GG player page.
    """
    try:
        async with session.get(player_url) as resp:
            if resp.status != 200:
                print(f"⚠️ Failed to fetch {player_url} — status {resp.status}")
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # Locate the price container
            price_container = soup.find(
                "div",
                class_="flex items-center justify-center"
            )

            if price_container:
                # Extract numeric price only
                price_text = price_container.get_text(strip=True).replace(",", "")
                try:
                    return int(price_text)
                except ValueError:
                    print(f"⚠️ Could not parse price for {player_url}: {price_text}")
                    return None
            else:
                print(f"⚠️ No price found for {player_url}")
                return None

    except Exception as e:
        print(f"❌ Error fetching price for {player_url}: {e}")
        return None


async def update_prices():
    """
    Fetches player prices and updates them in the DB.
    """
    print(f"⏳ Starting price sync at {datetime.now(timezone.utc)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
        async with aiohttp.ClientSession() as session:
            # Get players that have a valid URL
            players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")
            print(f"🔍 Found {len(players)} players to process.")

            updated_count = 0

            for player in players:
                player_id = player["id"]
                url = player["player_url"]

                if not url or url.strip() == "":
                    continue

                price = await fetch_price(session, url)
                if price is not None:
                    try:
                        await conn.execute("""
                            UPDATE fut_players
                            SET price = $1, created_at = $2
                            WHERE id = $3
                        """, price, datetime.now(timezone.utc), player_id)
                        updated_count += 1
                    except Exception as e:
                        print(f"⚠️ Failed to update player {player_id}: {e}")

                # Be nice to FUT.GG → small delay between requests
                await asyncio.sleep(0.25)

            print(f"🎯 Price sync complete — updated {updated_count} players.")

        await conn.close()

    except Exception as e:
        print(f"❌ Price sync failed: {e}")


async def scheduler():
    """
    Runs price sync every 5 minutes.
    """
    while True:
        await update_prices()
        await asyncio.sleep(SYNC_INTERVAL)


if __name__ == "__main__":
    asyncio.run(scheduler())