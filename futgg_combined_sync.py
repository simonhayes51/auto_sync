import os
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

FUTGG_PLAYERS_URL = "https://www.fut.gg/players/"
FUTGG_PRICE_URL = "https://www.fut.gg/player/{player_id}/"

LONDON_TZ = timezone(timedelta(hours=1))  # BST / London time


# ------------------------
# Connect to PostgreSQL
# ------------------------
async def connect_db():
    return await asyncpg.connect(DATABASE_URL)


# ------------------------
# Fetch FUT.GG HTML
# ------------------------
async def fetch_html(session, url):
    async with session.get(url) as response:
        if response.status != 200:
            print(f"⚠️ Failed to fetch {url} — status {response.status}")
            return None
        return await response.text()


# ------------------------
# Scrape Player Data (Names, Ratings, Versions, Images)
# ------------------------
async def scrape_players():
    print("🔄 Fetching all player data from FUT.GG...")
    players = []

    async with aiohttp.ClientSession() as session:
        html = await fetch_html(session, FUTGG_PLAYERS_URL)
        if not html:
            return players

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("a.group\\/player")
        print(f"✅ Found {len(cards)} player cards.")

        for card in cards:
            try:
                img = card.select_one("img")
                if not img:
                    continue

                alt = img.get("alt", "").strip()
                src = img.get("src", "").strip()

                parts = [p.strip() for p in alt.split("-")]
                if len(parts) >= 3:
                    name, rating, version = parts[0], int(parts[1]), "-".join(parts[2:])
                else:
                    name, rating, version = alt, 0, "N/A"

                players.append({
                    "name": name,
                    "rating": rating,
                    "version": version,
                    "image_url": src,
                    "created_at": datetime.now(timezone.utc)
                })

            except Exception as e:
                print(f"⚠️ Failed to parse player card: {e}")
                continue

    return players


# ------------------------
# Save Player Data
# ------------------------
async def save_players(players):
    conn = await connect_db()
    try:
        for p in players:
            await conn.execute("""
                INSERT INTO fut_players (name, rating, version, image_url, created_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name, rating)
                DO UPDATE SET version=$3, image_url=$4, created_at=$5
            """, p["name"], p["rating"], p["version"], p["image_url"], p["created_at"])
        print(f"🎯 Saved {len(players)} players to DB.")
    finally:
        await conn.close()


# ------------------------
# Scrape Player Prices
# ------------------------
async def scrape_price(session, player_id):
    url = FUTGG_PRICE_URL.format(player_id=player_id)
    html = await fetch_html(session, url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    price_tag = soup.select_one("div.font-bold.text-2xl")
    if price_tag:
        price = price_tag.text.strip().replace(",", "").replace(" ", "")
        return int(price) if price.isdigit() else None
    return None


# ------------------------
# Update Prices for All Players
# ------------------------
async def sync_prices():
    print("💰 Updating player prices...")
    conn = await connect_db()
    async with aiohttp.ClientSession() as session:
        rows = await conn.fetch("SELECT id FROM fut_players")
        for row in rows:
            try:
                price = await scrape_price(session, row["id"])
                if price:
                    await conn.execute("""
                        UPDATE fut_players
                        SET price = $1, updated_at = $2
                        WHERE id = $3
                    """, price, datetime.now(timezone.utc), row["id"])
                    print(f"✅ Updated price for Player {row['id']}: {price}")
            except Exception as e:
                print(f"⚠️ Failed to update price for Player {row['id']}: {e}")
    await conn.close()


# ------------------------
# Scheduler: Daily Player Sync + 5-Min Price Sync
# ------------------------
async def scheduler():
    while True:
        now = datetime.now(LONDON_TZ)

        # Run daily sync at 18:15 London time
        if now.hour == 18 and now.minute == 02:
            print("🌍 Running daily player sync...")
            players = await scrape_players()
            await save_players(players)

        # Run price sync every 5 mins
        if now.minute % 5 == 0:
            await sync_prices()

        await asyncio.sleep(60)  # Check every minute


# ------------------------
# Entry Point
# ------------------------
if __name__ == "__main__":
    print(f"🚀 Starting combined FUT.GG sync at {datetime.now(timezone.utc)}")
    asyncio.run(scheduler())