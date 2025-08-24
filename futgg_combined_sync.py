import os
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dotenv import load_dotenv
from aiohttp import ClientSession, TCPConnector

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_URL = "https://www.fut.gg/players/"

# ---------------------- DB SETUP ----------------------
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS fut_players (
            id SERIAL PRIMARY KEY,
            name TEXT,
            rating INT,
            version TEXT,
            image_url TEXT,
            player_slug TEXT,
            player_url TEXT,
            price BIGINT,
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT fut_players_unique UNIQUE (name, rating)
        )
    """)
    await conn.close()

# ---------------------- FETCH FUT.GG HTML ----------------------
async def fetch_html(session: ClientSession, url: str):
    try:
        async with session.get(url, timeout=15) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"⚠️ Failed to fetch {url} — status {response.status}")
                return None
    except Exception as e:
        print(f"❌ Error fetching {url}: {e}")
        return None

# ---------------------- DAILY PLAYER SYNC ----------------------
async def sync_players():
    print(f"\n🚀 Starting daily FUT.GG player sync at {datetime.now(timezone.utc)}")

    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        page = 1
        total_added = 0

        while True:
            url = f"{FUTGG_URL}?page={page}"
            html = await fetch_html(session, url)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            player_cards = soup.select("a.group\\/player")

            if not player_cards:
                print("✅ No more player cards found, ending sync.")
                break

            for card in player_cards:
                try:
                    img_tag = card.select_one("img")
                    alt_text = img_tag.get("alt", "").strip() if img_tag else ""
                    img_url = img_tag.get("src", "") if img_tag else ""

                    parts = [p.strip() for p in alt_text.split("-")]
                    if len(parts) >= 3:
                        name = parts[0]
                        rating = int(parts[1])
                        version = "-".join(parts[2:])
                    else:
                        name = alt_text
                        rating = None
                        version = "N/A"

                    player_href = card.get("href", "").strip()
                    player_slug = player_href.strip("/").split("/")[-1]
                    player_url = f"https://www.fut.gg{player_href}"

                    await conn.execute("""
                        INSERT INTO fut_players (name, rating, version, image_url, player_slug, player_url, updated_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                        ON CONFLICT (name, rating)
                        DO UPDATE SET version=$3, image_url=$4, player_slug=$5, player_url=$6, updated_at=NOW()
                    """, name, rating, version, img_url, player_slug, player_url)

                    total_added += 1

                except Exception as e:
                    print(f"⚠️ Failed to parse player card: {e}")
                    continue

            print(f"✅ Synced page {page} ({len(player_cards)} players)")
            page += 1

        await conn.close()
        print(f"🎯 Player sync complete — {total_added} players updated.")

# ---------------------- PRICE SYNC (ASYNC + PARALLEL) ----------------------
async def fetch_and_update_price(session, conn, player):
    try:
        html = await fetch_html(session, player["player_url"])
        if not html:
            return

        soup = BeautifulSoup(html, "html.parser")
        price_tag = soup.select_one("div.price-box div.price")
        if not price_tag:
            return

        price_str = price_tag.text.strip().replace(",", "").replace(" ", "")
        price = int(price_str) if price_str.isdigit() else None

        await conn.execute("""
            UPDATE fut_players
            SET price=$1, updated_at=NOW()
            WHERE id=$2
        """, price, player["id"])

    except Exception as e:
        print(f"⚠️ Failed price fetch for {player['player_url']}: {e}")

async def sync_prices():
    print(f"\n💰 Starting FUT.GG price sync at {datetime.now(timezone.utc)}")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")

    # Limit concurrent requests to avoid rate-limiting
    connector = TCPConnector(limit=20)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_and_update_price(session, conn, player) for player in players]
        await asyncio.gather(*tasks)

    await conn.close()
    print("🎯 Price sync complete — database updated.")

# ---------------------- SCHEDULER ----------------------
async def scheduler():
    while True:
        now = datetime.now(timezone.utc)

        # Daily new player sync at 18:02 London (17:02 UTC)
        if now.hour == 17 and now.minute == 2:
            await sync_players()

        # Price sync runs every 10 minutes
        await sync_prices()
        await asyncio.sleep(600)

# ---------------------- MAIN ENTRY ----------------------
async def main():
    await init_db()
    await scheduler()

if __name__ == "__main__":
    asyncio.run(main())