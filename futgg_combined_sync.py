import os
import re
import sys
import json
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
HEADERS = {"User-Agent": "Mozilla/5.0"}
FUTGG_BASE = "https://www.fut.gg"

# ---------------------- UTILS ---------------------- #
def extract_card_id(image_url: str):
    """Extract card_id from image_url if available"""
    if not image_url:
        return None
    match = re.search(r'/(25-\d+)/', image_url)
    return match.group(1) if match else None

def generate_player_url(player_id: str, player_slug: str, card_id: str = None):
    """Generate correct player URL depending on card variation"""
    base = f"{FUTGG_BASE}/players/{player_id}-{player_slug}"
    return f"{base}/{card_id}/" if card_id else base

# ---------------------- SYNC PLAYERS ---------------------- #
async def sync_players():
    print(f"🔄 Starting full player sync at {datetime.now(timezone.utc)}")
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            page = 1
            total_synced = 0

            while True:
                url = f"{FUTGG_BASE}/players?page={page}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        print(f"⚠️ Failed to fetch {url} — status {resp.status}")
                        break

                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    player_cards = soup.select("a.player-card-link")

                    if not player_cards:
                        break  # No more pages

                    for card in player_cards:
                        try:
                            name = card.select_one("div.player-name").text.strip()
                            rating = int(card.select_one("div.player-rating").text.strip())
                            version = card.select_one("div.player-version").text.strip() if card.select_one("div.player-version") else "Base"
                            image_url = card.select_one("img")["src"].strip()
                            player_href = card["href"].strip()  # e.g. /players/158023-lionel-messi
                            player_slug = player_href.split("/")[-1]
                            player_id = player_href.split("/")[2].split("-")[0]
                            card_id = extract_card_id(image_url)
                            player_url = generate_player_url(player_id, player_slug, card_id)

                            await conn.execute("""
                                INSERT INTO fut_players (name, rating, version, image_url, created_at, player_slug, player_url, card_id)
                                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                                ON CONFLICT (name, rating) DO UPDATE
                                SET version = EXCLUDED.version,
                                    image_url = EXCLUDED.image_url,
                                    player_slug = EXCLUDED.player_slug,
                                    player_url = EXCLUDED.player_url,
                                    card_id = EXCLUDED.card_id
                            """, name, rating, version, image_url, datetime.now(timezone.utc), player_slug, player_url, card_id)

                            total_synced += 1
                        except Exception as e:
                            print(f"⚠️ Failed to save player: {e}")

                print(f"✅ Synced page {page} ({total_synced} total players)")
                page += 1

    finally:
        await conn.close()
        print("🎯 Player sync complete — database updated.")

# ---------------------- UPDATE PRICES ---------------------- #
async def update_prices():
    print(f"⏳ Starting price sync at {datetime.now(timezone.utc)}")
    conn = await asyncpg.connect(DATABASE_URL)

    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players WHERE player_url IS NOT NULL")

        async with aiohttp.ClientSession(headers=HEADERS) as session:
            for player in players:
                try:
                    async with session.get(player["player_url"]) as resp:
                        if resp.status != 200:
                            print(f"⚠️ Failed to fetch {player['player_url']} — status {resp.status}")
                            continue

                        html = await resp.text()
                        soup = BeautifulSoup(html, "html.parser")
                        price_elem = soup.select_one("div.font-bold.text-2xl")
                        price = int(price_elem.text.replace(",", "").replace(" ", "")) if price_elem else None

                        if price:
                            await conn.execute("UPDATE fut_players SET latest_price = $1 WHERE id = $2", price, player["id"])
                            print(f"💰 Updated price for {player['id']} → {price}")
                        else:
                            print(f"⚠️ Price not found for {player['id']}")

                except Exception as e:
                    print(f"⚠️ Failed to update price for {player['id']}: {e}")

    finally:
        await conn.close()
        print("🎯 Price sync complete — database updated.")

# ---------------------- SCHEDULER ---------------------- #
async def scheduler():
    while True:
        now = datetime.now(timezone.utc)
        # Price sync every 10 mins
        if now.minute % 10 == 0:
            await update_prices()
        # Player sync daily at 18:02 London (17:02 UTC)
        if now.hour == 17 and now.minute == 2:
            await sync_players()
        await asyncio.sleep(60)

# ---------------------- ENTRY POINT ---------------------- #
if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "--force":
            print("⚡ Manual override: running FULL sync now!")
            asyncio.run(sync_players())
            asyncio.run(update_prices())
        elif sys.argv[1] == "--players":
            print("⚡ Manual override: running PLAYER sync only!")
            asyncio.run(sync_players())
        elif sys.argv[1] == "--prices":
            print("⚡ Manual override: running PRICE sync only!")
            asyncio.run(update_prices())
        else:
            print("❌ Invalid argument. Use --force | --players | --prices")
    else:
        asyncio.run(scheduler())