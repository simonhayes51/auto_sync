import os
import sys
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_BASE_URL = "https://www.fut.gg"

# ========================
# FUT.GG PLAYER AUTO-SYNC
# ========================
async def sync_players():
    """Fetch all players from FUT.GG and populate DB with slugs, URLs, images, and card IDs."""
    print(f"⏳ Starting player auto-sync at {datetime.now(timezone.utc)}")

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{FUTGG_BASE_URL}/players/"
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"⚠️ Failed to fetch players list — status {resp.status}")
                    return

                html = await resp.text()
                soup = BeautifulSoup(html, "html.parser")

                players = []
                for player_card in soup.select("a.player-item"):
                    try:
                        name = player_card.select_one("div.name").text.strip()
                        rating = int(player_card.select_one("div.rating").text.strip())

                        player_href = player_card["href"]
                        player_slug = player_href.split("/")[-1]
                        player_url = FUTGG_BASE_URL + player_href

                        image_url = player_card.select_one("img")["src"]
                        # Extract card_id from the image URL
                        card_id = image_url.split("futgg-player-item-card/")[1].split('"')[0].replace('"', "")

                        players.append((name, rating, player_slug, player_url, image_url, card_id))
                    except Exception as e:
                        print(f"⚠️ Failed to parse player: {e}")

                # Insert or update DB
                for p in players:
                    try:
                        await conn.execute("""
                            INSERT INTO fut_players (name, rating, player_slug, player_url, image_url, card_id, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, NOW())
                            ON CONFLICT (name, rating)
                            DO UPDATE SET player_slug = EXCLUDED.player_slug,
                                          player_url = EXCLUDED.player_url,
                                          image_url = EXCLUDED.image_url,
                                          card_id = EXCLUDED.card_id;
                        """, p[0], p[1], p[2], p[3], p[4], p[5])
                    except Exception as e:
                        print(f"⚠️ Failed to save {p[0]}: {e}")

        print(f"✅ Player auto-sync complete — {len(players)} players updated.")
    finally:
        await conn.close()

# =====================
# FUT.GG PRICE SYNC
# =====================
async def update_prices():
    """Fetch prices for all players every 5 minutes."""
    print(f"⏳ Starting price sync at {datetime.now(timezone.utc)}")

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        players = await conn.fetch("SELECT id, player_url FROM fut_players")
        async with aiohttp.ClientSession() as session:
            for player in players:
                try:
                    url = player["player_url"]
                    if not url:
                        continue

                    async with session.get(url) as resp:
                        if resp.status != 200:
                            print(f"⚠️ Failed to fetch {url} — status {resp.status}")
                            continue

                        html = await resp.text()
                        soup = BeautifulSoup(html, "html.parser")
                        price_elem = soup.select_one("div.font-bold.text-2xl.flex.flex-row.items-center.gap-1.justify-self-end")
                        price = price_elem.text.replace(",", "").replace(" ", "") if price_elem else None

                        if price and price.isdigit():
                            await conn.execute("""
                                UPDATE fut_players
                                SET price = $1
                                WHERE id = $2
                            """, int(price), player["id"])
                except Exception as e:
                    print(f"⚠️ Failed to fetch price for {url}: {e}")

        print("🎯 Price sync complete — database updated.")
    finally:
        await conn.close()

# =========================
# SCHEDULER (COMBINED)
# =========================
async def scheduler():
    """Schedules player auto-sync daily + price sync every 5 mins."""
    while True:
        now = datetime.now(timezone.utc)
        london_now = now.astimezone(tz=timezone(timedelta(hours=1)))  # BST (London time)

        # Run daily player sync at 18:02 London time
        if london_now.hour == 18 and london_now.minute == 2:
            await sync_players()

        # Run price sync every 5 mins
        if now.minute % 5 == 0:
            await update_prices()

        await asyncio.sleep(60)  # Check every minute

# =====================
# ENTRY POINT
# =====================
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--players":
        # Force run player auto-sync manually
        asyncio.run(sync_players())
    else:
        # Start combined scheduler
        asyncio.run(scheduler())