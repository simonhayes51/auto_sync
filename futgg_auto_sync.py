import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone
from dotenv import load_dotenv
from bs4 import BeautifulSoup

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_BASE_URL = "https://www.fut.gg/players/"

# Extract card_id from image_url
def extract_card_id(image_url: str):
    try:
        return image_url.split("/")[-1].split('"')[0].split(".")[0]
    except:
        return None

# -------------------------------
# SYNC ALL PLAYERS DAILY @ 18:02
# -------------------------------
async def sync_players():
    print(f"⏳ Starting player sync at {datetime.now(timezone.utc)} UTC")
    conn = await asyncpg.connect(DATABASE_URL)
    session = aiohttp.ClientSession()

    page = 1
    total_synced = 0

    try:
        while True:
            url = f"https://www.fut.gg/api/fc25/players/?page={page}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"⚠️ Failed to fetch page {page} — status {resp.status}")
                    break

                data = await resp.json()
                players = data.get("items", [])
                if not players:
                    break

                for player in players:
                    player_id = player.get("id")
                    name = player.get("name")
                    rating = player.get("rating")
                    version = player.get("version", "Unknown")
                    image_url = player.get("image", "")
                    player_slug = player.get("slug")
                    player_url = player_slug
                    card_id = extract_card_id(image_url)

                    try:
                        await conn.execute("""
                            INSERT INTO fut_players 
                                (id, name, rating, version, image_url, player_slug, player_url, card_id, created_at)
                            VALUES ($1,$2,$3,$4,$5,$6,$7,$8, NOW())
                            ON CONFLICT (id) DO UPDATE SET
                                name = EXCLUDED.name,
                                rating = EXCLUDED.rating,
                                version = EXCLUDED.version,
                                image_url = EXCLUDED.image_url,
                                player_slug = EXCLUDED.player_slug,
                                player_url = EXCLUDED.player_url,
                                card_id = EXCLUDED.card_id
                        """, player_id, name, rating, version, image_url, player_slug, player_url, card_id)
                    except Exception as e:
                        print(f"⚠️ Failed to save {name}: {e}")

                total_synced += len(players)
                print(f"✅ Synced page {page} ({len(players)} players)")
                page += 1

        print(f"🎯 Player sync complete — {total_synced} players updated.")

    finally:
        await session.close()
        await conn.close()

# -------------------------------
# PRICE SYNC EVERY 10 MINUTES
# -------------------------------
async def update_prices():
    print(f"⏳ Starting price sync at {datetime.now(timezone.utc)} UTC")
    conn = await asyncpg.connect(DATABASE_URL)
    session = aiohttp.ClientSession()

    try:
        players = await conn.fetch("SELECT id, player_slug, card_id FROM fut_players")
        total_updated = 0

        for player in players:
            slug = player["player_slug"]
            card_id = player["card_id"]

            if not slug:
                print(f"⚠️ Skipping player {player['id']} — missing slug")
                continue

            # Build proper FUT.GG URL
            if card_id:
                url = f"{FUTGG_BASE_URL}{slug}/{card_id}/"
            else:
                url = f"{FUTGG_BASE_URL}{slug}/"

            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        print(f"⚠️ Failed to fetch {url} — status {resp.status}")
                        continue

                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")

                    # Locate the Console (PS/Xbox) price
                    price_el = soup.select_one("div.flex.flex-col.items-start > span")
                    if not price_el:
                        print(f"⚠️ Could not find price for {slug}")
                        continue

                    price_text = price_el.text.strip().replace(",", "").replace(" ", "")
                    if price_text.isdigit():
                        await conn.execute(
                            "UPDATE fut_players SET price = $1 WHERE id = $2",
                            int(price_text),
                            player["id"]
                        )
                        total_updated += 1

            except Exception as e:
                print(f"⚠️ Error fetching price for {slug}: {e}")

        print(f"🎯 Price sync complete — updated {total_updated} players.")

    finally:
        await session.close()
        await conn.close()

# -------------------------------
# SCHEDULER
# -------------------------------
async def scheduler():
    while True:
        now = datetime.now(timezone.utc)

        # Run price sync every 10 minutes
        if now.minute % 10 == 0:
            await update_prices()

        # Run player sync daily at 18:02 UTC (London BST ~ 19:02 in summer)
        if now.hour == 17 and now.minute == 2:  # 18:02 London = 17:02 UTC during BST
            await sync_players()

        await asyncio.sleep(60)

if __name__ == "__main__":
    print(f"🚀 Starting combined auto-sync at {datetime.now(timezone.utc)} UTC")
    asyncio.run(scheduler())