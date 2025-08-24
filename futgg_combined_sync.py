import os
import re
import aiohttp
import asyncio
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL is not set!")

FUTGG_BASE_URL = "https://www.fut.gg/players"
CARD_ID_REGEX = re.compile(r"/(\d{2}-\d+)/?$")

# ---------------------------
# DB CONNECTION
# ---------------------------
async def get_db_connection():
    return await asyncpg.connect(DATABASE_URL)

# ---------------------------
# FETCH PAGE HTML
# ---------------------------
async def fetch_html(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"⚠️ Failed to fetch {url} — status {response.status}")
                return None
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None

# ---------------------------
# EXTRACT CARD ID FROM IMAGE URL
# ---------------------------
def extract_card_id(image_url):
    match = CARD_ID_REGEX.search(image_url)
    return match.group(1) if match else None

# ---------------------------
# UPDATE PRICES
# ---------------------------
async def update_prices():
    conn = await get_db_connection()
    players = await conn.fetch("SELECT id, player_slug, card_id, image_url FROM fut_players")
    print(f"🔄 Updating prices for {len(players)} players...")

    async with aiohttp.ClientSession() as session:
        for player in players:
            slug = player["player_slug"]
            card_id = player["card_id"]

            # If no card_id stored, extract it from image_url
            if not card_id:
                card_id = extract_card_id(player["image_url"])
                if card_id:
                    await conn.execute(
                        "UPDATE fut_players SET card_id = $1 WHERE id = $2",
                        card_id, player["id"]
                    )

            # Build proper FUT.GG URL
            if card_id:
                url = f"{FUTGG_BASE_URL}/{slug}/{card_id}/"
            else:
                url = f"{FUTGG_BASE_URL}/{slug}"

            html = await fetch_html(session, url)
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")

            # Find console prices (PS + Xbox)
            price_box = soup.find("div", class_="price-box-console")
            if not price_box:
                print(f"⚠️ No console price found for {slug}")
                continue

            try:
                price = int(price_box.get_text(strip=True).replace(",", "").replace("Coins", "").strip())
                await conn.execute("""
                    UPDATE fut_players 
                    SET price = $1 
                    WHERE id = $2
                """, price, player["id"])
                print(f"✅ {slug} → {price} coins")
            except Exception as e:
                print(f"⚠️ Failed to parse price for {slug}: {e}")

    await conn.close()
    print("🎯 Price sync complete — database updated.")

# ---------------------------
# SYNC NEW PLAYERS (DAILY AT 18:02 LONDON)
# ---------------------------
async def sync_new_players():
    print("⏳ Checking for new players...")
    # We'll hook your FUT.GG scraping logic here.
    # This will fetch any new FUT cards released at 6PM.
    # It will INSERT them into fut_players.
    pass

# ---------------------------
# MAIN SCHEDULER
# ---------------------------
async def scheduler():
    while True:
        now = datetime.now(timezone.utc)

        # Run price sync every 5 minutes
        if now.minute % 5 == 0:
            print(f"\n⏳ Starting price sync at {now}")
            await update_prices()

        # Run new-player sync at 18:02 London (17:02 UTC)
        if now.hour == 17 and now.minute == 2:
            print(f"\n⏳ Running new-player sync at {now}")
            await sync_new_players()

        await asyncio.sleep(60)

# ---------------------------
# ENTRYPOINT
# ---------------------------
if __name__ == "__main__":
    print(f"🚀 Starting combined auto-sync at {datetime.now(timezone.utc)} UTC")
    asyncio.run(scheduler())