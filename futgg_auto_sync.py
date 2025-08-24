import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime

FUTGG_URL = "https://www.fut.gg/players/"
DATABASE_URL = "DATABASE_URL"  # <- use your Railway DB URL here

async def fetch_html(session, page):
    url = f"{FUTGG_URL}?page={page}"
    async with session.get(url) as response:
        return await response.text()

def parse_alt_text(alt_text):
    try:
        parts = [p.strip() for p in alt_text.split("-")]
        if len(parts) >= 3:
            name = parts[0]
            rating = int(parts[1])
            version = "-".join(parts[2:])
            return name, rating, version
        return alt_text, 0, "N/A"
    except:
        return alt_text, 0, "N/A"

async def sync_players():
    print(f"🚀 Starting auto-sync at {datetime.utcnow()} UTC")

    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        for page in range(1, 335):  # 334 total pages on FUT.GG
            try:
                html = await fetch_html(session, page)
                soup = BeautifulSoup(html, "html.parser")
                cards = soup.select("a.group\\/player")

                if not cards:
                    print(f"⚠️ Page {page}: No players found.")
                    continue

                players = []
                for card in cards:
                    img_tag = card.select_one("img")
                    if not img_tag:
                        continue

                    alt_text = img_tag.get("alt", "").strip()
                    img_url = img_tag.get("src", "")
                    name, rating, version = parse_alt_text(alt_text)

                    players.append((name, rating, version, img_url, datetime.utcnow()))

                # Save to DB
                for p in players:
                    await conn.execute("""
                        INSERT INTO fut_players (name, rating, version, image_url, updated_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (name, rating)
                        DO UPDATE SET version=$3, image_url=$4, updated_at=$5
                    """, *p)

                print(f"✅ Synced page {page} ({len(players)} players)")

            except Exception as e:
                print(f"❌ Failed on page {page}: {e}")
                continue

    await conn.close()
    print("🎯 All players synced successfully!")

async def scheduler():
    while True:
        await sync_players()
        print("⏳ Waiting 10 minutes before next sync...")
        await asyncio.sleep(600)

if __name__ == "__main__":
    asyncio.run(scheduler())

