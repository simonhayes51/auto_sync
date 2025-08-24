import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import os

DATABASE_URL = os.getenv("DATABASE_URL")
FUTGG_URL = "https://www.fut.gg/players/"

# Scheduled run time (6:15 PM London / 17:15 UTC)
LONDON_OFFSET = 1
SYNC_HOUR_UTC = 17
SYNC_MINUTE = 02

async def fetch_html(session, url):
    async with session.get(url) as resp:
        return await resp.text()

async def sync_players():
    conn = await asyncpg.connect(DATABASE_URL)
    async with aiohttp.ClientSession() as session:
        players_fetched = 0
        page = 1

        while True:
            url = f"{FUTGG_URL}?page={page}"
            print(f"🌍 Fetching: {url}")
            html = await fetch_html(session, url)
            soup = BeautifulSoup(html, "html.parser")
            cards = soup.select("a.group\\/player")

            if not cards:
                print(f"✅ No players on page {page}, stopping pagination.")
                break

            for card in cards:
                try:
                    img_tag = card.select_one("img")
                    if not img_tag:
                        continue

                    alt_text = img_tag.get("alt", "").strip()
                    img_url = img_tag.get("src", "")

                    parts = [p.strip() for p in alt_text.split("-")]
                    if len(parts) >= 3:
                        name, rating, version = parts[0], int(parts[1]), "-".join(parts[2:])
                    else:
                        continue

                    await conn.execute("""
                        INSERT INTO fut_players (name, rating, version, image_url, updated_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (name, rating)
                        DO UPDATE SET version=$3, image_url=$4, updated_at=$5
                    """, name, rating, version, img_url, datetime.now(timezone.utc))

                    players_fetched += 1
                except Exception as e:
                    print(f"⚠️ Failed to save player: {e}")
                    continue

            print(f"📦 Page {page}: {len(cards)} players fetched.")
            page += 1

        await conn.close()
        print(f"🎯 Daily sync complete — Total players fetched: {players_fetched}")

async def scheduler():
    while True:
        now = datetime.now(timezone.utc)
        if now.hour == SYNC_HOUR_UTC and now.minute == SYNC_MINUTE:
            print(f"🚀 Starting daily auto-sync at {now}")
            await sync_players()
            await asyncio.sleep(60)  # Prevent multiple runs in the same minute
        await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(scheduler())