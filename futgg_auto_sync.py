import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/futtrader")
FUTGG_URL = "https://www.fut.gg/players/"

# ---------- DB INITIALISATION ----------
async def init_db():
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            rating TEXT,
            version TEXT,
            image_url TEXT,
            updated_at TIMESTAMP DEFAULT NOW(),
            CONSTRAINT unique_player UNIQUE (name, rating)
        )
    """)
    await conn.close()

# ---------- PARSE PLAYER NAME / RATING / VERSION ----------
def parse_alt_text(alt_text):
    try:
        parts = [p.strip() for p in alt_text.split("-")]
        if len(parts) >= 3:
            name = parts[0]
            rating = parts[1]
            version = "-".join(parts[2:])
            return name, rating, version
        return alt_text, "N/A", "N/A"
    except:
        return alt_text, "N/A", "N/A"

# ---------- FETCH & PARSE FUT.GG PLAYER DATA ----------
async def fetch_player_page(session, page):
    url = f"{FUTGG_URL}?page={page}"
    async with session.get(url) as resp:
        if resp.status != 200:
            print(f"⚠️ Failed to fetch page {page} → Status {resp.status}")
            return []
        html = await resp.text()
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("a.group\\/player")  # Escape the "/" in class
        players = []

        for card in cards:
            try:
                img_tag = card.select_one("img")
                if not img_tag:
                    continue

                alt_text = img_tag.get("alt", "").strip()
                img_url = img_tag.get("src", "")
                name, rating, version = parse_alt_text(alt_text)

                players.append({
                    "name": name,
                    "rating": rating,
                    "version": version,
                    "image_url": img_url,
                    "updated_at": datetime.utcnow()
                })
            except Exception as e:
                print(f"⚠️ Parse error on page {page}: {e}")
                continue
        return players

# ---------- SAVE TO DATABASE ----------
async def save_players(players):
    conn = await asyncpg.connect(DATABASE_URL)
    for p in players:
        try:
            await conn.execute("""
                INSERT INTO players (name, rating, version, image_url, updated_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name, rating)
                DO UPDATE SET version=$3, image_url=$4, updated_at=$5
            """, p["name"], p["rating"], p["version"], p["image_url"], p["updated_at"])
        except Exception as e:
            print(f"⚠️ Failed to save {p['name']}: {e}")
    await conn.close()

# ---------- MAIN AUTO-SYNC LOOP ----------
async def auto_sync():
    await init_db()

    while True:
        print(f"🚀 Starting auto-sync at {datetime.utcnow()} UTC")
        all_players = []
        page = 1

        async with aiohttp.ClientSession() as session:
            while True:
                players = await fetch_player_page(session, page)
                if not players:
                    break
                print(f"✅ Synced page {page} ({len(players)} players)")
                all_players.extend(players)
                page += 1

        if all_players:
            print(f"💾 Saving {len(all_players)} players to database...")
            await save_players(all_players)
            print(f"🎯 Sync complete — {len(all_players)} total players updated.\n")
        else:
            print("⚠️ No players found — FUT.GG layout might have changed!")

        print("⏳ Waiting 10 minutes before next sync...\n")
        await asyncio.sleep(600)  # 10 minutes

# ---------- RUN ----------
if __name__ == "__main__":
    asyncio.run(auto_sync())
