import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime

FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

print(f"🔌 DEBUG: Using DATABASE_URL = {DATABASE_URL}")

def parse_alt_text(alt_text):
    try:
        parts = [p.strip() for p in alt_text.split("-")]
        if len(parts) >= 3:
            name = parts[0]
            rating = int(parts[1]) if parts[1].isdigit() else None
            version = "-".join(parts[2:])
            return name, rating, version
        return alt_text, None, "N/A"
    except:
        return alt_text, None, "N/A"

def extract_player_details(card):
    try:
        href = card.get("href", "")
        slug = href.split("/")[2] if "/players/" in href else None
        player_url = f"https://www.fut.gg{href}" if slug else None
        img_tag = card.select_one("img")
        img_url = img_tag.get("src", "") if img_tag else ""
        card_id = None
        if "futgg-player-item-card/" in img_url:
            try:
                card_id = img_url.split("futgg-player-item-card/")[1].split('"')[0]
            except:
                card_id = None
        return slug, player_url, card_id
    except:
        return None, None, None

def fetch_players_from_page(page_number):
    url = FUTGG_BASE_URL.format(page_number)
    print(f"🌐 Fetching page {page_number}...")

    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch page {page_number}: {response.status_code}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    cards = soup.select("a.group\\/player")

    players = []
    for card in cards:
        try:
            img_tag = card.select_one("img")
            if not img_tag:
                continue
            alt_text = img_tag.get("alt", "").strip()
            img_url = img_tag.get("src", "")
            name, rating, version = parse_alt_text(alt_text)
            if not rating:
                continue
            player_slug, player_url, card_id = extract_player_details(card)
            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,
                "player_slug": player_slug,
                "player_url": player_url,
                "card_id": card_id,
                "created_at": datetime.utcnow()
            })
        except:
            continue

    return players

async def sync_players():
    print(f"\n🚀 Starting auto-sync at {datetime.utcnow()} UTC")
    all_players = []
    page = 1
    total_inserted = 0
    total_failed = 0

    while True:
        players = fetch_players_from_page(page)
        if not players:
            print(f"✅ No players found on page {page}, stopping.")
            break
        all_players.extend(players)
        print(f"📦 Page {page}: {len(players)} players fetched.")
        page += 1
        await asyncio.sleep(0.5)

    print(f"🔍 Total players fetched: {len(all_players)}")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    for p in all_players:
        try:
            await conn.execute("""
                INSERT INTO fut_players (name, rating, version, image_url, created_at, player_slug, player_url, card_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (name, rating)
                DO UPDATE SET
                    version=$3,
                    image_url=$4,
                    created_at=$5,
                    player_slug=$6,
                    player_url=$7,
                    card_id=$8
            """,
                p["name"], p["rating"], p["version"], p["image_url"], p["created_at"],
                p["player_slug"], p["player_url"], p["card_id"]
            )
            total_inserted += 1
        except:
            total_failed += 1

    await conn.close()
    print(f"🎯 FUT.GG sync complete — {total_inserted} saved, {total_failed} failed.")

async def scheduler():
    try:
        await sync_players()
    except Exception as e:
        print(f"❌ Sync error: {e}")

if __name__ == "__main__":
    asyncio.run(scheduler())