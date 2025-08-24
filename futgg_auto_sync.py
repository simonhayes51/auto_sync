import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# FUT.GG player pages
FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"

# Railway DB URL
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

print(f"🔌 DEBUG: Using DATABASE_URL = {DATABASE_URL}")


def parse_alt_text(alt_text):
    """
    Extract player name, rating, and version from FUT.GG <img alt="...">
    Example: "Konaté - 98 - Shapeshifters"
    """
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


def fetch_players_from_page(page_number):
    """Fetch FUT.GG players from a single page."""
    url = FUTGG_BASE_URL.format(page_number)
    print(f"🌐 Fetching: {url}")

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

            # Player URL & slug
            player_url = f"https://www.fut.gg{card['href']}"
            player_slug = card["href"].split("/")[2]

            # Correctly extract card_id (remove trailing hash)
            card_id = img_url.split("futgg-player-item-card/")[1].split('.')[0]

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,
                "player_url": player_url,
                "player_slug": player_slug,
                "card_id": card_id,
                "created_at": datetime.now(timezone.utc)
            })
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players


async def sync_players():
    """Sync FUT.GG player data into Railway PostgreSQL."""
    print(f"\n🚀 Starting full FUT.GG sync at {datetime.now(timezone.utc)} UTC")

    all_players = []
    page = 1

    # Loop until no more players are found
    while True:
        players = fetch_players_from_page(page)
        if not players:
            print(f"✅ No players found on page {page}, stopping pagination.")
            break
        all_players.extend(players)
        print(f"📦 Page {page}: {len(players)} players fetched.")
        page += 1
        await asyncio.sleep(0.5)  # Be nice to FUT.GG servers

    print(f"🔍 Total players fetched: {len(all_players)}")

    # Save players into DB
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    saved, failed = 0, 0
    for p in all_players:
        try:
            await conn.execute("""
                INSERT INTO fut_players 
                (name, rating, version, image_url, player_url, player_slug, card_id, created_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (name, rating)
                DO UPDATE SET 
                    version=$3, 
                    image_url=$4, 
                    player_url=$5, 
                    player_slug=$6,
                    card_id=$7,
                    created_at=$8
            """, p["name"], p["rating"], p["version"], p["image_url"], p["player_url"],
                 p["player_slug"], p["card_id"], p["created_at"])
            saved += 1
        except Exception as e:
            failed += 1
            if failed <= 10:  # Limit noisy logs
                print(f"⚠️ Failed to save {p['name']}: {e}")

    await conn.close()
    print(f"🎯 FUT.GG sync complete — {saved} players saved, {failed} failed.")


async def main():
    await sync_players()


if __name__ == "__main__":
    asyncio.run(main())