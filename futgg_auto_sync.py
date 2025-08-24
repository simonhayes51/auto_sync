import os
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

FUTGG_BASE_URL = "https://www.fut.gg/players/?page={}"
FUTGG_PRICE_API = "https://www.fut.gg/api/fut/player-prices/25/{}"
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

print(f"🔌 DEBUG: Using DATABASE_URL = {DATABASE_URL}")

# -----------------------
#  HELPERS
# -----------------------
def parse_alt_text(alt_text):
    """Extract player name, rating, and version from FUT.GG <img alt="...">"""
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

def extract_card_id(img_url: str) -> str:
    """Extract numeric FUT.GG player ID from the image URL"""
    try:
        # Example: https://game-assets.fut.gg/.../25-117667614.something.webp
        filename = img_url.split("/")[-1]
        parts = filename.split("-")
        if len(parts) >= 2:
            return parts[1].split(".")[0]  # Extract numeric ID only
        return None
    except:
        return None

def fetch_players_from_page(page_number):
    """Fetch FUT.GG players from a single page"""
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
            card_id = extract_card_id(img_url)

            if not rating or not card_id:
                continue

            player_url = "https://www.fut.gg" + card.get("href", "")

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,
                "card_id": card_id,  # Numeric ID for API lookups
                "player_url": player_url,
                "updated_at": datetime.utcnow()
            })
        except Exception as e:
            print(f"⚠️ Failed to parse card: {e}")
            continue

    return players

# -----------------------
#  MAIN SYNC LOGIC
# -----------------------
async def sync_players_and_prices():
    """Sync FUT.GG player data + prices into Railway PostgreSQL"""
    print(f"\n🚀 Starting full sync at {datetime.utcnow()} UTC")

    all_players = []
    page = 1

    # STEP 1: FETCH ALL PLAYERS
    while True:
        players = fetch_players_from_page(page)
        if not players:
            print(f"✅ No players found on page {page}, stopping pagination.")
            break
        all_players.extend(players)
        print(f"📦 Page {page}: {len(players)} players fetched.")
        page += 1
        await asyncio.sleep(0.5)

    print(f"🔍 Total players fetched: {len(all_players)}")

    # STEP 2: CONNECT TO DB
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    # STEP 3: SAVE PLAYER DATA + FETCH PRICES
    for p in all_players:
        try:
            # Insert or update player info
            await conn.execute("""
                INSERT INTO fut_players (name, rating, version, image_url, card_id, player_url, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (name, rating)
                DO UPDATE SET version=$3, image_url=$4, card_id=$5, player_url=$6, created_at=$7
            """, p["name"], p["rating"], p["version"], p["image_url"], p["card_id"], p["player_url"], p["updated_at"])

            # STEP 4: FETCH PRICE FROM FUT.GG API
            api_url = FUTGG_PRICE_API.format(p["card_id"])
            resp = requests.get(api_url, timeout=10)

            if resp.status_code == 403:
                print(f"⚠️ Forbidden: API blocked for {p['name']} ({p['card_id']})")
                continue

            if resp.status_code != 200:
                print(f"⚠️ API error {resp.status_code} for {p['name']} ({p['card_id']})")
                continue

            data = resp.json()

            # Check if we have a console price
            price = data.get("ps", {}).get("LCPrice") or data.get("xbox", {}).get("LCPrice")
            if price is None:
                print(f"ℹ️ No price for card_id {p['card_id']} ({p['name']}) — likely SBC/Reward")
                continue

            # STEP 5: UPDATE PRICE IN DB
            await conn.execute("""
                UPDATE fut_players
                SET price = $1, created_at = $2
                WHERE card_id = $3
            """, price, p["updated_at"], p["card_id"])

            print(f"✅ Updated {p['name']} → {price} coins")

            await asyncio.sleep(0.15)  # Prevent hammering the API

        except Exception as e:
            print(f"⚠️ Failed to sync {p['name']}: {e}")

    await conn.close()
    print("🎯 Full sync complete — DB fully updated with prices.")

# -----------------------
#  EXECUTION
# -----------------------
if __name__ == "__main__":
    asyncio.run(sync_players_and_prices())
