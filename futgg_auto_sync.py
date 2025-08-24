import os
import asyncio
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# === FUT.GG Players Page ===
FUTGG_URL = "https://www.fut.gg/players/"

# === Database URL from Railway ===
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Make sure it's set in Railway → Variables.")

print(f"🔌 DEBUG: DATABASE_URL = {DATABASE_URL}")

# === Selenium Scraper Setup ===
def fetch_player_data():
    """Launch Selenium, scroll until all players load, and return HTML."""
    try:
        print("🌐 Launching headless browser...")

        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920x1080")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.get(FUTGG_URL)
        time.sleep(2)

        print("🔄 Scrolling to load all players...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("✅ All players loaded.")
                break
            last_height = new_height

        html = driver.page_source
        driver.quit()
        return html

    except Exception as e:
        print(f"❌ Selenium fetch failed: {e}")
        return None


def parse_alt_text(alt_text):
    """
    Extract player name, rating, and card type from the <img alt="..."> text.
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


async def sync_players():
    """Fetch FUT.GG player data and update PostgreSQL."""
    print(f"\n🚀 Starting auto-sync at {datetime.now(timezone.utc)} UTC")

    html = await asyncio.to_thread(fetch_player_data)
    if not html:
        print("❌ Failed to fetch FUT.GG HTML, skipping update.")
        return

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("a.group\\/player")  # FUT.GG's player cards

    if not cards:
        print("⚠️ No player cards found. FUT.GG layout may have changed.")
        return

    print(f"🔍 Found {len(cards)} player cards. Parsing...")

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
                continue  # Skip invalid ratings

            players.append({
                "name": name,
                "rating": rating,
                "version": version,
                "image_url": img_url,
                "updated_at": datetime.now(timezone.utc)
            })

        except Exception as e:
            print(f"⚠️ Failed to parse player card: {e}")
            continue

    print(f"📦 Parsed {len(players)} players. Saving to database...")

    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ DB connection failed: {e}")
        return

    for p in players:
        try:
            await conn.execute("""
                INSERT INTO fut_players (name, rating, version, image_url, updated_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name, rating)
                DO UPDATE SET version=$3, image_url=$4, updated_at=$5
            """, p["name"], p["rating"], p["version"], p["image_url"], p["updated_at"])
        except Exception as e:
            print(f"⚠️ Failed to save {p['name']}: {e}")

    await conn.close()
    print("🎯 FUT.GG sync complete — database updated.")


async def scheduler():
    """Run sync every 10 minutes forever."""
    while True:
        try:
            await sync_players()
        except Exception as e:
            print(f"❌ Sync error: {e}")
        await asyncio.sleep(600)  # 10 minutes


if __name__ == "__main__":
    asyncio.run(scheduler())
