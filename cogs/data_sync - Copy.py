import asyncio
import asyncpg
from bs4 import BeautifulSoup
from discord.ext import commands, tasks
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import re

FUTGG_URL = "https://www.fut.gg/players/"

class DataSync(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db = None
        self.sync_data.start()

    async def cog_load(self):
        self.db = self.bot.db

    def parse_alt_text(self, alt_text):
        """
        Extract player name, rating, and card type from the <img alt="..."> text.
        Example: "Konaté - 98 - Shapeshifters"
        """
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

    def fetch_player_data(self):
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

    @tasks.loop(minutes=10)
    async def sync_data(self):
        print("🔄 Fetching FUT.GG player data via Selenium...")

        html = await asyncio.to_thread(self.fetch_player_data)
        if not html:
            print("❌ Failed to fetch FUT.GG HTML, skipping update.")
            return

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("a.group\\/player")  # Escape the "/"

        if not cards:
            print("⚠️ No player cards found. FUT.GG layout may have changed.")
            return

        print(f"🔍 Found {len(cards)} player cards. Parsing first 5 for testing...\n")
        players = []

        for card in cards[:5]:  # Only log the first 5 for now
            try:
                img_tag = card.select_one("img")
                if not img_tag:
                    continue

                alt_text = img_tag.get("alt", "").strip()
                img_url = img_tag.get("src", "")
                name, rating, version = self.parse_alt_text(alt_text)

                player = {
                    "name": name,
                    "rating": rating,
                    "version": version,
                    "image_url": img_url,
                    "updated_at": datetime.utcnow()
                }

                players.append(player)
                print(f"  • {name} ({rating}) [{version}] → {img_url}")

            except Exception as e:
                print(f"⚠️ Failed to parse player card: {e}")
                continue

        print("\n✅ Player parsing confirmed. Saving to database...")

        # Save parsed players into the DB
        async with self.bot.db.acquire() as conn:
            for p in players:
                try:
                    await conn.execute("""
                        INSERT INTO players (name, rating, version, image_url, updated_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (name, rating)
                        DO UPDATE SET version=$3, image_url=$4, updated_at=$5
                    """, p["name"], p["rating"], p["version"], p["image_url"], p["updated_at"])
                except Exception as e:
                    print(f"⚠️ Failed to save {p['name']} to DB: {e}")

        print("🎯 FUT.GG sync complete — database updated.")

    def cog_unload(self):
        self.sync_data.cancel()

async def setup(bot):
    await bot.add_cog(DataSync(bot))
