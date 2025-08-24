import os
import aiohttp
import asyncpg
import logging
from discord.ext import commands, tasks
from bs4 import BeautifulSoup

DATABASE_URL = os.getenv("DATABASE_URL")

class DataSync(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="sync")
    async def sync_players(self, ctx):
        """Sync FUT.GG players into the database"""
        await ctx.send("🔄 Starting player data sync...")

        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                name TEXT,
                rating INT,
                version TEXT,
                image_url TEXT
            )
        """)

        total_added = 0
        async with aiohttp.ClientSession() as session:
            for page in range(1, 3):  # test with 2 pages first
                url = f"https://www.fut.gg/players/?page={page}"
                async with session.get(url) as resp:
                    if resp.status != 200:
                        await ctx.send(f"⚠️ Failed page {page} — HTTP {resp.status}")
                        continue

                    html = await resp.text()
                    soup = BeautifulSoup(html, "html.parser")
                    cards = soup.select("a.group\\/player")

                    if not cards:
                        await ctx.send(f"⚠️ No cards found on page {page}")
                        continue

                    for card in cards:
                        img = card.select_one("img")
                        if not img:
                            continue

                        player_name = img.get("alt", "Unknown")
                        image_url = img.get("src", "")
                        rating = "0"
                        version = "Unknown"

                        # Insert into database
                        await conn.execute("""
                            INSERT INTO players (name, rating, version, image_url)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT DO NOTHING
                        """, player_name, rating, version, image_url)
                        total_added += 1

        await conn.close()
        await ctx.send(f"✅ Player sync complete. Added {total_added} players.")

async def setup(bot):
    await bot.add_cog(DataSync(bot))
