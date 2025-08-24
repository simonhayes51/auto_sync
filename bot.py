import os
import logging
import discord
from discord.ext import commands
from dotenv import load_dotenv

load_dotenv()

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(name)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# Load cogs
initial_cogs = ["cogs.data_sync"]

@bot.event
async def on_ready():
    logging.info(f"✅ Logged in as {bot.user.name}")

    for cog in initial_cogs:
        try:
            await bot.load_extension(cog)
            logging.info(f"📦 Loaded {cog}")
        except Exception as e:
            logging.error(f"❌ Failed to load {cog}: {e}")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

if not DISCORD_TOKEN:
    logging.error("❌ DISCORD_TOKEN is missing in your .env file")
else:
    bot.run(DISCORD_TOKEN)
