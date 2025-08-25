import aiohttp
import asyncio
import asyncpg
import logging
import random
import os
import json
from typing import Optional, List

# Configuration
API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3
BATCH_SIZE = 10
DELAY_BETWEEN_REQUESTS = 0.5
DELAY_BETWEEN_BATCHES = 2.0

# Railway PostgreSQL configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:xxx@shuttle.proxy.rlwy.net:19669/railway")

# Table and column names
TABLE_NAME = "fut_players"
CARD_ID_COLUMN = "card_id"
PRICE_COLUMN = "price"

# Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg",
    "Connection": "keep-alive"
}

# Logging
logger = logging.getLogger("fut-price-sync")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

class DatabaseManager:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Create database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(DATABASE_URL)
            logger.info("✅ Connected to Railway PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    async def close(self):
        """Close database connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("🔌 Database connection closed")

    async def get_card_ids(self, limit: Optional[int] = None) -> List[int]:
        """Fetch all card IDs from the database"""
        try:
            async with self.pool.acquire() as conn:
                query = f"SELECT {CARD_ID_COLUMN} FROM {TABLE_NAME} WHERE {CARD_ID_COLUMN} IS NOT NULL"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query)
                return [row[CARD_ID_COLUMN] for row in rows]
        except Exception as e:
            logger.error(f"❌ Failed to fetch card IDs: {e}")
            return []

    # ... (rest of your methods corrected the same way)

if __name__ == "__main__":
    try:
        # asyncio.run(test_database_connection())
        asyncio.run(test_api_with_sample_cards())
        # asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")