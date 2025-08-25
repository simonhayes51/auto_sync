import aiohttp
import asyncio
import asyncpg
import logging
import random
import os
import json
from typing import Optional, List

# Configuration - Optimized for speed
API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3
BATCH_SIZE = 50  # Increased from 10 to 50
DELAY_BETWEEN_REQUESTS = 0.1  # Reduced from 0.5s to 0.1s
DELAY_BETWEEN_BATCHES = 0.5   # Reduced from 2.0s to 0.5s
MAX_CONCURRENT_REQUESTS = 20  # New: concurrent requests per batch

# Railway PostgreSQL configuration - use the exact Railway DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:FiwuZKPRyUKvzWMMqTWxfpRGtZrOYLCa@shuttle.proxy.rlwy.net:19669/railway")

if not DATABASE_URL:
    logger.error("❌ DATABASE_URL environment variable not found!")
    raise ValueError("DATABASE_URL is required")

# Table and column names for your fut_players table
TABLE_NAME = "fut_players"
CARD_ID_COLUMN = "card_id"
PRICE_COLUMN = "price"

# Use proper browser headers to bypass detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg",
    "Connection": "keep-alive"
}

# Setup logging with debug level to see more details
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
            logger.info(f"🔌 Connecting to database...")
            self.pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=5,
                command_timeout=60
            )
            logger.info("✅ Connected to Railway PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            logger.error(f"❌ Using URL: {DATABASE_URL[:50]}...")
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
                # Convert card IDs to integers (in case they're stored as strings)
                card_ids = []
                for row in rows:
                    try:
                        card_id = int(row[CARD_ID_COLUMN])
                        card_ids.append(card_id)
                    except (ValueError, TypeError):
                        logger.warning(f"⚠️ Skipping invalid card_id: {row[CARD_ID_COLUMN]}")
                        continue
                
                logger.info(f"📊 Retrieved {len(card_ids)} valid card IDs from database")
                return card_ids
        except Exception as e:
            logger.error(f"❌ Failed to fetch card IDs: {e}")
            return []

    async def update_price(self, card_id: int, price: int) -> bool:
        """Update the price for a specific card ID"""
        try:
            async with self.pool.acquire() as conn:
                query = f"UPDATE {TABLE_NAME} SET {PRICE_COLUMN} = $1 WHERE {CARD_ID_COLUMN} = $2"
                # Convert card_id to string since database expects string
                result = await conn.execute(query, price, str(card_id))
                
                # Check if any row was updated
                rows_updated = int(result.split()[-1])
                if rows_updated > 0:
                    logger.info(f"💾 Updated {card_id} with price {price}")
                    return True
                else:
                    logger.warning(f"⚠️ No rows updated for card_id {card_id}")
                    return False
        except Exception as e:
            logger.error(f"❌ Failed to update price for {card_id}: {e}")
            return False

    async def update_prices_batch(self, price_updates: List[tuple]) -> int:
        """Update multiple prices in a single transaction"""
        if not price_updates:
            return 0
        
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    query = f"UPDATE {TABLE_NAME} SET {PRICE_COLUMN} = $1 WHERE {CARD_ID_COLUMN} = $2"
                    
                    updated_count = 0
                    for price, card_id in price_updates:
                        # Convert card_id to string since database expects string
                        result = await conn.execute(query, price, str(card_id))
                        rows_updated = int(result.split()[-1])
                        if rows_updated > 0:
                            updated_count += 1
                    
                    logger.info(f"💾 Batch updated {updated_count} prices")
                    return updated_count
        except Exception as e:
            logger.error(f"❌ Batch update failed: {e}")
            return 0

async def fetch_price(session: aiohttp.ClientSession, card_id: int) -> Optional[int]:
    """Fetch price for a single card ID with enhanced debugging"""
    url = f"{API_URL}/{card_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=15) as resp:
                text = await resp.text()

                if resp.status == 200:
                    try:
                        data = await resp.json()
                        
                        # Enhanced debugging - let's see what we actually get
                        logger.debug(f"🔍 {card_id} → Full response: {json.dumps(data, indent=2)[:500]}")
                        
                        # Try multiple possible paths to find the price
                        price_paths = [
                            ("data.currentPrice.price", data.get("data", {}).get("currentPrice", {}).get("price")),
                            ("data.price", data.get("data", {}).get("price")),
                            ("price", data.get("price")),
                            ("currentPrice.price", data.get("currentPrice", {}).get("price") if isinstance(data.get("currentPrice"), dict) else None),
                            ("currentPrice", data.get("currentPrice")),
                            ("data.marketPrice", data.get("data", {}).get("marketPrice")),
                            ("marketPrice", data.get("marketPrice")),
                        ]
                        
                        for path, value in price_paths:
                            if value is not None:
                                logger.info(f"✅ {card_id} → {value} (found at {path})")
                                return int(value)
                        
                        # If no price found, log the structure for debugging
                        logger.warning(f"⚠️ {card_id} → No price found. Response keys: {list(data.keys())}")
                        if "data" in data:
                            logger.warning(f"⚠️ {card_id} → Data keys: {list(data['data'].keys()) if isinstance(data['data'], dict) else type(data['data'])}")
                        return None

                    except Exception as e:
                        logger.error(f"❌ {card_id} → JSON parse failed: {e}")
                        logger.error(f"❌ {card_id} → Raw response: {text[:200]}")
                        return None

                elif resp.status in [403, 429]:
                    logger.warning(f"🚫 {card_id} blocked ({resp.status}), retrying...")
                    await asyncio.sleep(random.uniform(2, 4))
                    continue

                elif resp.status == 404:
                    logger.info(f"⏭️ {card_id} → Not found (404)")
                    return None

                else:
                    logger.error(f"❌ {card_id} → Unexpected status {resp.status}: {text[:100]}")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout fetching {card_id} (attempt {attempt})")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ {card_id} → Request failed: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"⏭️ {card_id} → All retries failed")
    return None

async def process_batch_concurrent(session: aiohttp.ClientSession, db_manager: DatabaseManager,
                                   card_ids: List[int]) -> int:
    """Process a batch of card IDs concurrently for speed"""
    
    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    
    async def fetch_with_semaphore(card_id):
        async with semaphore:
            return await fetch_price(session, card_id)
    
    # Execute all requests concurrently
    tasks = [fetch_with_semaphore(card_id) for card_id in card_ids]
    prices = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Collect successful price updates
    price_updates = []
    successful = 0
    errors = 0
    
    for card_id, price in zip(card_ids, prices):
        if isinstance(price, Exception):
            logger.error(f"❌ {card_id} → Exception: {price}")
            errors += 1
        elif price is not None:
            price_updates.append((price, card_id))
            successful += 1
    
    # Update all prices in database
    updated_count = await db_manager.update_prices_batch(price_updates)
    
    logger.info(f"🚀 Batch complete: {successful} fetched, {updated_count} updated, {errors} errors")
    return updated_count

async def main():
    """Main function to orchestrate the price scraping and database updates"""
    db_manager = DatabaseManager()
    
    try:
        # Connect to database
        await db_manager.connect()
        
        # Get all card IDs from database
        card_ids = await db_manager.get_card_ids()
        
        if not card_ids:
            logger.warning("⚠️ No card IDs found in database")
            return
        
        logger.info(f"🚀 Starting to process {len(card_ids)} cards")
        
        # Process cards in batches with concurrent requests
        total_updated = 0
        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=50),
            timeout=aiohttp.ClientTimeout(total=30)
        ) as session:
            for i in range(0, len(card_ids), BATCH_SIZE):
                batch = card_ids[i:i + BATCH_SIZE]
                batch_num = (i // BATCH_SIZE) + 1
                total_batches = (len(card_ids) + BATCH_SIZE - 1) // BATCH_SIZE
                
                logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} cards)")
                
                updated_count = await process_batch_concurrent(session, db_manager, batch)
                total_updated += updated_count
                
                # Shorter delay between batches
                if i + BATCH_SIZE < len(card_ids):
                    logger.info(f"⏳ Waiting {DELAY_BETWEEN_BATCHES}s before next batch...")
                    await asyncio.sleep(DELAY_BETWEEN_BATCHES)
        
        logger.info(f"🎉 Process completed! Updated {total_updated} prices out of {len(card_ids)} cards")
        
    except Exception as e:
        logger.error(f"❌ Main process failed: {e}")
        raise
    finally:
        await db_manager.close()

async def test_api_with_sample_cards():
    """Test API with a few cards from your database"""
    db_manager = DatabaseManager()
    try:
        await db_manager.connect()
        
        # Get first 5 card IDs from your database
        sample_cards = await db_manager.get_card_ids(limit=5)
        logger.info(f"🧪 Testing API with {len(sample_cards)} sample cards: {sample_cards}")
        
        async with aiohttp.ClientSession() as session:
            for card_id in sample_cards:
                logger.info(f"\n🔍 Testing card_id: {card_id}")
                price = await fetch_price(session, card_id)
                logger.info(f"Result: {price}")
                await asyncio.sleep(1)  # Small delay between tests
                
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
    finally:
        await db_manager.close()

async def test_database_connection():
    """Test database connection and show table structure"""
    db_manager = DatabaseManager()
    try:
        await db_manager.connect()
        
        async with db_manager.pool.acquire() as conn:
            # Check if table exists and show structure
            result = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, TABLE_NAME)
            
            if result:
                logger.info(f"✅ Table '{TABLE_NAME}' found with columns:")
                for row in result:
                    logger.info(f"  - {row['column_name']} ({row['data_type']})")
                
                # Show sample data
                sample = await conn.fetch(f"SELECT * FROM {TABLE_NAME} LIMIT 3")
                logger.info(f"📊 Sample data from {TABLE_NAME}:")
                for row in sample:
                    logger.info(f"  {dict(row)}")
            else:
                logger.error(f"❌ Table '{TABLE_NAME}' not found!")
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
    finally:
        await db_manager.close()

async def test_api_with_sample_cards():
    """Test API with a few cards from your database"""
    db_manager = DatabaseManager()
    try:
        await db_manager.connect()
        
        # Get first 5 card IDs from your database
        sample_cards = await db_manager.get_card_ids(limit=5)
        logger.info(f"🧪 Testing API with {len(sample_cards)} sample cards: {sample_cards}")
        
        async with aiohttp.ClientSession() as session:
            for card_id in sample_cards:
                logger.info(f"\n🔍 Testing card_id: {card_id}")
                price = await fetch_price(session, card_id)
                logger.info(f"Result: {price}")
                await asyncio.sleep(1)  # Small delay between tests
                
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
    finally:
        await db_manager.close()

if __name__ == "__main__":
    try:
        # Add some startup logging
        logger.info("🚀 FUT Price Sync starting...")
        logger.info(f"📊 DATABASE_URL configured: {'Yes' if DATABASE_URL else 'No'}")
        logger.info(f"🔗 Using connection: postgresql://postgres:***@shuttle.proxy.rlwy.net:19669/railway")
        
        # Test database connection and table structure (optional)
        # asyncio.run(test_database_connection())
        
        # Test API with sample cards from your database (optional)  
        # asyncio.run(test_api_with_sample_cards())
        
        # Run the full price scraping process
        asyncio.run(main())
        
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        raise  # Re-raise to show full traceback
