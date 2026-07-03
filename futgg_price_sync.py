import aiohttp
import asyncio
import asyncpg
import logging
import random
import os
import re
import json
import urllib.parse
from typing import Optional, List

# Configuration - Optimized for speed but stable
API_URL = "https://www.fut.gg/api/fut/player-prices/26"
MAX_RETRIES = 3

# ScraperAPI's rendering mode is far slower per-request and free/trial plans
# cap concurrency much lower than a direct fetch would need - default a lot
# more conservatively when it's in use, but still allow override via env.
_using_scraperapi = bool(os.getenv("SCRAPERAPI_KEY"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5" if _using_scraperapi else "30"))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.1"))
DELAY_BETWEEN_BATCHES = float(os.getenv("DELAY_BETWEEN_BATCHES", "3" if _using_scraperapi else "0.5"))
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", "3" if _using_scraperapi else "15"))

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

PROXY_URL = os.getenv("PROXY_URL")

# If set, requests go through ScraperAPI's rendering endpoint instead of
# fetching fut.gg directly - it runs a real headless browser to solve
# Cloudflare's JS challenge, which a plain proxy (PROXY_URL) cannot do.
SCRAPERAPI_KEY = os.getenv("SCRAPERAPI_KEY")
# Toggle so we can A/B test: a raw JSON endpoint may not need (or may choke
# on) render=true, which is meant for pages that need real JS execution.
SCRAPERAPI_RENDER = os.getenv("SCRAPERAPI_RENDER", "true").lower() not in ("false", "0", "")
# The API endpoint requires a signed verify= token minted by JS running on
# the actual player page - hitting it directly can never work, even through
# a real rendering browser. This fetches the real page instead and reads the
# price out of the rendered HTML, same as a human browsing would see it.
SCRAPERAPI_USE_PAGE = os.getenv("SCRAPERAPI_USE_PAGE", "true").lower() not in ("false", "0", "")
# ScraperAPI's own error told us fut.gg is a "protected domain" needing
# premium=true or ultra_premium=true - plain premium wasn't enough in
# testing, so default to the higher tier. Empty string disables this param.
SCRAPERAPI_PREMIUM_PARAM = os.getenv("SCRAPERAPI_PREMIUM_PARAM", "ultra_premium")
# The page renders fine but shows loading skeletons - fut.gg's own
# client-side data fetch hasn't finished when ScraperAPI snapshots it.
# wait_for_selector tells ScraperAPI to hold off returning the result until
# this element exists, i.e. until real content has replaced the skeleton.
# Best-effort guess at fut.gg's own section anchor for the PRICES tab, based
# on the pattern seen in a real page dump (id="playstyles" for that tab).
# Empty string disables this param if the guess turns out wrong.
SCRAPERAPI_WAIT_SELECTOR = os.getenv("SCRAPERAPI_WAIT_SELECTOR", "#prices")
_debug_html_samples_left = int(os.getenv("SCRAPERAPI_DEBUG_SAMPLES", "3"))
# What to search for when picking the debug-dump window - not the exact key
# name we ultimately parse on, just something likely to sit near it.
_DEBUG_MARKERS = ["currentPrice", "buyNowPrice", "\"price\"", "eaId"]

# fut.gg embeds this same currentPrice/price shape in the page's hydration
# data - matches by eaId first so we don't grab a different player's price
# off a "related players" widget on the same page.
_PRICE_RE_TEMPLATE = r'"eaId"\s*:\s*{card_id}\s*,\s*"currentPrice"\s*:\s*\{{[^{{}}]*?"price"\s*:\s*(\d+)'
_PRICE_RE_FALLBACK = r'"currentPrice"\s*:\s*\{[^{}]*?"price"\s*:\s*(\d+)'


def wrap_scraperapi(target_url: str) -> str:
    params = {"api_key": SCRAPERAPI_KEY, "url": target_url}
    if SCRAPERAPI_RENDER:
        params["render"] = "true"
    return "https://api.scraperapi.com/?" + urllib.parse.urlencode(params)

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

    async def get_card_urls(self, limit: Optional[int] = None) -> dict:
        """Fetch {card_id: player_url} for cards that have a stored player_url."""
        try:
            async with self.pool.acquire() as conn:
                query = f"SELECT {CARD_ID_COLUMN}, player_url FROM {TABLE_NAME} WHERE {CARD_ID_COLUMN} IS NOT NULL AND player_url IS NOT NULL"
                if limit:
                    query += f" LIMIT {limit}"
                rows = await conn.fetch(query)
                out = {}
                for row in rows:
                    try:
                        out[int(row[CARD_ID_COLUMN])] = row["player_url"]
                    except (ValueError, TypeError):
                        continue
                return out
        except Exception as e:
            logger.error(f"❌ Failed to fetch player URLs: {e}")
            return {}

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

async def fetch_price_from_page(session: aiohttp.ClientSession, card_id: int, player_url: str) -> Optional[int]:
    """Render the actual player page (via ScraperAPI) and read the price out of
    its hydration data - the API endpoint requires a verify= token minted by
    that page's own JS, so hitting it directly never works, however it's fetched."""
    global _debug_html_samples_left
    params = {"api_key": SCRAPERAPI_KEY, "url": player_url, "render": "true"}
    if SCRAPERAPI_WAIT_SELECTOR:
        params["wait_for_selector"] = SCRAPERAPI_WAIT_SELECTOR
    if SCRAPERAPI_PREMIUM_PARAM:
        params[SCRAPERAPI_PREMIUM_PARAM] = "true"
    url = "https://api.scraperapi.com/?" + urllib.parse.urlencode(params)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, timeout=60) as resp:
                if resp.status != 200:
                    body = (await resp.text())[:200]
                    logger.error(f"❌ {card_id} → page fetch status {resp.status}: {body}")
                    if resp.status in (429, 500, 502, 503):
                        await asyncio.sleep(random.uniform(2, 4))
                        continue
                    return None

                html = await resp.text()

                if _debug_html_samples_left > 0:
                    _debug_html_samples_left -= 1
                    dump_at = None
                    for marker in _DEBUG_MARKERS:
                        idx = html.find(marker)
                        if idx != -1:
                            dump_at = (marker, idx)
                            break
                    if dump_at:
                        marker, idx = dump_at
                        start = max(0, idx - 200)
                        logger.info(f"🔍 {card_id} → found '{marker}' at offset {idx}, window: {html[start:idx + 800]}")
                    else:
                        logger.info(f"🔍 {card_id} → none of {_DEBUG_MARKERS} found anywhere in {len(html)} chars; middle sample: {html[len(html)//2:len(html)//2 + 1500]}")

                m = re.search(_PRICE_RE_TEMPLATE.format(card_id=card_id), html)
                if not m:
                    m = re.search(_PRICE_RE_FALLBACK, html)
                if m:
                    price = int(m.group(1))
                    logger.info(f"✅ {card_id} → {price} (parsed from rendered page)")
                    return price

                logger.warning(f"⚠️ {card_id} → page fetched OK but no price pattern matched (len={len(html)})")
                return None
        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout rendering page for {card_id} (attempt {attempt})")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ {card_id} → page render failed: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"⏭️ {card_id} → all page-render retries failed")
    return None


async def fetch_price(session: aiohttp.ClientSession, card_id: int, player_url: Optional[str] = None) -> Optional[int]:
    """Fetch price for a single card ID with enhanced debugging"""
    if SCRAPERAPI_KEY and SCRAPERAPI_USE_PAGE:
        if player_url:
            return await fetch_price_from_page(session, card_id, player_url)
        logger.warning(f"⚠️ {card_id} → no player_url stored, skipping (can't render a page without one)")
        return None

    target_url = f"{API_URL}/{card_id}"

    if SCRAPERAPI_KEY:
        url = wrap_scraperapi(target_url)
        proxy = None
        timeout = 60  # rendering a real browser page is much slower than a direct request
    else:
        url = target_url
        proxy = PROXY_URL
        timeout = 15

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=timeout, proxy=proxy) as resp:
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
                                   card_ids: List[int], url_map: Optional[dict] = None) -> int:
    """Process a batch of card IDs concurrently for speed"""

    # Create semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    url_map = url_map or {}

    async def fetch_with_semaphore(card_id):
        async with semaphore:
            return await fetch_price(session, card_id, url_map.get(card_id))
    
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
        url_map = {}
        if SCRAPERAPI_KEY and SCRAPERAPI_USE_PAGE:
            url_map = await db_manager.get_card_urls()
            card_ids = list(url_map.keys())
            logger.info(f"📊 {len(card_ids)} cards have a stored player_url (needed for page rendering)")
        else:
            card_ids = await db_manager.get_card_ids()

        test_limit = os.getenv("PRICE_SYNC_LIMIT")
        if test_limit:
            card_ids = card_ids[: int(test_limit)]

        if not card_ids:
            logger.warning("⚠️ No card IDs found in database")
            return
        
        logger.info(f"🚀 Starting to process {len(card_ids)} cards")
        
        # Show progress more frequently
        processed_cards = 0
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
                
                updated_count = await process_batch_concurrent(session, db_manager, batch, url_map)
                total_updated += updated_count
                processed_cards += len(batch)
                
                # Show progress every few batches
                progress = (processed_cards / len(card_ids)) * 100
                logger.info(f"📈 Progress: {processed_cards}/{len(card_ids)} cards ({progress:.1f}%) - {total_updated} prices updated")
                
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
        if SCRAPERAPI_KEY and SCRAPERAPI_USE_PAGE:
            logger.info(f"🌐 ScraperAPI: Yes, rendering player pages (premium_param={SCRAPERAPI_PREMIUM_PARAM or 'none'}, wait_for_selector={SCRAPERAPI_WAIT_SELECTOR or 'none'}) and parsing price from HTML")
        elif SCRAPERAPI_KEY:
            logger.info(f"🌐 ScraperAPI: Yes, hitting API URL directly, render={SCRAPERAPI_RENDER}")
        else:
            logger.info("🌐 ScraperAPI: No (direct/proxy fetch)")
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
