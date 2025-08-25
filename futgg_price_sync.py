import aiohttp
import asyncio
import logging
import random
import csv
import os

API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3
SKIPPED_FILE = "skipped_players.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.248 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
]

# Logging setup
logger = logging.getLogger("fut-price-sync")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Create skipped players CSV if not exists
if not os.path.exists(SKIPPED_FILE):
    with open(SKIPPED_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["card_id", "reason"])

async def fetch_price(session, card_id):
    """Fetch a player's price from FUT.GG using their card_id."""
    # ✅ Strip prefix if card_id starts with "25-"
    clean_id = str(card_id).replace("25-", "").strip()
    url = f"{API_URL}/{clean_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Referer": f"https://www.fut.gg/players/{clean_id}/"
            }

            async with session.get(url, headers=headers, timeout=15) as resp:
                logger.info(f"🔹 Fetching {clean_id} → Attempt {attempt} → {url}")

                # Handle rate limiting / blocking
                if resp.status in [403, 429, 500, 502, 503]:
                    logger.warning(f"⚠️ Blocked {clean_id} → Status {resp.status}")
                    await asyncio.sleep(random.uniform(1, 3))
                    continue

                # Not found on FUT.GG
                if resp.status == 404:
                    logger.info(f"⏭️ {clean_id} → Not found")
                    log_skipped(clean_id, "Not found (404)")
                    return None

                # Success
                data = await resp.json()
                price = data.get("data", {}).get("currentPrice", {}).get("price")

                if price is not None:
                    logger.info(f"✅ {clean_id} → {price}")
                    return price
                else:
                    logger.warning(f"⚠️ {clean_id} → No price found in JSON")
                    log_skipped(clean_id, "No price in JSON")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout {clean_id} → Attempt {attempt}")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ Error fetching {clean_id}: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    # If all retries fail, log skipped
    logger.error(f"⏭️ Skipping {clean_id} → All retries failed")
    log_skipped(clean_id, "All retries failed")
    return None

def log_skipped(card_id, reason):
    """Save skipped players into CSV for debugging."""
    with open(SKIPPED_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([card_id, reason])

async def main():
    """Main entry point for fetching prices."""
    card_ids = []  # TODO: Replace with DB fetch later
    # Example for testing:
    card_ids = ["25-100664475", "100664475", "25-8219", "8219"]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_price(session, cid) for cid in card_ids]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())