import aiohttp
import asyncio
import logging
import random

API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3

# ✅ Use proper browser headers to bypass detection
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.fut.gg/",
    "Origin": "https://www.fut.gg",
    "Connection": "keep-alive"
}

logger = logging.getLogger("fut-price-sync")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

async def fetch_price(session, card_id):
    url = f"{API_URL}/{card_id}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=15) as resp:
                text = await resp.text()

                if resp.status == 200:
                    try:
                        data = await resp.json()
                        price = data.get("data", {}).get("currentPrice", {}).get("price")

                        if price is not None:
                            logger.info(f"✅ {card_id} → {price}")
                            return price
                        else:
                            logger.warning(f"⚠️ {card_id} → No price in response: {text[:80]}")
                            return None

                    except Exception as e:
                        logger.error(f"❌ {card_id} → JSON parse failed: {e} | Response: {text[:80]}")
                        return None

                elif resp.status in [403, 429]:
                    logger.warning(f"🚫 {card_id} blocked ({resp.status}), retrying...")
                    await asyncio.sleep(random.uniform(1.5, 3))
                    continue

                elif resp.status == 404:
                    logger.info(f"⏭️ {card_id} → Not found (404)")
                    return None

                else:
                    logger.error(f"❌ {card_id} → Unexpected status {resp.status}: {text[:80]}")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout fetching {card_id} (attempt {attempt})")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ {card_id} → Request failed: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"⏭️ {card_id} → All retries failed")
    return None

async def main():
    test_ids = [100664475, 50509123, 246236, 213648, 207435]  # Change to real IDs from DB
    async with aiohttp.ClientSession() as session:
        for cid in test_ids:
            price = await fetch_price(session, cid)
            await asyncio.sleep(0.5)  # 🔹 Small delay to prevent getting blocked

if __name__ == "__main__":
    asyncio.run(main())