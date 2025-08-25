import aiohttp
import asyncio
import logging
import random

API_URL = "https://www.fut.gg/api/fut/player-prices/25"
MAX_RETRIES = 3

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:117.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.248 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1"
]

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
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json, text/plain, */*",
                "Referer": f"https://www.fut.gg/players/{card_id}/",
            }

            async with session.get(url, headers=headers, timeout=15) as resp:
                # ✅ Handle rate limits or temporary blocks
                if resp.status in [403, 429, 500, 502, 503]:
                    logger.warning(f"⚠️ Attempt {attempt}/{MAX_RETRIES} → {card_id} blocked ({resp.status})")
                    await asyncio.sleep(random.uniform(1, 3))  # Backoff
                    continue

                # ❌ Player doesn't exist on FUT.GG
                if resp.status == 404:
                    logger.info(f"⏭️ {card_id} → Not found (404)")
                    return None

                # ✅ Success
                data = await resp.json()
                price = data.get("data", {}).get("currentPrice", {}).get("price")

                if price is not None:
                    logger.info(f"✅ {card_id} → {price}")
                    return price
                else:
                    logger.warning(f"⚠️ {card_id} → No price found")
                    return None

        except asyncio.TimeoutError:
            logger.error(f"⏳ Timeout fetching {card_id} (attempt {attempt})")
            await asyncio.sleep(random.uniform(1, 3))
        except Exception as e:
            logger.error(f"❌ Error fetching {card_id}: {e}")
            await asyncio.sleep(random.uniform(1, 2))

    logger.error(f"⏭️ Skipping {card_id} → All retries failed")
    return None