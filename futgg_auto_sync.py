import os
import asyncio
import asyncpg
from datetime import datetime, timezone
from playwright.async_api import async_playwright

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found. Set it in Railway → Variables.")

API_URL = "https://www.fut.gg/api/fut/player-prices/25/{}"

async def fetch_price(playwright, card_id):
    """Bypass Cloudflare & fetch prices from FUT.GG API."""
    url = API_URL.format(card_id)

    try:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto(url, timeout=20000)
        content = await page.content()

        # FUT.GG returns JSON — we need to extract it
        json_data = await page.evaluate("() => document.body.innerText")
        await browser.close()

        import json
        data = json.loads(json_data)

        ps_price = data.get("prices", {}).get("ps", {}).get("LCPrice")
        xbox_price = data.get("prices", {}).get("xbox", {}).get("LCPrice")
        return ps_price or xbox_price

    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
        return None

async def sync_prices():
    print(f"\n🚀 Starting price sync at {datetime.now(timezone.utc)} UTC")
    conn = await asyncpg.connect(DATABASE_URL)

    players = await conn.fetch("SELECT id, name, card_id FROM fut_players WHERE card_id IS NOT NULL")
    print(f"📦 Found {len(players)} players to update prices for.")

    async with async_playwright() as p:
        updated = 0
        skipped = 0

        for player in players:
            card_id = player["card_id"].split(".")[0]
            price = await fetch_price(p, card_id)

            if price is None:
                skipped += 1
                continue

            try:
                await conn.execute("""
                    UPDATE fut_players
                    SET price = $1, created_at = $2
                    WHERE id = $3
                """, price, datetime.now(timezone.utc), player["id"])
                updated += 1
                print(f"✅ {player['name']} → {price} coins")
            except Exception as e:
                print(f"⚠️ Failed to update {player['name']}: {e}")

    await conn.close()
    print(f"\n🎯 Sync complete → {updated} prices updated, {skipped} skipped.")

if __name__ == "__main__":
    asyncio.run(sync_prices())