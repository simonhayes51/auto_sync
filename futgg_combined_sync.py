import aiohttp
import asyncio
import asyncpg
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ---------------------------
# FETCH PRICE FOR ONE PLAYER
# ---------------------------
async def fetch_price(session, player):
    player_id = player["id"]
    player_url = player["player_url"]
    try:
        async with session.get(player_url, timeout=15) as resp:
            if resp.status != 200:
                print(f"⚠️ Failed to fetch {player_url} — status {resp.status}")
                return None

            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")

            # FUT.GG price element for Console
            price_element = soup.select_one("div.price-box > div.price-value")
            if price_element:
                price_text = price_element.get_text(strip=True).replace(",", "")
                return (player_id, int(price_text), datetime.now(timezone.utc))
            return None

    except Exception as e:
        print(f"⚠️ Error fetching {player_url}: {e}")
        return None

# ---------------------------
# UPDATE ALL PRICES
# ---------------------------
async def update_prices():
    print(f"\n⏳ Starting price sync at {datetime.now(timezone.utc)}")

    conn = await asyncpg.connect(DATABASE_URL)
    players = await conn.fetch("SELECT id, player_url FROM fut_players")
    await conn.close()

    print(f"📦 {len(players)} players found in DB, starting async price fetch...")

    # Async session setup
    async with aiohttp.ClientSession() as session:
        tasks = []
        for player in players:
            tasks.append(fetch_price(session, dict(player)))

        # Process in batches of 75 to avoid hammering FUT.GG
        results = []
        for i in range(0, len(tasks), 75):
            batch = tasks[i:i + 75]
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)
            await asyncio.sleep(1)  # Small pause between batches

    # Filter valid results only
    results = [r for r in results if r]

    if results:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.executemany("""
            UPDATE fut_players
            SET price = $2, updated_at = $3
            WHERE id = $1
        """, results)
        await conn.close()

        print(f"✅ Price sync complete — {len(results)} players updated.")
    else:
        print("⚠️ No price updates found.")