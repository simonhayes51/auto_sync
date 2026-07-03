import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

EA_SID = os.getenv("EA_X_UT_SID")
if not EA_SID:
    raise RuntimeError("❌ EA_X_UT_SID not found! Log into the FUT Web App and set a fresh session token.")

EA_GAME = os.getenv("EA_GAME", "fc26")
EA_BASE = f"https://utas.mob.v5.prd.futc-ext.gcp.ea.com/ut/game/{EA_GAME}"

REQUEST_DELAY = float(os.getenv("EA_REQUEST_DELAY", "1.5"))  # seconds between cards
MAX_RETRIES = int(os.getenv("EA_MAX_RETRIES", "3"))

HEADERS = {
    "x-ut-sid": EA_SID,
    "Origin": "https://www.ea.com",
    "Referer": "https://www.ea.com/",
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}


class ExpiredSession(Exception):
    pass


_DEBUG_SAMPLES_LEFT = int(os.getenv("EA_DEBUG_SAMPLES", "5"))


async def fetch_lowest_bin(session: aiohttp.ClientSession, card_id: int):
    """Lowest active buyNowPrice for a card, or None if no live listings."""
    global _DEBUG_SAMPLES_LEFT
    url = f"{EA_BASE}/transfermarket"
    params = {"start": 0, "num": 21, "type": "player", "maskedDefId": card_id}

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=20) as resp:
                if resp.status == 401:
                    raise ExpiredSession()
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "30"))
                    print(f"⏳ Rate limited, waiting {retry_after}s", flush=True)
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status != 200:
                    body = (await resp.text())[:300]
                    print(f"⚠️ Unexpected status {resp.status} for card {card_id}: {body}", flush=True)
                    return None

                raw = await resp.text()
                if _DEBUG_SAMPLES_LEFT > 0:
                    _DEBUG_SAMPLES_LEFT -= 1
                    print(f"🔍 DEBUG raw response for card {card_id}: {raw[:500]}", flush=True)

                data = await resp.json()
                if not isinstance(data, dict) or "auctionInfo" not in data:
                    print(f"⚠️ Unexpected response shape for card {card_id} (no auctionInfo key): {raw[:200]}", flush=True)
                prices = [
                    a["buyNowPrice"]
                    for a in (data.get("auctionInfo") or [])
                    if a.get("tradeState") == "active"
                    and isinstance(a.get("buyNowPrice"), (int, float))
                ]
                return min(prices) if prices else None
        except ExpiredSession:
            raise
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                print(f"⚠️ Failed to fetch price for {card_id}: {e}", flush=True)
                return None
            await asyncio.sleep(2 ** attempt)
    return None


async def detect_price_columns(conn: asyncpg.Connection):
    cols = {
        r["column_name"]
        for r in await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='fut_players'
            """
        )
    }
    return {
        "has_price": "price" in cols,
        "has_price_num": "price_num" in cols,
        "has_price_updated_at": "price_updated_at" in cols,
    }


async def main():
    print(f"🚀 Starting EA PRICE SYNC at {datetime.now(timezone.utc)} UTC", flush=True)
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        cols = await detect_price_columns(conn)

        set_parts = []
        arg_kinds = []  # "text" or "num", one per placeholder before card_id
        idx = 1
        if cols["has_price"]:
            set_parts.append(f"price = ${idx}")
            arg_kinds.append("text")
            idx += 1
        if cols["has_price_num"]:
            set_parts.append(f"price_num = ${idx}")
            arg_kinds.append("num")
            idx += 1
        if cols["has_price_updated_at"]:
            set_parts.append("price_updated_at = NOW()")
        if not set_parts:
            raise RuntimeError("❌ fut_players has none of price/price_num/price_updated_at columns")

        update_sql = f"UPDATE fut_players SET {', '.join(set_parts)} WHERE card_id = ${idx}"

        rows = await conn.fetch("SELECT card_id FROM fut_players WHERE card_id IS NOT NULL")
        card_ids = [int(r["card_id"]) for r in rows]

        limit = os.getenv("EA_SYNC_LIMIT")
        if limit:
            card_ids = card_ids[: int(limit)]

        total = len(card_ids)
        print(f"📊 {total} cards to price (platform: PS/Xbox shared market)", flush=True)

        updated = skipped = 0
        async with aiohttp.ClientSession() as session:
            for i, card_id in enumerate(card_ids, start=1):
                try:
                    price = await fetch_lowest_bin(session, card_id)
                except ExpiredSession:
                    print("❌ EA session expired (401) — get a fresh EA_X_UT_SID and restart.", flush=True)
                    break

                if price is not None:
                    args = [str(price) if kind == "text" else int(price) for kind in arg_kinds]
                    args.append(card_id)
                    await conn.execute(update_sql, *args)
                    updated += 1
                else:
                    skipped += 1

                if i % 100 == 0 or i == total:
                    print(f"💾 Progress: {i}/{total} ({updated} priced, {skipped} skipped)", flush=True)

                await asyncio.sleep(REQUEST_DELAY)

        print(f"✅ EA PRICE SYNC complete: {updated} updated, {skipped} skipped", flush=True)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
