"""
Quick, throwaway test: does futbin.com still respond to plain scraping the
way it apparently did back in FC25, or has it locked down like fut.gg did?
Also checks whether our card_id (fut.gg/EA's definition ID) even lines up
with futbin's own player ID scheme, since futbin may use a different one.
"""
import os
import re
import asyncio
import asyncpg
import aiohttp
from bs4 import BeautifulSoup

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

GAME = os.getenv("FUTBIN_GAME", "26")
LIMIT = int(os.getenv("PRICE_SYNC_LIMIT", "5"))

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}
SEM = asyncio.Semaphore(4)


def _num(txt: str) -> int:
    if not txt:
        return 0
    t = txt.lower().replace(",", "").strip()
    if t.endswith("k"):
        try:
            return int(float(t[:-1]) * 1000)
        except Exception:
            return 0
    m = re.search(r"\d[\d,]*", t)
    return int(m.group(0).replace(",", "")) if m else 0


def _parse_platform_price(soup: BeautifulSoup, platform: str) -> int:
    plat = {"ps": "ps", "xbox": "xbox", "pc": "pc"}.get((platform or "ps").lower(), "ps")
    box = (
        soup.find("div", class_=re.compile(r"price[- ]?box", re.I))
        or soup.find("div", class_=re.compile(r"price-box-original-player", re.I))
        or soup
    )
    for tag in box.find_all(string=re.compile(rf"\b{plat}\b", re.I)):
        txt = tag.parent.get_text(" ", strip=True)
        m = re.search(r"(\d[\d,\.kK]+)", txt)
        if m:
            return _num(m.group(1))
    for d in box.find_all("div", class_=re.compile(r"lowest-price", re.I)):
        val = _num(d.get_text(" ", strip=True))
        if val:
            return val
    nums = re.findall(r"\d[\d,\.kK]+", box.get_text(" ", strip=True))
    return max((_num(x) for x in nums), default=0)


async def fetch_one(session: aiohttp.ClientSession, card_id, name: str, url: str = None):
    url = url or f"https://www.futbin.com/{GAME}/player/{card_id}"
    try:
        async with SEM:
            async with session.get(url, headers=HEADERS, timeout=25) as r:
                status = r.status
                server = r.headers.get("server", "")
                has_cf_cookie = any("cf_clearance" in c for c in r.headers.getall("Set-Cookie", []))
                html = await r.text()
    except Exception as e:
        print(f"❌ {card_id} ({name}) → request failed: {e}", flush=True)
        return

    print(f"🌐 {card_id} ({name}) → {url}", flush=True)
    print(f"   status={status} server={server or 'n/a'} cf_clearance_cookie={has_cf_cookie} html_len={len(html)}", flush=True)

    if status != 200:
        print(f"   ⚠️ body sample: {html[:300]}", flush=True)
        return

    soup = BeautifulSoup(html, "html.parser")

    title = soup.find("title")
    print(f"   page title: {title.get_text(strip=True) if title else 'none'}", flush=True)

    price = _parse_platform_price(soup, "ps")
    if price:
        print(f"   ✅ parsed price: {price}", flush=True)
    else:
        print("   ⚠️ no price parsed - looking for a 'price' marker in the raw HTML:", flush=True)
        idx = html.lower().find("price")
        if idx != -1:
            start = max(0, idx - 200)
            print(f"   found 'price' at offset {idx}, window: {html[start:idx + 1000]}", flush=True)
        else:
            body = soup.find("body")
            print(f"   no 'price' text anywhere in {len(html)} chars; body sample: {body.get_text(' ', strip=True)[:800] if body else html[:800]}", flush=True)


async def main():
    # If set, test these exact known-good URLs directly instead of guessing
    # IDs from our own card_id (futbin uses its own independent ID scheme,
    # confirmed unrelated to fut.gg/EA's definition ID).
    explicit_urls = os.getenv("FUTBIN_TEST_URLS")
    if explicit_urls:
        urls = [u.strip() for u in explicit_urls.split(",") if u.strip()]
        print(f"🚀 Testing {len(urls)} explicit futbin URL(s)", flush=True)
        async with aiohttp.ClientSession() as session:
            for url in urls:
                await fetch_one(session, "n/a", url, url=url)
                await asyncio.sleep(1)
        return

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await conn.fetch(
            "SELECT card_id, name FROM fut_players WHERE card_id IS NOT NULL ORDER BY rating DESC NULLS LAST LIMIT $1",
            LIMIT,
        )
    finally:
        await conn.close()

    if not rows:
        print("⚠️ No cards found", flush=True)
        return

    print(f"🚀 Testing futbin.com against {len(rows)} cards (game={GAME})", flush=True)
    async with aiohttp.ClientSession() as session:
        for row in rows:
            await fetch_one(session, int(row["card_id"]), row["name"])
            await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
