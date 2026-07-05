"""
Extends the existing futbin scraper/DB/scheduled-sync setup with a second,
independent process that builds a historical timeline of BIN prices and
sales for Gold Rare cards - it does not touch fut_players, futbin_full_sync.py,
or any API endpoint. It's a separate Railway process (see Procfile's new
`history_worker` line) so the existing daily full-catalog crawl keeps running
exactly as it does today.

Every HISTORY_INTERVAL_SECONDS (default 600 = 10 minutes):
  1. Select every Gold Rare card from fut_players (rating 75-99, version
     'Rare' - see _GOLD_RARE_WHERE below for the exact filter and why).
  2. For each one, scrape the current lowest BIN (both ps and pc markets)
     and the visible sales history from futbin.com, using the same
     proven parsing approach as backend/app/futbin_client.py - that module
     lives in the separate `backend` repo/service with no import path from
     here, so the relevant parsing logic (price cell + sales-history table)
     is ported verbatim below rather than reinvented.
  3. Insert one new bin_history row per (player, platform) every run - never
     UPDATE/overwrite a previous value, so this builds a real timeline.
  4. Insert sales_history rows for any sale not already stored, identified
     by (player_id, sold_at, sold_price) - futbin's sales table has no
     explicit transaction id, but that triple is a reliable natural key for
     "is this the same real-world sale", enforced by a UNIQUE constraint
     with ON CONFLICT DO NOTHING.

Nothing is ever deleted. A per-player failure is logged and skipped; it
never aborts the run for the rest of the batch.

Confirmed against a real "Player Sales History" table (futbin.com/26/sales/{id}/{slug}?platform=ps):
columns are Date | Listed For | Sold For | EA Tax | Net Price | Type, in
that order, with EA Tax and Net Price already computed by futbin itself -
all four figures are read straight from their own columns (tds[1..4]) rather
than re-derived, so this stores exactly what futbin displays. In every
observed row Listed For equals Sold For, consistent with FUT's own market
mechanics (a completed sale always settles at its Buy-Now listed price,
there's no partial-bid/negotiation mechanic) - but the real column is what's
stored, not an assumption of equality.
"""
import os
import re
import sys
import signal
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from aiohttp import web  # health server, same pattern as futbin_full_sync.py

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bin_sales_history_sync")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

HISTORY_INTERVAL_SECONDS = int(os.getenv("HISTORY_INTERVAL_SECONDS", "600"))  # 10 minutes
HISTORY_CONCURRENCY = int(os.getenv("HISTORY_CONCURRENCY", "6"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}

# "Gold" = rating 75-99 (this app's/futbin's own rarity-tier convention
# elsewhere - bronze <65, silver 65-74, gold 75+). "Rare" is futbin's own
# `table-player-revision` label (the same field futbin_full_sync.py already
# stores as fut_players.version) - ordinary/common gold cards show no
# revision label at all, only "Rare" ones do, so this is a case-insensitive
# exact match rather than a substring check to avoid also matching e.g. an
# "Ones To Watch"/"TOTW" card that happens to contain "rare" nowhere - it
# doesn't, but exact-match is the safer default for a filter this specific.
_GOLD_RARE_WHERE = "rating BETWEEN 75 AND 99 AND LOWER(COALESCE(version, '')) = 'rare'"

_PLATFORM_CLASS = {"ps": "platform-ps-only", "pc": "platform-pc-only"}
_SALE_DATE_RE = re.compile(r"[A-Za-z]{3} \d{1,2}, \d{1,2}:\d{2} [AP]M")


# ---------------------------------------------------------------------------
# Parsing helpers - ported from backend/app/futbin_client.py (proven against
# real futbin pages earlier in this project), not reinvented.
# ---------------------------------------------------------------------------
def _num(txt: str) -> int:
    if not txt:
        return 0
    t = txt.lower().replace(",", "").strip()
    if t.endswith("m"):
        try:
            return int(float(t[:-1]) * 1_000_000)
        except Exception:
            return 0
    if t.endswith("k"):
        try:
            return int(float(t[:-1]) * 1_000)
        except Exception:
            return 0
    m = re.search(r"\d+(\.\d+)?", t)
    return int(float(m.group(0))) if m else 0


def parse_lowest_bin(html: str, platform: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")
    plat_class = _PLATFORM_CLASS.get(platform, "platform-ps-only")

    cell = soup.find(class_=re.compile(rf"\b{plat_class}\b"))
    if cell:
        price_div = cell.find("div", class_=re.compile(r"\bprice\b"))
        if price_div:
            val = _num(price_div.get_text(strip=True))
            if val:
                return val

    box = (
        soup.find("div", class_=re.compile(r"price[- ]?box", re.I))
        or soup.find("div", class_=re.compile(r"price-box-original-player", re.I))
        or soup
    )
    plat_word = "pc" if platform == "pc" else "ps"
    for tag in box.find_all(string=re.compile(rf"\b{plat_word}\b", re.I)):
        txt = tag.parent.get_text(" ", strip=True)
        m = re.search(r"(\d[\d,\.kK]+)", txt)
        if m:
            val = _num(m.group(1))
            if val:
                return val
    for d in box.find_all("div", class_=re.compile(r"lowest-price", re.I)):
        val = _num(d.get_text(" ", strip=True))
        if val:
            return val

    return None


async def fetch_lowest_bin(session: aiohttp.ClientSession, player_url: str, platform: str) -> Optional[int]:
    try:
        async with session.get(player_url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                return None
            html = await r.text()
    except Exception:
        return None
    return parse_lowest_bin(html, platform)


async def _resolve_sales_path(session: aiohttp.ClientSession, player_url: str) -> Optional[str]:
    market_url = player_url.rstrip("/") + "/market"
    try:
        async with session.get(market_url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                return None
            html = await r.text()
    except Exception:
        return None

    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", class_=re.compile(r"\bmarket-grid-lates-sale-link\b"))
    if not a or not a.get("href"):
        return None
    return a["href"].split("?")[0]


async def _sales_url(session: aiohttp.ClientSession, player_url: str, platform: str) -> Optional[str]:
    fb_plat = "pc" if platform == "pc" else "ps"
    path = await _resolve_sales_path(session, player_url)
    if path:
        return f"https://www.futbin.com{path}?platform={fb_plat}"
    if "/player/" in player_url:
        sales_base = player_url.replace("/player/", "/sales/")
        return f"{sales_base}?platform={fb_plat}"
    return None


def _parse_sale_date(date_text: str, now: Optional[datetime] = None) -> Optional[datetime]:
    if not date_text:
        return None
    now = now or datetime.now(timezone.utc)
    try:
        dt = datetime.strptime(f"{date_text} {now.year}", "%b %d, %I:%M %p %Y")
    except ValueError:
        return None
    dt = dt.replace(tzinfo=timezone.utc)
    if dt > now + timedelta(days=1):
        dt = dt.replace(year=now.year - 1)
    return dt


def parse_sales_table(html: str, limit: int = 30) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="auctions-table")
    if not table:
        return []
    body = table.find("tbody")
    if not body:
        return []

    sales: List[Dict[str, Any]] = []
    for row in body.find_all("tr"):
        if len(sales) >= limit:
            break
        tds = row.find_all("td")
        if len(tds) < 5:
            continue

        date_div = tds[0].find("div")
        icon = date_div.find("i") if date_div else None
        sold = bool(icon and any("fa-check" in c for c in icon.get("class", [])))
        if not sold:
            continue

        date_span = date_div.find("span", class_="sales-date-time") if date_div else None
        date_text = date_span.get_text(strip=True) if date_span else None
        m = _SALE_DATE_RE.search(date_text or "")
        sold_at = _parse_sale_date(m.group(0)) if m else None
        if sold_at is None:
            # Can't dedupe or timestamp this row honestly - skip rather than
            # store a sale with a fabricated/missing sold_at.
            continue

        # Real column layout confirmed against a live page (Player Sales
        # History table): Date | Listed For | Sold For | EA Tax | Net Price |
        # Type - so every figure is scraped directly rather than derived.
        # (In practice Listed For always equals Sold For - a completed FUT
        # sale settles at exactly its Buy-Now listed price - but we read the
        # real column instead of assuming that.)
        listed_price = _num(tds[1].get_text(strip=True))
        sold_price = _num(tds[2].get_text(strip=True))
        ea_tax = _num(tds[3].get_text(strip=True))
        net_price = _num(tds[4].get_text(strip=True))
        if not sold_price:
            continue

        sales.append({
            "sold_at": sold_at,
            "listed_price": listed_price or sold_price,
            "sold_price": sold_price,
            "ea_tax": ea_tax,
            "net_price": net_price or (sold_price - ea_tax),
        })

    return sales


async def fetch_sales_history(session: aiohttp.ClientSession, player_url: str, platform: str = "ps") -> List[Dict[str, Any]]:
    url = await _sales_url(session, player_url, platform)
    if not url:
        return []
    try:
        async with session.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
            if r.status != 200:
                return []
            html = await r.text()
    except Exception:
        return []
    return parse_sales_table(html)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
async def ensure_tables(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bin_history (
            id BIGSERIAL PRIMARY KEY,
            player_id BIGINT NOT NULL REFERENCES fut_players(card_id),
            platform TEXT NOT NULL,
            lowest_bin INTEGER,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS bin_history_player_captured_idx ON bin_history (player_id, captured_at)"
    )

    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sales_history (
            id BIGSERIAL PRIMARY KEY,
            player_id BIGINT NOT NULL REFERENCES fut_players(card_id),
            listed_price INTEGER,
            sold_price INTEGER NOT NULL,
            ea_tax INTEGER NOT NULL,
            net_price INTEGER NOT NULL,
            sold_at TIMESTAMPTZ NOT NULL,
            captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (player_id, sold_at, sold_price)
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS sales_history_player_sold_idx ON sales_history (player_id, sold_at)"
    )


# ---------------------------------------------------------------------------
# Per-player scrape + insert
# ---------------------------------------------------------------------------
async def _scrape_one(
    pool: asyncpg.Pool,
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    card_id: int,
    player_url: str,
    counters: Dict[str, int],
) -> None:
    async with sem:
        # --- BIN history: both markets, always insert, never overwrite ---
        for platform in ("ps", "pc"):
            try:
                bin_price = await fetch_lowest_bin(session, player_url, platform)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO bin_history (player_id, platform, lowest_bin, captured_at) "
                        "VALUES ($1, $2, $3, NOW())",
                        card_id, platform, bin_price,
                    )
                counters["bin_ok"] += 1
            except Exception as e:
                counters["bin_failed"] += 1
                log.warning("BIN scrape failed for card_id=%s platform=%s: %s", card_id, platform, e)

        # --- Sales history: dedupe on (player_id, sold_at, sold_price) ---
        try:
            sales = await fetch_sales_history(session, player_url, "ps")
        except Exception as e:
            counters["sales_failed"] += 1
            log.warning("Sales scrape failed for card_id=%s: %s", card_id, e)
            return

        for s in sales:
            try:
                async with pool.acquire() as conn:
                    result = await conn.execute(
                        """
                        INSERT INTO sales_history
                            (player_id, listed_price, sold_price, ea_tax, net_price, sold_at, captured_at)
                        VALUES ($1, $2, $3, $4, $5, $6, NOW())
                        ON CONFLICT (player_id, sold_at, sold_price) DO NOTHING
                        """,
                        card_id, s["listed_price"], s["sold_price"], s["ea_tax"], s["net_price"], s["sold_at"],
                    )
                # asyncpg's execute() returns a command tag "INSERT <oid> <rowcount>" -
                # rowcount is 0 when ON CONFLICT DO NOTHING skipped an existing row.
                rowcount = int(result.rsplit(" ", 1)[-1])
                if rowcount == 0:
                    counters["sales_dupe"] += 1
                else:
                    counters["sales_new"] += 1
            except Exception as e:
                counters["sales_failed"] += 1
                log.warning("Sales insert failed for card_id=%s sold_at=%s: %s", card_id, s["sold_at"], e)


async def crawl_once() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=HISTORY_CONCURRENCY + 2)
    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)
            rows = await conn.fetch(
                f"SELECT card_id, player_url FROM fut_players "
                f"WHERE {_GOLD_RARE_WHERE} AND player_url IS NOT NULL"
            )

        log.info("Gold Rare candidates this run: %d", len(rows))
        if not rows:
            return

        sem = asyncio.Semaphore(HISTORY_CONCURRENCY)
        counters = {"bin_ok": 0, "bin_failed": 0, "sales_new": 0, "sales_dupe": 0, "sales_failed": 0}

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                _scrape_one(pool, session, sem, r["card_id"], r["player_url"], counters)
                for r in rows
            ])

        log.info(
            "Run complete. bin_ok=%d bin_failed=%d sales_new=%d sales_dupe=%d sales_failed=%d",
            counters["bin_ok"], counters["bin_failed"],
            counters["sales_new"], counters["sales_dupe"], counters["sales_failed"],
        )
    finally:
        await pool.close()


# ================== HEALTH & SCHEDULER (same pattern as futbin_full_sync.py) ================== #
async def start_health():
    app = web.Application()
    app.add_routes([
        web.get("/", lambda _: web.Response(text="OK")),
        web.get("/health", lambda _: web.Response(text="OK")),
    ])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8081")))
    await site.start()
    return asyncio.create_task(asyncio.Event().wait())


shutdown_evt = asyncio.Event()


def _sig():
    shutdown_evt.set()


async def main_loop():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _sig)
        except Exception:
            pass
    health = None
    if os.getenv("PORT"):
        health = await start_health()
    while not shutdown_evt.is_set():
        try:
            await crawl_once()
        except Exception as e:
            log.error("crawl_once() failed, will retry next interval: %s", e)
        try:
            await asyncio.wait_for(shutdown_evt.wait(), timeout=HISTORY_INTERVAL_SECONDS)
        except asyncio.TimeoutError:
            pass
    if health:
        health.cancel()


if __name__ == "__main__":
    if "--now" in sys.argv:
        asyncio.run(crawl_once())
    else:
        asyncio.run(main_loop())
