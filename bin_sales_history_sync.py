"""
Extends the existing futbin scraper/DB/scheduled-sync setup with a second,
independent process that builds a historical timeline of BIN prices and
sales for Gold Rare cards - it does not touch fut_players, futbin_full_sync.py,
or any API endpoint. It's a separate Railway process (see Procfile's new
`history_worker` line) so the existing daily full-catalog crawl keeps running
exactly as it does today.

Every HISTORY_INTERVAL_SECONDS (default 600 = 10 minutes):
  1. Select every Gold Rare card from fut_players (rating 75-99, version
     'Normal' - see _GOLD_RARE_WHERE below; fut_players.version tracks card
     EDITION (Normal vs TOTW/TOTS/Icon/etc promos), not the separate Rare/
     Non-Rare cosmetic art style, which was never scraped into this schema
     at all - confirmed by querying the live data (top values were Normal/
     normal/TOTW/TOTS/Icon, no "Rare" anywhere). Common and Rare gold cards
     both show up as "Normal" here, so this is every ordinary (non-promo)
     gold card, not a Rare-only subset.
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
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup
from aiohttp import web  # health server, same pattern as futbin_full_sync.py

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bin_sales_history_sync")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

HISTORY_INTERVAL_SECONDS = int(os.getenv("HISTORY_INTERVAL_SECONDS", "600"))  # 10 minutes
# Lowered from 6 after the first run against the real Gold Rare population
# (2463 candidates, up to 4 requests each) got rate-limited (HTTP 429) by
# futbin - this is client-side politeness, not a hard technical limit.
HISTORY_CONCURRENCY = int(os.getenv("HISTORY_CONCURRENCY", "3"))
HISTORY_MAX_RETRIES = int(os.getenv("HISTORY_MAX_RETRIES", "3"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}

# "Gold" = rating 75-99 (this app's/futbin's own rarity-tier convention
# elsewhere - bronze <65, silver 65-74, gold 75+).
#
# version was originally assumed to carry a "Rare"/"Non-Rare" cosmetic-style
# tag - it doesn't. Querying the live data showed the real top values are
# Normal (2080), TOTW (575), normal lowercase (383), TOTS (240), Icon (128) -
# version tracks card EDITION (ordinary vs. promo type), and every ordinary
# gold card shows up as "Normal" regardless of whether it's visually Rare or
# Non-Rare in-game - that finer distinction was never scraped into this
# schema. So this matches every ordinary (non-promo) gold card; ILIKE covers
# the "Normal"/"normal" casing inconsistency seen in the real data (looks
# like two different crawl eras wrote different casing for the same thing).
_GOLD_RARE_WHERE = "rating BETWEEN 75 AND 99 AND version ILIKE 'normal'"

_PLATFORM_CLASS = {"ps": "platform-ps-only", "pc": "platform-pc-only"}
_SALE_DATE_RE = re.compile(r"[A-Za-z]{3} \d{1,2}, \d{1,2}:\d{2} [AP]M")


async def _get_with_retry(
    session: aiohttp.ClientSession, url: str, diag: Dict[str, Any]
) -> "tuple[int, Optional[str]]":
    """GET with 429-aware backoff retry.

    A run against the real ~2500-card Gold Rare population (up to 4 requests
    each) got HTTP 429s back from futbin - this is a rate limit, not a hard
    block like fut.gg's 403, so it's worth backing off and retrying rather
    than counting the very first 429 as a permanent failure for that player.
    """
    backoff = 1.0
    for attempt in range(HISTORY_MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
                if r.status == 429:
                    diag["http_429_hits"] += 1
                    if attempt < HISTORY_MAX_RETRIES:
                        retry_after = r.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else backoff
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    return 429, None
                if r.status != 200:
                    return r.status, None
                return 200, await r.text()
        except Exception:
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            diag["http_exceptions"] += 1
            return 0, None
    return 0, None


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


async def fetch_lowest_bin(
    session: aiohttp.ClientSession, player_url: str, platform: str, diag: Dict[str, Any]
) -> Optional[int]:
    status, html = await _get_with_retry(session, player_url, diag)
    if status != 200 or html is None:
        return None
    return parse_lowest_bin(html, platform)


async def _resolve_sales_path(
    session: aiohttp.ClientSession, player_url: str, diag: Dict[str, int]
) -> Optional[str]:
    market_url = player_url.rstrip("/") + "/market"
    status, html = await _get_with_retry(session, market_url, diag)
    if status != 200 or html is None:
        diag["sales_market_fetch_failed"] += 1
        diag.setdefault("sales_market_fetch_sample", f"status={status} url={market_url}")
        return None

    soup = BeautifulSoup(html, "html.parser")
    a = soup.find("a", class_=re.compile(r"\bmarket-grid-lates-sale-link\b"))
    if not a or not a.get("href"):
        diag["sales_no_history_link"] += 1
        diag.setdefault("sales_no_history_link_sample", f"html_len={len(html)} url={market_url}")
        return None
    return a["href"].split("?")[0]


async def _sales_url(
    session: aiohttp.ClientSession, player_url: str, platform: str, diag: Dict[str, int]
) -> Optional[str]:
    fb_plat = "pc" if platform == "pc" else "ps"
    path = await _resolve_sales_path(session, player_url, diag)
    if path:
        return f"https://www.futbin.com{path}?platform={fb_plat}"
    if "/player/" in player_url:
        diag["sales_used_fallback_url"] += 1
        sales_base = player_url.replace("/player/", "/sales/")
        return f"{sales_base}?platform={fb_plat}"
    diag["sales_no_url_at_all"] += 1
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


def parse_sales_table(html: str, diag: Dict[str, int], limit: int = 30) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="auctions-table")
    if not table:
        diag["sales_no_table"] += 1
        diag.setdefault("sales_no_table_sample", f"html_len={len(html)} has_auctions_str={'auctions-table' in html}")
        return []
    body = table.find("tbody")
    if not body:
        diag["sales_no_tbody"] += 1
        return []

    raw_rows = body.find_all("tr")
    diag["sales_raw_rows"] += len(raw_rows)

    sales: List[Dict[str, Any]] = []
    for row in raw_rows:
        if len(sales) >= limit:
            break
        tds = row.find_all("td")
        if len(tds) < 5:
            diag["sales_rows_too_few_tds"] += 1
            continue

        date_div = tds[0].find("div")
        icon = date_div.find("i") if date_div else None
        sold = bool(icon and any("fa-check" in c for c in icon.get("class", [])))
        if not sold:
            diag["sales_rows_not_sold"] += 1
            continue

        date_span = date_div.find("span", class_="sales-date-time") if date_div else None
        date_text = date_span.get_text(strip=True) if date_span else None
        m = _SALE_DATE_RE.search(date_text or "")
        sold_at = _parse_sale_date(m.group(0)) if m else None
        if sold_at is None:
            # Can't dedupe or timestamp this row honestly - skip rather than
            # store a sale with a fabricated/missing sold_at.
            diag["sales_rows_bad_date"] += 1
            diag.setdefault("sales_rows_bad_date_sample", repr(date_text))
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
            diag["sales_rows_zero_price"] += 1
            continue

        sales.append({
            "sold_at": sold_at,
            "listed_price": listed_price or sold_price,
            "sold_price": sold_price,
            "ea_tax": ea_tax,
            "net_price": net_price or (sold_price - ea_tax),
        })

    return sales


async def fetch_sales_history(
    session: aiohttp.ClientSession, player_url: str, diag: Dict[str, int], platform: str = "ps"
) -> List[Dict[str, Any]]:
    url = await _sales_url(session, player_url, platform, diag)
    if not url:
        return []
    diag.setdefault("sales_first_resolved_url", url)
    status, html = await _get_with_retry(session, url, diag)
    if status != 200 or html is None:
        diag["sales_page_fetch_failed"] += 1
        diag.setdefault("sales_page_fetch_sample", f"status={status} url={url}")
        return []
    return parse_sales_table(html, diag)


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
    diag: Dict[str, Any],
) -> None:
    async with sem:
        # This script trusts fut_players.player_url completely - it has no
        # site of its own to fetch from. If the main futbin_full_sync.py
        # worker hasn't (re)crawled a given row since before the futbin
        # migration, that column can still hold an old fut.gg URL, which
        # 403s on every request (fut.gg blocks scrapers - the whole reason
        # this project moved to futbin). Catch that up front with zero
        # requests wasted, instead of taking a market-page fetch + a sales
        # fetch to eventually surface a generic 403.
        if "futbin.com" not in player_url:
            diag["stale_non_futbin_url"] += 1
            diag.setdefault("stale_non_futbin_url_sample", f"card_id={card_id} url={player_url}")
            return

        # --- BIN history: both markets, always insert, never overwrite ---
        for platform in ("ps", "pc"):
            try:
                bin_price = await fetch_lowest_bin(session, player_url, platform, diag)
                async with pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO bin_history (player_id, platform, lowest_bin, captured_at) "
                        "VALUES ($1, $2, $3, NOW())",
                        card_id, platform, bin_price,
                    )
                # A successful INSERT doesn't mean a real price was found - a
                # None scrape result inserts a NULL just as "successfully" as
                # a real number does, so those are counted separately here
                # rather than both landing in one misleading "ok" bucket.
                if bin_price is not None:
                    diag["bin_price_found"] += 1
                else:
                    diag["bin_price_null"] += 1
            except Exception as e:
                diag["bin_failed"] += 1
                log.warning("BIN scrape failed for card_id=%s platform=%s: %s", card_id, platform, e)

        # --- Sales history: dedupe on (player_id, sold_at, sold_price) ---
        try:
            sales = await fetch_sales_history(session, player_url, diag, "ps")
        except Exception as e:
            diag["sales_failed"] += 1
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
                    diag["sales_dupe"] += 1
                else:
                    diag["sales_new"] += 1
            except Exception as e:
                diag["sales_failed"] += 1
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
        diag: Dict[str, Any] = defaultdict(int)

        async with aiohttp.ClientSession() as session:
            await asyncio.gather(*[
                _scrape_one(pool, session, sem, r["card_id"], r["player_url"], diag)
                for r in rows
            ])

        log.info(
            "Run complete. stale_non_futbin_url=%d | bin_price_found=%d bin_price_null=%d bin_failed=%d | "
            "sales_new=%d sales_dupe=%d sales_failed=%d | http_429_hits=%d http_exceptions=%d",
            diag["stale_non_futbin_url"],
            diag["bin_price_found"], diag["bin_price_null"], diag["bin_failed"],
            diag["sales_new"], diag["sales_dupe"], diag["sales_failed"],
            diag["http_429_hits"], diag["http_exceptions"],
        )
        if diag["stale_non_futbin_url"]:
            log.warning(
                "%d/%d candidates have a non-futbin.com player_url (e.g. %s) - "
                "the main futbin_full_sync.py worker hasn't refreshed these rows yet; "
                "nothing to fix here until it does.",
                diag["stale_non_futbin_url"], len(rows), diag.get("stale_non_futbin_url_sample"),
            )
        # Detailed sales-pipeline breakdown - only printed when something
        # other than a clean "no history yet" is going on, so a healthy run
        # doesn't spam the log with a wall of zeros.
        diagnostic_keys = [
            "sales_market_fetch_failed", "sales_no_history_link", "sales_used_fallback_url",
            "sales_no_url_at_all", "sales_page_fetch_failed", "sales_no_table", "sales_no_tbody",
            "sales_rows_too_few_tds", "sales_rows_not_sold", "sales_rows_bad_date", "sales_rows_zero_price",
        ]
        if any(diag.get(k) for k in diagnostic_keys):
            log.info("Sales pipeline diagnostics: %s", {k: diag[k] for k in diagnostic_keys if diag.get(k)})
        for sample_key in (
            "sales_first_resolved_url", "sales_market_fetch_sample", "sales_no_history_link_sample",
            "sales_page_fetch_sample", "sales_no_table_sample", "sales_rows_bad_date_sample",
        ):
            if sample_key in diag:
                log.info("%s: %s", sample_key, diag[sample_key])

        # Heartbeat for /api/ops/freshness. A run where every single scrape
        # failed (and nothing new landed) is a markup change or a block -
        # that's the failure mode that silently kills the fair-value data.
        total_attempted = diag["bin_price_found"] + diag["bin_price_null"] + diag["bin_failed"]
        run_ok = total_attempted == 0 or diag["bin_failed"] < total_attempted
        async with pool.acquire() as hb_conn:
            await heartbeat(
                hb_conn,
                "bin_sales_history_sync",
                ok=run_ok,
                detail=(
                    f"candidates={len(rows)} sales_new={diag['sales_new']} "
                    f"bin_found={diag['bin_price_found']} bin_failed={diag['bin_failed']} "
                    f"http_429={diag['http_429_hits']}"
                ),
            )
        if not run_ok:
            await alert(
                "bin_sales_history_sync: every BIN scrape failed this run "
                f"(bin_failed={diag['bin_failed']}/{total_attempted}, 429s={diag['http_429_hits']}) - "
                "futbin markup change or block? Sales/BIN history has stopped growing."
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
