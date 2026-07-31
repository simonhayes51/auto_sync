"""
Extends the existing futbin scraper/DB/scheduled-sync setup with a second,
independent process that builds a historical timeline of BIN prices and
sales for Gold Rare cards - it does not touch fut_players, futbin_full_sync.py,
or any API endpoint. It's a separate Railway process (see Procfile's
`history_worker` line) so the existing daily full-catalog crawl keeps running
exactly as it does today.

Runs as a single one-shot crawl per invocation (see `if __name__ ==
"__main__"` below) - deployed as a Railway Cron Job on a 10-minute schedule
rather than a permanent always-on worker, since there's no in-process
scheduling loop anymore. Each invocation:
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

Confirmed against a real "Player Sales History" table: columns are Date |
Listed For | Sold For | EA Tax | Net Price | Type, in that order, with EA
Tax and Net Price already computed by futbin itself - all four figures
are read straight from their own columns (tds[1..4]) rather than
re-derived, so this stores exactly what futbin displays. In every
observed row Listed For equals Sold For, consistent with FUT's own market
mechanics (a completed sale always settles at its Buy-Now listed price,
there's no partial-bid/negotiation mechanic) - but the real column is what's
stored, not an assumption of equality.

The dedicated futbin.com/26/sales/{id}/{slug} endpoint that table used to
live on is now blocked by Cloudflare and is never requested - the same
table is confirmed present inline on the /market page
(player_url + "/market", already fetched here for other reasons), so
sales history is parsed straight from that page's HTML instead (see
fetch_market_page/parse_market_page below).

Transport: plain aiohttp now gets an HTTP 403 Cloudflare challenge on
every request (confirmed live - a 10-player test produced bin_price_found=0,
sales_market_fetch_failed=10, status=403), while a real Playwright
Chromium session loads the same pages successfully from the same
connection. All FUTBIN page loads go through one persistent Chromium
browser context per run (see crawl_once()) and a small pool of reusable
pages (PLAYWRIGHT_CONCURRENCY, default 1) instead of an aiohttp
ClientSession - see fetch_page_html() below. A run-level circuit breaker
(HISTORY_403_ABORT_THRESHOLD/HISTORY_429_ABORT_THRESHOLD) stops scheduling
new work if FUTBIN starts hard-blocking mid-run.

For a local manual test against a small batch:
    $env:TEST_LIMIT="10"
    $env:PLAYWRIGHT_CONCURRENCY="1"
    $env:PLAYWRIGHT_HEADLESS="false"
    python bin_sales_history_sync.py
TEST_LIMIT=0 (the default) preserves full, unmodified production behavior.
"""
import os
import re
import sys
import asyncio
import logging
import random
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

# futbin serves its sale timestamps in UK local time (see _parse_sale_date).
_FUTBIN_TZ = ZoneInfo("Europe/London")
from typing import Any, Dict, List, Optional

import asyncpg
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bin_sales_history_sync")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

HISTORY_MAX_RETRIES = int(os.getenv("HISTORY_MAX_RETRIES", "3"))

# A real Playwright Chromium session loads FUTBIN pages successfully where
# plain aiohttp now gets Cloudflare 403s on every request (confirmed live -
# see the module docstring). One persistent browser context per run, with a
# small pool of pre-opened pages, replaces the old aiohttp ClientSession -
# HISTORY_CONCURRENCY (the old semaphore size) is retired in favor of this
# pool's size, since page count is now the real concurrency ceiling.
#
# PLAYWRIGHT_HEADLESS is deliberately NOT given a hardcoded default here:
# this repo already tried Playwright for a different FUTBIN worker
# (futbin_sbc_sync.py, see README.md section 6) and found headless
# Chromium ALSO gets 403'd, and headed Chromium is additionally blocked
# outright when the request comes from Railway's own datacentre IP - only
# working from a home/residential connection. Whether this worker ends up
# running headless, headed, or off-Railway entirely is an open question
# this change deliberately does not prejudge - if the env var is unset,
# Playwright's own upstream default applies (headless), not a "production"
# assumption encoded in this file.
PLAYWRIGHT_CONCURRENCY = int(os.getenv("PLAYWRIGHT_CONCURRENCY", "1"))
_PLAYWRIGHT_HEADLESS_RAW = os.getenv("PLAYWRIGHT_HEADLESS")
PLAYWRIGHT_HEADLESS = (
    _PLAYWRIGHT_HEADLESS_RAW.strip().lower() in ("1", "true", "yes")
    if _PLAYWRIGHT_HEADLESS_RAW is not None
    else None
)
PLAYWRIGHT_NAV_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_MS", "20000"))

# Mirrors a standalone single-page Playwright connectivity test (not part
# of this repo) that got a real 200 on a real player URL from this same
# home connection/IP, where this worker was still getting 403'd - a real
# Chrome UA string, not the bot-identifying "SBCSolver/1.5" one this
# worker used before Playwright was even introduced, and not left to
# whatever default Chromium negotiates on its own (which is a real
# Chromium UA too, but unproven against FUTBIN; this exact string is the
# one already confirmed to get a 200).
PLAYWRIGHT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

# Run-level circuit breaker (item 8): a hard Cloudflare block shows up as a
# burst of 403s/challenge pages, not the 429 rate-limit this file was
# originally tuned around - stop scheduling new work well before grinding
# through the whole candidate list against a site that's actively blocking
# this IP/session.
HISTORY_403_ABORT_THRESHOLD = int(os.getenv("HISTORY_403_ABORT_THRESHOLD", "5"))
HISTORY_429_ABORT_THRESHOLD = int(os.getenv("HISTORY_429_ABORT_THRESHOLD", "20"))

# 0 = full production behavior (no limit). Set >0 to cap each tier's
# candidate count for a local manual test run (see README/module docstring
# for the exact PowerShell invocation). Bound as a real SQL parameter in
# _fetch_tier() below - never string-interpolated.
TEST_LIMIT = int(os.getenv("TEST_LIMIT", "0"))

# How often this script is actually invoked - purely a phase-math input for
# _tier_b_due() below, not an in-process sleep interval (this became a
# one-shot Cron Job; there's no loop left to sleep in). Must match the real
# Railway Cron schedule (*/10 * * * * = 600s) or Tier B's time-anchored
# phase drifts out of sync with the real invocation cadence.
CRON_INTERVAL_SECONDS = int(os.getenv("CRON_INTERVAL_SECONDS", "600"))

# Distinct from every REFRESH_LOCK_KEY in backend/app/services/*.py
# (7741001-7741002, 7741004-7741007, 7741009-7741010 are all taken there -
# same Postgres instance/key space as those processes). Guards against
# overlapping Cron invocations: Tier A alone can now take well over the
# 10-minute Cron interval (candidate count has grown ~1.7x since the
# ~37-47min/2500-candidate baseline this schedule was tuned against), and
# with no lock, multiple containers ended up scraping futbin concurrently -
# multiplying effective request volume well past the level (concurrency=6
# in a single process) already known to trigger sustained 429s/403s.
OVERLAP_LOCK_KEY = 7741011

_CHALLENGE_MARKERS = (
    "just a moment",
    "attention required",
    "challenge-platform",
    "cf-browser-verification",
)


def _looks_like_challenge(status: int, html: Optional[str]) -> bool:
    """A Cloudflare block/challenge, not real player HTML - never hand this
    to a parser. Checked via response status plus HTML markers (checking
    the HTML source also covers the page's <title> text, so no separate
    page.title() call is needed)."""
    if status == 403:
        return True
    if not html:
        return False
    lower = html.lower()
    if any(marker in lower for marker in _CHALLENGE_MARKERS):
        return True
    return "oops, there was an error" in lower and "403" in lower


def _jittered_backoff(base: float) -> float:
    return base * random.uniform(0.5, 1.5)

# ---------------------------------------------------------------------------
# Tiered coverage
# ---------------------------------------------------------------------------
# Originally this scraped only ordinary gold cards (rating 75-99, version
# 'Normal') - which excluded every special/promo card (TOTW/TOTS/Icon/...),
# i.e. the most-traded, highest-value segment of the market. Sweeping the
# whole ~25k-card catalog instead is worse, not better: at the politeness
# rate futbin tolerates (429s appeared at concurrency 6), a full sweep takes
# ~10h, collapsing per-card sample resolution to 2-3/day and multiplying
# ban risk for data that's ~80% illiquid bronze/silver noise.
#
# So: tiers, computed at selection time (no schema change, can't drift from
# the catalog).
#   Tier A - every sweep: all special/promo cards (version != Normal) plus
#            ordinary golds rated 82+. The liquid, volatile segment.
#   Tier B - every TIER_B_EVERY'th sweep (time-anchored, so restarts don't
#            reset the phase): ordinary golds 75-81 (slow-moving fodder).
#   Everything else (silver/bronze fodder): the daily futbin_full_sync
#            catalog crawl already refreshes their price - enough for SBC
#            purposes; no BIN/sales timeline is worth requests on them.
#
# If a sweep gets heavily 429'd during Tier A, Tier B is skipped for that
# run - when throttled, spend the remaining budget on the data that matters.
#
# version tracks card EDITION (Normal vs TOTW/TOTS/Icon/etc) - confirmed by
# querying live data (top values: Normal 2080, TOTW 575, normal 383, TOTS
# 240, Icon 128); ILIKE covers the Normal/normal casing split from two
# different crawl eras.
TIER_A_WHERE = (
    "((version IS NOT NULL AND version NOT ILIKE 'normal') "
    "OR (rating >= 82 AND version ILIKE 'normal'))"
)
TIER_B_WHERE = "(rating BETWEEN 75 AND 81 AND version ILIKE 'normal')"
TIER_B_EVERY = int(os.getenv("TIER_B_EVERY", "4"))
TIER_B_SKIP_429_THRESHOLD = int(os.getenv("TIER_B_SKIP_429_THRESHOLD", "25"))

_PLATFORM_CLASS = {"ps": "platform-ps-only", "pc": "platform-pc-only"}
_SALE_DATE_RE = re.compile(r"[A-Za-z]{3} \d{1,2}, \d{1,2}:\d{2} [AP]M")


async def fetch_page_html(page, url: str, diag: Dict[str, Any]) -> "tuple[int, Optional[str]]":
    """Loads `url` in a real Playwright Chromium page and returns
    (status, html) - the direct replacement for the old aiohttp-based
    _get_with_retry, same retry-count/doubling-backoff shape (now with
    jitter) and same (status, html-or-None) contract, so every call site
    below is otherwise unchanged.

    wait_until="domcontentloaded" (never "networkidle" - FUTBIN's ads and
    tracking scripts can keep the network "busy" indefinitely, which would
    make networkidle hang or time out for reasons unrelated to whether the
    page we actually want already loaded). A Cloudflare challenge/block
    page is detected via _looks_like_challenge() and never returned as
    valid HTML, even if Cloudflare served it with a 200 status.
    """
    backoff = 1.0
    for attempt in range(HISTORY_MAX_RETRIES + 1):
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_NAV_TIMEOUT_MS)
        except PlaywrightTimeoutError:
            diag["browser_timeouts"] += 1
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(_jittered_backoff(backoff))
                backoff *= 2
                continue
            return 0, None
        except Exception:
            diag["browser_navigation_failures"] += 1
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(_jittered_backoff(backoff))
                backoff *= 2
                continue
            return 0, None

        status = response.status if response else 0
        try:
            html = await page.content()
        except Exception:
            html = None

        if _looks_like_challenge(status, html):
            # One navigation, one blocked attempt - cloudflare_challenge_hits
            # and http_403_hits can both fire for the very same event (a
            # 403-status challenge page), so blocked_navigation_attempts is
            # the single counter the circuit breaker actually checks
            # (see _check_circuit_breaker). The other two stay as separate,
            # more granular diagnostics (hard-403 vs soft-JS-challenge).
            diag["blocked_navigation_attempts"] += 1
            diag["cloudflare_challenge_hits"] += 1
            if status == 403:
                diag["http_403_hits"] += 1
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(_jittered_backoff(backoff))
                backoff *= 2
                continue
            return status or 403, None

        if status == 429:
            diag["http_429_hits"] += 1
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(_jittered_backoff(backoff))
                backoff *= 2
                continue
            return 429, None

        if status != 200:
            if attempt < HISTORY_MAX_RETRIES:
                await asyncio.sleep(_jittered_backoff(backoff))
                backoff *= 2
                continue
            return status, None

        return 200, html
    return 0, None


async def _dismiss_cookie_banner(page) -> None:
    """Best-effort, once per run (see crawl_once) - OneTrust's overlay can
    intercept clicks on some layouts. Navigation itself doesn't need it
    (nothing here clicks a Market tab; /market is always a direct goto),
    so this is purely a courtesy and never allowed to fail the scrape."""
    try:
        btn = page.locator("#onetrust-accept-btn-handler")
        if await btn.count() > 0:
            await btn.click(timeout=2000)
    except Exception:
        pass


def _check_circuit_breaker(diag: Dict[str, Any], abort_event: asyncio.Event) -> None:
    """Trips the shared abort_event once either threshold is crossed - a
    burst of blocked navigations (403s/challenge pages - a hard block) or
    429s (rate limiting) across the whole run, not per-card. Idempotent:
    does nothing once already tripped, so concurrent workers calling this
    don't re-log.

    Uses blocked_navigation_attempts, not http_403_hits + cloudflare_
    challenge_hits summed - those two can both increment for the exact
    same navigation (a 403-status challenge page), which previously
    double-counted a single blocked attempt as 2 toward the threshold."""
    if abort_event.is_set():
        return
    blocked = diag["blocked_navigation_attempts"]
    if blocked >= HISTORY_403_ABORT_THRESHOLD:
        abort_event.set()
        diag["circuit_breaker_tripped"] = 1
        log.warning(
            "Circuit breaker tripped: %d blocked navigation attempts (403/challenge) >= threshold %d - "
            "stopping new work, letting in-flight work finish.",
            blocked, HISTORY_403_ABORT_THRESHOLD,
        )
    elif diag["http_429_hits"] >= HISTORY_429_ABORT_THRESHOLD:
        abort_event.set()
        diag["circuit_breaker_tripped"] = 1
        log.warning(
            "Circuit breaker tripped: %d HTTP 429s >= threshold %d - "
            "stopping new work, letting in-flight work finish.",
            diag["http_429_hits"], HISTORY_429_ABORT_THRESHOLD,
        )


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


def parse_lowest_bin(html: str, platform: str, diag: Optional[Dict[str, Any]] = None) -> Optional[int]:
    """
    Confirmed live 2026-07-31 via real browser-captured HTML: each
    platform gets its own top-level price-box div (class="price-box
    platform-ps-only price-box-original-player" / "...platform-pc-only
    ..."), not one shared box with nested per-platform children - the
    platform marker is a class ON the box itself, not a descendant's.
    The previous version here scoped to the FIRST price-box div in the
    whole page (always the same one, regardless of which platform was
    requested), then searched for a DESCENDANT with the platform class -
    which never matched, since that class is on the box itself, not a
    child. Confirmed via 10 hours of real production logs: this made
    bin_platform_scoped_hit == 0 on every single run, so every price
    this scraper has ever recorded came from the (explicitly not
    platform-scoped) fallback below - the reason PS and PC prices have
    always come out identical.

    Select the box whose OWN class list contains the platform marker
    (bs4's plain-string class_ argument matches an exact class TOKEN,
    not a substring - matters here since sibling classes like
    "price-box-original-player" share a "price-box" prefix). The actual
    price value is a child div whose exact class token is "price" -
    exact-token matching (not a \\bprice\\b regex) avoids falsely
    matching "price-header"/"price-box-full-width"/etc, which a
    word-boundary regex would also match (a hyphen counts as a boundary).
    """
    soup = BeautifulSoup(html, "html.parser")
    plat_class = _PLATFORM_CLASS.get(platform, "platform-ps-only")

    box = None
    for candidate in soup.find_all("div", class_="price-box"):
        if plat_class in (candidate.get("class") or []):
            box = candidate
            break

    if box:
        price_div = box.find("div", class_="price")
        if price_div:
            val = _num(price_div.get_text(strip=True))
            if val:
                if diag is not None:
                    diag["bin_platform_scoped_hit"] += 1
                return val

    # Last-resort fallback only - NOT platform-scoped if `box` above
    # wasn't found (falls back to the whole page), so it can return the
    # same value for both ps and pc in that case. Kept so a further
    # markup change still yields something rather than nothing, but
    # every hit here is logged so this staying at 0 (or not) is visible
    # in the run summary instead of silently masking bad data.
    if diag is not None:
        diag["bin_platform_fallback_used"] += 1
    box = box or soup
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


# Games played / goals-per-game / best chem style live in the player-bio
# paragraph as a natural-language sentence, not a table - confirmed live
# against futbin.com/26/player/67/erling-haaland's bio section: "He has
# been used in 6,688,772 games with a GPG (goals per game) of 1.346."
# and "The best chemistry style for him is Basic." Each sentence appears
# twice, once per platform, in platform-ps-only/platform-pc-only spans -
# the same convention already used for BIN price on this page. This data
# only lives on the main player page (not /market or the sales page), so
# it piggybacks on the same player-page fetch _scrape_one already makes
# for BIN price rather than costing an extra request.
_BIO_GAMES_GOALS_RE = re.compile(r"used in ([\d,]+) games with a GPG \(goals per game\) of (\d+(?:\.\d+)?)\.")
_BIO_CHEM_RE = re.compile(r"best chemistry style for \w+ is ([A-Za-z][A-Za-z \-]*?)\.")


def parse_bio_stats(html: str, platform: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    bio = soup.find("div", class_=re.compile(r"\bplayer-text-section\b"))
    if not bio:
        return {}

    plat_class = _PLATFORM_CLASS.get(platform, "platform-ps-only")
    games = avg_goals = top_chem_style = None
    for span in bio.find_all(class_=re.compile(rf"\b{plat_class}\b")):
        text = span.get_text(" ", strip=True)
        m = _BIO_GAMES_GOALS_RE.search(text)
        if m:
            games = _num(m.group(1))
            try:
                avg_goals = float(m.group(2))
            except ValueError:
                avg_goals = None
            continue
        m = _BIO_CHEM_RE.search(text)
        if m:
            top_chem_style = m.group(1).strip()

    return {"games": games, "avg_goals": avg_goals, "top_chem_style": top_chem_style}


def _parse_sale_date(date_text: str, now: Optional[datetime] = None) -> Optional[datetime]:
    if not date_text:
        return None
    now = now or datetime.now(timezone.utc)
    try:
        naive = datetime.strptime(f"{date_text} {now.year}", "%b %d, %I:%M %p %Y")
    except ValueError:
        return None
    # futbin renders sale times in UK local time (BST/GMT), not UTC -
    # treating them as UTC stored every summer sale ~1h in the future
    # (surfaced live by /api/ops/freshness reporting a negative data age).
    dt = naive.replace(tzinfo=_FUTBIN_TZ).astimezone(timezone.utc)
    if dt > now + timedelta(days=1):
        dt = naive.replace(year=now.year - 1, tzinfo=_FUTBIN_TZ).astimezone(timezone.utc)
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


async def fetch_market_page(page, player_url: str, diag: Dict[str, int]) -> "tuple[int, Optional[str]]":
    """The dedicated /sales/{id}/{slug} endpoint is now Cloudflare-blocked -
    confirmed the /market page (already fetched here previously, only to
    find the "latest sale" link that pointed at /sales/...) contains the
    full Player Sales History table inline, so that endpoint is never
    requested at all anymore. One fetch replaces the old two-hop
    market-page-for-a-link + dedicated-sales-page dance."""
    market_url = player_url.rstrip("/") + "/market"
    return await fetch_page_html(page, market_url, diag)


def parse_market_prices(html: str, diag: Optional[Dict[str, Any]] = None) -> Dict[str, Optional[int]]:
    """BIN prices as they'd be read from the /market page, reusing the
    already-correct, real-HTML-verified parse_lowest_bin. NOT currently
    wired in as a live BIN source anywhere - the /market page's price-box
    markup has never been confirmed against real captured HTML the way
    the player page's was (see parse_lowest_bin's own docstring for that
    verification history), so BIN stays sourced from the player page.
    Kept available for a future follow-up once that's confirmed."""
    return {
        "ps": parse_lowest_bin(html, "ps", diag),
        "pc": parse_lowest_bin(html, "pc", diag),
    }


def parse_market_page(html: str, diag: Dict[str, int]) -> Dict[str, Any]:
    """Single entry point for everything this scraper reads off the
    /market page. Only "sales" is consumed by _scrape_one today; "prices"
    exists for the same not-yet-verified reason documented on
    parse_market_prices()."""
    return {
        "prices": parse_market_prices(html, diag),
        "sales": parse_sales_table(html, diag),
    }


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

    # Bio stats (games played, avg goals, top chem style) - "console" here
    # means the combined PS+Xbox market/stat set futbin itself reports as
    # a single "ps-only" value (same convention BIN price already uses).
    # Nullable, safe to add to an existing table.
    for col, ddl in {
        "games_played_console": "INTEGER",
        "games_played_pc": "INTEGER",
        "avg_goals_console": "NUMERIC(6,3)",
        "avg_goals_pc": "NUMERIC(6,3)",
        "top_chem_style_console": "TEXT",
        "top_chem_style_pc": "TEXT",
    }.items():
        await conn.execute(f"ALTER TABLE fut_players ADD COLUMN IF NOT EXISTS {col} {ddl}")


# ---------------------------------------------------------------------------
# Per-player scrape + insert
# ---------------------------------------------------------------------------
async def _scrape_one(
    pool: asyncpg.Pool,
    page,
    card_id: int,
    player_url: str,
    diag: Dict[str, Any],
    abort_event: asyncio.Event,
) -> None:
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

    # --- BIN history: both markets, only insert a real observation ---
    # Used to always insert regardless of whether a price was found -
    # a failed/blocked fetch (futbin 403/429) wrote a NULL lowest_bin
    # row just as "successfully" as a real number, and since
    # backend's fair_value_mv picks its current_bin from the single
    # MOST RECENT bin_history row per card with no NULL check, a bad
    # scraper run could silently overwrite everyone's real price with
    # NULL pool-wide (confirmed live: a run that got 403/429'd on
    # nearly every request wiped current_bin for all 3,147 tracked
    # cards). backend's migration 036 now filters NULLs out
    # defensively too, but the correct fix is here: never claim to
    # have observed a price we didn't actually get.
    # Both platforms' price-box divs live on the one player-page fetch
    # (confirmed against real captured HTML - see parse_lowest_bin's
    # docstring), so this used to fetch the identical player_url twice
    # (once "for ps", once "for pc") for no reason - one fetch, parsed
    # twice against the same HTML, halves this request.
    bio_by_platform: Dict[str, Dict[str, Any]] = {}
    try:
        status, player_html = await fetch_page_html(page, player_url, diag)
    except Exception as e:
        status, player_html = 0, None
        log.warning("Player page fetch failed for card_id=%s: %s", card_id, e)
    _check_circuit_breaker(diag, abort_event)

    if status != 200 or player_html is None:
        diag["bin_failed"] += 2  # both platforms, one shared fetch failed
    else:
        await _dismiss_cookie_banner(page)
        for platform in ("ps", "pc"):
            try:
                bin_price = parse_lowest_bin(player_html, platform, diag)
                bio_by_platform[platform] = parse_bio_stats(player_html, platform)
                if bin_price is not None:
                    async with pool.acquire() as conn:
                        await conn.execute(
                            "INSERT INTO bin_history (player_id, platform, lowest_bin, captured_at) "
                            "VALUES ($1, $2, $3, NOW())",
                            card_id, platform, bin_price,
                        )
                    diag["bin_price_found"] += 1
                else:
                    diag["bin_price_null"] += 1
            except Exception as e:
                diag["bin_failed"] += 1
                log.warning("BIN parse failed for card_id=%s platform=%s: %s", card_id, platform, e)

    # --- Bio stats: games played / avg goals / top chem style, parsed
    # from the same page fetched above - zero extra requests. Only
    # covers these three (not avg assists/yellow/red - those aren't in
    # the bio text, only on the separate /pgp bulk listing). COALESCE
    # against the existing value so a failed/partial parse this run
    # doesn't blow away a good value from a previous run.
    console_bio = bio_by_platform.get("ps") or {}
    pc_bio = bio_by_platform.get("pc") or {}
    if any(v is not None for v in (
        console_bio.get("games"), console_bio.get("avg_goals"), console_bio.get("top_chem_style"),
        pc_bio.get("games"), pc_bio.get("avg_goals"), pc_bio.get("top_chem_style"),
    )):
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    """
                    UPDATE fut_players SET
                        games_played_console = COALESCE($1, games_played_console),
                        avg_goals_console = COALESCE($2, avg_goals_console),
                        top_chem_style_console = COALESCE($3, top_chem_style_console),
                        games_played_pc = COALESCE($4, games_played_pc),
                        avg_goals_pc = COALESCE($5, avg_goals_pc),
                        top_chem_style_pc = COALESCE($6, top_chem_style_pc)
                    WHERE card_id = $7
                    """,
                    console_bio.get("games"), console_bio.get("avg_goals"), console_bio.get("top_chem_style"),
                    pc_bio.get("games"), pc_bio.get("avg_goals"), pc_bio.get("top_chem_style"),
                    card_id,
                )
            diag["bio_stats_updated"] += 1
        except Exception as e:
            diag["bio_stats_failed"] += 1
            log.warning("Bio stats update failed for card_id=%s: %s", card_id, e)

    # --- Sales history: parsed directly from the /market page (the
    # dedicated /sales/{id}/{slug} endpoint is now Cloudflare-blocked;
    # the /market page already contains the full auctions-table inline,
    # so this is one fetch, no link-following, no second request) -
    # dedupe on (player_id, sold_at, sold_price) ---
    try:
        status, market_html = await fetch_market_page(page, player_url, diag)
        _check_circuit_breaker(diag, abort_event)
        if status != 200 or market_html is None:
            diag["sales_market_fetch_failed"] += 1
            diag.setdefault(
                "sales_market_fetch_sample",
                f"status={status} url={player_url.rstrip('/')}/market",
            )
            return
        sales = parse_sales_table(market_html, diag)
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


async def _fetch_tier(conn: asyncpg.Connection, where: str, limit: int = 0) -> list:
    """
    Ordered oldest-refreshed-first (nulls - never captured - first), not
    left to whatever order Postgres happens to return. A full Tier A sweep
    can take longer than the 10-minute cron interval (confirmed: ~37-47min
    for ~2500 candidates at current concurrency), and with no ordering the
    same subset of cards can end up chronically last in a stable query plan
    - starving specific cards run after run rather than spreading staleness
    evenly. Prioritizing the least-recently-updated cards means every run
    spends its budget where it matters most, and no card can starve forever
    even if a sweep never fully completes within one invocation.
    """
    sql = f"""
        SELECT fp.card_id, fp.player_url
        FROM fut_players fp
        LEFT JOIN LATERAL (
            SELECT MAX(captured_at) AS last_captured_at
            FROM bin_history bh
            WHERE bh.player_id = fp.card_id
        ) lb ON true
        WHERE {where} AND fp.player_url IS NOT NULL
        ORDER BY lb.last_captured_at ASC NULLS FIRST
    """
    # TEST_LIMIT support (item 9): bound as a real asyncpg parameter, never
    # string-interpolated - 0 (the default) means no LIMIT clause at all,
    # i.e. full, unmodified production behavior.
    if limit and limit > 0:
        return await conn.fetch(sql + " LIMIT $1", limit)
    return await conn.fetch(sql)


def _tier_b_due(now_epoch: Optional[int] = None) -> bool:
    """Time-anchored phase so separate Cron invocations don't reset the rotation."""
    import time as _time
    now_epoch = now_epoch if now_epoch is not None else int(_time.time())
    return (now_epoch // CRON_INTERVAL_SECONDS) % TIER_B_EVERY == 0


async def _worker_loop(
    pool: asyncpg.Pool,
    page_pool: "asyncio.Queue",
    queue: "asyncio.Queue",
    diag: Dict[str, Any],
    abort_event: asyncio.Event,
) -> None:
    """One page-pool slot's worth of sequential work. Pulls the next row
    only if the circuit breaker hasn't tripped yet - this is what actually
    stops scheduling new players after a clear block (asyncio.gather over
    every candidate up front, the old approach, has no way to do that once
    started). A page is always returned to the pool in `finally`, success
    or failure, so a mid-scrape exception can't leak it."""
    while not abort_event.is_set():
        try:
            row = queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        page = await page_pool.get()
        try:
            await _scrape_one(pool, page, row["card_id"], row["player_url"], diag, abort_event)
        finally:
            await page_pool.put(page)
            queue.task_done()


async def _scrape_batch(pool, page_pool: "asyncio.Queue", rows, diag, abort_event: asyncio.Event) -> None:
    queue: asyncio.Queue = asyncio.Queue()
    for r in rows:
        queue.put_nowait(r)
    await asyncio.gather(*[
        _worker_loop(pool, page_pool, queue, diag, abort_event)
        for _ in range(page_pool.qsize())
    ])


async def crawl_once() -> None:
    # Held on a dedicated connection (not one from the pool below) for the
    # entire run - pg_advisory_lock/unlock are session-scoped, so the same
    # connection must hold it start to finish. If a previous invocation is
    # still in flight (a real sweep can now run well past the 10-minute
    # Cron interval - see OVERLAP_LOCK_KEY's comment), this exits
    # immediately rather than piling up a second concurrent scrape against
    # futbin - a skip, not a failure, so __main__'s sys.exit(0) path is taken.
    lock_conn = await asyncpg.connect(DATABASE_URL)
    got_lock = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", OVERLAP_LOCK_KEY)
    if not got_lock:
        log.info("Previous bin_sales_history_sync run still in flight - skipping this invocation.")
        await lock_conn.close()
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=PLAYWRIGHT_CONCURRENCY + 2)
    playwright_ctx = None
    browser = None
    context = None
    page_pool: Optional["asyncio.Queue"] = None
    try:
        async with pool.acquire() as conn:
            await ensure_tables(conn)
            tier_a = await _fetch_tier(conn, TIER_A_WHERE, TEST_LIMIT)
            b_due = _tier_b_due()
            tier_b = await _fetch_tier(conn, TIER_B_WHERE, TEST_LIMIT) if b_due else []

        log.info(
            "Candidates this run: tier_a=%d (specials + golds 82+) tier_b=%d (golds 75-81, due=%s)%s",
            len(tier_a), len(tier_b), b_due,
            f" [TEST_LIMIT={TEST_LIMIT}]" if TEST_LIMIT else "",
        )
        rows = list(tier_a) + list(tier_b)
        if not rows:
            return

        diag: Dict[str, Any] = defaultdict(int)
        abort_event = asyncio.Event()

        # One persistent browser context for the whole run (item 2) - a
        # real Playwright Chromium session loads FUTBIN pages successfully
        # where plain aiohttp now gets Cloudflare 403s on every request
        # (confirmed live - see module docstring). headless is only passed
        # at all if PLAYWRIGHT_HEADLESS is explicitly set (see its own
        # comment above) - otherwise Playwright's own upstream default
        # applies, deliberately not a "production" assumption this file
        # encodes itself.
        playwright_ctx = await async_playwright().start()
        launch_kwargs: Dict[str, Any] = {"args": ["--no-sandbox", "--disable-dev-shm-usage"]}
        if PLAYWRIGHT_HEADLESS is not None:
            launch_kwargs["headless"] = PLAYWRIGHT_HEADLESS
        browser = await playwright_ctx.chromium.launch(**launch_kwargs)
        # Mirrors the standalone test_futbin.py connectivity test exactly
        # (same UA, viewport, locale) - that script gets a real 200 on the
        # same URL from the same home IP where this worker was still
        # getting 403'd, so the fix is to match it precisely rather than
        # rely on Chromium's own unproven default fingerprint. No extra
        # request headers, no persisted browser storage, no resource
        # blocking, no disabled JS, and no Playwright request API fallback
        # anywhere in this file.
        context = await browser.new_context(
            user_agent=PLAYWRIGHT_USER_AGENT,
            viewport={"width": 1365, "height": 768},
            locale="en-GB",
        )

        # Small reusable page pool (item 3) - this, not a separate
        # semaphore, is what bounds concurrency now (HISTORY_CONCURRENCY is
        # retired). Sized conservatively (PLAYWRIGHT_CONCURRENCY=1 default)
        # so the worker cannot accidentally run several browser pages at
        # once unless explicitly configured.
        page_pool = asyncio.Queue()
        for _ in range(PLAYWRIGHT_CONCURRENCY):
            page_pool.put_nowait(await context.new_page())

        # Hot tier first, always.
        await _scrape_batch(pool, page_pool, tier_a, diag, abort_event)

        if abort_event.is_set() and tier_b:
            diag["tier_b_skipped_circuit_breaker"] = len(tier_b)
            log.warning(
                "Skipping tier B (%d cards) this run: circuit breaker already tripped during tier A.",
                len(tier_b),
            )
        elif tier_b:
            # Fodder tier only if the sweep isn't being throttled - promo
            # nights are exactly when 429s spike AND when Tier A freshness
            # matters most, so a constrained budget goes to A alone.
            if diag["http_429_hits"] > TIER_B_SKIP_429_THRESHOLD:
                diag["tier_b_skipped_throttled"] = len(tier_b)
                log.warning(
                    "Skipping tier B (%d cards) this run: %d 429s during tier A - throttled, "
                    "keeping remaining budget on the hot tier.",
                    len(tier_b), diag["http_429_hits"],
                )
            else:
                await _scrape_batch(pool, page_pool, tier_b, diag, abort_event)

        log.info(
            "Run complete. stale_non_futbin_url=%d | bin_price_found=%d bin_price_null=%d bin_failed=%d "
            "bin_platform_scoped_hit=%d bin_platform_fallback_used=%d | "
            "sales_new=%d sales_dupe=%d sales_failed=%d | bio_stats_updated=%d bio_stats_failed=%d | "
            "http_429_hits=%d http_exceptions=%d http_403_hits=%d cloudflare_challenge_hits=%d "
            "blocked_navigation_attempts=%d "
            "browser_navigation_failures=%d browser_timeouts=%d circuit_breaker_tripped=%s",
            diag["stale_non_futbin_url"],
            diag["bin_price_found"], diag["bin_price_null"], diag["bin_failed"],
            diag["bin_platform_scoped_hit"], diag["bin_platform_fallback_used"],
            diag["sales_new"], diag["sales_dupe"], diag["sales_failed"],
            diag["bio_stats_updated"], diag["bio_stats_failed"],
            diag["http_429_hits"], diag["http_exceptions"],
            diag["http_403_hits"], diag["cloudflare_challenge_hits"],
            diag["blocked_navigation_attempts"],
            diag["browser_navigation_failures"], diag["browser_timeouts"],
            bool(diag.get("circuit_breaker_tripped")),
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
            "sales_market_fetch_failed", "sales_no_table", "sales_no_tbody",
            "sales_rows_too_few_tds", "sales_rows_not_sold", "sales_rows_bad_date", "sales_rows_zero_price",
        ]
        if any(diag.get(k) for k in diagnostic_keys):
            log.info("Sales pipeline diagnostics: %s", {k: diag[k] for k in diagnostic_keys if diag.get(k)})
        for sample_key in (
            "sales_market_fetch_sample", "sales_no_table_sample", "sales_rows_bad_date_sample",
        ):
            if sample_key in diag:
                log.info("%s: %s", sample_key, diag[sample_key])

        # Heartbeat for /api/ops/freshness. A run where every single scrape
        # failed (and nothing new landed) is a markup change or a block -
        # that's the failure mode that silently kills the fair-value data.
        # A tripped circuit breaker is unconditionally unhealthy, even in
        # the edge case where bin_failed's own accounting looks fine (e.g.
        # the breaker tripped on 429s partway through, not 403s/challenges) -
        # a blocked run must never be reported healthy.
        total_attempted = diag["bin_price_found"] + diag["bin_price_null"] + diag["bin_failed"]
        run_ok = (
            not diag.get("circuit_breaker_tripped")
            and (total_attempted == 0 or diag["bin_failed"] < total_attempted)
        )
        async with pool.acquire() as hb_conn:
            await heartbeat(
                hb_conn,
                "bin_sales_history_sync",
                ok=run_ok,
                detail=(
                    f"tier_a={len(tier_a)} tier_b={len(tier_b)}"
                    + (f" (skipped, throttled)" if diag.get("tier_b_skipped_throttled") else "")
                    + (f" (skipped, circuit breaker)" if diag.get("tier_b_skipped_circuit_breaker") else "")
                    + f" sales_new={diag['sales_new']} bin_found={diag['bin_price_found']} "
                    f"bin_failed={diag['bin_failed']} http_429={diag['http_429_hits']} "
                    f"http_403={diag['http_403_hits']} cf_challenge={diag['cloudflare_challenge_hits']} "
                    f"blocked_nav={diag['blocked_navigation_attempts']} "
                    f"circuit_breaker_tripped={bool(diag.get('circuit_breaker_tripped'))}"
                ),
            )
        if not run_ok:
            await alert(
                "bin_sales_history_sync: "
                + (
                    "circuit breaker tripped this run "
                    f"(blocked_nav={diag['blocked_navigation_attempts']} "
                    f"http_403={diag['http_403_hits']} cf_challenge={diag['cloudflare_challenge_hits']} "
                    f"http_429={diag['http_429_hits']}) - "
                    if diag.get("circuit_breaker_tripped")
                    else "every BIN scrape failed this run "
                    f"(bin_failed={diag['bin_failed']}/{total_attempted}, 429s={diag['http_429_hits']}) - "
                )
                + "futbin markup change or block? Sales/BIN history has stopped growing."
            )
    finally:
        if page_pool is not None:
            while not page_pool.empty():
                try:
                    page = page_pool.get_nowait()
                    await page.close()
                except Exception:
                    pass
        if context is not None:
            try:
                await context.close()
            except Exception:
                pass
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
        if playwright_ctx is not None:
            try:
                await playwright_ctx.stop()
            except Exception:
                pass
        await pool.close()
        await lock_conn.execute("SELECT pg_advisory_unlock($1)", OVERLAP_LOCK_KEY)
        await lock_conn.close()


# ================== ONE-SHOT ENTRY POINT ==================
# Deployed as a Railway Cron Job (10-minute schedule) rather than a
# permanent worker - Railway starts a fresh container for each scheduled
# run and expects it to exit when done, so there's no in-process scheduling
# loop, signal handling, or health server here anymore (a health-check
# endpoint is for long-lived services Railway pings on an interval; a Cron
# Job's container doesn't stay up between runs for that to apply to).
if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())
    except Exception as e:
        log.error("crawl_once() failed: %s", e)
        sys.exit(1)
    sys.exit(0)
