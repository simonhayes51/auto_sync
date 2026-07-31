"""
Focused tests for the Playwright-transport rewrite of
bin_sales_history_sync.py (item 14). Deliberately avoid anything needing a
live browser launch or a live DB connection - neither is available in
this sandbox (no network egress to futbin.com, no Postgres) - so these
cover the pure-function pieces plus page-pool/queue mechanics with stub
objects standing in for a real Playwright Page / asyncpg Connection.

No fixture of real captured FUTBIN market-page HTML exists anywhere in
this repo/session at the time this was written, and this sandbox cannot
fetch live FUTBIN HTML to capture one - the sales-table test below uses a
SYNTHETIC fixture built to the same `<table class="auctions-table">`
structure parse_sales_table() already expects (columns: Date | Listed For
| Sold For | EA Tax | Net Price | Type). Swap in real saved HTML if it
becomes available - the assertions here don't depend on it being fake,
only on the DOM shape being right.
"""
import asyncio
import functools
import os
import sys
from pathlib import Path
from collections import defaultdict

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# The module raises at import time if DATABASE_URL is unset (same
# fail-fast guard every worker in this repo uses) - never connected to in
# these tests, just needs to be present for the import to succeed.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import bin_sales_history_sync as mod


def run_async(coro_fn):
    """No pytest-asyncio in this environment - a plain asyncio.run()
    wrapper avoids adding a new test-only dependency for a handful of
    coroutine tests."""
    @functools.wraps(coro_fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(coro_fn(*args, **kwargs))
    return wrapper


# ---------------------------------------------------------------------------
# Synthetic market-page fixture (auctions-table only - the piece this
# change actually reads from /market; BIN price-box parsing already has
# its own real-HTML-verified coverage from the prior session's fix and
# isn't touched by this change).
# ---------------------------------------------------------------------------
SYNTHETIC_MARKET_HTML = """
<html><body>
<table class="auctions-table">
  <tbody>
    <tr>
      <td><div><i class="fa fa-check"></i><span class="sales-date-time">Jul 30, 6:32 PM</span></div></td>
      <td>750,000</td>
      <td>750,000</td>
      <td>37,500</td>
      <td>712,500</td>
      <td>Buy Now</td>
    </tr>
    <tr>
      <td><div><i class="fa fa-check"></i><span class="sales-date-time">Jul 30, 5:10 PM</span></div></td>
      <td>745,000</td>
      <td>745,000</td>
      <td>37,250</td>
      <td>707,750</td>
      <td>Buy Now</td>
    </tr>
    <tr>
      <td><div><i class="fa fa-times"></i><span class="sales-date-time">Jul 30, 4:00 PM</span></div></td>
      <td>760,000</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Expired</td>
    </tr>
    <tr>
      <td>too-few-tds</td>
    </tr>
  </tbody>
</table>
</body></html>
"""


def test_parse_sales_table_from_market_html():
    diag = defaultdict(int)
    sales = mod.parse_sales_table(SYNTHETIC_MARKET_HTML, diag)

    assert len(sales) == 2
    assert sales[0]["sold_price"] == 750_000
    assert sales[0]["ea_tax"] == 37_500
    assert sales[0]["net_price"] == 712_500
    assert sales[1]["sold_price"] == 745_000

    assert diag["sales_rows_not_sold"] == 1
    assert diag["sales_rows_too_few_tds"] == 1
    assert diag["sales_no_table"] == 0


def test_parse_sales_table_missing_table():
    diag = defaultdict(int)
    sales = mod.parse_sales_table("<html><body>no table here</body></html>", diag)
    assert sales == []
    assert diag["sales_no_table"] == 1


# ---------------------------------------------------------------------------
# Cloudflare challenge detection
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "status,html,expected",
    [
        (403, "<html><body>anything</body></html>", True),
        (200, "<html><title>Just a moment...</title></html>", True),
        (200, "<html><body>Attention Required! | Cloudflare</body></html>", True),
        (200, '<html><body><div class="challenge-platform"></div></body></html>', True),
        (200, "<html><body>cf-browser-verification</body></html>", True),
        (403, None, True),
        (200, "<html><body>Oops, there was an error - 403</body></html>", True),
        (200, "<html><body>real player page content</body></html>", False),
        (200, None, False),
    ],
)
def test_looks_like_challenge(status, html, expected):
    assert mod._looks_like_challenge(status, html) is expected


# ---------------------------------------------------------------------------
# Circuit breaker
# ---------------------------------------------------------------------------
def test_circuit_breaker_trips_on_blocked_navigation_threshold():
    diag = defaultdict(int)
    abort_event = asyncio.Event()
    diag["blocked_navigation_attempts"] = mod.HISTORY_403_ABORT_THRESHOLD - 1
    mod._check_circuit_breaker(diag, abort_event)
    assert not abort_event.is_set()

    diag["blocked_navigation_attempts"] += 1
    mod._check_circuit_breaker(diag, abort_event)
    assert abort_event.is_set()
    assert diag["circuit_breaker_tripped"] == 1


def test_circuit_breaker_does_not_double_count_one_challenged_navigation():
    """A single Cloudflare-403-challenge navigation increments BOTH
    http_403_hits and cloudflare_challenge_hits (see fetch_page_html) -
    the breaker must count that as exactly one blocked attempt, not two,
    so summing the two diagnostic-only counters must not be what trips
    it."""
    diag = defaultdict(int)
    abort_event = asyncio.Event()
    # One real blocked navigation below threshold, but the two diagnostic
    # counters alone would (incorrectly, pre-fix) sum to the threshold.
    diag["blocked_navigation_attempts"] = mod.HISTORY_403_ABORT_THRESHOLD - 1
    diag["http_403_hits"] = mod.HISTORY_403_ABORT_THRESHOLD
    diag["cloudflare_challenge_hits"] = mod.HISTORY_403_ABORT_THRESHOLD
    mod._check_circuit_breaker(diag, abort_event)
    assert not abort_event.is_set()


def test_circuit_breaker_trips_on_429_threshold():
    diag = defaultdict(int)
    abort_event = asyncio.Event()
    diag["http_429_hits"] = mod.HISTORY_429_ABORT_THRESHOLD
    mod._check_circuit_breaker(diag, abort_event)
    assert abort_event.is_set()


def test_circuit_breaker_idempotent_once_tripped():
    diag = defaultdict(int)
    abort_event = asyncio.Event()
    diag["blocked_navigation_attempts"] = mod.HISTORY_403_ABORT_THRESHOLD
    mod._check_circuit_breaker(diag, abort_event)
    assert diag["circuit_breaker_tripped"] == 1

    # Further calls (even with counts still over threshold) must not
    # re-trip/re-log - abort_event.is_set() short-circuits immediately.
    diag["blocked_navigation_attempts"] += 100
    mod._check_circuit_breaker(diag, abort_event)
    assert abort_event.is_set()


def test_circuit_breaker_does_not_trip_below_thresholds():
    diag = defaultdict(int)
    abort_event = asyncio.Event()
    diag["blocked_navigation_attempts"] = 1
    diag["http_429_hits"] = 1
    mod._check_circuit_breaker(diag, abort_event)
    assert not abort_event.is_set()
    assert "circuit_breaker_tripped" not in diag


# ---------------------------------------------------------------------------
# TEST_LIMIT - must be a bound parameter, never string-interpolated
# ---------------------------------------------------------------------------
class _FakeConn:
    """Stands in for asyncpg.Connection - just records what _fetch_tier()
    asks it to run, so we can assert the LIMIT is a bound $N parameter
    rather than baked into the SQL text."""

    def __init__(self):
        self.calls = []

    async def fetch(self, sql, *params):
        self.calls.append((sql, params))
        return []


@run_async
async def test_fetch_tier_no_limit_by_default():
    conn = _FakeConn()
    await mod._fetch_tier(conn, "true", limit=0)
    sql, params = conn.calls[0]
    assert "LIMIT" not in sql
    assert params == ()


@run_async
async def test_fetch_tier_binds_limit_as_parameter():
    conn = _FakeConn()
    await mod._fetch_tier(conn, "true", limit=10)
    sql, params = conn.calls[0]
    assert "LIMIT $1" in sql
    assert params == (10,)
    # The limit value itself must never appear string-interpolated into
    # the SQL text - only ever passed as a bound parameter.
    assert "LIMIT 10" not in sql


# ---------------------------------------------------------------------------
# Page-pool acquisition / release, including on a failure path
# ---------------------------------------------------------------------------
class _FakePage:
    def __init__(self, name):
        self.name = name


@run_async
async def test_worker_loop_returns_page_to_pool_on_success():
    page_pool: asyncio.Queue = asyncio.Queue()
    page = _FakePage("p1")
    page_pool.put_nowait(page)

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"card_id": 1, "player_url": "https://www.futbin.com/26/player/1/x"})

    diag = defaultdict(int)
    abort_event = asyncio.Event()
    seen_pages = []

    async def fake_scrape_one(pool, page, card_id, player_url, diag, abort_event):
        seen_pages.append(page)

    orig = mod._scrape_one
    mod._scrape_one = fake_scrape_one
    try:
        await mod._worker_loop(None, page_pool, queue, diag, abort_event)
    finally:
        mod._scrape_one = orig

    assert seen_pages == [page]
    assert page_pool.qsize() == 1  # page returned
    assert page_pool.get_nowait() is page


@run_async
async def test_worker_loop_returns_page_to_pool_on_failure():
    page_pool: asyncio.Queue = asyncio.Queue()
    page = _FakePage("p1")
    page_pool.put_nowait(page)

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"card_id": 1, "player_url": "https://www.futbin.com/26/player/1/x"})

    diag = defaultdict(int)
    abort_event = asyncio.Event()

    async def failing_scrape_one(pool, page, card_id, player_url, diag, abort_event):
        raise RuntimeError("boom")

    orig = mod._scrape_one
    mod._scrape_one = failing_scrape_one
    try:
        with pytest.raises(RuntimeError):
            await mod._worker_loop(None, page_pool, queue, diag, abort_event)
    finally:
        mod._scrape_one = orig

    # Even though _scrape_one raised, the page must still be back in the
    # pool - a page pool is worthless if a single card's failure leaks it.
    assert page_pool.qsize() == 1


@run_async
async def test_worker_loop_stops_pulling_new_work_once_aborted():
    page_pool: asyncio.Queue = asyncio.Queue()
    page_pool.put_nowait(_FakePage("p1"))

    queue: asyncio.Queue = asyncio.Queue()
    queue.put_nowait({"card_id": 1, "player_url": "https://www.futbin.com/26/player/1/x"})
    queue.put_nowait({"card_id": 2, "player_url": "https://www.futbin.com/26/player/2/y"})

    diag = defaultdict(int)
    abort_event = asyncio.Event()
    abort_event.set()  # already tripped before the worker even starts

    processed = []

    async def fake_scrape_one(pool, page, card_id, player_url, diag, abort_event):
        processed.append(card_id)

    orig = mod._scrape_one
    mod._scrape_one = fake_scrape_one
    try:
        await mod._worker_loop(None, page_pool, queue, diag, abort_event)
    finally:
        mod._scrape_one = orig

    assert processed == []
    assert queue.qsize() == 2  # nothing pulled


# ---------------------------------------------------------------------------
# No /sales/ endpoint is ever requested anymore
# ---------------------------------------------------------------------------
def test_dedicated_sales_endpoint_functions_removed():
    for name in ("_resolve_sales_path", "_sales_url", "fetch_sales_history", "fetch_lowest_bin", "_get_with_retry"):
        assert not hasattr(mod, name), f"{name} should have been removed"


def test_fetch_market_page_url_is_market_not_sales():
    # Check the executable body's string constants only, excluding the
    # docstring - the docstring deliberately names the now-removed
    # /sales/{id}/{slug} endpoint for historical context, which would
    # otherwise false-positive a naive whole-source substring check.
    doc = mod.fetch_market_page.__doc__
    consts = [
        c for c in mod.fetch_market_page.__code__.co_consts
        if isinstance(c, str) and c != doc
    ]
    joined = " ".join(consts)
    assert "/market" in joined
    assert "/sales/" not in joined


def test_no_aiohttp_import():
    assert "aiohttp" not in sys.modules or not hasattr(mod, "aiohttp")


# ---------------------------------------------------------------------------
# No custom/bot-identifying user-agent override on the browser context
# ---------------------------------------------------------------------------
def test_headers_constant_removed():
    assert not hasattr(mod, "HEADERS")


def test_new_context_has_no_user_agent_override():
    import inspect

    src = inspect.getsource(mod.crawl_once)
    assert "new_context(" in src
    # Check the actual kwarg usage, not just the substring "user_agent" -
    # a nearby explanatory comment mentions it by name for context.
    assert "user_agent=" not in src
    assert 'locale="en-GB"' in src
    assert '"width": 1440' in src
