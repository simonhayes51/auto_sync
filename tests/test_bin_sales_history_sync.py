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
from bs4 import BeautifulSoup

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
# Cloudflare challenge detection - title + visible-body-excerpt based, NOT
# a full-HTML substring scan (that scan produced false positives: a real
# FUTBIN page can embed Cloudflare's own anti-bot JS even when not
# actively challenging - confirmed live, see _looks_like_challenge's
# docstring).
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "status,title,body_excerpt,expected",
    [
        (403, None, None, True),
        (403, "anything", "anything", True),
        (200, "Just a moment...", None, True),
        (200, "  JUST A MOMENT...  ", None, True),  # case/whitespace-insensitive
        (200, "Attention Required! | Cloudflare", None, True),
        (200, None, "Oops, there was an error - 403 - please try again later", True),
        (200, "Kenan Yıldız - FUTBIN", None, False),
        (200, None, None, False),
        (200, "Some other page", "nothing suspicious here", False),
    ],
)
def test_looks_like_challenge(status, title, body_excerpt, expected):
    assert mod._looks_like_challenge(status, title, body_excerpt) is expected


def test_looks_like_challenge_ignores_cloudflare_script_markers_in_a_real_page():
    """Regression test: a real, valid player page's raw HTML can embed
    Cloudflare's own script tags (e.g. mentioning "challenge-platform" or
    "cf-browser-verification") purely as normal anti-bot tooling, without
    the page actually being a block/challenge. Since _looks_like_challenge
    only ever looks at title/body-excerpt (extracted from the live page,
    not the raw HTML source), these markers being present in <script>
    tags elsewhere in the document must never affect the result - this is
    exactly the false-positive class that a naive full-HTML substring scan
    produced live (a one-card test recorded 2 challenge hits for only 1
    real 403 across 2 navigations).

    Synthetic fixture (no real captured Pogba HTML exists in this sandbox -
    see the module-level fixture note at the top of this file) shaped like
    a real player page: a real <title>, a real visible body excerpt, and
    an embedded Cloudflare script block containing both marker strings.
    """
    synthetic_pogba_html = """
    <html>
      <head>
        <title>Paul Pogba - FUTBIN</title>
        <script>
          // Cloudflare's own anti-bot tooling, present on real pages too
          window.__CF$cv$params = {r: 'challenge-platform', t: 'cf-browser-verification'};
        </script>
      </head>
      <body>
        <div class="price-box platform-ps-only price-box-original-player">
          <div class="price inline-with-icon lowest-price-1">120,000</div>
        </div>
        <p>Paul Pogba is a French footballer...</p>
      </body>
    </html>
    """
    soup = BeautifulSoup(synthetic_pogba_html, "html.parser")
    title = soup.title.get_text(strip=True)
    body_excerpt = soup.body.get_text(" ", strip=True)[:300]

    # Sanity check the fixture actually contains the markers that used to
    # false-positive, so this test would have failed against the old
    # full-HTML-scan implementation.
    assert "challenge-platform" in synthetic_pogba_html
    assert "cf-browser-verification" in synthetic_pogba_html

    assert mod._looks_like_challenge(200, title, body_excerpt) is False


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
# Browser context mirrors the proven standalone test_futbin.py connectivity
# test exactly (real Chrome UA, not the old bot-identifying string; same
# viewport/locale) - that script gets a real 200 from the same home IP
# where this worker was still getting 403'd.
# ---------------------------------------------------------------------------
def test_headers_constant_removed():
    assert not hasattr(mod, "HEADERS")


def test_playwright_user_agent_is_not_bot_identifying():
    ua = mod.PLAYWRIGHT_USER_AGENT.lower()
    assert "sbcsolver" not in ua
    assert "bot" not in ua
    assert "chrome" in ua


def test_new_context_uses_real_chrome_user_agent_and_matching_viewport():
    # 1440x900 matches the actual test_futbin.py file exactly (confirmed
    # by direct line-by-line comparison) - a prior turn's instruction used
    # 1365x768, which did not match the real script and has been corrected.
    import inspect

    src = inspect.getsource(mod.crawl_once)
    assert "new_context(" in src
    assert "user_agent=PLAYWRIGHT_USER_AGENT" in src
    assert 'locale="en-GB"' in src
    assert '"width": 1440' in src
    assert '"height": 900' in src


def test_nav_timeout_matches_test_futbin():
    assert mod.PLAYWRIGHT_NAV_TIMEOUT_MS == 60000


def test_no_route_blocking_storage_state_or_request_api():
    import inspect

    src = inspect.getsource(mod)
    for forbidden in (".route(", "storage_state", "java_script_enabled", ".request."):
        assert forbidden not in src, f"found disallowed pattern: {forbidden}"


# ---------------------------------------------------------------------------
# fetch_page_html() mirrors test_futbin.py's exact read order: title, then
# a body-locator read, and ONLY THEN page.content() - not the other way
# round, and not gated on status. Plus the new pre-content() diagnostics
# and once-per-run debug-artifact save.
# ---------------------------------------------------------------------------
class _FakeLocator:
    def __init__(self, count=0, text="", raise_on_count=False, raise_on_text=False):
        self._count = count
        self._text = text
        self._raise_on_count = raise_on_count
        self._raise_on_text = raise_on_text

    async def count(self):
        if self._raise_on_count:
            raise RuntimeError("locator count failed")
        return self._count

    async def inner_text(self, timeout=None):
        if self._raise_on_text:
            raise RuntimeError("inner_text failed")
        return self._text


class _FakeResponse:
    def __init__(self, status):
        self.status = status


class _FakeRecordingPage:
    """Records the order of calls that matter for the mirrored flow -
    title() and body.inner_text() must both happen before content()."""

    def __init__(self, status=200, title="Kenan Yıldız - FUTBIN", body="real player content", html="<html>real</html>"):
        self.calls = []
        self.goto_urls = []
        self.url = "https://www.futbin.com/26/player/24583/kenan-yldz"
        self._status = status
        self._title = title
        self._body = body
        self._html = html

    async def goto(self, url, wait_until=None, timeout=None):
        self.calls.append("goto")
        self.goto_urls.append(url)
        return _FakeResponse(self._status)

    async def title(self):
        self.calls.append("title")
        return self._title

    def locator(self, selector):
        if selector == "body":
            self.calls.append("body_locator")
            return _FakeLocator(text=self._body)
        return _FakeLocator(count=1)

    async def content(self):
        self.calls.append("content")
        return self._html

    async def screenshot(self, path=None, full_page=None):
        self.calls.append("screenshot")


@run_async
async def test_fetch_page_html_reads_title_and_body_before_content():
    diag = defaultdict(int)
    page = _FakeRecordingPage()
    status, html = await mod.fetch_page_html(page, "https://example.com", diag)

    title_idx = page.calls.index("title")
    body_idx = page.calls.index("body_locator")
    content_idx = page.calls.index("content")
    assert title_idx < content_idx
    assert body_idx < content_idx
    assert status == 200
    assert html == "<html>real</html>"


def test_fetch_page_html_reads_title_and_body_even_on_403(monkeypatch, tmp_path):
    """test_futbin.py never special-cases status before reading title/body -
    this worker previously skipped both entirely for a non-200 status.
    HISTORY_MAX_RETRIES patched to 0 just to keep this test fast (a 403
    still retries by default, each with a real jittered sleep) - retry
    behavior itself is untouched by this change and not what's under test
    here. Runs from tmp_path since a 403 also triggers the real
    debug-artifact save (_save_debug_block_artifacts), which must not
    write into this repo's working directory."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(mod, "HISTORY_MAX_RETRIES", 0)
    diag = defaultdict(int)
    page = _FakeRecordingPage(status=403)
    status, html = asyncio.run(mod.fetch_page_html(page, "https://example.com", diag))

    assert "title" in page.calls
    assert "body_locator" in page.calls
    assert status == 403
    assert html is None


def test_locator_present_returns_bool_and_none_on_error():
    async def _run():
        class _Page:
            def locator(self, selector):
                if selector == ".raises":
                    return _FakeLocator(raise_on_count=True)
                return _FakeLocator(count=1 if selector == ".present" else 0)

        page = _Page()
        present = await mod._locator_present(page, ".present")
        absent = await mod._locator_present(page, ".absent")
        unknown = await mod._locator_present(page, ".raises")
        return present, absent, unknown

    present, absent, unknown = asyncio.run(_run())
    assert present is True
    assert absent is False
    assert unknown is None


@run_async
async def test_save_debug_block_artifacts_once_per_run(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    diag = defaultdict(int)
    page = _FakeRecordingPage()

    await mod._save_debug_block_artifacts(page, diag)
    await mod._save_debug_block_artifacts(page, diag)  # second call must be a no-op

    assert page.calls.count("content") == 1
    assert page.calls.count("screenshot") == 1
    assert diag["debug_block_artifacts_saved"] is True
    assert (tmp_path / "debug_last_block.html").read_text(encoding="utf-8") == "<html>real</html>"


# ---------------------------------------------------------------------------
# _scrape_one() - exactly one browser navigation per card: BIN, bio, AND
# sales all parsed from the same player_html. fetch_market_page() must
# never be called; no /market URL is ever navigated to.
# ---------------------------------------------------------------------------
class _FakeExecConn:
    def __init__(self):
        self.executed = []

    async def execute(self, sql, *params):
        self.executed.append((sql, params))
        return "INSERT 0 1"


class _FakeAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc_info):
        return False


class _FakePool:
    def __init__(self):
        self.conn = _FakeExecConn()

    def acquire(self):
        return _FakeAcquireCtx(self.conn)


# Combines the real, already-verified price-box structure (see
# test_parse_lowest_bin-style fixtures used elsewhere this session) with
# the synthetic auctions-table fixture above - a single player_html that
# should yield BIN (both platforms), bio, AND sales all from one page.
COMBINED_PLAYER_PAGE_HTML = """
<html><head><title>Kenan Yildiz - FUTBIN</title></head><body>
<div class="price-box platform-ps-only price-box-original-player" data-id="24583">
  <div class="column">
    <div class="price inline-with-icon lowest-price-1">120,000<img alt="Coin" src="x"></div>
  </div>
</div>
<div class="price-box platform-pc-only price-box-original-player" data-id="24583">
  <div class="column">
    <div class="price inline-with-icon lowest-price-1">130,000<img alt="Coin" src="x"></div>
  </div>
</div>
<div class="player-text-section">
  <span class="platform-ps-only">He has been used in 1,234 games with a GPG (goals per game) of 0.500. The best chemistry style for him is Basic.</span>
  <span class="platform-pc-only">He has been used in 500 games with a GPG (goals per game) of 0.400. The best chemistry style for him is Basic.</span>
</div>
<table class="auctions-table">
  <tbody>
    <tr>
      <td><div><i class="fa fa-check"></i><span class="sales-date-time">Jul 30, 6:32 PM</span></div></td>
      <td>115,000</td>
      <td>115,000</td>
      <td>5,750</td>
      <td>109,250</td>
      <td>Buy Now</td>
    </tr>
  </tbody>
</table>
</body></html>
"""


@run_async
async def test_scrape_one_makes_exactly_one_navigation_and_parses_sales_from_player_html():
    pool = _FakePool()
    page = _FakeRecordingPage(html=COMBINED_PLAYER_PAGE_HTML)
    diag = defaultdict(int)
    abort_event = asyncio.Event()

    await mod._scrape_one(pool, page, 24583, "https://www.futbin.com/26/player/24583/kenan-yldz", diag, abort_event)

    # Exactly one navigation - no /market, no /sales/ URL ever visited.
    assert page.calls.count("goto") == 1
    assert page.goto_urls == ["https://www.futbin.com/26/player/24583/kenan-yldz"]
    assert not any("/market" in u for u in page.goto_urls)
    assert not any("/sales/" in u for u in page.goto_urls)

    assert diag["bin_price_found"] == 2
    assert diag["bin_failed"] == 0
    assert diag["sales_new"] == 1
    assert diag["sales_player_page_no_table"] == 0

    # BIN inserts (2) + bio update (1) + sales insert (1) = 4 DB calls.
    insert_bin_calls = [c for c in pool.conn.executed if "INSERT INTO bin_history" in c[0]]
    sales_calls = [c for c in pool.conn.executed if "INSERT INTO sales_history" in c[0]]
    assert len(insert_bin_calls) == 2
    assert len(sales_calls) == 1


@run_async
async def test_scrape_one_never_calls_fetch_market_page(monkeypatch):
    called = {"count": 0}

    async def fake_fetch_market_page(page, player_url, diag):
        called["count"] += 1
        return 200, "<html></html>"

    monkeypatch.setattr(mod, "fetch_market_page", fake_fetch_market_page)

    pool = _FakePool()
    page = _FakeRecordingPage(html=COMBINED_PLAYER_PAGE_HTML)
    diag = defaultdict(int)
    abort_event = asyncio.Event()

    await mod._scrape_one(pool, page, 24583, "https://www.futbin.com/26/player/24583/kenan-yldz", diag, abort_event)

    assert called["count"] == 0


def test_scrape_one_sets_sales_player_page_no_table_when_player_fetch_fails(monkeypatch, tmp_path):
    # 403 also triggers the real debug-artifact save - run from tmp_path so
    # it can't write into this repo's working directory.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(mod, "HISTORY_MAX_RETRIES", 0)  # keep the 403 retry path fast
    pool = _FakePool()
    page = _FakeRecordingPage(status=403, html=None)
    diag = defaultdict(int)
    abort_event = asyncio.Event()

    asyncio.run(mod._scrape_one(
        pool, page, 24583, "https://www.futbin.com/26/player/24583/kenan-yldz", diag, abort_event,
    ))

    assert diag["sales_player_page_no_table"] == 1
    assert diag["sales_new"] == 0
