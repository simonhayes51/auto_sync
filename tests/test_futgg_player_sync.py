"""
Focused tests for futgg_player_sync.py's pagination-based discovery
(collect_listing_urls). Deliberately avoid anything needing a live browser
launch or a live DB connection - neither is available in this sandbox (no
network egress to fut.gg, no Postgres) - so these use a fake Playwright
Page simulating a paginated FUT.GG listing.
"""
import asyncio
import functools
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
# The module raises at import time if DATABASE_URL is unset (same
# fail-fast guard every worker in this repo uses) - never connected to in
# these tests, just needs to be present for the import to succeed.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import futgg_player_sync as mod


def run_async(coro_fn):
    """No pytest-asyncio in this environment - a plain asyncio.run()
    wrapper avoids adding a new test-only dependency for a handful of
    coroutine tests."""
    @functools.wraps(coro_fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(coro_fn(*args, **kwargs))
    return wrapper


def _card_href(card_id: int) -> str:
    return f"/players/{card_id}-player-{card_id}/26-{1000 + card_id}/"


class _FakeResponse:
    def __init__(self, status):
        self.status = status


class _FakeLocator:
    def __init__(self, hrefs=None, should_timeout=False):
        self._hrefs = hrefs or []
        self._should_timeout = should_timeout

    @property
    def first(self):
        return self

    async def is_visible(self, timeout=None):
        return False

    async def click(self, timeout=None):
        pass

    async def wait_for(self, state=None, timeout=None):
        if self._should_timeout:
            raise mod.PlaywrightTimeoutError("no matching element")
        return None

    async def evaluate_all(self, script):
        return self._hrefs


class _FakeListingPage:
    """Simulates FUT.GG's paginated listing. pages_hrefs maps page number
    -> list of hrefs found there; a page number beyond the dict (or one
    marked in empty_from) simulates running past the end of the
    catalogue (no card links present)."""

    def __init__(self, pages_hrefs, status=200, empty_from=None):
        self.goto_urls = []
        self._pages_hrefs = pages_hrefs
        self._status = status
        self._empty_from = empty_from
        self._current_page = 1

    async def goto(self, url, wait_until=None, timeout=None):
        self.goto_urls.append(url)
        match = re.search(r"page=(\d+)", url)
        self._current_page = int(match.group(1)) if match else 1
        return _FakeResponse(self._status)

    def locator(self, selector):
        hrefs = self._pages_hrefs.get(self._current_page, [])
        if selector == "a[href*='/players/']":
            no_links = self._empty_from is not None and self._current_page >= self._empty_from
            return _FakeLocator(should_timeout=no_links)
        if selector == "a[href]":
            return _FakeLocator(hrefs=hrefs)
        return _FakeLocator()


@run_async
async def test_collect_listing_urls_navigates_pages_in_order(monkeypatch):
    monkeypatch.setattr(mod, "MAX_PAGES", 3)
    monkeypatch.setattr(mod, "PLAYER_LIMIT", 0)
    monkeypatch.setattr(mod, "IDLE_ROUNDS", 10)
    monkeypatch.setattr(mod, "PAGE_DELAY", 0)
    pages_hrefs = {
        1: [_card_href(1), _card_href(2)],
        2: [_card_href(3)],
        3: [_card_href(4)],
    }
    page = _FakeListingPage(pages_hrefs)

    urls = await mod.collect_listing_urls(page)

    assert page.goto_urls == [
        "https://www.fut.gg/players/new/?page=1",
        "https://www.fut.gg/players/new/?page=2",
        "https://www.fut.gg/players/new/?page=3",
    ]
    assert len(urls) == 4


def test_collect_listing_urls_no_scroll_calls():
    """collect_listing_urls itself must never call window.scrollTo or a
    scroll-settle wait_for_timeout - confirmed via source inspection of
    the function BODY, excluding its own docstring (which explains what
    it no longer does, by name, for historical context). The fake page
    used elsewhere in this file doesn't even implement .evaluate()/
    .wait_for_timeout(), so a real call would raise AttributeError too."""
    doc = mod.collect_listing_urls.__doc__
    consts = [
        c for c in mod.collect_listing_urls.__code__.co_consts
        if isinstance(c, str) and c != doc
    ]
    joined = " ".join(consts)
    assert "scrollTo" not in joined
    assert "wait_for_timeout" not in joined


@run_async
async def test_collect_listing_urls_stops_at_max_pages(monkeypatch):
    monkeypatch.setattr(mod, "MAX_PAGES", 2)
    monkeypatch.setattr(mod, "PLAYER_LIMIT", 0)
    monkeypatch.setattr(mod, "IDLE_ROUNDS", 10)
    monkeypatch.setattr(mod, "PAGE_DELAY", 0)
    # Pages 1-5 all have distinct cards, but MAX_PAGES caps the walk at 2.
    pages_hrefs = {n: [_card_href(n)] for n in range(1, 6)}
    page = _FakeListingPage(pages_hrefs)

    urls = await mod.collect_listing_urls(page)

    assert page.goto_urls == [
        "https://www.fut.gg/players/new/?page=1",
        "https://www.fut.gg/players/new/?page=2",
    ]
    assert len(urls) == 2


@run_async
async def test_collect_listing_urls_stops_early_after_idle_pages(monkeypatch):
    monkeypatch.setattr(mod, "MAX_PAGES", 400)
    monkeypatch.setattr(mod, "PLAYER_LIMIT", 0)
    monkeypatch.setattr(mod, "IDLE_ROUNDS", 2)
    monkeypatch.setattr(mod, "PAGE_DELAY", 0)
    # Page 1 has cards; pages 2+ repeat the SAME cards (zero new) - should
    # stop after 2 consecutive idle pages (pages 2 and 3), well short of
    # the 400-page cap.
    pages_hrefs = {
        1: [_card_href(1), _card_href(2)],
        2: [_card_href(1), _card_href(2)],
        3: [_card_href(1), _card_href(2)],
        4: [_card_href(1), _card_href(2)],
    }
    page = _FakeListingPage(pages_hrefs)

    urls = await mod.collect_listing_urls(page)

    assert page.goto_urls == [
        "https://www.fut.gg/players/new/?page=1",
        "https://www.fut.gg/players/new/?page=2",
        "https://www.fut.gg/players/new/?page=3",
    ]
    assert len(urls) == 2


@run_async
async def test_collect_listing_urls_stops_when_no_card_links_present(monkeypatch):
    monkeypatch.setattr(mod, "MAX_PAGES", 400)
    monkeypatch.setattr(mod, "PLAYER_LIMIT", 0)
    monkeypatch.setattr(mod, "IDLE_ROUNDS", 4)
    monkeypatch.setattr(mod, "PAGE_DELAY", 0)
    # Simulates running past the real end of a 3-page catalogue - page 4
    # onward has no card links at all (not just zero NEW ones).
    pages_hrefs = {
        1: [_card_href(1)],
        2: [_card_href(2)],
        3: [_card_href(3)],
    }
    page = _FakeListingPage(pages_hrefs, empty_from=4)

    urls = await mod.collect_listing_urls(page)

    assert page.goto_urls == [
        "https://www.fut.gg/players/new/?page=1",
        "https://www.fut.gg/players/new/?page=2",
        "https://www.fut.gg/players/new/?page=3",
        "https://www.fut.gg/players/new/?page=4",
    ]
    assert len(urls) == 3


@run_async
async def test_collect_listing_urls_respects_player_limit(monkeypatch):
    monkeypatch.setattr(mod, "MAX_PAGES", 400)
    monkeypatch.setattr(mod, "PLAYER_LIMIT", 3)
    monkeypatch.setattr(mod, "IDLE_ROUNDS", 10)
    monkeypatch.setattr(mod, "PAGE_DELAY", 0)
    pages_hrefs = {
        1: [_card_href(1), _card_href(2)],
        2: [_card_href(3), _card_href(4)],
        3: [_card_href(5)],
    }
    page = _FakeListingPage(pages_hrefs)

    urls = await mod.collect_listing_urls(page)

    assert len(urls) == 3
    assert page.goto_urls == [
        "https://www.fut.gg/players/new/?page=1",
        "https://www.fut.gg/players/new/?page=2",
    ]


def test_full_scan_selects_the_full_page_cap(monkeypatch):
    monkeypatch.setenv("FUTGG_LISTING_FULL_SCAN", "true")
    monkeypatch.setenv("FUTGG_LISTING_MAX_PAGES", "5")
    monkeypatch.setenv("FUTGG_LISTING_MAX_PAGES_FULL", "400")
    import importlib
    reloaded = importlib.reload(mod)
    try:
        assert reloaded.FULL_SCAN is True
        assert reloaded.MAX_PAGES == 400
    finally:
        monkeypatch.delenv("FUTGG_LISTING_FULL_SCAN", raising=False)
        monkeypatch.delenv("FUTGG_LISTING_MAX_PAGES", raising=False)
        monkeypatch.delenv("FUTGG_LISTING_MAX_PAGES_FULL", raising=False)
        importlib.reload(mod)


def test_daily_mode_is_the_default():
    assert mod.FULL_SCAN is False
    assert mod.MAX_PAGES == mod.MAX_PAGES_DAILY
    assert mod.MAX_PAGES_DAILY == 5
    assert mod.MAX_PAGES_FULL == 400


def test_scroll_round_config_removed():
    assert not hasattr(mod, "SCROLL_ROUNDS")
    assert not hasattr(mod, "LISTING_URL")
