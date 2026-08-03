"""
Tests for the price-outcome classification added to futgg_common.py and
the circuit breaker added to futgg_price_sync.py - see the FUT.GG
migration plan section 5.6/5.7. These are pure-function/pure-class tests,
no browser or DB needed.

The "hot opportunity" fast-track tests at the bottom of this file DO need
a real Postgres (record_success() executes real SQL) - they're skipped
automatically if FUTGG_TEST_DATABASE_URL isn't set, same opt-in pattern
as other DB-touching tests in this repo.
"""
import asyncio
import functools
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from futgg_common import FutggCard, SaleObservation, detect_price_outcome
import futgg_price_sync as price_sync
from futgg_price_sync import CircuitBreaker, _is_hot_opportunity


def _card(**overrides) -> FutggCard:
    base = dict(source_card_id=1, source_player_id=2, source_slug="x", game_year=26, source_url="u")
    base.update(overrides)
    return FutggCard(**base)


def test_success_when_lowest_bin_present():
    soup = BeautifulSoup("<div id='prices-overview'></div>", "html.parser")
    card = _card(lowest_bin=12000)
    assert detect_price_outcome(soup, card) == "success"


def test_success_when_only_sales_present():
    soup = BeautifulSoup("<div id='prices-overview'></div>", "html.parser")
    from futgg_common import SaleObservation
    from datetime import datetime, timezone
    card = _card()
    card.recent_sales.append(
        SaleObservation("1 minute ago", 60, 5000, datetime.now(timezone.utc), 1, 1, "fp")
    )
    assert detect_price_outcome(soup, card) == "success"


def test_untradeable_marker_detected():
    soup = BeautifulSoup(
        "<div id='prices-overview'>This item is Not Tradeable on the transfer market.</div>",
        "html.parser",
    )
    card = _card()
    assert detect_price_outcome(soup, card) == "untradeable"


def test_price_section_missing_when_no_overview_node():
    soup = BeautifulSoup("<div>Some other content</div>", "html.parser")
    card = _card()
    assert detect_price_outcome(soup, card) == "price_section_missing"


def test_no_active_market_when_overview_present_but_empty():
    soup = BeautifulSoup("<div id='prices-overview'></div>", "html.parser")
    card = _card()
    assert detect_price_outcome(soup, card) == "no_active_market"


def test_circuit_breaker_trips_after_threshold_consecutive_failures():
    breaker = CircuitBreaker(threshold=3)
    breaker.record_failure("HTTP 429")
    assert not breaker.tripped
    breaker.record_failure("HTTP 429")
    assert not breaker.tripped
    breaker.record_failure("HTTP 403")
    assert breaker.tripped
    assert breaker.trip_reason == "HTTP 403"


def test_circuit_breaker_resets_streak_on_success():
    breaker = CircuitBreaker(threshold=2)
    breaker.record_failure("HTTP 429")
    breaker.record_success()
    breaker.record_failure("HTTP 429")
    assert not breaker.tripped


def _sale(price: int, index: int = 1) -> SaleObservation:
    return SaleObservation(
        age_text=f"{index} minutes ago", age_seconds=index * 60, sold_price=price,
        approximate_sold_at=datetime.now(timezone.utc), row_position=index,
        occurrence_index=1, fingerprint=f"fp-{price}-{index}",
    )


class TestHotOpportunityDetection:
    def test_not_hot_when_too_few_sales(self):
        card = _card(lowest_bin=8000)
        card.recent_sales = [_sale(10000, i) for i in range(4)]  # below HOT_MIN_SALES=5
        assert _is_hot_opportunity(card) is False

    def test_not_hot_when_bin_close_to_median(self):
        card = _card(lowest_bin=9800)
        card.recent_sales = [_sale(10000, i) for i in range(6)]  # 2% below median
        assert _is_hot_opportunity(card) is False

    def test_hot_when_bin_well_below_median(self):
        card = _card(lowest_bin=8500)
        card.recent_sales = [_sale(10000, i) for i in range(6)]  # 15% below median
        assert _is_hot_opportunity(card) is True

    def test_not_hot_when_no_bin(self):
        card = _card(lowest_bin=None)
        card.recent_sales = [_sale(10000, i) for i in range(6)]
        assert _is_hot_opportunity(card) is False


def run_async(coro_fn):
    @functools.wraps(coro_fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(coro_fn(*args, **kwargs))
    return wrapper


TEST_DSN = os.getenv("FUTGG_TEST_DATABASE_URL")
requires_db = pytest.mark.skipif(not TEST_DSN, reason="FUTGG_TEST_DATABASE_URL not set")


@requires_db
class TestRecordSuccessHotFastTrack:
    """record_success() end-to-end against a real Postgres: a hot
    opportunity must get next_price_due_at shrunk to HOT_INTERVAL_MIN
    regardless of its tier's normal (much longer) interval, and a normal
    card must keep its tier's ordinary interval untouched."""

    @run_async
    async def test_hot_card_gets_short_interval_despite_bronze_tier(self):
        import asyncpg
        conn = await asyncpg.connect(TEST_DSN)
        try:
            await conn.execute("DROP TABLE IF EXISTS futgg_sales_history, futgg_bin_history, futgg_players CASCADE")
            from futgg_player_sync import ensure_schema as ensure_player_schema
            await ensure_player_schema(conn)
            await price_sync.ensure_schema(conn)
            await conn.execute(
                """
                INSERT INTO futgg_players (source_card_id, source_player_id, source_slug, source_url, game_year, price_tier)
                VALUES (1, 1, 'x', 'u', 26, 'bronze')
                """
            )
            card = _card(source_card_id=1, lowest_bin=8000, price_outcome="success")
            card.recent_sales = [_sale(10000, i) for i in range(6)]  # 20% below median -> hot
            row = {"price_tier": "bronze"}
            captured_at = datetime.now(timezone.utc)

            _, _, _, is_hot = await price_sync.record_success(conn, row, card, captured_at)
            assert is_hot is True

            due = await conn.fetchval("SELECT next_price_due_at FROM futgg_players WHERE source_card_id = 1")
            minutes_ahead = (due - captured_at).total_seconds() / 60
            assert minutes_ahead <= price_sync.HOT_INTERVAL_MIN + 0.1
            # Bronze's normal interval (4320 min) must NOT have been used.
            assert minutes_ahead < price_sync.INTERVALS["bronze"]
        finally:
            await conn.close()

    @run_async
    async def test_normal_card_keeps_tier_interval(self):
        import asyncpg
        conn = await asyncpg.connect(TEST_DSN)
        try:
            await conn.execute("DROP TABLE IF EXISTS futgg_sales_history, futgg_bin_history, futgg_players CASCADE")
            from futgg_player_sync import ensure_schema as ensure_player_schema
            await ensure_player_schema(conn)
            await price_sync.ensure_schema(conn)
            await conn.execute(
                """
                INSERT INTO futgg_players (source_card_id, source_player_id, source_slug, source_url, game_year, price_tier)
                VALUES (2, 2, 'x', 'u', 26, 'special')
                """
            )
            card = _card(source_card_id=2, lowest_bin=9900, price_outcome="success")
            card.recent_sales = [_sale(10000, i) for i in range(6)]  # 1% below median -> not hot
            row = {"price_tier": "special"}
            captured_at = datetime.now(timezone.utc)

            _, _, _, is_hot = await price_sync.record_success(conn, row, card, captured_at)
            assert is_hot is False

            due = await conn.fetchval("SELECT next_price_due_at FROM futgg_players WHERE source_card_id = 2")
            minutes_ahead = (due - captured_at).total_seconds() / 60
            assert abs(minutes_ahead - price_sync.INTERVALS["special"]) < 0.1
        finally:
            await conn.close()
