"""
Tests for the price-outcome classification added to futgg_common.py and
the circuit breaker added to futgg_price_sync.py - see the FUT.GG
migration plan section 5.6/5.7. These are pure-function/pure-class tests,
no browser or DB needed.
"""
import os
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from futgg_common import FutggCard, detect_price_outcome
from futgg_price_sync import CircuitBreaker


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
