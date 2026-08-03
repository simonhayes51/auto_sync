"""Tests for the sales-sync payload mapping. Pure - no browser, no DB."""
from __future__ import annotations

import os, sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DATABASE_URL", "postgresql://x:y@localhost/z")

from futgg_sales_sync import (  # noqa: E402
    _parse_sold_at, build_sale_rows, sales_interval_minutes,
)

NOW = datetime(2026, 8, 3, 23, 0, tzinfo=timezone.utc)


class TestParseSoldAt:
    def test_iso_with_z(self):
        assert _parse_sold_at("2026-08-03T21:55:13.434330Z", NOW).tzinfo is not None

    def test_iso_with_offset(self):
        assert _parse_sold_at("2026-08-03T21:55:13+00:00", NOW) is not None

    def test_naive_iso_is_utc(self):
        assert _parse_sold_at("2026-08-03T21:55:13", NOW).tzinfo == timezone.utc

    def test_epoch_seconds_and_millis(self):
        secs = _parse_sold_at(1785794113, NOW)
        millis = _parse_sold_at(1785794113000, NOW)
        assert secs is not None and millis is not None
        assert abs((secs - millis).total_seconds()) < 1

    def test_garbage_returns_none(self):
        for bad in (None, "", "not a date", {}, []):
            assert _parse_sold_at(bad, NOW) is None


class TestBuildSaleRows:
    def _auctions(self, n=3):
        return [
            {"soldPrice": 100000 + i, "soldDate": f"2026-08-03T22:{50+i:02d}:00Z"}
            for i in range(n)
        ]

    def test_maps_price_and_tax(self):
        rows = build_sale_rows(555, self._auctions(1), NOW)
        assert len(rows) == 1
        card_id, listed, sold, tax, net = rows[0][:5]
        assert card_id == 555 and sold == 100000
        assert tax == 5000 and net == 95000  # EA's 5%

    def test_fingerprints_unique_per_sale(self):
        rows = build_sale_rows(555, self._auctions(3), NOW)
        assert len({r[10] for r in rows}) == 3

    def test_identical_sales_get_distinct_occurrence_and_fingerprint(self):
        same = [{"soldPrice": 100000, "soldDate": "2026-08-03T22:50:00Z"}] * 3
        rows = build_sale_rows(555, same, NOW)
        assert [r[9] for r in rows] == [1, 2, 3]
        assert len({r[10] for r in rows}) == 3

    def test_fingerprint_is_stable_across_runs(self):
        # Re-reading the same card must dedupe, not duplicate.
        a = build_sale_rows(555, self._auctions(2), NOW)
        b = build_sale_rows(555, self._auctions(2), NOW + __import__("datetime").timedelta(hours=1))
        assert [r[10] for r in a] == [r[10] for r in b]

    def test_different_cards_never_collide(self):
        a = build_sale_rows(1, self._auctions(2), NOW)
        b = build_sale_rows(2, self._auctions(2), NOW)
        assert not ({r[10] for r in a} & {r[10] for r in b})

    def test_skips_unusable_entries(self):
        rows = build_sale_rows(555, [
            {"soldPrice": None, "soldDate": "2026-08-03T22:50:00Z"},
            {"soldPrice": 0, "soldDate": "2026-08-03T22:50:00Z"},
            {"soldPrice": -5, "soldDate": "2026-08-03T22:50:00Z"},
            {"soldPrice": 1000, "soldDate": None},
            {"soldPrice": "abc", "soldDate": "2026-08-03T22:50:00Z"},
            "not a dict",
            {"soldPrice": 1000, "soldDate": "2026-08-03T22:50:00Z"},
        ], NOW)
        assert len(rows) == 1

    def test_age_seconds_is_never_negative(self):
        future = [{"soldPrice": 1000, "soldDate": "2027-01-01T00:00:00Z"}]
        assert build_sale_rows(555, future, NOW)[0][7] >= 0

    def test_accepts_alternate_field_names(self):
        assert len(build_sale_rows(555, [{"price": 1000, "date": "2026-08-03T22:50:00Z"}], NOW)) == 1

    def test_empty_input(self):
        assert build_sale_rows(555, [], NOW) == []


class TestIntervals:
    def test_rating_bands(self):
        assert sales_interval_minutes(91) < sales_interval_minutes(82)
        assert sales_interval_minutes(82) < sales_interval_minutes(77)
        assert sales_interval_minutes(77) < sales_interval_minutes(60)

    def test_none_rating_is_slowest(self):
        assert sales_interval_minutes(None) == sales_interval_minutes(50)


from futgg_sales_sync import AdaptiveThrottle  # noqa: E402


class TestAdaptiveThrottle:
    def test_backs_off_on_throttling(self):
        t = AdaptiveThrottle(batch_size=8, delay=0.5)
        t.record(attempted=8, throttled=8)
        assert t.delay > 0.5
        assert t.batch_size < 8

    def test_backoff_scales_with_severity(self):
        light = AdaptiveThrottle(8, 0.5); light.record(8, 1)
        heavy = AdaptiveThrottle(8, 0.5); heavy.record(8, 8)
        assert heavy.delay > light.delay

    def test_light_throttling_does_not_shrink_batch(self):
        t = AdaptiveThrottle(8, 0.5)
        t.record(attempted=8, throttled=1)   # 12.5% - under the 25% bar
        assert t.batch_size == 8
        assert t.delay > 0.5                  # but still slows down

    def test_recovers_only_after_sustained_success(self):
        t = AdaptiveThrottle(8, 0.5)
        t.record(8, 8)
        slowed, shrunk = t.delay, t.batch_size
        for _ in range(4):
            t.record(8, 0)
        assert t.delay == slowed and t.batch_size == shrunk  # not yet
        t.record(8, 0)                                        # fifth
        assert t.delay < slowed and t.batch_size > shrunk

    def test_never_exceeds_starting_limits(self):
        t = AdaptiveThrottle(8, 0.5)
        for _ in range(200):
            t.record(8, 0)
        assert t.batch_size <= 8
        assert t.delay >= 0.5

    def test_delay_is_bounded_under_relentless_throttling(self):
        t = AdaptiveThrottle(8, 0.5)
        for _ in range(500):
            t.record(8, 8)
        assert t.delay <= t.max_delay
        assert t.batch_size >= 1        # never stalls completely

    def test_zero_delay_start_still_backs_off(self):
        # A configured delay of 0 must not multiply to stay at 0.
        t = AdaptiveThrottle(8, 0.0)
        t.record(8, 8)
        assert t.delay > 0
