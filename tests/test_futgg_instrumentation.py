"""Tests for the stage-timing collector and log redaction helpers.

Pure - no browser, no database, no worker import (futgg_price_sync
requires DATABASE_URL at import time).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from futgg_instrumentation import (  # noqa: E402
    MAX_SAMPLES_PER_STAGE,
    NullTimers,
    StageTimers,
    is_sensitive_header,
    redact_headers,
    redact_structure,
    redact_value,
)


class TestStageTimers:
    def test_records_count_total_and_max(self):
        timers = StageTimers()
        for seconds in (0.1, 0.2, 0.3):
            timers.record("page_goto", seconds)

        summary = timers.summary()["page_goto"]
        assert summary["count"] == 3
        assert abs(summary["total_seconds"] - 0.6) < 1e-6
        assert abs(summary["avg_ms"] - 200.0) < 0.5
        assert abs(summary["max_ms"] - 300.0) < 0.5

    def test_percentiles(self):
        timers = StageTimers()
        for i in range(1, 101):
            timers.record("stage", i / 1000.0)
        summary = timers.summary()["stage"]
        assert 45.0 <= summary["p50_ms"] <= 55.0
        assert 93.0 <= summary["p95_ms"] <= 97.0
        assert abs(summary["max_ms"] - 100.0) < 0.5

    def test_track_context_manager_measures_elapsed(self):
        import time as _time

        timers = StageTimers()
        with timers.track("work"):
            _time.sleep(0.01)
        assert timers.count("work") == 1
        assert timers.summary()["work"]["avg_ms"] >= 9.0

    def test_track_records_even_when_the_block_raises(self):
        # A stage that fails still cost time. If failures were dropped, a
        # navigation that times out at 45s would be invisible and the
        # averages would describe only the happy path.
        timers = StageTimers()
        try:
            with timers.track("page_goto"):
                raise RuntimeError("navigation timeout")
        except RuntimeError:
            pass
        assert timers.count("page_goto") == 1

    def test_empty_stage_never_raises(self):
        assert StageTimers().summary() == {}
        assert StageTimers().format_lines() == []

    def test_sample_retention_is_bounded_but_totals_stay_exact(self):
        timers = StageTimers()
        overshoot = MAX_SAMPLES_PER_STAGE + 250
        for _ in range(overshoot):
            timers.record("stage", 0.001)
        summary = timers.summary()["stage"]
        assert summary["count"] == overshoot          # exact
        assert summary["sampled"] == MAX_SAMPLES_PER_STAGE
        assert summary["unsampled"] == 250

    def test_format_lines_orders_by_total_time_descending(self):
        timers = StageTimers()
        timers.record("cheap", 0.01)
        timers.record("expensive", 5.0)
        timers.record("middling", 1.0)
        lines = timers.format_lines()
        assert "expensive" in lines[0]
        assert "cheap" in lines[-1]


class TestNullTimers:
    def test_interface_matches_and_discards(self):
        null = NullTimers()
        with null.track("anything"):
            pass
        null.record("anything", 1.0)
        assert null.summary() == {}
        assert null.format_lines() == []
        assert null.count("anything") == 0

    def test_retains_nothing_under_sustained_use(self):
        # The disabled path must not become the memory leak that the
        # performance instrumentation introduced.
        null = NullTimers()
        for _ in range(100_000):
            null.record("stage", 0.001)
        assert null.summary() == {}
        assert not hasattr(null, "__dict__")  # __slots__, no per-instance state


class TestRedaction:
    def test_sensitive_header_names_detected(self):
        for name in (
            "cookie", "Set-Cookie", "authorization", "x-auth-token",
            "cf_clearance", "x-api-key", "x-csrf-token", "session-id",
        ):
            assert is_sensitive_header(name), name

    def test_sensitive_headers_are_redacted_but_reported_as_present(self):
        out = redact_headers({"Cookie": "cf_clearance=abc123", "Accept": "application/json"})
        assert "cf_clearance" not in out["cookie"]
        assert "abc123" not in out["cookie"]
        assert out["cookie"].startswith("<redacted:")
        # Knowing the header was present is the diagnostic's whole point.
        assert "cookie" in out
        assert out["accept"] == "application/json"

    def test_unknown_headers_keep_name_but_lose_value(self):
        out = redact_headers({"X-Weird-Internal": "some-value"})
        assert out["x-weird-internal"].startswith("<omitted:")
        assert "some-value" not in out["x-weird-internal"]

    def test_token_like_strings_are_replaced(self):
        token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abcdefghijklmnop.signature123456"
        assert "token-like" in redact_value(token)
        assert redact_value("gold_rare") == "gold_rare"

    def test_structure_preview_keeps_shape_and_drops_values(self):
        payload = {
            "lowestBin": 125000,
            "auth": {"cf_clearance": "secret-value-here"},
            "sales": [{"price": 120000, "age": "1m"}, {"price": 121000, "age": "2m"}],
        }
        preview = redact_structure(payload)
        assert preview["lowestBin"] == 125000            # numbers survive
        assert preview["auth"] == "<redacted>"           # sensitive key pruned
        assert isinstance(preview["sales"], list)
        assert "+1 more items" in preview["sales"][1]

    def test_structure_preview_is_depth_limited(self):
        deep = {"a": {"b": {"c": {"d": {"e": 1}}}}}
        preview = redact_structure(deep, max_depth=2)
        assert "dict" in str(preview["a"]["b"])

    def test_no_secret_survives_a_realistic_header_set(self):
        headers = {
            "Cookie": "cf_clearance=SECRET_A; __cf_bm=SECRET_B",
            "Authorization": "Bearer SECRET_C",
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.fut.gg/players/26-1-1/",
        }
        rendered = str(redact_headers(headers))
        for secret in ("SECRET_A", "SECRET_B", "SECRET_C"):
            assert secret not in rendered
        assert "Mozilla/5.0" in rendered
