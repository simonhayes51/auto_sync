"""Low-overhead stage timing and log redaction helpers.

Used by futgg_price_sync.py (per-stage batch timings) and
futgg_discover.py (network diagnostics). Kept in its own module so both
can be unit-tested without importing the worker, which requires
DATABASE_URL at import time.

DESIGN CONSTRAINT: timing collection must not materially slow the worker.
Recording a sample is a float subtraction plus a list append (~100ns), and
nothing is formatted, sorted or aggregated until the batch ends. Sample
lists are capped so a long-lived process cannot grow them without bound -
count/total/max stay exact past the cap, only the percentiles degrade to
being computed over the retained window.
"""

from __future__ import annotations

import re
import time
from contextlib import contextmanager
from typing import Any, Iterator


# Past this many retained samples per stage, further samples stop being
# stored. Percentiles are then computed over the retained window while
# count/total/max remain exact across every sample.
MAX_SAMPLES_PER_STAGE = 5000


class StageTimers:
    """Collects elapsed times per named stage, aggregating only on demand."""

    __slots__ = ("_samples", "_count", "_total", "_max", "_overflow")

    def __init__(self) -> None:
        self._samples: dict[str, list[float]] = {}
        self._count: dict[str, int] = {}
        self._total: dict[str, float] = {}
        self._max: dict[str, float] = {}
        self._overflow: dict[str, int] = {}

    def record(self, stage: str, seconds: float) -> None:
        self._count[stage] = self._count.get(stage, 0) + 1
        self._total[stage] = self._total.get(stage, 0.0) + seconds
        if seconds > self._max.get(stage, 0.0):
            self._max[stage] = seconds

        bucket = self._samples.get(stage)
        if bucket is None:
            bucket = []
            self._samples[stage] = bucket
        if len(bucket) < MAX_SAMPLES_PER_STAGE:
            bucket.append(seconds)
        else:
            self._overflow[stage] = self._overflow.get(stage, 0) + 1

    @contextmanager
    def track(self, stage: str) -> Iterator[None]:
        """Time a block. Safe around `await` - it measures wall time from
        entry to exit, which for an awaited block is exactly the elapsed
        time that stage cost.

        The sample is recorded even when the block raises, so a stage that
        fails (a navigation timeout, say) still contributes its cost. A
        stage that only ever shows up in the fast path would otherwise
        make the slow path invisible.
        """
        started = time.perf_counter()
        try:
            yield
        finally:
            self.record(stage, time.perf_counter() - started)

    def stages(self) -> list[str]:
        return sorted(self._count)

    def count(self, stage: str) -> int:
        return self._count.get(stage, 0)

    def total_seconds(self, stage: str) -> float:
        return self._total.get(stage, 0.0)

    def summary(self) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for stage in self.stages():
            samples = sorted(self._samples.get(stage, []))
            count = self._count[stage]
            total = self._total[stage]
            out[stage] = {
                "count": count,
                "total_seconds": round(total, 3),
                "avg_ms": round((total / count) * 1000.0, 1) if count else 0.0,
                "p50_ms": _percentile_ms(samples, 0.50),
                "p95_ms": _percentile_ms(samples, 0.95),
                "max_ms": round(self._max.get(stage, 0.0) * 1000.0, 1),
                "sampled": len(samples),
                "unsampled": self._overflow.get(stage, 0),
            }
        return out

    def format_lines(self) -> list[str]:
        """One aligned line per stage, ordered by total time descending -
        so the stage that actually dominates the batch is the first thing
        read, rather than having to scan an alphabetical list."""
        summary = self.summary()
        ordered = sorted(
            summary.items(),
            key=lambda item: item[1]["total_seconds"],
            reverse=True,
        )
        lines = []
        for stage, s in ordered:
            lines.append(
                f"  {stage:<22} n={s['count']:<5} "
                f"total={s['total_seconds']:>8.2f}s "
                f"avg={s['avg_ms']:>8.1f}ms "
                f"p50={s['p50_ms']:>8.1f}ms "
                f"p95={s['p95_ms']:>8.1f}ms "
                f"max={s['max_ms']:>9.1f}ms"
            )
        return lines


class NullTimers:
    """Discarding sink with StageTimers' interface.

    Exists so instrumentation can be switched off without every call site
    growing a None check, and - more importantly - so a long-lived worker
    with timings disabled retains nothing. A disabled collector that still
    appended samples would be a slow memory leak introduced by the very
    code meant to diagnose performance.
    """

    __slots__ = ()

    def record(self, stage: str, seconds: float) -> None:
        return None

    @contextmanager
    def track(self, stage: str) -> Iterator[None]:
        yield

    def stages(self) -> list[str]:
        return []

    def count(self, stage: str) -> int:
        return 0

    def total_seconds(self, stage: str) -> float:
        return 0.0

    def summary(self) -> dict[str, dict[str, Any]]:
        return {}

    def format_lines(self) -> list[str]:
        return []


def _percentile_ms(sorted_samples: list[float], fraction: float) -> float:
    """Nearest-rank percentile, in milliseconds.

    Returns 0.0 for an empty sample rather than raising - a stage that
    never ran in a batch is a normal state (no failures recorded, for
    instance), not an error.
    """
    if not sorted_samples:
        return 0.0
    index = int(round(fraction * (len(sorted_samples) - 1)))
    index = max(0, min(index, len(sorted_samples) - 1))
    return round(sorted_samples[index] * 1000.0, 1)


# ---------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------
#
# The discovery diagnostic runs against a Cloudflare-protected origin and
# logs to Railway, which is shared, retained and not a secret store.
# Anything that could re-authenticate a session must never reach it.

_SENSITIVE_HEADER_MARKERS = (
    "cookie",
    "authorization",
    "auth",
    "token",
    "secret",
    "session",
    "csrf",
    "xsrf",
    "api-key",
    "apikey",
    "x-key",
    "signature",
    "bearer",
    "clearance",
)

# Header names safe to log verbatim - an explicit allow-list, because a
# deny-list silently leaks anything nobody thought of.
_SAFE_HEADER_NAMES = frozenset(
    {
        "accept",
        "accept-encoding",
        "accept-language",
        "cache-control",
        "content-type",
        "content-length",
        "origin",
        "pragma",
        "referer",
        "sec-fetch-dest",
        "sec-fetch-mode",
        "sec-fetch-site",
        "user-agent",
        "x-requested-with",
        "x-nextjs-data",
        "next-router-state-tree",
        "next-router-prefetch",
        "rsc",
    }
)

# Long opaque strings are treated as credential-shaped regardless of the
# key they sit under.
_TOKENISH = re.compile(r"^[A-Za-z0-9._\-+/=]{40,}$")


def is_sensitive_header(name: str) -> bool:
    lowered = name.lower()
    return any(marker in lowered for marker in _SENSITIVE_HEADER_MARKERS)


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Allow-list request headers for logging.

    Sensitive names are reported as present-but-redacted rather than
    dropped: knowing that a request carried an Authorization header is
    exactly the kind of thing the diagnostic exists to establish, while
    its value is exactly what must not be logged.
    """
    out: dict[str, str] = {}
    for name, value in (headers or {}).items():
        lowered = name.lower()
        if is_sensitive_header(lowered):
            out[lowered] = f"<redacted:{len(value or '')} chars>"
        elif lowered in _SAFE_HEADER_NAMES:
            out[lowered] = value
        else:
            # Unknown header: log the name, redact the value.
            out[lowered] = f"<omitted:{len(value or '')} chars>"
    return out


def redact_value(value: Any) -> Any:
    if isinstance(value, str):
        if _TOKENISH.match(value):
            return f"<token-like:{len(value)} chars>"
        if len(value) > 80:
            return value[:77] + "..."
        return value
    return value


def redact_structure(value: Any, *, depth: int = 0, max_depth: int = 3) -> Any:
    """Shape-preserving preview of a JSON body with values redacted.

    The goal is to answer "what does this endpoint return" without
    reproducing its contents: keys and types are preserved, arrays are
    summarised by length plus their first element, and long or
    credential-shaped strings are replaced.
    """
    if depth >= max_depth:
        if isinstance(value, dict):
            return f"<dict:{len(value)} keys>"
        if isinstance(value, list):
            return f"<list:{len(value)} items>"
        return redact_value(value)

    if isinstance(value, dict):
        preview: dict[str, Any] = {}
        for key in list(value)[:25]:
            if is_sensitive_header(str(key)):
                preview[str(key)] = "<redacted>"
            else:
                preview[str(key)] = redact_structure(
                    value[key], depth=depth + 1, max_depth=max_depth
                )
        if len(value) > 25:
            preview["<...>"] = f"{len(value) - 25} more keys"
        return preview

    if isinstance(value, list):
        if not value:
            return []
        return [
            redact_structure(value[0], depth=depth + 1, max_depth=max_depth),
            f"<+{len(value) - 1} more items>",
        ] if len(value) > 1 else [
            redact_structure(value[0], depth=depth + 1, max_depth=max_depth)
        ]

    return redact_value(value)
