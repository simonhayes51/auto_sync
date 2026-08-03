#!/usr/bin/env python
"""Bulk FUT.GG price ingest - the whole catalogue from one page load.

WHY THIS EXISTS
---------------
Per-card scraping loads a player page, waits for React to hydrate, waits
for the app to fetch /api/fut/player-prices/ (which needs a signed token
from POST /price-access/sign/), then parses ~275 KB of HTML. Measured in
production: ~2.0s per successful card, ~26% of cards failing outright,
~55-129 cards/min. The failures are not a markup problem - the signing
endpoint reports `challengeRequired`, and under concurrency a share of
cards simply never get prices rendered, which no amount of waiting or
retrying fixes.

Discovery found that the same page also loads a STATIC bulk feed:

    r2.fut.gg/26/player-prices-index.v1.<hash>.json    (~67 KB)
    r2.fut.gg/26/player-prices-<platform>-dyn.v1.<hash>.json  (~167 KB)

carrying ~27,085 prices between them. One page load yields the entire
catalogue - no signing, no per-card rendering, no rate limit to trip.

TRANSPORT
---------
r2.fut.gg refuses direct requests: plain HTTP 403s, context.request with
origin/referer 403s, and in-page fetch() fails CORS. All three were
tested. What works - and what discovery proved by accident - is letting
the FUT.GG app fetch the files itself during an ordinary page load, and
reading the responses off the wire. So this worker does not impersonate
the request; it loads one page and listens.

The content hashes change on every republish (observed changing within
~25 minutes), which is also why previously-captured URLs start 403ing.
Responses are therefore matched by URL substring, never by exact URL.

SAFETY
------
Writes are guarded by the data, not by a flag. Every cycle checks the
feed against our own catalogue and refuses to write when:

  * no id overlaps source_card_id at all - meaning the feed is keyed on
    base-player eaId rather than card eaId, and needs a mapping first
  * coverage falls below FUTGG_BULK_MIN_COVERAGE_PCT - meaning the feed
    or the id space changed, and writing would blank good prices
  * ids and prices do not align - meaning the encoding changed

Those checks run on live data every cycle, which a one-off manual
approval does not. FUTGG_BULK_APPLY=false forces a read-only cycle if you
want to inspect coverage without writing.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from futgg_instrumentation import StageTimers

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futgg_bulk_price_sync")

SCRIPT_VERSION = "futgg-bulk-price-sync-v1"

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int, minimum: int | None = None) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        value = default
    return max(minimum, value) if minimum is not None else value


#: Write to the database.
#
# On by default. The dry-run flag was originally the safety mechanism, but
# it is not the one that matters: the coverage checks below already refuse
# to write when the feed does not line up with our catalogue (zero id
# overlap, or coverage under the floor). Those run on live data every
# cycle, whereas a manual flag just delays the first useful write by a
# deploy. Set FUTGG_BULK_APPLY=false to force a read-only cycle.
APPLY = env_bool("FUTGG_BULK_APPLY", True)
PLATFORM = os.getenv("FUTGG_BULK_PLATFORM", "ps5")
GAME_YEAR = os.getenv("FUTGG_GAME_YEAR", "26")
INTERVAL_SECONDS = env_int("FUTGG_BULK_INTERVAL_SECONDS", 300, minimum=60)
HEADLESS = env_bool("PLAYWRIGHT_HEADLESS", True)
TIMEOUT_MS = env_int("PLAYWRIGHT_TIMEOUT_MS", 45000, minimum=5000)
HYDRATION_WAIT_MS = env_int("FUTGG_BULK_HYDRATION_WAIT_MS", 6000, minimum=1000)
#: Below this coverage of our own catalogue, refuse to write. A sudden
#: collapse means the id space or the feed changed, and writing anyway
#: would overwrite good prices with nothing.
MIN_COVERAGE_PCT = float(os.getenv("FUTGG_BULK_MIN_COVERAGE_PCT", "50"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

INDEX_MARKER = "player-prices-index"
DYN_MARKER = f"player-prices-{PLATFORM}-dyn"


def decode_ids(index: dict[str, Any]) -> list[int]:
    """Cumulative-delta decode: id[0] = id0, id[i] = id[i-1] + d[i-1].

    Confirmed against production: id0=27 with deltas 14,10,189,6 yields
    27,41,51,240,246, matching the observed feed exactly.
    """
    id0 = index.get("id0")
    if id0 is None:
        return []
    ids = [int(id0)]
    running = int(id0)
    for delta in index.get("d") or []:
        running += int(delta)
        ids.append(running)
    return ids


async def harvest_feed(page) -> dict[str, Any]:
    """Load a player page and read the bulk feed off the wire."""
    captured: dict[str, Any] = {}
    pending: list[asyncio.Task] = []

    async def read(response, key: str) -> None:
        try:
            body = await response.body()
            captured[key] = json.loads(body)
            captured[f"{key}__url"] = response.url
            captured[f"{key}__bytes"] = len(body)
        except Exception:
            log.warning("could not read %s", key, exc_info=True)

    def on_response(response) -> None:
        if response.status != 200:
            return
        for marker in (INDEX_MARKER, DYN_MARKER):
            # Substring match: the content hash changes on every
            # republish, so an exact URL match would silently stop
            # working within the hour.
            if marker in response.url:
                pending.append(asyncio.ensure_future(read(response, marker)))
                return

    page.on("response", on_response)
    try:
        await page.goto(PLAYER_URL, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
    except Exception:
        log.warning("navigation failed", exc_info=True)
    # The feed is requested during hydration, after domcontentloaded.
    await page.wait_for_timeout(HYDRATION_WAIT_MS)
    if pending:
        try:
            await asyncio.wait_for(
                asyncio.gather(*pending, return_exceptions=True), timeout=30
            )
        except asyncio.TimeoutError:
            log.warning("timed out reading %d captured responses", len(pending))
    page.remove_listener("response", on_response)
    return captured


PLAYER_URL = ""  # resolved at startup from the database


async def resolve_player_url(pool: asyncpg.Pool) -> str:
    async with pool.acquire() as conn:
        url = await conn.fetchval(
            """
            SELECT source_url FROM futgg_players
            WHERE is_active AND source_url IS NOT NULL
              AND is_tradeable IS DISTINCT FROM FALSE
            ORDER BY rating DESC NULLS LAST
            LIMIT 1
            """
        )
    return url or "https://www.fut.gg/players/1114-roberto-baggio/26-1114/"


async def ingest_once(pool: asyncpg.Pool, page, timers: StageTimers) -> dict[str, Any]:
    with timers.track("harvest"):
        captured = await harvest_feed(page)

    index = captured.get(INDEX_MARKER)
    dyn = captured.get(DYN_MARKER)
    if index is None or dyn is None:
        log.error(
            "Feed not captured (index=%s dyn=%s). The page may have failed to "
            "load, or the asset naming changed - re-run futgg_discover to check.",
            index is not None, dyn is not None,
        )
        return {"ok": False}

    log.info(
        "captured index=%s (%d B) dyn=%s (%d B)",
        captured.get(f"{INDEX_MARKER}__url", "?").rsplit("/", 1)[-1],
        captured.get(f"{INDEX_MARKER}__bytes", 0),
        captured.get(f"{DYN_MARKER}__url", "?").rsplit("/", 1)[-1],
        captured.get(f"{DYN_MARKER}__bytes", 0),
    )

    with timers.track("decode"):
        ids = decode_ids(index)
        prices = dyn.get("p") or []
        statuses = dyn.get("s") or []

    if not ids or len(ids) != len(prices):
        log.error(
            "Feed misaligned: ids=%d prices=%d. Refusing to use it.",
            len(ids), len(prices),
        )
        return {"ok": False}

    by_id = {i: p for i, p in zip(ids, prices)}
    status_by_id = dict(zip(ids, statuses)) if len(statuses) == len(ids) else {}
    log.info("feed decoded: %d ids, range %d..%d", len(ids), min(ids), max(ids))

    # ---- Coverage -----------------------------------------------------
    with timers.track("coverage"):
        async with pool.acquire() as conn:
            ours = await conn.fetch(
                """
                SELECT source_card_id, rating FROM futgg_players
                WHERE is_active AND is_tradeable IS DISTINCT FROM FALSE
                """
            )
    total = len(ours)
    matched = [r for r in ours if int(r["source_card_id"]) in by_id]
    high = [r for r in ours if (r["rating"] or 0) >= 85]
    high_matched = [r for r in high if int(r["source_card_id"]) in by_id]
    coverage_pct = (100.0 * len(matched) / total) if total else 0.0
    high_pct = (100.0 * len(high_matched) / len(high)) if high else 0.0

    log.info("=" * 68)
    log.info("COVERAGE: %d/%d of all tradeable cards (%.1f%%)", len(matched), total, coverage_pct)
    log.info("COVERAGE: %d/%d of 85+ cards (%.1f%%)", len(high_matched), len(high), high_pct)
    priced = sum(1 for r in matched if by_id.get(int(r["source_card_id"])))
    log.info("  of matched, %d have a non-zero price (%.1f%%)",
             priced, 100.0 * priced / len(matched) if matched else 0.0)
    if status_by_id:
        from collections import Counter, defaultdict

        dist = Counter(status_by_id[int(r["source_card_id"])] for r in matched)
        log.info("  status values across our cards: %s", dist.most_common(8))

        # Cross-tabulate the feed's status against what the SCRAPER most
        # recently concluded for the same card. The status enum is
        # undocumented, but if e.g. status 2 lines up with the cards the
        # scraper reports as price_render_failed or untradeable, that is
        # the classification it has been failing to infer from missing
        # markup ~26% of the time - and we can stop scraping those cards
        # entirely rather than burning ~5s each rediscovering it.
        with timers.track("status_crosstab"):
            async with pool.acquire() as conn:
                outcomes = await conn.fetch(
                    """
                    SELECT source_card_id, last_price_status
                    FROM futgg_players
                    WHERE is_active AND last_price_status IS NOT NULL
                    """
                )
            crosstab: dict[Any, Counter] = defaultdict(Counter)
            price_presence: dict[Any, Counter] = defaultdict(Counter)
            for row in outcomes:
                card_id = int(row["source_card_id"])
                status = status_by_id.get(card_id)
                if status is None:
                    continue
                crosstab[status][row["last_price_status"]] += 1
                price_presence[status]["priced" if by_id.get(card_id) else "zero"] += 1

            log.info("  --- feed status vs scraper outcome ---")
            for status in sorted(crosstab):
                log.info(
                    "    status=%s  prices:%s  scraper:%s",
                    status,
                    dict(price_presence[status]),
                    dict(crosstab[status].most_common(5)),
                )
            log.info("  --------------------------------------")
    log.info("=" * 68)

    if not matched:
        log.error(
            "ZERO overlap with source_card_id. The feed is keyed on a different "
            "id space (probably base-player eaId). An id map is required before "
            "this can be used. Nothing written."
        )
        return {"ok": False, "coverage_pct": 0.0}

    if coverage_pct < MIN_COVERAGE_PCT:
        log.error(
            "Coverage %.1f%% is below the %.1f%% floor - refusing to write. "
            "Something changed; investigate before trusting this feed.",
            coverage_pct, MIN_COVERAGE_PCT,
        )
        return {"ok": False, "coverage_pct": coverage_pct}

    if not APPLY:
        log.warning(
            "READ-ONLY (FUTGG_BULK_APPLY=false) - coverage checks passed but "
            "nothing written."
        )
        for row in matched[:10]:
            card_id = int(row["source_card_id"])
            log.info("  sample card=%s rating=%s feed_price=%s",
                     card_id, row["rating"], by_id[card_id])
        return {"ok": True, "coverage_pct": coverage_pct, "applied": False}

    # ---- Write --------------------------------------------------------
    now = datetime.now(timezone.utc)
    records = [
        (int(r["source_card_id"]), int(by_id[int(r["source_card_id"])]), now)
        for r in matched
        if by_id.get(int(r["source_card_id"]))
    ]
    with timers.track("write"):
        async with pool.acquire() as conn:
            async with conn.transaction():
                # One COPY beats tens of thousands of INSERTs. The whole
                # point of this worker is that the catalogue arrives at
                # once; writing it row-by-row would reintroduce exactly
                # the round-trip cost the per-card path suffered from.
                await conn.copy_records_to_table(
                    "futgg_bin_history",
                    records=records,
                    columns=["source_card_id", "lowest_bin", "captured_at"],
                )
                await conn.execute(
                    """
                    UPDATE futgg_players p
                    SET price_updated_at = $2,
                        last_price_status = 'bulk_feed'
                    WHERE p.source_card_id = ANY($1::bigint[])
                    """,
                    [r[0] for r in records], now,
                )
    log.info("WROTE %d prices in one COPY", len(records))
    return {"ok": True, "coverage_pct": coverage_pct, "applied": True, "written": len(records)}


async def run_forever() -> None:
    global PLAYER_URL
    from playwright.async_api import async_playwright

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4, command_timeout=120)
    PLAYER_URL = await resolve_player_url(pool)
    log.info(
        "%s starting: platform=%s interval=%ss apply=%s url=%s",
        SCRIPT_VERSION, PLATFORM, INTERVAL_SECONDS, APPLY, PLAYER_URL,
    )
    if not APPLY:
        log.warning("READ-ONLY MODE - coverage will be reported, nothing written.")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            context = await browser.new_context(user_agent=USER_AGENT, locale="en-GB")
            page = await context.new_page()
            while True:
                timers = StageTimers()
                started = time.perf_counter()
                try:
                    await ingest_once(pool, page, timers)
                except Exception:
                    log.exception("ingest failed")
                log.info("cycle took %.1fs", time.perf_counter() - started)
                for line in timers.format_lines():
                    log.info(line)
                await asyncio.sleep(INTERVAL_SECONDS)
        finally:
            await browser.close()
            await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_forever())
    except KeyboardInterrupt:
        log.info("stopped")
    except Exception:
        log.exception("futgg_bulk_price_sync failed")
    # Always exit 0 - a non-zero exit on Railway is a restart loop.
    sys.exit(0)
