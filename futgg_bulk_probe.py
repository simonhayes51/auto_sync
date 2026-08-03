"""Verify the FUT.GG bulk price feed - read-only, no writes, no worker impact.

WHAT DISCOVERY FOUND
--------------------
Two independent ways to get prices without rendering a page:

  1. Per-card, signed:
         POST /api/fut/price-access/sign/  {"url": "/api/fut/player-prices/26/<eaId>/?platform=ps5"}
         -> {"data": {"url": "...?verify=<token>", "challengeRequired": false, "expiresIn": 120}}
         GET  <signed url>  -> ~25 KB of everything (currentPrice, completedAuctions,
                               liveAuctions, history, priceRange, overview, momentum)
     Two requests per card, cookie-authenticated, token valid 120s.

  2. Bulk, static, unauthenticated:
         https://r2.fut.gg/26/<manifest>            -> content hashes + _published_at
         https://r2.fut.gg/26/player-prices-index.v1.<hash>.json
         https://r2.fut.gg/26/player-prices-ps5-dyn.v1.<hash>.json
     ~167 KB carrying ~27,085 prices in ONE request, served from R2 with no
     cookie header at all.

If (2) is real and covers our catalogue, it replaces ~27,000 page loads with a
single fetch, and the per-card endpoint becomes a targeted top-up for cards
needing sales history rather than the primary path.

This probe answers, with evidence and without changing anything:
  * does the bulk feed decode, and what is its exact encoding
  * how many ids does it carry, and how many of OUR cards does it cover
  * how fresh is it (_published_at vs now)
  * do its prices agree with what the scraper most recently stored

Run:  python futgg_bulk_probe.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("futgg_bulk_probe")

BASE = os.getenv("FUTGG_R2_BASE", "https://r2.fut.gg").rstrip("/")
GAME_YEAR = os.getenv("FUTGG_GAME_YEAR", "26")
PLATFORM = os.getenv("FUTGG_BULK_PLATFORM", "ps5")
DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)
HEADERS = {
    "user-agent": USER_AGENT,
    "accept": "*/*",
    "accept-language": "en-GB",
    "origin": "https://www.fut.gg",
    "referer": "https://www.fut.gg/",
}


async def fetch_json(client, url: str) -> tuple[Any, int, float]:
    started = time.perf_counter()
    response = await client.get(url, headers=HEADERS, timeout=30.0)
    elapsed = time.perf_counter() - started
    response.raise_for_status()
    return response.json(), len(response.content), elapsed


def decode_ids(index: dict[str, Any]) -> list[int]:
    """Decode the delta-encoded id list.

    Observed shape: {"v": 2, "id0": <first id>, "d": [<deltas>], ...}. The
    deltas are cumulative, so id[0] = id0 and id[i] = id[i-1] + d[i-1].
    This is a hypothesis from the discovery preview and is exactly what
    this probe exists to confirm - the cross-check against our own
    card ids below is what proves or refutes it.
    """
    id0 = index.get("id0")
    deltas = index.get("d") or []
    if id0 is None:
        return []
    ids = [int(id0)]
    running = int(id0)
    for delta in deltas:
        running += int(delta)
        ids.append(running)
    return ids


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        import httpx
    except ImportError:
        log.error("httpx is required: pip install httpx")
        return 1

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # ---- 1. Manifest -------------------------------------------------
        manifest_url = f"{BASE}/{GAME_YEAR}/manifest.json"
        log.info("Fetching manifest: %s", manifest_url)
        try:
            manifest, size, elapsed = await fetch_json(client, manifest_url)
        except Exception as exc:
            log.error(
                "Manifest fetch failed (%s). The manifest path is a guess - "
                "check the discovery log for the exact r2.fut.gg URL the page "
                "requested and set FUTGG_R2_BASE / adjust this path.", exc,
            )
            return 1
        log.info("  manifest %d B in %.2fs", size, elapsed)

        index_hash = manifest.get("player-prices-index")
        dyn_key = f"player-prices-{PLATFORM}-dyn"
        dyn_hash = manifest.get(dyn_key)
        published = (manifest.get("_published_at") or {}).get(dyn_key)

        log.info("  player-prices-index=%s  %s=%s", index_hash, dyn_key, dyn_hash)
        if published:
            age = time.time() - float(published)
            log.info(
                "  %s published %s (%.1f minutes ago)",
                dyn_key,
                datetime.fromtimestamp(float(published), timezone.utc).isoformat(),
                age / 60.0,
            )
            log.info("  >>> FEED AGE IS THE KEY NUMBER: it caps how fresh bulk prices can ever be.")

        if not index_hash or not dyn_hash:
            log.error("Manifest lacks the expected keys; got: %s", list(manifest)[:30])
            return 1

        # ---- 2. Index + prices ------------------------------------------
        index, isize, ielapsed = await fetch_json(
            client, f"{BASE}/{GAME_YEAR}/player-prices-index.v1.{index_hash}.json"
        )
        dyn, dsize, delapsed = await fetch_json(
            client, f"{BASE}/{GAME_YEAR}/{dyn_key}.v1.{dyn_hash}.json"
        )
        log.info("  index %d B in %.2fs | prices %d B in %.2fs", isize, ielapsed, dsize, delapsed)
        log.info("  index keys=%s  prices keys=%s", list(index)[:12], list(dyn)[:12])

        ids = decode_ids(index)
        prices = dyn.get("p") or []
        statuses = dyn.get("s") or []
        log.info(
            "  decoded ids=%d  prices=%d  statuses=%d  aligned=%s",
            len(ids), len(prices), len(statuses), len(ids) == len(prices),
        )
        if not ids or len(ids) != len(prices):
            log.error(
                "ENCODING HYPOTHESIS REFUTED - ids and prices do not align. "
                "Do not build on this until the encoding is understood. "
                "index sample keys=%s", list(index)[:12],
            )
            return 1

        by_id = dict(zip(ids, prices))
        log.info("  id range %d .. %d", min(ids), max(ids))
        log.info("  sample: %s", list(by_id.items())[:5])

        # ---- 3. Cross-check against our own catalogue --------------------
        if not DATABASE_URL:
            log.warning("DATABASE_URL unset - skipping coverage/accuracy check")
            return 0

        import asyncpg

        conn = await asyncpg.connect(DATABASE_URL)
        try:
            rows = await conn.fetch(
                """
                SELECT p.source_card_id, p.rating, b.lowest_bin, b.captured_at
                FROM futgg_players p
                LEFT JOIN LATERAL (
                    SELECT lowest_bin, captured_at
                    FROM futgg_bin_history h
                    WHERE h.source_card_id = p.source_card_id
                    ORDER BY captured_at DESC
                    LIMIT 1
                ) b ON TRUE
                WHERE p.is_active AND p.rating >= 85
                  AND p.is_tradeable IS DISTINCT FROM FALSE
                LIMIT 4000
                """
            )
        finally:
            await conn.close()

        if not rows:
            log.warning("No 85+ cards in futgg_players to check against")
            return 0

        covered = [r for r in rows if int(r["source_card_id"]) in by_id]
        log.info(
            "\nCOVERAGE: %d/%d of our 85+ cards present in the bulk feed (%.1f%%)",
            len(covered), len(rows), 100.0 * len(covered) / len(rows),
        )
        if not covered:
            log.error(
                "ZERO overlap - the feed is keyed on a DIFFERENT id space than "
                "source_card_id (likely base player eaId, not card eaId). The "
                "bulk path needs an id mapping before it is usable."
            )
            return 1

        # ---- 4. Do the numbers agree with what we scraped? ---------------
        comparable = [
            r for r in covered
            if r["lowest_bin"] is not None and by_id.get(int(r["source_card_id"]))
        ]
        if comparable:
            deltas = []
            for r in comparable:
                ours = float(r["lowest_bin"])
                theirs = float(by_id[int(r["source_card_id"])])
                if ours > 0:
                    deltas.append(abs(theirs - ours) / ours)
            deltas.sort()
            exact = sum(1 for d in deltas if d < 1e-9)
            within5 = sum(1 for d in deltas if d <= 0.05)
            log.info(
                "ACCURACY vs our last scraped BIN (n=%d): exact=%d (%.0f%%) "
                "within5%%=%d (%.0f%%) median_diff=%.2f%%",
                len(deltas), exact, 100.0 * exact / len(deltas),
                within5, 100.0 * within5 / len(deltas),
                deltas[len(deltas) // 2] * 100.0,
            )
            log.info(
                "  (differences are expected - our scrapes are minutes old and so "
                "is the feed. Large systematic gaps would mean a different metric.)"
            )
            for r in comparable[:8]:
                log.info(
                    "   card=%s rating=%s ours=%s theirs=%s captured=%s",
                    r["source_card_id"], r["rating"], r["lowest_bin"],
                    by_id[int(r["source_card_id"])], r["captured_at"],
                )

        log.info(
            "\nVERDICT: bulk feed decodes, covers %.1f%% of our 85+ catalogue, "
            "in ONE request of %d KB.", 100.0 * len(covered) / len(rows), dsize // 1024,
        )
        return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
