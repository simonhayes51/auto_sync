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

Uses aiohttp (already a pinned dependency) - this probe deliberately adds
no new package, since it is meant to be runnable on the existing image.

NEVER exits non-zero. It is run as a Railway service command, and a
non-zero exit there is a restart loop, not an error report.

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

# Captured verbatim from the discovery run. The browser fetched all three
# of these successfully, so they are known-good paths - the hashes are
# content-addressed and will go stale as FUT.GG republishes, which is why
# each is overridable.
CONTROL_URL_DEFAULT = "https://r2.fut.gg/26/config-web.v1.e939bf94.json"
INDEX_URL_DEFAULT = "https://r2.fut.gg/26/player-prices-index.v1.9df1dcb7.json"
DYN_URL_DEFAULT = "https://r2.fut.gg/26/player-prices-ps5-dyn.v1.7765c504.json"

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


async def fetch_json(session, url: str) -> tuple[Any, int, float]:
    started = time.perf_counter()
    async with session.get(url, headers=HEADERS) as response:
        raw = await response.read()
        elapsed = time.perf_counter() - started
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status} for {url}")
    return json.loads(raw), len(raw), elapsed


async def fetch_via_browser(urls: list[str]) -> dict[str, Any]:
    """Fetch through a real browser context instead of plain HTTP.

    If r2.fut.gg refuses plain requests (TLS fingerprint, IP reputation,
    WAF), this is the fallback that still avoids ALL rendering:
    context.request issues the HTTP call using the browser's own TLS stack
    and cookie jar, but never creates a page for it, never runs the app's
    JavaScript and never lays anything out. It is "a browser" only in the
    sense Cloudflare cares about.

    If this works where plain HTTP does not, the production design is to
    keep one lightweight Playwright context alive purely as transport and
    pull the bulk feed through it - still one request for ~27,000 prices,
    still zero page renders.
    """
    from playwright.async_api import async_playwright

    out: dict[str, Any] = {}
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        try:
            context = await browser.new_context(user_agent=USER_AGENT, locale="en-GB")
            # Establish origin + cookies the way a real visit would, so the
            # asset requests that follow look ordinary.
            page = await context.new_page()
            try:
                await page.goto(
                    "https://www.fut.gg/", wait_until="domcontentloaded", timeout=45000
                )
            except Exception:
                log.warning("  browser: homepage load failed; continuing anyway")
            for url in urls:
                started = time.perf_counter()
                try:
                    response = await context.request.get(url, timeout=45000)
                    elapsed = time.perf_counter() - started
                    body = await response.body()
                    if response.status != 200:
                        log.warning("  browser fetch %s -> HTTP %s", url, response.status)
                        continue
                    out[url] = json.loads(body)
                    log.info(
                        "  browser fetch OK: %d B in %.2fs  %s", len(body), elapsed, url
                    )
                except Exception as exc:
                    log.warning("  browser fetch failed %s (%s)", url, exc)
        finally:
            await browser.close()
    return out


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
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        # ---- 0. CONTROL ---------------------------------------------------
        # Every guessed manifest path returned 403, which is ambiguous: it
        # could mean the paths are wrong, or that plain HTTP requests are
        # refused regardless of path. Fetching a URL the browser is KNOWN
        # to have loaded successfully separates those two cases, and the
        # answer decides the whole architecture. Do this before anything
        # else - without it, every later failure is uninterpretable.
        control_url = os.getenv("FUTGG_CONTROL_URL", CONTROL_URL_DEFAULT).strip()
        control_ok = False
        if control_url:
            log.info("CONTROL fetch (a URL the browser definitely loaded): %s", control_url)
            try:
                _, csize, celapsed = await fetch_json(client, control_url)
                control_ok = True
                log.info("  CONTROL OK: %d B in %.2fs", csize, celapsed)
                log.info(
                    "  => plain HTTP to r2.fut.gg WORKS. Earlier 403s were wrong "
                    "paths, not blocking."
                )
            except Exception as exc:
                log.warning("  CONTROL FAILED: %s", exc)
                log.warning(
                    "  => r2.fut.gg refuses plain HTTP requests from here. The bulk "
                    "feed is not reachable without browser context (TLS "
                    "fingerprint, headers or IP reputation). This does NOT rule "
                    "the approach out - it means the fetch must happen through "
                    "the existing Playwright context (page.request / "
                    "context.request), which still skips all rendering."
                )

        # ---- 1. Locate the price files ------------------------------------
        # The manifest path was never captured, but the discovery log gives
        # the exact hashed URLs. Those are enough to validate the encoding
        # and coverage now; discovering the manifest matters only for
        # long-term freshness tracking, which is a separate problem.
        index_url = os.getenv("FUTGG_PRICES_INDEX_URL", "").strip()
        dyn_url = os.getenv("FUTGG_PRICES_DYN_URL", "").strip()
        dyn_key = f"player-prices-{PLATFORM}-dyn"

        manifest_url = os.getenv("FUTGG_MANIFEST_URL", "").strip()
        if manifest_url and not (index_url and dyn_url):
            try:
                manifest, size, elapsed = await fetch_json(client, manifest_url)
                log.info("Manifest OK: %s (%d B in %.2fs)", manifest_url, size, elapsed)
                index_hash = manifest.get("player-prices-index")
                dyn_hash = manifest.get(dyn_key)
                published = (manifest.get("_published_at") or {}).get(dyn_key)
                if published:
                    age = time.time() - float(published)
                    log.info(
                        "  %s published %s (%.1f min ago) - THIS CAPS BULK FRESHNESS",
                        dyn_key,
                        datetime.fromtimestamp(float(published), timezone.utc).isoformat(),
                        age / 60.0,
                    )
                if index_hash and dyn_hash:
                    index_url = f"{BASE}/{GAME_YEAR}/player-prices-index.v1.{index_hash}.json"
                    dyn_url = f"{BASE}/{GAME_YEAR}/{dyn_key}.v1.{dyn_hash}.json"
            except Exception as exc:
                log.warning("Manifest fetch failed (%s); falling back to explicit URLs", exc)

        if not index_url or not dyn_url:
            index_url = index_url or INDEX_URL_DEFAULT
            dyn_url = dyn_url or DYN_URL_DEFAULT
            log.info(
                "Using the hashed URLs captured by discovery. These hashes go stale "
                "as FUT.GG republishes - override with FUTGG_PRICES_INDEX_URL / "
                "FUTGG_PRICES_DYN_URL if they 404."
            )

        if not control_ok:
            log.warning(
                "Proceeding without a working control fetch - the following "
                "failures are most likely blocking rather than bad paths."
            )

        # ---- 2. Index + prices ------------------------------------------
        log.info("index: %s", index_url)
        log.info("dyn:   %s", dyn_url)
        index = dyn = None
        isize = dsize = 0
        transport = "plain-http"
        try:
            index, isize, ielapsed = await fetch_json(client, index_url)
            dyn, dsize, delapsed = await fetch_json(client, dyn_url)
            log.info("  index %d B in %.2fs | prices %d B in %.2fs",
                     isize, ielapsed, dsize, delapsed)
        except Exception as exc:
            log.warning("Plain HTTP fetch failed: %s", exc)
            if control_ok:
                log.error(
                    "  Control SUCCEEDED, so this is a stale hash rather than "
                    "blocking. Re-run discovery for current hashes and set "
                    "FUTGG_PRICES_INDEX_URL / FUTGG_PRICES_DYN_URL."
                )
                return 0

            # Control failed too - so the question is not "is the path
            # right" but "will anything other than a browser be served".
            # Answer it now rather than making this a second round trip.
            log.info("Retrying through a Playwright browser context...")
            try:
                fetched = await fetch_via_browser([index_url, dyn_url])
            except Exception:
                log.exception("  browser transport unavailable")
                fetched = {}

            index = fetched.get(index_url)
            dyn = fetched.get(dyn_url)
            transport = "browser-context"
            if index is None or dyn is None:
                log.error(
                    "BLOCKED on both transports. The bulk feed is not usable from "
                    "this host as-is. Remaining options, in order: (1) fetch the "
                    "feed inside the existing price worker's live browser context, "
                    "which already holds valid Cloudflare cookies; (2) use the "
                    "per-card signed endpoint through that same context; "
                    "(3) stay on rendering. Do not conclude the feed is unusable "
                    "in production from this result alone - the price worker's "
                    "context is more established than this probe's cold one."
                )
                return 0

        log.info("TRANSPORT THAT WORKED: %s", transport)
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
            return 0

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
            return 0

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
    # Always exit 0. This is run as a Railway service command, where a
    # non-zero exit is a restart loop rather than an error report - the
    # same mistake that took the price worker down earlier. An unexpected
    # exception is logged in full and the process still exits cleanly.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception:
        log.exception("bulk probe failed")
    sys.exit(0)
