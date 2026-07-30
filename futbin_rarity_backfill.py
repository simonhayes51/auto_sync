"""
Incrementally fills fut_players.rarity (backend migration
025_fut_players_rarity.sql) - the card TYPE (Bronze/Silver/Gold Common/
Gold Rare/etc), a different concept from fut_players.version (card
EDITION - Normal/TOTW/TOTS/Icon/etc promos, see bin_sales_history_sync.py's
own docstring). rarity already exists as a column but has never been
populated by anything: futbin_full_sync.py's listing-page crawl never
sees this value at all - it's only shown on the individual player page's
Nation/League/Club/Card-type info row, a link to
/26/players?version=<slug> (futbin's own query-param name, unrelated to
this schema's version column - pure naming coincidence) with a visible
text label like "Gold Rare".

Deliberately a separate script from futbin_card_art_backfill.py rather
than folded into it - that script's one job is generating card art
(card_bg_image/card_cutout_image/card_cutout_type/card_name), and this
one's one job is rarity. Both happen to need one GET per card against
the same player_url, but that's the only thing they share; keeping them
separate means either can be scheduled, tuned, or disabled independently
without touching the other's proven, already-deployed logic.

One-shot-per-invocation Cron Job design, same as futbin_card_art_backfill.py
and futbin_sbc_sync.py - not a permanent worker, no Procfile entry yet.
Do one supervised manual run and read the heartbeat's http_429/http_exc
counts before scheduling this as a real Cron Job, same discipline as
every other worker in this repo (see README.md's "Card art backfill"
section for why that matters for this exact fetch pattern - up to
BATCH_SIZE individual player-page GETs back-to-back).

A per-card failure is logged and skipped; it never aborts the run for
the rest of the batch, and a failed card is simply retried on the next
invocation (rarity stays NULL until a fetch actually succeeds and
actually finds the card-type link).
"""
import os
import re
import random
import asyncio
import logging
from typing import Any, Dict, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futbin_rarity_backfill")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

BATCH_SIZE = int(os.getenv("RARITY_BATCH_SIZE", "300"))
REQUEST_DELAY = float(os.getenv("RARITY_REQUEST_DELAY", "1.5"))
MAX_RETRIES = int(os.getenv("RARITY_MAX_RETRIES", "3"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}


def _jittered_delay() -> float:
    return random.uniform(REQUEST_DELAY * 0.6, REQUEST_DELAY * 1.6)


async def _get_with_retry(session: aiohttp.ClientSession, url: str, diag: Dict[str, Any]) -> "tuple[int, Optional[str]]":
    """GET with 429-aware backoff retry - same shape as
    bin_sales_history_sync.py's/futbin_card_art_backfill.py's own helper
    (see those files' comments for why a 429 here is worth retrying
    rather than counting as a hard fail)."""
    backoff = 1.0
    for attempt in range(MAX_RETRIES + 1):
        try:
            async with session.get(url, headers=HEADERS, timeout=HTTP_TIMEOUT) as r:
                if r.status == 429:
                    diag["http_429_hits"] += 1
                    if attempt < MAX_RETRIES:
                        retry_after = r.headers.get("Retry-After")
                        wait = float(retry_after) if retry_after and retry_after.replace(".", "", 1).isdigit() else backoff
                        await asyncio.sleep(wait)
                        backoff *= 2
                        continue
                    return 429, None
                if r.status != 200:
                    return r.status, None
                return 200, await r.text()
        except Exception:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            diag["http_exceptions"] += 1
            return 0, None
    return 0, None


# The player page's Nation/League/Club/Card-type info row - the last item
# is the card's TYPE (e.g. "Gold Rare"), a link to
# /26/players?version=gold_rare with a visible <span class="text-ellipsis">
# label. futbin's own query-param happens to be named "version" here -
# pure naming coincidence with this schema's unrelated fut_players.version
# column (card EDITION), not the same concept.
#
# FLAGGED: matches the first such link found on the page. The sampled
# markup this was built from didn't show a class scoping this info row
# to a single "hero" section, so if futbin ever renders more than one
# player's info row on the same page (e.g. inside a "similar players"
# section), this could grab the wrong one - revisit if a bad rarity
# value is ever observed on a real card.
RARITY_HREF_RE = re.compile(r"/players\?version=([a-z0-9_]+)", re.I)


def parse_card_rarity(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=RARITY_HREF_RE):
        span = a.find("span", class_="text-ellipsis")
        label = span.get_text(strip=True) if span else None
        if label:
            return label
    return None


async def ensure_rarity_column(conn: asyncpg.Connection) -> None:
    """rarity already exists ad hoc on some live deployments (formalized,
    not created, by backend's 025_fut_players_rarity.sql) - IF NOT EXISTS
    makes this a true no-op there, and self-heals any environment that
    hasn't run that migration yet, same convention futbin_full_sync.py's
    own ensure_new_columns() already follows."""
    await conn.execute("ALTER TABLE fut_players ADD COLUMN IF NOT EXISTS rarity TEXT")


async def _fetch_batch(conn: asyncpg.Connection) -> list:
    """Cards with a real player_url and no rarity yet, prioritized
    toward cards that actually appear in fair_value_mv (real trading
    activity) so this backfill's limited per-run budget goes to the
    cards v2's list surfaces actually render first - same prioritization
    futbin_card_art_backfill.py uses for the same reason."""
    return await conn.fetch(
        """
        SELECT p.card_id, p.player_url
        FROM fut_players p
        WHERE p.rarity IS NULL AND p.player_url IS NOT NULL
        ORDER BY (EXISTS (SELECT 1 FROM fair_value_mv f WHERE f.card_id = p.card_id)) DESC,
                 p.price_num DESC NULLS LAST
        LIMIT $1
        """,
        BATCH_SIZE,
    )


async def run_once() -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    diag: Dict[str, Any] = {"http_429_hits": 0, "http_exceptions": 0}
    written = not_found = failed = 0
    try:
        await ensure_rarity_column(conn)
        rows = await _fetch_batch(conn)
        # At the default BATCH_SIZE=300 / REQUEST_DELAY=1.5s, jittered
        # per-card delay alone adds up to ~8+ minutes before real HTTP
        # time on top - with no log line until the run finished, a
        # legitimately-still-working run was indistinguishable from a
        # hung one on Railway's deploy log ("Starting Container" then
        # nothing for minutes). Log the batch size up front and progress
        # every 25 cards so "still working" is visible, not silent.
        log.info("starting batch of %d card(s)", len(rows))
        async with aiohttp.ClientSession() as session:
            for i, r in enumerate(rows, start=1):
                status, html = await _get_with_retry(session, r["player_url"], diag)
                if status != 200 or not html:
                    failed += 1
                    await asyncio.sleep(_jittered_delay())
                    continue
                try:
                    rarity = parse_card_rarity(html)
                    if rarity is None:
                        # Page fetched fine but the card-type link wasn't
                        # found - leave rarity NULL rather than writing a
                        # false "checked, nothing there"; retried next run.
                        not_found += 1
                    else:
                        await conn.execute(
                            "UPDATE fut_players SET rarity = $1 WHERE card_id = $2",
                            rarity, r["card_id"],
                        )
                        written += 1
                except Exception as e:
                    log.warning("card_id=%s parse/write failed: %s", r["card_id"], e)
                    failed += 1
                if i % 25 == 0 or i == len(rows):
                    log.info(
                        "progress %d/%d - written=%d not_found=%d failed=%d",
                        i, len(rows), written, not_found, failed,
                    )
                await asyncio.sleep(_jittered_delay())

        detail = (
            f"batch={len(rows)} written={written} not_found={not_found} failed={failed} "
            f"http_429={diag['http_429_hits']} http_exc={diag['http_exceptions']}"
        )
        log.info(detail)
        await heartbeat(conn, "futbin_rarity_backfill", ok=True, detail=detail)

        if len(rows) > 0 and failed >= len(rows) * 0.5:
            await alert(f"futbin_rarity_backfill: {failed}/{len(rows)} cards failed this run - {detail}")
    except Exception as e:
        log.error("run failed: %s", e)
        await heartbeat(conn, "futbin_rarity_backfill", ok=False, detail=str(e)[:500])
        await alert(f"futbin_rarity_backfill crashed: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_once())
