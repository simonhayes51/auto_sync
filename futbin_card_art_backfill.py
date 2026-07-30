"""
Incrementally fills fut_players.card_bg_image/card_cutout_image/
card_cutout_type/card_name (backend/migrations/022_fut_players_card_art.sql)
by fetching each card's individual futbin.com player page and parsing the
same background/cutout <img> markup backend/app/futbin_client.py's
parse_card_layers() already parses live, on every call, for the Player Page
detail view - ported here (not imported: auto_sync is a separate deployable
from backend, same convention bin_sales_history_sync.py's own ported _num()
already follows) because these two repos don't share a package today.

Unlike futbin_full_sync.py's listing-page crawl (one GET covers ~25 cards
at once), this needs ONE request PER CARD - the listing rows never carry
this background/cutout markup, confirmed by reading futbin_full_sync.py's
own parse_row(). Not viable as a blind daily full-catalog sweep (~25k+
cards at this politeness rate would take many hours and meaningfully raise
403-block risk on a site that has already shown real bot-protection
behavior on this project - see futbin_sbc_sync.py's own findings), so this
only ever processes a bounded BATCH_SIZE per run, prioritized toward cards
that actually appear in fair_value_mv (real recent trading activity - the
cards v2's Home Dashboard/Player Page/SBC Hub list surfaces actually
render) over the long tail of untraded cards nobody will ever see rendered
with card art.

Deployed as a Railway Cron Job on a short interval (see README - suggested
every 15-30 minutes, much more frequent than futbin_full_sync's daily
sweep) so the backfill makes steady, low-risk progress on its own schedule
without ever competing with the full catalog crawl. Each invocation is a
single one-shot pass (see `if __name__ == "__main__"` below), same pattern
as bin_sales_history_sync.py.

A per-card failure is logged and skipped; it never aborts the run for the
rest of the batch, and a failed card is simply retried on the next
invocation (card_bg_image stays NULL until a fetch actually succeeds).
"""
import os
import random
import asyncio
import logging
from typing import Any, Dict, Optional

import asyncpg
import aiohttp
from bs4 import BeautifulSoup

from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futbin_card_art_backfill")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found!")

BATCH_SIZE = int(os.getenv("CARD_ART_BATCH_SIZE", "300"))
REQUEST_DELAY = float(os.getenv("CARD_ART_REQUEST_DELAY", "1.5"))
MAX_RETRIES = int(os.getenv("CARD_ART_MAX_RETRIES", "3"))
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=15)
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SBCSolver/1.5)"}


def _jittered_delay() -> float:
    return random.uniform(REQUEST_DELAY * 0.6, REQUEST_DELAY * 1.6)


async def _get_with_retry(session: aiohttp.ClientSession, url: str, diag: Dict[str, Any]) -> "tuple[int, Optional[str]]":
    """GET with 429-aware backoff retry - same shape as
    bin_sales_history_sync.py's own helper (see that file's comments for
    why a 429 here is worth retrying rather than counting as a hard fail)."""
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


# Ported verbatim from backend/app/futbin_client.py::parse_card_layers() -
# see that function's own comments for why hero-scoping ("playercard-l")
# and exact-token class matching (not a \bword\b regex, which a hyphen
# defeats) both matter here.
def parse_card_layers(html: str) -> Dict[str, Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")
    hero = soup.find("div", class_="playercard-l")
    scope = hero or soup
    bg = scope.find("img", class_="playercard-26-bg")
    special_cutout = scope.find("img", class_="playercard-26-special-img")
    cutout = special_cutout or scope.find("img", class_="playercard-26-base-img")
    cutout_type = "special" if special_cutout else ("base" if cutout else None)
    name_el = scope.find("div", class_="playercard-26-name")
    return {
        "bgImageUrl": bg.get("src") if bg else None,
        "cutoutImageUrl": cutout.get("src") if cutout else None,
        "cutoutType": cutout_type,
        "cardName": name_el.get_text(strip=True) if name_el else None,
    }


async def _fetch_batch(conn: asyncpg.Connection) -> list:
    """Cards with a real player_url but no card art yet, prioritized
    toward cards that actually appear in fair_value_mv (real trading
    activity) so the backfill's limited per-run budget goes to the cards
    v2's list surfaces actually render first."""
    return await conn.fetch(
        """
        SELECT p.card_id, p.player_url
        FROM fut_players p
        WHERE p.card_bg_image IS NULL AND p.player_url IS NOT NULL
        ORDER BY (EXISTS (SELECT 1 FROM fair_value_mv f WHERE f.card_id = p.card_id)) DESC,
                 p.price_num DESC NULLS LAST
        LIMIT $1
        """,
        BATCH_SIZE,
    )


async def run_once() -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    diag: Dict[str, Any] = {"http_429_hits": 0, "http_exceptions": 0}
    written = failed = 0
    try:
        rows = await _fetch_batch(conn)
        async with aiohttp.ClientSession() as session:
            for r in rows:
                status, html = await _get_with_retry(session, r["player_url"], diag)
                if status != 200 or not html:
                    failed += 1
                    await asyncio.sleep(_jittered_delay())
                    continue
                try:
                    layers = parse_card_layers(html)
                    await conn.execute(
                        """
                        UPDATE fut_players
                        SET card_bg_image = $1, card_cutout_image = $2,
                            card_cutout_type = $3, card_name = $4
                        WHERE card_id = $5
                        """,
                        layers["bgImageUrl"], layers["cutoutImageUrl"],
                        layers["cutoutType"], layers["cardName"], r["card_id"],
                    )
                    written += 1
                except Exception as e:
                    log.warning("card_id=%s parse/write failed: %s", r["card_id"], e)
                    failed += 1
                await asyncio.sleep(_jittered_delay())

        detail = f"batch={len(rows)} written={written} failed={failed} http_429={diag['http_429_hits']} http_exc={diag['http_exceptions']}"
        log.info(detail)
        await heartbeat(conn, "futbin_card_art_backfill", ok=True, detail=detail)

        if len(rows) > 0 and failed >= len(rows) * 0.5:
            await alert(f"futbin_card_art_backfill: {failed}/{len(rows)} cards failed this run - {detail}")
    except Exception as e:
        log.error("run failed: %s", e)
        await heartbeat(conn, "futbin_card_art_backfill", ok=False, detail=str(e)[:500])
        await alert(f"futbin_card_art_backfill crashed: {e}")
        raise
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(run_once())
