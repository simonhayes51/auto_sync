"""
Promo/event detector - populates market_events for anything that isn't
an SBC (TOTW/TOTS/Icon/Hero drops, campaign releases, etc).

Every existing market_events writer (futbin_sbc_sync.py, easysbc_sbc_sync.py)
only ever writes kind='sbc'. market_events.kind is free-text and the
downstream schema (backend's event_market_impact) was always built generic
enough to take other kinds - nothing has ever written one. Promo timing is
one of the most predictable, most-requested signals in FUT trading, so this
is a real, live gap, not a hypothetical one.

Why this doesn't scrape a new page
-----------------------------------
Every other market_events writer works against a live futbin page with
selectors confirmed against real markup - this repo doesn't have a
"promo calendar" page it already scrapes, and guessing new CSS selectors
against a page nobody here has inspected live is a good way to ship
something that silently parses zero rows on day one (see
futbin_full_sync.py's own alert for exactly that failure mode).

Instead, this reads a signal the pipeline already collects reliably:
fut_players.version (TOTW/TOTS/Icon/Hero/etc - see bin_sales_history_sync.py's
own docstring, which already treats this as the authoritative edition
field) combined with a new first_seen_at column (added here, backfilled
safely below - see _ensure_schema). When a cluster of newly-discovered
cards shares a non-"Normal" version within a short window, that cluster
IS a promo event - a burst of TOTW cards means Team of the Week just
dropped, a burst of Icon cards means an Icons release, etc. This needs
zero new HTTP requests to futbin at all - it only reads fut_players,
which futbin_full_sync.py (both crawl() and crawl_latest()) already keeps
current. Pure SQL, so nothing here can be blocked by Cloudflare the way
futbin_sbc_sync.py's Playwright crawl can be (see README.md section 6) -
safe to run as a normal Railway Cron Job.

The trade-off: a card discovered via crawl_latest() has no version yet
(that field isn't scraped by the lightweight /latest parser - see
parse_latest_row's docstring) until the next full crawl() classifies it,
so detection lags behind the promo's real start by however long that
gap is in your deployment (up to ~1 day if crawl_latest runs more often
than the daily full crawl). That's an acceptable trade for "detects
promos at all" vs "detects them with zero HTTP requests and zero new
scraper fragility".

Required environment variable
------------------------------
DATABASE_URL

Optional environment variables
-------------------------------
PROMO_DETECT_WINDOW_HOURS=48   how far back "newly discovered" looks
PROMO_MIN_NEW_CARDS=5          minimum cluster size to count as an event
PROMO_EXCLUDED_VERSIONS=normal (case-insensitive, comma-separated)

Deployment: single one-shot run per invocation (matches
bin_sales_history_sync.py) - deploy as a Railway Cron Job, e.g. daily,
not a permanent worker. Locally: `python promo_event_detector.py`.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List

import asyncpg

from monitoring import alert, heartbeat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("promo_event_detector")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")

WINDOW_HOURS = int(os.getenv("PROMO_DETECT_WINDOW_HOURS", "48"))
MIN_NEW_CARDS = int(os.getenv("PROMO_MIN_NEW_CARDS", "5"))
EXCLUDED_VERSIONS = {
    v.strip().lower()
    for v in os.getenv("PROMO_EXCLUDED_VERSIONS", "normal").split(",")
    if v.strip()
}


async def _ensure_schema(conn: asyncpg.Connection) -> None:
    """Adds fut_players.first_seen_at if this deployment predates it.
    Backfills existing rows to their last known price_updated_at (a
    genuinely-old value for every card that existed before today) rather
    than NOW() - an ALTER ... DEFAULT NOW() would stamp every existing
    row with the migration's own execution time, which would make this
    detector's very first run see the entire catalog as "new" and fire a
    false promo event for every version in the database at once."""
    has_column = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'fut_players' AND column_name = 'first_seen_at'
        )
        """
    )
    if not has_column:
        log.info("first_seen_at column missing - adding and backfilling from price_updated_at")
        await conn.execute("ALTER TABLE fut_players ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ")
        await conn.execute(
            """
            UPDATE fut_players
            SET first_seen_at = COALESCE(price_updated_at, NOW() - INTERVAL '365 days')
            WHERE first_seen_at IS NULL
            """
        )
        await conn.execute("ALTER TABLE fut_players ALTER COLUMN first_seen_at SET DEFAULT NOW()")
        await conn.execute("ALTER TABLE fut_players ALTER COLUMN first_seen_at SET NOT NULL")

    # market_events itself is created by futbin_sbc_sync.py/easysbc_sbc_sync.py
    # (both already run before this in every real deployment); IF NOT EXISTS
    # here only protects a from-scratch database running this script alone.
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_events (
            id BIGSERIAL PRIMARY KEY,
            kind TEXT NOT NULL,
            source TEXT NOT NULL,
            external_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            starts_at TIMESTAMPTZ,
            ends_at TIMESTAMPTZ,
            fingerprint TEXT[] NOT NULL DEFAULT '{}',
            payload JSONB NOT NULL DEFAULT '{}',
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (kind, source, external_id)
        )
        """
    )


def _version_slug(version: str) -> str:
    return "".join(c.lower() if c.isalnum() else "-" for c in version).strip("-") or "unknown"


async def _find_clusters(conn: asyncpg.Connection) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT version, card_id, name, rating, first_seen_at
        FROM fut_players
        WHERE first_seen_at >= NOW() - ($1 || ' hours')::interval
          AND version IS NOT NULL
          AND version <> ''
        ORDER BY version, first_seen_at
        """,
        str(WINDOW_HOURS),
    )

    clusters: Dict[str, List[asyncpg.Record]] = {}
    for r in rows:
        version = r["version"]
        if version.strip().lower() in EXCLUDED_VERSIONS:
            continue
        clusters.setdefault(version, []).append(r)

    out = []
    for version, members in clusters.items():
        if len(members) < MIN_NEW_CARDS:
            continue
        earliest = min(m["first_seen_at"] for m in members)
        latest = max(m["first_seen_at"] for m in members)
        out.append({
            "version": version,
            "card_ids": [int(m["card_id"]) for m in members],
            "sample_names": [m["name"] for m in members[:5]],
            "count": len(members),
            "earliest": earliest,
            "latest": latest,
        })
    return out


async def _upsert_promo_event(conn: asyncpg.Connection, cluster: Dict[str, Any]) -> int:
    version = cluster["version"]
    # ISO week of the cluster's earliest card anchors the event's identity -
    # stable across re-runs within the same week (so a daily cron doesn't
    # create a new row every run for one ongoing promo), while a genuinely
    # new week's cluster (e.g. next TOTW) naturally gets a new external_id.
    iso_year, iso_week, _ = cluster["earliest"].isocalendar()
    external_id = f"{_version_slug(version)}-{iso_year}-w{iso_week:02d}"

    sample = ", ".join(cluster["sample_names"])
    more = cluster["count"] - len(cluster["sample_names"])
    title = f"{version} — {cluster['count']} new cards"
    description = f"Detected {cluster['count']} newly-discovered {version} card(s): {sample}" + (
        f" (+{more} more)" if more > 0 else ""
    )
    payload = {
        "card_ids": cluster["card_ids"],
        "version": version,
        "detected_count": cluster["count"],
    }

    row = await conn.fetchrow(
        """
        INSERT INTO market_events (
            kind, source, external_id, title, description, starts_at, ends_at, payload, updated_at
        ) VALUES ('promo', 'auto_sync', $1, $2, $3, $4, $5, $6::jsonb, now())
        ON CONFLICT (kind, source, external_id) DO UPDATE SET
            title = EXCLUDED.title,
            description = EXCLUDED.description,
            ends_at = EXCLUDED.ends_at,
            payload = EXCLUDED.payload,
            updated_at = now()
        RETURNING id
        """,
        external_id, title, description, cluster["earliest"], cluster["latest"], json.dumps(payload),
    )
    return int(row["id"])


async def run_once() -> int:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        try:
            await _ensure_schema(conn)
            clusters = await _find_clusters(conn)

            written = 0
            for cluster in clusters:
                event_id = await _upsert_promo_event(conn, cluster)
                written += 1
                log.info(
                    "promo event upserted: id=%s version=%s cards=%d window=%s..%s",
                    event_id, cluster["version"], cluster["count"], cluster["earliest"], cluster["latest"],
                )

            detail = f"clusters={written}" + (
                f" ({', '.join(c['version'] + ':' + str(c['count']) for c in clusters)})" if clusters else ""
            )
            log.info("promo_event_detector complete: %s", detail)
            await heartbeat(conn, "promo_event_detector", ok=True, detail=detail)
            return written
        except Exception as e:
            log.exception("promo_event_detector failed")
            await heartbeat(conn, "promo_event_detector", ok=False, detail=str(e)[:400])
            await alert(f"promo_event_detector: run failed - {e}")
            raise
    finally:
        await conn.close()


if __name__ == "__main__":
    import asyncio
    import sys

    try:
        asyncio.run(run_once())
    except Exception:
        sys.exit(1)
    sys.exit(0)
