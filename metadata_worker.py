"""
Recomputes scrape_queue priority and backfills newly-catalogued cards into
it - the queue-maintenance half of the redesign, run periodically (Railway
Cron, e.g. hourly) alongside (not replacing) the existing daily
futbin_full_sync.py catalog crawl. futbin_full_sync.py still owns
discovering new cards/refreshing fut_players itself; this worker only
keeps scrape_queue's priority ordering and candidate set in sync with
that catalog, for the 'bin' and 'sales' worktypes bin_worker.py/
sales_worker.py consume from.

Priority is computed live each pass (not stored as a second source of
truth that could drift) from:
  - promo/special version flag (highest weight - the most volatile,
    highest-value segment, matching Tier A's existing rationale)
  - popularity score, if present (card_scores_latest)
  - recent sale velocity (sales_history count, last 24h)
  - current staleness (time since last bin_history capture)
Same coverage the old TIER_A_WHERE/TIER_B_WHERE gave (specials/promos any
rating, ordinary golds 75+) - ordinary sub-75 cards stay out of this fast
queue, left to the daily full-catalog crawl, matching the documented
"~80% illiquid bronze/silver noise" rationale in bin_sales_history_sync.py.
"""
import asyncio
import logging
import os
import sys
from typing import Dict

import asyncpg

import scrape_queue as queue
import http_client
from monitoring import heartbeat, alert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("metadata_worker")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found!")

# Same candidate scope as bin_sales_history_sync.py's TIER_A_WHERE/TIER_B_WHERE
# combined - every special/promo card plus ordinary golds 75+.
CANDIDATE_WHERE = (
    "((version IS NOT NULL AND version NOT ILIKE 'normal') "
    "OR (rating >= 75 AND version ILIKE 'normal'))"
)

_PRIORITY_SQL = f"""
SELECT
    fp.card_id,
    (
        CASE WHEN fp.version IS NOT NULL AND fp.version NOT ILIKE 'normal' THEN 100 ELSE 0 END
        + CASE WHEN fp.rating >= 82 THEN 30 WHEN fp.rating >= 75 THEN 10 ELSE 0 END
        + LEAST(COALESCE(cs.popularity, 0)::int, 40)
        + LEAST(COALESCE(sv.sales_24h, 0) * 2, 40)
        + LEAST(COALESCE(EXTRACT(EPOCH FROM (now() - bh.last_captured_at)) / 3600, 48)::int, 48)
    )::int AS priority
FROM fut_players fp
LEFT JOIN LATERAL (
    SELECT value AS popularity
    FROM card_scores_latest csl
    WHERE csl.card_id = fp.card_id AND csl.score_type = 'popularity'
) cs ON true
LEFT JOIN LATERAL (
    SELECT count(*) AS sales_24h
    FROM sales_history sh
    WHERE sh.player_id = fp.card_id AND sh.sold_at >= now() - interval '24 hours'
) sv ON true
LEFT JOIN LATERAL (
    SELECT max(captured_at) AS last_captured_at
    FROM bin_history bh2
    WHERE bh2.player_id = fp.card_id
) bh ON true
WHERE {CANDIDATE_WHERE} AND fp.player_url IS NOT NULL
"""


async def run_once() -> None:
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=4)
    try:
        async with pool.acquire() as conn:
            await http_client.ensure_crawler_tables(conn)

        try:
            rows = await pool.fetch(_PRIORITY_SQL)
        except asyncpg.exceptions.UndefinedTableError:
            # card_scores_latest may not exist yet in some environments -
            # popularity just drops out of the score rather than failing
            # the whole recompute.
            log.warning("card_scores_latest not found - recomputing priority without popularity term")
            rows = await pool.fetch(_PRIORITY_SQL.replace(
                "LEFT JOIN LATERAL (\n    SELECT value AS popularity\n"
                "    FROM card_scores_latest csl\n"
                "    WHERE csl.card_id = fp.card_id AND csl.score_type = 'popularity'\n) cs ON true",
                "LEFT JOIN LATERAL (SELECT NULL::numeric AS popularity) cs ON true",
            ))

        priorities: Dict[int, int] = {r["card_id"]: r["priority"] for r in rows}
        await queue.upsert_candidates(pool, "bin", priorities)
        await queue.upsert_candidates(pool, "sales", priorities)

        log.info("metadata_worker recomputed priority for %d candidates (bin + sales worktypes)", len(priorities))
        await heartbeat(pool, "metadata_worker", ok=True, detail=f"candidates={len(priorities)}")
    except Exception as e:
        log.error("metadata_worker failed: %s", e)
        await alert(f"metadata_worker: priority recompute failed: {e}")
        raise
    finally:
        await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(run_once())
    except Exception as e:
        log.error("run_once() failed: %s", e)
        sys.exit(1)
    sys.exit(0)
