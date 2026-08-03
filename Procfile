worker: python futbin_full_sync.py
history_worker: python bin_sales_history_sync.py

# New queue-driven workers (production-crawler redesign). Deployed
# alongside history_worker, not replacing it yet - see auto_sync/README.md
# for the shadow-mode rollout plan. Each is a Railway Cron Job, same
# one-shot-per-invocation model as history_worker.
bin_worker: python bin_worker.py
sales_worker: python sales_worker.py
metadata_worker: python metadata_worker.py

# FUT.GG replacement pipeline.
#
# futgg_player_worker: one-shot. Run daily as a Cron Job to discover and
# refresh card metadata.
#
# futgg_price_worker: a CONTINUOUS worker - deploy as a normal Railway
# service, NOT a Cron Job. It holds a Postgres advisory lock, keeps its
# browser alive between batches, and picks due cards on its own schedule
# (next_price_due_at, keyed off rating). On a cron it would fight that
# lock and pay the browser cold-start cost every invocation - a cold
# context previously failed an entire startup batch on its own.
#
# DATABASE_URL is the only variable it requires. Every other setting has a
# production-tuned default; override only with a measured reason.
futgg_player_worker: python futgg_player_sync.py
futgg_price_worker: python futgg_price_sync.py

# One-shot diagnostics. Run on a SEPARATE service - never the price
# worker. Each exits after a single run, so pointing the price service at
# one silently stops price syncing.
futgg_bulk_probe: python futgg_bulk_probe.py
