worker: python futbin_full_sync.py
history_worker: python bin_sales_history_sync.py

# New queue-driven workers (production-crawler redesign). Deployed
# alongside history_worker, not replacing it yet - see auto_sync/README.md
# for the shadow-mode rollout plan. Each is a Railway Cron Job, same
# one-shot-per-invocation model as history_worker.
bin_worker: python bin_worker.py
sales_worker: python sales_worker.py
metadata_worker: python metadata_worker.py
