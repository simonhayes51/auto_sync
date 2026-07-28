# FUT Traders Local Test Environment

## 1. Create a Local PostgreSQL Database
```bash
docker run --name futtrader-db \
-e POSTGRES_USER=postgres \
-e POSTGRES_PASSWORD=postgres \
-e POSTGRES_DB=futtrader \
-p 5432:5432 -d postgres
```
Then create the table:
```bash
psql -h localhost -U postgres -d futtrader -f sql/create_tables.sql
```

## 2. Install Dependencies
```bash
pip install -r requirements.txt
```

## 3. Run the Bot
```bash
python bot.py
```

## 4. Run the futbin sync worker
```bash
python futbin_full_sync.py --now
```
`--now` runs a single crawl and exits, for local testing. Without it, the
script runs forever, doing one full crawl daily at 19:00 UK (this is what
Railway's `worker` process runs, per the Procfile).

## 5. Run the BIN/sales history collector
```bash
python bin_sales_history_sync.py
```
Runs a single crawl and exits - scrapes every Gold Rare card's current
lowest BIN (both ps and pc) and any newly-seen sales, appending to
`bin_history`/`sales_history` (never overwriting or deleting). In
production this is a Railway Cron Job (not a permanent worker) invoked
every 10 minutes with the cron expression `*/10 * * * *`, running the
same `python bin_sales_history_sync.py` start command per scheduled
execution - it doesn't share state with `futbin_full_sync.py`.

## 6. SBC collector - NOT YET SCHEDULED, READ BEFORE DEPLOYING
```bash
python futbin_sbc_sync.py
```
Scrapes futbin.com's SBC hub + per-set detail pages into the backend's
`market_events`/`sbc_details`/`sbc_challenges` tables (backend migrations
018/019). Same one-shot-per-invocation Cron Job design as
`bin_sales_history_sync.py` (not a permanent worker, no Procfile entry).

**Do not add this to a real Railway Cron schedule yet.** It was written
without live network access to futbin.com, so its parsing logic is a
best-effort first draft against *assumed* page structure, not confirmed
real markup - see the numbered verification checklist at the top of
`futbin_sbc_sync.py` for exactly what needs confirming first. Everything
else about it (database writes, upsert idempotency, fingerprint
generation, failure/heartbeat/alert handling) is fully tested and
working - only the HTML parsing itself is unverified.
