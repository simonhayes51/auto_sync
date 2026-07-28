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

## 7. Card art backfill - NOT YET SCHEDULED, READ BEFORE DEPLOYING
```bash
python futbin_card_art_backfill.py
```
Fills `fut_players.card_bg_image`/`card_cutout_image`/`card_cutout_type`/
`card_name` (backend migration 022) so v2's FIFA-card-art UI can render
real card art in list views instead of only the single-card Player Page.
One-shot-per-invocation Cron Job design, same as `futbin_sbc_sync.py` -
not a permanent worker, no Procfile entry yet.

Unlike the SBC collector above, this file's HTML parsing (`parse_card_layers`)
is **not** a fresh guess - it's ported verbatim from `backend/app/
futbin_client.py`'s `parse_card_layers()`, which is already live in
production today via `GET /api/fut-player-definition/{card_id}` (used by
v1's Player Search page on every single-card lookup). So the parsing logic
itself is proven against real futbin markup.

**What's actually unverified is the batch/volume behavior**: this worker
fetches up to `CARD_ART_BATCH_SIZE` (default 300) individual player pages
in one run, back-to-back with jittered delays - a bulk, unattended traffic
pattern quite different from the occasional single fetch real user
navigation produces. futbin.com has already shown real 429 rate-limiting
under `bin_sales_history_sync.py`'s concurrent-history-crawl load and real
403 blocking under `futbin_sbc_sync.py`'s testing - whether 300
sequential single-card GETs per run trips either has not been confirmed in
this environment (no live network access to futbin.com here). Do a small
manual run first (`CARD_ART_BATCH_SIZE=10 python futbin_card_art_backfill.py`)
and check the resulting heartbeat's `http_429`/`http_exc` counts before
scheduling this as a real Cron Job, and if adding it, prefer a small batch
size and a generous interval (e.g. every 15-30 minutes) over a big
one-shot sweep.
