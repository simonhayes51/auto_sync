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
python bin_sales_history_sync.py --now
```
Same `--now` convention. Without it, runs forever, scraping every Gold Rare
card's current lowest BIN (both ps and pc) and any newly-seen sales every
10 minutes, appending to `bin_history`/`sales_history` (never overwriting
or deleting). This is Railway's separate `history_worker` process in the
Procfile - enable it as its own service alongside the existing `worker` one,
they run independently and don't share state.
