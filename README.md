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

## 6. SBC collector - headed Chromium required, do one supervised run first
```bash
pip install -r requirements.txt   # includes playwright==1.61.0
playwright install chromium
SBC_HEADLESS=false SBC_MAX_DETAIL_PAGES=1 python futbin_sbc_sync.py
```
Scrapes FUTBIN's single "ALL SBCs" listing page
(`https://www.futbin.com/26/squad-building-challenges` - the per-category
filter URLs are the same dataset and are deliberately not used) plus each
due SBC's detail page, into the backend's `market_events`/`sbc_details`/
`sbc_challenges` tables (backend migrations 018/019), used to track how
card prices move around SBC releases/requirements. Same
one-shot-per-invocation Cron Job design as `bin_sales_history_sync.py`
(not a permanent worker, no Procfile entry) - except this one uses
**Playwright + Chromium, not aiohttp**, because SBC pages render their
listing grid and detail content client-side and need a real browser.

The CSS selectors are **confirmed against real, saved FUTBIN HTML**
(both the listing page and an individual SBC detail page, cross-checked
independently with BeautifulSoup). A real local test also confirmed
something selectors alone don't fix: **FUTBIN's Cloudflare returns HTTP
403 for headless Playwright Chromium but HTTP 200 for the identical
request under headed Chrome** - so this worker must run headed
(`SBC_HEADLESS=false`, the default), which needs a real X display. See
the "Railway SBC Worker" section below for how that's deployed.

Still genuinely open, because this sandbox has no live network access to
futbin.com to check them:
- FUTBIN redesigns occasionally - selectors confirmed at validation time
  could have drifted since. A run logging "zero SBCs parsed" is the
  signal to re-check.
- The exact text format of the "expires"/"repeatable" fields on the
  listing card - the selectors are confirmed to find the right elements,
  but this file's parsing of their literal text content
  (`parse_expiry`, `parse_repeatable`) wasn't asserted against real
  strings.
- Whether headed-via-Xvfb from Railway's datacentre IP is actually
  accepted by FUTBIN - the local headed test proved headless is the
  problem, not that Railway's IP will be treated the same as a home
  connection. Confirm with a real deployed run before trusting a
  schedule.

**Do one supervised manual run and read the log/heartbeat output before
adding this to a real Cron schedule** - same discipline as
`futbin_card_art_backfill.py` below. Recommended cadence once verified:
once daily, not more often.

## Railway SBC Worker

Only the Railway service running `futbin_sbc_sync.py` should use:

```
RAILWAY_DOCKERFILE_PATH=Dockerfile.sbc
```

Every other service in this repo keeps its existing Nixpacks/Procfile
setup untouched - `Dockerfile.sbc` is not the repo-root `Dockerfile` and
nothing else picks it up automatically.

Initial test variables for that service (in addition to `DATABASE_URL`,
which this worker also requires):

```
SBC_HEADLESS=false
SBC_MAX_DETAIL_PAGES=1
SBC_REQUEST_DELAY_SECONDS=8
SBC_DETAIL_STALE_HOURS=20
SBC_NAV_TIMEOUT_MS=45000
SBC_SELECTOR_TIMEOUT_MS=20000
SBC_MAX_RETRIES=1
```

- **Railway Start Command should be left blank** so `Dockerfile.sbc`'s
  own `CMD` (which runs `docker-entrypoint-sbc.sh` - starts Xvfb on a
  fixed display, then the worker) is used. A manually configured Start
  Command overrides the Dockerfile `CMD` entirely - if you do set one, it
  must start Xvfb first (e.g. run `docker-entrypoint-sbc.sh` yourself),
  or headed Chromium has no display to attach to and will fail to
  launch. Deliberately not `xvfb-run` directly - its own display
  auto-detection hung indefinitely on a real Railway deploy with zero
  log output, which `docker-entrypoint-sbc.sh`'s fixed-display, bounded
  readiness check avoids.
- **Do not initially set `SBC_MAX_DETAIL_PAGES=0`.** Prove one detail
  page works end-to-end first (`SBC_MAX_DETAIL_PAGES=1`), check the
  heartbeat/log output, then increase it cautiously or move to `0`
  (no limit) once you trust the run.
- The scraper only ever uses the single "ALL SBCs" listing page - the
  per-category filter URLs must not be reintroduced, they're the same
  underlying dataset.
- Headed-via-Xvfb fixes the headless-vs-headed 403 difference confirmed
  in local testing. It does **not** prove Railway's datacentre IP will
  be accepted by FUTBIN - that's only provable by watching the first
  real deployed run's logs/heartbeat.

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
