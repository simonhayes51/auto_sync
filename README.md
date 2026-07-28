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

## 6. SBC collector - run from your own machine, NOT Railway (confirmed blocked)
```bash
pip install -r requirements.txt   # includes playwright==1.61.0
playwright install chromium
SBC_MAX_DETAIL_PAGES=1 python futbin_sbc_sync.py
```
Scrapes FUTBIN's single "ALL SBCs" listing page
(`https://www.futbin.com/26/squad-building-challenges` - the per-category
filter URLs are the same dataset and are deliberately not used) plus each
due SBC's detail page, into the backend's `market_events`/`sbc_details`/
`sbc_challenges` tables (backend migrations 018/019), used to track how
card prices move around SBC releases/requirements. Uses **Playwright +
Chromium, not aiohttp**, because SBC pages render their listing grid and
detail content client-side and need a real browser.

The CSS selectors are **confirmed against real, saved FUTBIN HTML**. Two
things beyond selectors were confirmed by real testing:
- **FUTBIN's Cloudflare returns HTTP 403 for headless Playwright
  Chromium but HTTP 200 for the identical request under headed
  Chrome** - so this worker runs headed (`SBC_HEADLESS=false`, the
  default) by default.
- **Headed Chromium is not enough on its own if the request is coming
  from a datacentre IP.** A real Railway deployment (headed, via Xvfb,
  everything working correctly right up to the actual page request)
  still got an immediate `HTTP 403 "Just a moment..."` from Cloudflare -
  a hard WAF block on the IP/network origin, not a solvable JS
  challenge, and not something any browser-fingerprint or timing change
  fixes. **Confirmed working from a home/residential connection with
  headed Chrome; confirmed blocked from Railway's network.** Run this
  worker from a machine on a normal home/residential connection, not
  Railway or another cloud/datacentre host - see "Running Locally" below.

Still genuinely open:
- FUTBIN redesigns occasionally - selectors confirmed at validation time
  could have drifted since. A run logging "zero SBCs parsed" is the
  signal to re-check.
- The exact text format of the "expires"/"repeatable" fields on the
  listing card - the selectors are confirmed to find the right elements,
  but this file's parsing of their literal text content
  (`parse_expiry`, `parse_repeatable`) wasn't asserted against real
  strings.

**Do one supervised manual run and read the log/heartbeat output before
scheduling anything unattended** - same discipline as
`futbin_card_art_backfill.py` below. Recommended cadence once verified:
once daily, not more often.

## Running Locally (recommended - Railway is blocked)

Since Railway's network is confirmed blocked by FUTBIN's Cloudflare
(see above), run this worker on a schedule from a machine on a normal
home/residential connection instead - the same setup your local test
already proved works.

**1. Point it at your Railway Postgres database from outside Railway's
network.** In the Railway dashboard, open your Postgres service ->
Settings -> Networking -> enable "Public Networking" if it isn't
already, then use the resulting public connection string (a
`postgresql://...proxy.rlwy.net:PORT/...`-style URL, not the internal
`DATABASE_URL` other Railway services use) as `DATABASE_URL` below -
the internal one is only reachable from inside Railway's own network.

**2. Install and do one manual run** (as in section 6 above) to confirm
it works from your connection and check the log/heartbeat output.

**3. Schedule it** - once daily is plenty (a full run is several
minutes; see the module docstring for why). Linux/macOS:
```bash
crontab -e
```
```
0 18 * * * cd /path/to/auto_sync && DATABASE_URL="postgresql://...proxy.rlwy.net:PORT/..." /usr/bin/python3 futbin_sbc_sync.py >> sbc_sync.log 2>&1
```
Windows: Task Scheduler -> Create Basic Task -> Daily -> Action "Start a
program" -> `python.exe` with argument `futbin_sbc_sync.py`, "Start in"
set to this folder, and `DATABASE_URL` set as a system/user environment
variable (Task Scheduler doesn't read a local `.env` file).

`Dockerfile.sbc`/`docker-entrypoint-sbc.sh` are still in this repo and
still work correctly (confirmed: Xvfb starts, Chromium launches headed,
navigation is attempted) in case Railway's IP reputation ever changes or
you want to try a different host later - the blocker is the network
origin, not the container setup.

## 6b. SBC collector v2 - EasySBC API (recommended over section 6)
```bash
pip install -r requirements.txt   # only needs aiohttp/asyncpg, already listed
python easysbc_sbc_sync.py
```
Same `market_events`/`sbc_details`/`sbc_challenges` tables as the FUTBIN
collector above, but `source='easysbc'` instead of `'futbin'` (the two
coexist - not deduplicated against each other), and a completely different
approach: EasySBC.io's frontend is a client-rendered SPA backed by a
plain, **unauthenticated JSON API** (`api-fc26.easysbc.io`), confirmed by
inspecting real DevTools Network requests against real loaded pages, not
guessed:

- `GET /sbc-sets?page=N&limit=M` - paginated listing of SBC sets
- `GET /sbc-sets/{id}` - one set's full metadata (real bool `repeatable`,
  Unix-second `startTime`/`endTime`, real int `psPrice`/`pcPrice` - no
  HTML text parsing of "37.3K" or "Expires in 6 days" needed at all)
- `GET /sbcs?setId={id}` - that set's challenge breakdown, each with a
  `requirements` field that's already a clean array of human-readable
  strings (e.g. `"Team Rating: Min. 89"`, `"Min. 1 Players Any
  TOTW/TOTS/FOF"`)

**No browser, no Xvfb, no Cloudflare challenge** - plain `aiohttp` GETs,
the same shape as `bin_sales_history_sync.py`'s approach to futbin.com's
HTML pages. This should run fine directly on Railway (a normal Nixpacks
worker/Cron Job, no `Dockerfile.sbc` needed) - unlike section 6, nothing
about this approach is currently known to require a residential IP, though
that's only proven for a handful of manual one-off requests, not sustained
crawl volume.

Requirement text is also cross-checked against **real `fut_players.nation`
/ `fut_players.league` values already in this database** (queried at
runtime, not a hardcoded guessed list) to tag the SBC's `market_events.
fingerprint` with things like `nation_spain` or `league_premier_league`
whenever a requirement names a real nation/league - alongside the
existing `requires_totw`/`requires_tots`/`requires_toty`/`requires_fof`
promo-keyword tags. Matching is whole-phrase/word-boundary (not naive
substring), so e.g. "Mali" cannot false-positive-match inside "Somalia" -
verified against the real Bárbara López payload plus synthetic
Premier-League/Somalia cases.

Still genuinely open, because this sandbox has no live network access to
verify further:
- The exact envelope shape of `GET /sbc-sets` (bare array vs wrapped
  object) - only single-item `/sbc-sets/{id}` and `/sbcs?setId=` responses
  were directly confirmed; `fetch_listing()` handles either shape
  defensively and logs a warning if it recognises neither.
- Whether sequential requests across dozens of sets in one run trips any
  rate limit on this API - `_get_json_with_retry` backs off on 429/5xx the
  same way `bin_sales_history_sync.py` does for futbin.com, but that's
  untested at real volume here.
- `categoryId` (e.g. `2`) has no confirmed name mapping, so `sbc_details.
  category` is left `NULL` rather than guessed.
- Reward `resourceId`/`assetId` are EA's own definition-id namespace, not
  this codebase's FUTBIN-derived `fut_players.card_id` - no mapping
  between the two is known, so `reward_card_id` is left `NULL`.

**Do one supervised manual run and read the log/heartbeat output before
scheduling this as a real Cron Job** - same discipline as every other
worker in this file. `futbin_sbc_sync.py`/`Dockerfile.sbc`/
`docker-entrypoint-sbc.sh` are left in the repo for now rather than
deleted, in case this API also turns out to be rate-limited or blocked at
scale and the Playwright approach needs revisiting - remove them once
this one is proven reliable over a few real runs.

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
