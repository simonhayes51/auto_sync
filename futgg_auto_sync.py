import os
import re
import sys
import time
import json
import asyncio
import asyncpg
import aiohttp
import logging
import signal
from datetime import datetime, timedelta
from html import unescape
from zoneinfo import ZoneInfo
from typing import Optional, Dict, List
from aiohttp import web  # health server

# ---------- Startup debug ----------
print("🚀 Script starting…", flush=True)
try:
    DATABASE_URL = os.getenv("DATABASE_URL")
    print(f"📊 DATABASE_URL exists: {bool(DATABASE_URL)}", flush=True)
    if not DATABASE_URL:
        print("❌ DATABASE_URL is not set!", flush=True)
        sys.exit(1)

    PORT = int(os.getenv("PORT", "8080"))
    print(f"🌐 PORT: {PORT}", flush=True)
    print("✅ Environment check passed", flush=True)
except Exception as e:
    print(f"💥 Startup error: {e}", flush=True)
    sys.exit(1)

# ================== CONFIG ==================
META_API = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
LISTING_URLS = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))
CONCURRENCY        = int(os.getenv("CONCURRENCY", "24"))
DISCOVERY_CONC     = int(os.getenv("DISCOVERY_CONCURRENCY", "8"))
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100"))

UA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FutGGMetaSync/3.1)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://www.fut.gg/players/new/"
}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_THROTTLE_MS = int(os.getenv("LOG_THROTTLE_MS", "300"))

HAS_NICK = False
HAS_FIRST = False
HAS_LAST = False

# ================== POSITION MAP ==================
POSITION_MAP = {
    0: "GK", 1: "GK", 2: "GK",
    3: "RB", 4: "RB",
    5: "CB", 6: "CB",
    7: "LB", 8: "LB", 9: "LB",
    10: "CDM", 11: "CDM",
    12: "RM", 13: "RM",
    14: "CM", 15: "CM",
    16: "LM", 17: "LM",
    18: "CAM", 19: "CAM", 20: "CAM", 21: "CAM", 22: "CAM",
    23: "RW", 24: "RW",
    25: "ST", 26: "ST",
    27: "LW",
}

# ================== LOGGING ==================
class _RateLimitFilter(logging.Filter):
    _last = {}
    def filter(self, rec: logging.LogRecord) -> bool:
        key = rec.msg
        now = time.monotonic() * 1000
        last = self._last.get(key, 0)
        if now - last < LOG_THROTTLE_MS:
            return False
        self._last[key] = now
        return True

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout
)
for h in logging.getLogger().handlers:
    h.addFilter(_RateLimitFilter())
log = logging.getLogger("futgg_meta_sync")
print(f"▶️ Starting: {os.path.basename(__file__)} | LOG_LEVEL={LOG_LEVEL}", flush=True)

# ================== DB ==================
async def get_db() -> asyncpg.Connection:
    return await asyncpg.connect(DATABASE_URL)

async def preflight(conn: asyncpg.Connection) -> bool:
    """Return True if schema ready, False otherwise (don’t crash)."""
    try:
        tbl = await conn.fetchval("SELECT to_regclass('public.fut_players')")
        if not tbl:
            log.warning("⏳ Table fut_players not found yet")
            return False
        idx = await conn.fetchval("""
            SELECT indexname FROM pg_indexes
            WHERE schemaname='public' AND tablename='fut_players'
              AND indexname='fut_players_card_id_key'
        """)
        if not idx:
            log.warning("⏳ Index fut_players_card_id_key missing")
            return False
        cols = {
            r["column_name"]
            for r in await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='public' AND table_name='fut_players'
            """)
        }
        global HAS_NICK, HAS_FIRST, HAS_LAST
        HAS_NICK  = "nickname" in cols
        HAS_FIRST = "first_name" in cols
        HAS_LAST  = "last_name" in cols
        log.info("✅ Preflight OK | nickname=%s first=%s last=%s", HAS_NICK, HAS_FIRST, HAS_LAST)
        return True
    except Exception as e:
        log.error("❌ Preflight error: %s", e)
        return False

# ================== HEALTH SERVER ==================
async def ok(_):
    return web.Response(text="OK")

async def start_health():
    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("🌐 Health server listening on :%d", PORT)
    return asyncio.create_task(asyncio.Event().wait())  # keeps running

shutdown_evt = asyncio.Event()
def _sig():
    log.info("🛑 Shutdown signal received")
    shutdown_evt.set()

# ================== MAIN LOOP ==================
async def run_once():
    conn = await get_db()
    try:
        ready = await preflight(conn)
        if not ready:
            return
        # TODO: call discover / upsert / enrich here
        log.info("✅ Cycle ran (stubbed)")
    finally:
        await conn.close()

async def sleep_until_19_uk():
    tz = ZoneInfo("Europe/London")
    now = datetime.now(tz)
    target = now.replace(hour=19, minute=0, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    log.info("🕖 Next run at %s", target.isoformat())
    await asyncio.sleep((target - now).total_seconds())

async def main_loop():
    loop = asyncio.get_running_loop()
    for s in (signal.SIGTERM, signal.SIGINT):
        try: loop.add_signal_handler(s, _sig)
        except NotImplementedError: pass

    # Start health server immediately
    health_task = await start_health()

    while not shutdown_evt.is_set():
        log.info("🚦 Cycle start")
        await run_once()
        log.info("✅ Cycle complete")
        await sleep_until_19_uk()

    health_task.cancel()

# ================== CLI ==================
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--now", action="store_true", help="Run once and exit")
    args = ap.parse_args()

    async def _runner():
        if args.now:
            ht = await start_health()
            await run_once()
            await asyncio.sleep(2)  # let health check pass once
            ht.cancel()
        else:
            await main_loop()

    try:
        asyncio.run(_runner())
    except Exception as e:
        print(f"💥 Fatal asyncio error: {e}", flush=True)
        sys.exit(1)