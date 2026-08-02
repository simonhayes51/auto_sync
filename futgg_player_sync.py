#!/usr/bin/env python
"""Daily FUT.GG player catalogue sync.

Discovers cards from https://www.fut.gg/players/new/?page=N (real
pagination - see collect_listing_urls()), visits only cards that are new
(unless FUTGG_REFRESH_EXISTING=true), and upserts metadata into the
independent futgg_players table. It intentionally does not write prices or
sales; futgg_price_sync.py owns those.

Recommended Railway Cron: once daily, e.g. 15 5 * * *

Two discovery modes, since new cards appear at the front of the listing:
  - Daily (default): only the first FUTGG_LISTING_MAX_PAGES pages, enough
    to catch what's new since yesterday without re-walking the whole
    catalogue every run.
  - Full scan (FUTGG_LISTING_FULL_SCAN=true): up to
    FUTGG_LISTING_MAX_PAGES_FULL pages, for an occasional full-catalogue
    sync (the real catalogue is 350+ pages as of writing).
Either mode also stops early after FUTGG_LISTING_IDLE_ROUNDS consecutive
pages with zero new cards - the real signal that pagination has run past
the end of the catalogue, so a full scan doesn't have to walk its whole
page cap every time and a daily run doesn't overrun a slow news day.

Important environment variables:
  DATABASE_URL                    required
  PLAYWRIGHT_HEADLESS             true by default
  PLAYWRIGHT_TIMEOUT_MS           45000
  FUTGG_PLAYER_LIMIT              0 = no card limit
  FUTGG_LISTING_FULL_SCAN         false
  FUTGG_LISTING_MAX_PAGES         5 (daily mode)
  FUTGG_LISTING_MAX_PAGES_FULL    400 (full-scan mode)
  FUTGG_LISTING_IDLE_ROUNDS       4 (consecutive pages w/ 0 new cards before stopping)
  FUTGG_LISTING_PAGE_DELAY        0.5 seconds, between listing-page navigations
  FUTGG_LISTING_STABILIZE_ATTEMPTS 6 (retries waiting for a page's cards to differ from the previous page's)
  FUTGG_LISTING_STABILIZE_POLL_MS 400 (wait between stabilize retries)
  FUTGG_REFRESH_EXISTING          false
  FUTGG_PLAYER_REQUEST_DELAY      0.35 seconds
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
from dataclasses import asdict
from typing import Any
from urllib.parse import urljoin, urlparse

import asyncpg
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

from futgg_common import BASE_URL, CARD_URL_RE, FutggCard, classify_price_tier, parse_futgg_card
from monitoring import alert, heartbeat

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("futgg_player_sync")

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not found")

HEADLESS = os.getenv("PLAYWRIGHT_HEADLESS", "true").strip().lower() in {"1", "true", "yes", "on"}
TIMEOUT_MS = max(5000, int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "45000")))
PLAYER_LIMIT = max(0, int(os.getenv("FUTGG_PLAYER_LIMIT", "0")))

# Pagination (replaces scroll-based discovery entirely - see
# collect_listing_urls()). Two page caps, selected by FULL_SCAN: a small
# one for the normal daily run (new cards appear at the front of the
# listing, so a handful of pages is enough) and a large one for an
# occasional full-catalogue sync (~350+ real pages as of writing).
FULL_SCAN = os.getenv("FUTGG_LISTING_FULL_SCAN", "false").strip().lower() in {"1", "true", "yes", "on"}
MAX_PAGES_DAILY = max(1, int(os.getenv("FUTGG_LISTING_MAX_PAGES", "5")))
MAX_PAGES_FULL = max(1, int(os.getenv("FUTGG_LISTING_MAX_PAGES_FULL", "400")))
MAX_PAGES = MAX_PAGES_FULL if FULL_SCAN else MAX_PAGES_DAILY
# Same name/default as before scrolling was removed - now counts
# consecutive PAGES with zero new cards instead of scroll rounds, same
# early-stop role either way.
IDLE_ROUNDS = max(1, int(os.getenv("FUTGG_LISTING_IDLE_ROUNDS", "4")))
PAGE_DELAY = max(0.0, float(os.getenv("FUTGG_LISTING_PAGE_DELAY", "0.5")))

# FUT.GG's listing is client-rendered - the previous page's cards can
# still be attached for a moment after a query-param-only navigation
# while new content is still being fetched (confirmed live: a full-scan
# run oscillated 30-new/0-new across pages instead of accumulating
# steadily). collect_listing_urls() polls up to this many times, waiting
# this long between attempts, until the visible cards differ from the
# previous page's - see its own docstring.
LISTING_STABILIZE_MAX_ATTEMPTS = max(1, int(os.getenv("FUTGG_LISTING_STABILIZE_ATTEMPTS", "6")))
LISTING_STABILIZE_POLL_MS = max(50, int(os.getenv("FUTGG_LISTING_STABILIZE_POLL_MS", "400")))

REFRESH_EXISTING = os.getenv("FUTGG_REFRESH_EXISTING", "false").strip().lower() in {"1", "true", "yes", "on"}
REQUEST_DELAY = max(0.0, float(os.getenv("FUTGG_PLAYER_REQUEST_DELAY", "0.35")))
OVERLAP_LOCK_KEY = int(os.getenv("FUTGG_PLAYER_LOCK_KEY", "7741021"))

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


async def ensure_schema(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS futgg_players (
            source_card_id BIGINT PRIMARY KEY,
            source_player_id BIGINT NOT NULL,
            source_slug TEXT NOT NULL,
            source_url TEXT NOT NULL,
            game_year SMALLINT NOT NULL,

            name TEXT,
            rating SMALLINT,
            primary_position TEXT,
            alternate_positions TEXT[] NOT NULL DEFAULT '{}',
            rarity TEXT,
            squad TEXT,
            price_tier TEXT NOT NULL DEFAULT 'bronze',

            club TEXT,
            league TEXT,
            nation TEXT,
            club_source_id INTEGER,
            league_source_id INTEGER,
            nation_source_id INTEGER,

            height_cm SMALLINT,
            weight_kg SMALLINT,
            foot TEXT,
            skill_moves SMALLINT,
            weak_foot SMALLINT,
            accelerate_type TEXT,
            body_type TEXT,
            real_face BOOLEAN,
            shirt_number SMALLINT,
            age SMALLINT,

            pace SMALLINT,
            shooting SMALLINT,
            passing SMALLINT,
            dribbling SMALLINT,
            defending SMALLINT,
            physicality SMALLINT,
            diving SMALLINT,
            handling SMALLINT,
            kicking SMALLINT,
            reflexes SMALLINT,
            speed SMALLINT,
            positioning SMALLINT,

            player_image_url TEXT,
            card_design_image_url TEXT,
            club_image_url TEXT,
            league_image_url TEXT,
            nation_image_url TEXT,

            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            metadata_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            price_updated_at TIMESTAMPTZ,
            next_price_due_at TIMESTAMPTZ,
            last_price_status TEXT,
            metadata_warnings TEXT[] NOT NULL DEFAULT '{}'
        )
        """
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_players_price_due_idx "
        "ON futgg_players (next_price_due_at, price_tier) WHERE is_active"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_players_player_idx ON futgg_players (source_player_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS futgg_players_name_idx ON futgg_players (LOWER(name))"
    )


async def dismiss_cookie_banner(page) -> None:
    for selector in (
        "#onetrust-accept-btn-handler",
        "#onetrust-reject-all-handler",
        "button:has-text('Accept all')",
        "button:has-text('Reject all')",
    ):
        try:
            button = page.locator(selector).first
            if await button.is_visible(timeout=500):
                await button.click(timeout=2000)
                return
        except Exception:
            pass


async def _read_card_hrefs(page) -> set[str]:
    """The set of canonical card URLs currently visible on the page."""
    hrefs = await page.locator("a[href]").evaluate_all(
        "nodes => nodes.map(node => node.getAttribute('href'))"
    )
    matched: set[str] = set()
    for href in hrefs:
        if not href:
            continue
        absolute = urljoin(BASE_URL, href)
        parsed = urlparse(absolute)
        if CARD_URL_RE.match(parsed.path):
            matched.add(urljoin(BASE_URL, parsed.path))
    return matched


async def collect_listing_urls(page) -> list[str]:
    """Discovers card URLs via real pagination (?page=1, ?page=2, ...) -
    no window.scrollTo() or scroll-settle waits anywhere. Each page is a
    direct navigation; MAX_PAGES caps how many pages are visited (small
    for a daily run, large for FULL_SCAN - see module docstring), and
    IDLE_ROUNDS consecutive pages with zero new cards stops early
    regardless of the cap, since that's the real signal pagination has
    run past the end of the catalogue."""
    seen: dict[str, None] = {}
    idle_pages = 0
    previous_page_hrefs: set[str] = set()

    for page_number in range(1, MAX_PAGES + 1):
        listing_url = f"{BASE_URL}/players/new/?page={page_number}"
        response = await page.goto(listing_url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        status = response.status if response else 0
        if status != 200:
            if page_number == 1:
                raise RuntimeError(f"FUT.GG listing returned HTTP {status}")
            log.warning("FUT.GG listing page %d returned HTTP %d - stopping pagination", page_number, status)
            break

        await dismiss_cookie_banner(page)
        try:
            await page.locator("a[href*='/players/']").first.wait_for(state="attached", timeout=TIMEOUT_MS)
        except PlaywrightTimeoutError as exc:
            if page_number == 1:
                raise RuntimeError("No card links appeared on FUT.GG listing") from exc
            log.info("No card links on listing page %d - treating as end of catalogue", page_number)
            break

        # FUT.GG's listing is client-rendered - a query-param-only
        # navigation can leave the PREVIOUS page's cards still attached
        # for a moment while the new content is still being fetched, so
        # the "attached" wait above only confirms *some* /players/ link
        # exists, not that it's THIS page's content. Confirmed live: a
        # full-scan run oscillated between 30-new and 0-new pages instead
        # of steadily accumulating, because some pages were read before
        # their content had actually swapped in. Poll until the visible
        # card hrefs differ from the previous page's, or give up after a
        # bounded number of attempts - a genuine end-of-catalogue page
        # that keeps re-serving the same content still correctly settles
        # into "0 new" once attempts are exhausted; this only adds
        # patience for the transient render-timing case.
        current_hrefs = await _read_card_hrefs(page)
        for _ in range(LISTING_STABILIZE_MAX_ATTEMPTS - 1):
            if current_hrefs != previous_page_hrefs or not previous_page_hrefs:
                break
            await page.wait_for_timeout(LISTING_STABILIZE_POLL_MS)
            current_hrefs = await _read_card_hrefs(page)

        before = len(seen)
        for card_url in current_hrefs:
            seen[card_url] = None
        new_this_page = len(seen) - before
        previous_page_hrefs = current_hrefs

        log.info(
            "Listing page %d: %d new cards, %d unique total", page_number, new_this_page, len(seen)
        )

        if PLAYER_LIMIT and len(seen) >= PLAYER_LIMIT:
            break

        if new_this_page == 0:
            idle_pages += 1
            if idle_pages >= IDLE_ROUNDS:
                log.info("%d consecutive pages with no new cards - stopping pagination", idle_pages)
                break
        else:
            idle_pages = 0

        if page_number < MAX_PAGES and PAGE_DELAY:
            await asyncio.sleep(random.uniform(PAGE_DELAY * 0.7, PAGE_DELAY * 1.3))

    urls = list(seen)
    return urls[:PLAYER_LIMIT] if PLAYER_LIMIT else urls


async def existing_ids(conn: asyncpg.Connection, urls: list[str]) -> set[int]:
    ids = []
    for url in urls:
        match = CARD_URL_RE.match(urlparse(url).path)
        if match:
            ids.append(int(match.group("card_id")))
    if not ids:
        return set()
    rows = await conn.fetch(
        "SELECT source_card_id FROM futgg_players WHERE source_card_id = ANY($1::bigint[])", ids
    )
    return {int(row["source_card_id"]) for row in rows}


async def upsert_card(conn: asyncpg.Connection, card: FutggCard) -> None:
    data = asdict(card)
    price_tier = classify_price_tier(card)
    await conn.execute(
        """
        INSERT INTO futgg_players (
            source_card_id, source_player_id, source_slug, source_url, game_year,
            name, rating, primary_position, alternate_positions, rarity, squad, price_tier,
            club, league, nation, club_source_id, league_source_id, nation_source_id,
            height_cm, weight_kg, foot, skill_moves, weak_foot, accelerate_type,
            body_type, real_face, shirt_number, age,
            pace, shooting, passing, dribbling, defending, physicality,
            diving, handling, kicking, reflexes, speed, positioning,
            player_image_url, card_design_image_url, club_image_url, league_image_url,
            nation_image_url, last_seen_at, metadata_updated_at, next_price_due_at,
            metadata_warnings
        ) VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
            $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,
            $35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,NOW(),NOW(),NOW(),$46
        )
        ON CONFLICT (source_card_id) DO UPDATE SET
            source_player_id = EXCLUDED.source_player_id,
            source_slug = EXCLUDED.source_slug,
            source_url = EXCLUDED.source_url,
            game_year = EXCLUDED.game_year,
            name = COALESCE(EXCLUDED.name, futgg_players.name),
            rating = COALESCE(EXCLUDED.rating, futgg_players.rating),
            primary_position = COALESCE(EXCLUDED.primary_position, futgg_players.primary_position),
            alternate_positions = EXCLUDED.alternate_positions,
            rarity = COALESCE(EXCLUDED.rarity, futgg_players.rarity),
            squad = COALESCE(EXCLUDED.squad, futgg_players.squad),
            price_tier = EXCLUDED.price_tier,
            club = COALESCE(EXCLUDED.club, futgg_players.club),
            league = COALESCE(EXCLUDED.league, futgg_players.league),
            nation = COALESCE(EXCLUDED.nation, futgg_players.nation),
            club_source_id = COALESCE(EXCLUDED.club_source_id, futgg_players.club_source_id),
            league_source_id = COALESCE(EXCLUDED.league_source_id, futgg_players.league_source_id),
            nation_source_id = COALESCE(EXCLUDED.nation_source_id, futgg_players.nation_source_id),
            height_cm = COALESCE(EXCLUDED.height_cm, futgg_players.height_cm),
            weight_kg = COALESCE(EXCLUDED.weight_kg, futgg_players.weight_kg),
            foot = COALESCE(EXCLUDED.foot, futgg_players.foot),
            skill_moves = COALESCE(EXCLUDED.skill_moves, futgg_players.skill_moves),
            weak_foot = COALESCE(EXCLUDED.weak_foot, futgg_players.weak_foot),
            accelerate_type = COALESCE(EXCLUDED.accelerate_type, futgg_players.accelerate_type),
            body_type = COALESCE(EXCLUDED.body_type, futgg_players.body_type),
            real_face = COALESCE(EXCLUDED.real_face, futgg_players.real_face),
            shirt_number = COALESCE(EXCLUDED.shirt_number, futgg_players.shirt_number),
            age = COALESCE(EXCLUDED.age, futgg_players.age),
            pace = COALESCE(EXCLUDED.pace, futgg_players.pace),
            shooting = COALESCE(EXCLUDED.shooting, futgg_players.shooting),
            passing = COALESCE(EXCLUDED.passing, futgg_players.passing),
            dribbling = COALESCE(EXCLUDED.dribbling, futgg_players.dribbling),
            defending = COALESCE(EXCLUDED.defending, futgg_players.defending),
            physicality = COALESCE(EXCLUDED.physicality, futgg_players.physicality),
            diving = COALESCE(EXCLUDED.diving, futgg_players.diving),
            handling = COALESCE(EXCLUDED.handling, futgg_players.handling),
            kicking = COALESCE(EXCLUDED.kicking, futgg_players.kicking),
            reflexes = COALESCE(EXCLUDED.reflexes, futgg_players.reflexes),
            speed = COALESCE(EXCLUDED.speed, futgg_players.speed),
            positioning = COALESCE(EXCLUDED.positioning, futgg_players.positioning),
            player_image_url = COALESCE(EXCLUDED.player_image_url, futgg_players.player_image_url),
            card_design_image_url = COALESCE(EXCLUDED.card_design_image_url, futgg_players.card_design_image_url),
            club_image_url = COALESCE(EXCLUDED.club_image_url, futgg_players.club_image_url),
            league_image_url = COALESCE(EXCLUDED.league_image_url, futgg_players.league_image_url),
            nation_image_url = COALESCE(EXCLUDED.nation_image_url, futgg_players.nation_image_url),
            is_active = TRUE,
            last_seen_at = NOW(),
            metadata_updated_at = NOW(),
            metadata_warnings = EXCLUDED.metadata_warnings
        """,
        data["source_card_id"], data["source_player_id"], data["source_slug"], data["source_url"], data["game_year"],
        data["name"], data["rating"], data["primary_position"], data["alternate_positions"], data["rarity"], data["squad"], price_tier,
        data["club"], data["league"], data["nation"], data["club_source_id"], data["league_source_id"], data["nation_source_id"],
        data["height_cm"], data["weight_kg"], data["foot"], data["skill_moves"], data["weak_foot"], data["accelerate_type"],
        data["body_type"], data["real_face"], data["shirt_number"], data["age"],
        data["pace"], data["shooting"], data["passing"], data["dribbling"], data["defending"], data["physicality"],
        data["diving"], data["handling"], data["kicking"], data["reflexes"], data["speed"], data["positioning"],
        data["player_image_url"], data["card_design_image_url"], data["club_image_url"], data["league_image_url"], data["nation_image_url"],
        data["parse_warnings"],
    )


async def crawl_once() -> None:
    lock_conn = await asyncpg.connect(DATABASE_URL)
    got_lock = await lock_conn.fetchval("SELECT pg_try_advisory_lock($1)", OVERLAP_LOCK_KEY)
    if not got_lock:
        log.info("Previous FUT.GG player sync still running; skipping")
        await lock_conn.close()
        return

    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)
    playwright = browser = context = None
    stats: dict[str, int] = {"discovered": 0, "new": 0, "updated": 0, "failed": 0, "skipped_existing": 0}
    try:
        async with pool.acquire() as conn:
            await ensure_schema(conn)

        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-GB",
            viewport={"width": 1440, "height": 1000},
        )
        page = await context.new_page()

        urls = await collect_listing_urls(page)
        stats["discovered"] = len(urls)
        async with pool.acquire() as conn:
            known = await existing_ids(conn, urls)

        targets: list[tuple[str, bool]] = []
        for url in urls:
            match = CARD_URL_RE.match(urlparse(url).path)
            if not match:
                continue
            card_id = int(match.group("card_id"))
            existed = card_id in known
            if existed and not REFRESH_EXISTING:
                stats["skipped_existing"] += 1
                continue
            targets.append((url, existed))

        log.info(
            "Discovered=%d known=%d targets=%d refresh_existing=%s",
            len(urls), len(known), len(targets), REFRESH_EXISTING,
        )

        for index, (url, existed) in enumerate(targets, start=1):
            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
                status = response.status if response else 0
                if status != 200:
                    raise RuntimeError(f"HTTP {status}")
                await page.locator(".fc-card").first.wait_for(state="attached", timeout=15000)
                await page.wait_for_timeout(800)
                card = parse_futgg_card(await page.content(), url)
                async with pool.acquire() as conn:
                    await upsert_card(conn, card)
                stats["updated" if existed else "new"] += 1
                log.info(
                    "[%d/%d] upserted id=%s name=%r rating=%s rarity=%r tier=%s",
                    index, len(targets), card.source_card_id, card.name, card.rating,
                    card.rarity, classify_price_tier(card),
                )
            except Exception as exc:
                stats["failed"] += 1
                log.warning("[%d/%d] failed %s: %s", index, len(targets), url, exc)

            if REQUEST_DELAY:
                await asyncio.sleep(random.uniform(REQUEST_DELAY * 0.7, REQUEST_DELAY * 1.3))

        ok = stats["failed"] == 0 or (stats["new"] + stats["updated"]) > 0
        async with pool.acquire() as conn:
            await heartbeat(
                conn,
                "futgg_player_sync",
                ok=ok,
                detail=" ".join(f"{key}={value}" for key, value in stats.items()),
            )
        if not ok:
            await alert(f"futgg_player_sync failed: {stats}")
        log.info("Run complete: %s", stats)

    finally:
        if context:
            await context.close()
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()
        await pool.close()
        await lock_conn.execute("SELECT pg_advisory_unlock($1)", OVERLAP_LOCK_KEY)
        await lock_conn.close()


if __name__ == "__main__":
    try:
        asyncio.run(crawl_once())
    except Exception as exc:
        log.exception("futgg_player_sync failed: %s", exc)
        sys.exit(1)
