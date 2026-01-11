# scripts/sync_futgg_players.py
import os
import re
import asyncio
import asyncpg
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

# ---------------- Config ----------------
GAME = os.getenv("GAME", "26").strip()  # FC26 default
BASE_URL = f"https://www.fut.gg/players/?page={{}}"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Safari/537.36"

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not found! Set it in Railway → Variables.")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA})

# Match: /players/<player_slug>/<GAME>-<card_id>/
HREF_RE = re.compile(rf'^/players/([^/]+)/{re.escape(GAME)}-(\d+)/?$', re.IGNORECASE)

# Optional fallback if FUTGG returns /players/<GAME>-<card_id>/
HREF_CARD_ONLY_RE = re.compile(rf'^/players/(?:{re.escape(GAME)}-)?(\d+)/?$', re.IGNORECASE)

# Try to parse "Name - 91 - Gold Rare" if present, but do NOT require it
def parse_alt_text(alt_text: str):
    if not alt_text:
        return None, None, None
    parts = [p.strip() for p in alt_text.split(" - ")]
    if len(parts) >= 2 and parts[1].isdigit():
        name = parts[0]
        rating = int(parts[1])
        version = " - ".join(parts[2:]).strip() if len(parts) > 2 else None
        return name, rating, version
    return alt_text.strip() or None, None, None

def fetch_players_from_page(page_number: int):
    url = BASE_URL.format(page_number)
    print(f"🌐 Fetching: {url}")

    try:
        resp = SESSION.get(url, timeout=25)
    except Exception as e:
        print(f"⚠️ Request failed: {e}")
        return []

    if resp.status_code != 200:
        print(f"⚠️ Failed page {page_number}: status={resp.status_code}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select('a[href^="/players/"]')

    players = {}
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href:
            continue

        m = HREF_RE.match(href)
        card_id = None
        player_slug = None

        if m:
            player_slug = m.group(1)
            card_id = m.group(2)
        else:
            m2 = HREF_CARD_ONLY_RE.match(href)
            if m2:
                card_id = m2.group(1)
                player_slug = None
            else:
                continue

        # Pull whatever we can from img, but don't depend on it
        img = a.select_one("img")
        img_url = None
        name = None
        rating = None
        version = None

        if img:
            img_url = img.get("src") or img.get("data-src") or None
            alt_text = (img.get("alt") or "").strip()
            n, r, v = parse_alt_text(alt_text)
            name, rating, version = n, r, v

        # Construct the canonical player_url
        if player_slug:
            player_url = f"https://www.fut.gg/players/{player_slug}/{GAME}-{card_id}/"
        else:
            player_url = f"https://www.fut.gg/players/{GAME}-{card_id}/"

        players[card_id] = {
            "card_id": int(card_id),
            "name": name,             # may be None; enrichment script fills this
            "rating": rating,         # may be None
            "version": version,       # may be None
            "image_url": img_url,     # may be None
            "created_at": datetime.now(timezone.utc),
            "player_slug": player_slug,
            "player_url": player_url,
        }

    return list(players.values())

async def ensure_unique(conn: asyncpg.Connection):
    # Ensures ON CONFLICT(card_id) works
    await conn.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname='public'
                  AND tablename='fut_players'
                  AND indexdef ILIKE '%UNIQUE%'
                  AND indexdef ILIKE '%(card_id)%'
            ) THEN
                CREATE UNIQUE INDEX fut_players_card_id_key ON public.fut_players(card_id);
            END IF;
        END $$;
    """)

async def sync_players():
    print(f"\n🚀 Starting FULL RESYNC (GAME={GAME}) at {datetime.now(timezone.utc)} UTC")
    all_players = []
    page = 1
    empty_streak = 0

    # Stop when 2 consecutive pages produce 0 parsed cards
    while True:
        players = fetch_players_from_page(page)
        if not players:
            empty_streak += 1
            if empty_streak >= 2:
                print("✅ Pagination complete.")
                break
        else:
            empty_streak = 0
            all_players.extend(players)
            print(f"📦 Page {page}: +{len(players)} (total {len(all_players)})")

        page += 1
        await asyncio.sleep(0.5)

    print(f"🔍 Total players found: {len(all_players)}")
    if not all_players:
        print("⚠️ No players fetched; aborting.")
        return

    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await ensure_unique(conn)

        # IMPORTANT:
        # Your table currently has NOT NULL constraints on name in some setups.
        # To avoid failing inserts, we COALESCE name to '' and rating to NULL-safe default.
        # But ideally: remove NOT NULL from name/rating, and let enrichment fill.
        stmt = """
        INSERT INTO public.fut_players
            (card_id, name, rating, version, image_url, created_at, player_slug, player_url)
        VALUES
            ($1,      $2,   $3,     $4,      $5,        $6,         $7,          $8)
        ON CONFLICT (card_id) DO UPDATE
        SET name        = COALESCE(EXCLUDED.name, public.fut_players.name),
            rating      = COALESCE(EXCLUDED.rating, public.fut_players.rating),
            version     = COALESCE(EXCLUDED.version, public.fut_players.version),
            image_url   = COALESCE(EXCLUDED.image_url, public.fut_players.image_url),
            created_at  = public.fut_players.created_at, -- keep original
            player_slug = COALESCE(EXCLUDED.player_slug, public.fut_players.player_slug),
            player_url  = COALESCE(EXCLUDED.player_url, public.fut_players.player_url);
        """

        rows = []
        for p in all_players:
            # If your DB still has name NOT NULL, avoid crashing:
            safe_name = p["name"] if p["name"] else ""
            rows.append((
                p["card_id"],
                safe_name,
                p["rating"],
                p["version"],
                p["image_url"],
                p["created_at"],
                p["player_slug"],
                p["player_url"],
            ))

        await conn.executemany(stmt, rows)
        print(f"🎯 Upserted {len(rows)} players into fut_players.")
        print("✅ Done. Next: run your futgg_auto_sync.py enrichment to fill missing fields.")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(sync_players())