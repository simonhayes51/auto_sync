import os
import time
import json
import psycopg2
import requests
from bs4 import BeautifulSoup

# ✅ Railway PostgreSQL connection
DATABASE_URL = "postgresql://postgres:FiwuZKPRyUKvzWMMqTWxfpRGtZrOYLCa@shuttle.proxy.rlwy.net:19669/railway"

# ✅ Connect to DB
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ✅ Create table if it doesn't exist
cur.execute("""
CREATE TABLE IF NOT EXISTS fut_players (
    id SERIAL PRIMARY KEY,
    name TEXT,
    rating INT,
    version TEXT,
    image_url TEXT
);
""")
conn.commit()

BASE_URL = "https://www.fut.gg/players/?page="

def scrape_players(page):
    url = f"{BASE_URL}{page}"
    print(f"🔄 Fetching page {page}...")
    r = requests.get(url, timeout=15)

    if r.status_code != 200:
        print(f"⚠️ Failed page {page}: {r.status_code}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    players = []

    for card in soup.select("a.group\\/player"):
        try:
            img_tag = card.select_one("img")
            name = img_tag["alt"].split("-")[0].strip()
            rating = int(img_tag["alt"].split("-")[1].strip())
            version = img_tag["alt"].split("-")[2].strip() if "-" in img_tag["alt"] else "Unknown"
            image_url = img_tag["src"]

            players.append((name, rating, version, image_url))
        except:
            continue

    return players

def insert_players(players):
    for p in players:
        cur.execute("""
            INSERT INTO fut_players (name, rating, version, image_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, p)
    conn.commit()

def main():
    total_inserted = 0
    for page in range(1, 335):  # 334 pages total
        players = scrape_players(page)
        if not players:
            continue
        insert_players(players)
        total_inserted += len(players)
        print(f"✅ Page {page}: Inserted {len(players)} players. Total: {total_inserted}")

        # Small pause to reduce laptop stress
        time.sleep(1.5)

    print(f"\n🎯 Completed! Inserted ~{total_inserted} players.")

if __name__ == "__main__":
    main()
