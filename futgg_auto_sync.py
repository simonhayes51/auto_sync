import os
import time
import requests
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://www.fut.gg/players/?page="

def create_table():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            id BIGINT PRIMARY KEY,
            name TEXT,
            rating INT,
            version TEXT,
            image_url TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def fetch_page(page):
    url = f"{BASE_URL}{page}"
    response = requests.get(url, timeout=15)
    if response.status_code != 200:
        print(f"⚠️ Failed to fetch page {page} — Status: {response.status_code}")
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Check new FUT.GG structure — updated selectors
    players = []
    for card in soup.select("a.player-item"):  # FUT.GG uses <a> for each card
        try:
            player_id = int(card["href"].split("/")[-1])
            name = card.select_one("div.player-name").text.strip()
            rating = int(card.select_one("div.player-rating").text.strip())
            version = card.select_one("div.player-card-variant").text.strip()
            img_tag = card.select_one("img")
            image_url = img_tag["src"] if img_tag else None
            players.append((player_id, name, rating, version, image_url))
        except Exception as e:
            continue
    return players

def save_players(players):
    if not players:
        return 0
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    query = """
        INSERT INTO players (id, name, rating, version, image_url)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
    """
    execute_values(cur, query, players)
    inserted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    return inserted

def run_sync():
    create_table()
    page = 1
    while True:
        players = fetch_page(page)
        if not players:
            break
        inserted = save_players(players)
        print(f"✅ Page {page}: Found {len(players)} | Inserted {inserted} | Skipped {len(players) - inserted}")
        page += 1
        time.sleep(0.5)
    print("🎯 Sync complete!")

if __name__ == "__main__":
    while True:
        print(f"\n🚀 Starting auto-sync at {datetime.utcnow()} UTC")
        run_sync()
        print("⏳ Waiting 10 minutes before next sync...\n")
        time.sleep(600)
