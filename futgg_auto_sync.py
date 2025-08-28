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
from typing import Optional, Dict, List, Set, Tuple
from aiohttp import web  # health server

# ------------------ CONFIG ------------------ #
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

PRICE_API = "https://www.fut.gg/api/fut/player-prices/25/{}"
META_API  = "https://www.fut.gg/api/fut/player-item-definitions/25/{}"
NEW_URLS  = [
    "https://www.fut.gg/players/new/?page={}",
    "https://www.fut.gg/players/?sort=new&page={}",
]

NEW_PAGES          = int(os.getenv("NEW_PAGES", "5"))           # discovery pages per URL
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT", "15"))    # seconds
BACKFILL_LIMIT     = int(os.getenv("BACKFILL_LIMIT", "300"))    # rows per backfill query
UPDATE_CHUNK_SIZE  = int(os.getenv("UPDATE_CHUNK_SIZE", "100")) # rows per DB executemany chunk

UA_HEADERS  = {
    "User-Agent": "Mozilla/5.0 (compatible; NewPlayersSync/2.3)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Refer