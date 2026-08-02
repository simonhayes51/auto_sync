#!/usr/bin/env python
"""
FUT.GG 10-card catalogue test for local use or Railway.

Workflow:
1. Open https://www.fut.gg/players/new/
2. Collect the first N unique FC 26 card URLs.
3. Visit each card page.
4. Extract as much stable, useful information as possible.
5. Save JSON and CSV reports.

No database writes are performed.

Environment variables:
    FUTGG_TEST_LIMIT=10
    PLAYWRIGHT_HEADLESS=true
    PLAYWRIGHT_TIMEOUT_MS=45000
    FUTGG_SAVE_HTML=false
    FUTGG_SAVE_SCREENSHOTS=false
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright


BASE_URL = "https://www.fut.gg"
LISTING_URL = f"{BASE_URL}/players/new/"
OUTPUT_DIR = Path("futgg_catalogue_test_output")

LIMIT = max(1, int(os.getenv("FUTGG_TEST_LIMIT", "10")))
TIMEOUT_MS = max(5_000, int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "45000")))

HEADLESS_RAW = os.getenv("PLAYWRIGHT_HEADLESS", "true").strip().lower()
HEADLESS = HEADLESS_RAW in {"1", "true", "yes", "on"}

SAVE_HTML = os.getenv("FUTGG_SAVE_HTML", "false").strip().lower() in {
    "1", "true", "yes", "on"
}
SAVE_SCREENSHOTS = os.getenv(
    "FUTGG_SAVE_SCREENSHOTS", "false"
).strip().lower() in {"1", "true", "yes", "on"}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

CARD_URL_RE = re.compile(
    r"^/players/(?P<player_id>\d+)-(?P<slug>[^/]+)/"
    r"(?P<game_year>\d+)-(?P<card_id>\d+)/?$"
)

RELATIVE_TIME_RE = re.compile(
    r"^\s*(?P<amount>\d+)\s+"
    r"(?P<unit>second|minute|hour|day)s?\s+ago\s*$",
    re.IGNORECASE,
)


@dataclass
class SaleRow:
    age_text: str
    price: int
    approximate_sold_at_utc: str | None
    row_number: int


@dataclass
class CardRecord:
    source: str = "fut.gg"
    source_url: str = ""
    source_player_id: int | None = None
    source_card_id: int | None = None
    source_slug: str | None = None
    game_year: int | None = None

    page_status: int | None = None
    page_title: str | None = None
    scraped_at_utc: str = ""

    name: str | None = None
    full_name: str | None = None
    rating: int | None = None
    primary_position: str | None = None
    alternate_positions: list[str] = field(default_factory=list)
    rarity_or_version: str | None = None

    pace: int | None = None
    shooting: int | None = None
    passing: int | None = None
    dribbling: int | None = None
    defending: int | None = None
    physicality: int | None = None

    diving: int | None = None
    handling: int | None = None
    kicking: int | None = None
    reflexes: int | None = None
    speed: int | None = None
    positioning: int | None = None

    foot: str | None = None
    skill_moves: int | None = None
    weak_foot: int | None = None
    height: str | None = None
    weight: str | None = None
    body_type: str | None = None

    nation_id: int | None = None
    nation_image_url: str | None = None
    league_id: int | None = None
    league_image_url: str | None = None
    club_id: int | None = None
    club_image_url: str | None = None
    player_image_url: str | None = None
    card_design_image_url: str | None = None

    lowest_bin: int | None = None
    lowest_bin_age: str | None = None
    price_range_text: str | None = None
    price_range_low: int | None = None
    price_range_high: int | None = None

    sbc_price: int | None = None
    community_upvote_percent: int | None = None
    community_downvote_percent: int | None = None
    comment_count: int | None = None

    recent_sales_count: int = 0
    recent_sales_latest: int | None = None
    recent_sales_low: int | None = None
    recent_sales_high: int | None = None
    recent_sales_average: int | None = None
    recent_sales: list[SaleRow] = field(default_factory=list)

    recommended_playstyles: list[str] = field(default_factory=list)
    all_visible_text_excerpt: str | None = None

    parse_warnings: list[str] = field(default_factory=list)
    error: str | None = None


def clean_text(value: str | None) -> str:
    return " ".join((value or "").split())


def parse_int(value: str | None) -> int | None:
    if not value:
        return None

    cleaned = clean_text(value).lower().replace(",", "")
    match = re.search(r"(?<![\d.])(\d+(?:\.\d+)?)\s*([km]?)\b", cleaned)

    if not match:
        return None

    number = float(match.group(1))
    suffix = match.group(2)

    if suffix == "k":
        number *= 1_000
    elif suffix == "m":
        number *= 1_000_000

    return int(number)


def parse_percent(value: str | None) -> int | None:
    if not value:
        return None

    match = re.search(r"(\d{1,3})\s*%", value)
    return int(match.group(1)) if match else None


def relative_time_to_utc(value: str, now: datetime) -> datetime | None:
    match = RELATIVE_TIME_RE.fullmatch(value)

    if not match:
        return None

    amount = int(match.group("amount"))
    unit = match.group("unit").lower()

    delta = {
        "second": timedelta(seconds=amount),
        "minute": timedelta(minutes=amount),
        "hour": timedelta(hours=amount),
        "day": timedelta(days=amount),
    }[unit]

    return now - delta


def image_id_from_url(url: str | None, asset_type: str) -> int | None:
    if not url:
        return None

    match = re.search(rf"/{re.escape(asset_type)}/(\d+)[.\-]", url)
    return int(match.group(1)) if match else None


def find_exact_heading(soup: BeautifulSoup, text: str):
    wanted = text.casefold()

    return soup.find(
        lambda tag: tag.name in {"h1", "h2", "h3", "h4", "span", "div"}
        and clean_text(tag.get_text(" ", strip=True)).casefold() == wanted
    )


def nearby_value_after_label(
    soup: BeautifulSoup,
    labels: list[str],
    max_ancestor_levels: int = 3,
) -> str | None:
    """
    Generic fallback for detail blocks such as:
        <div><span>Foot</span><span>Right</span></div>
    """
    wanted = {label.casefold() for label in labels}

    label_node = soup.find(
        lambda tag: tag.name in {"div", "span", "dt", "th", "p"}
        and clean_text(tag.get_text(" ", strip=True)).casefold() in wanted
    )

    if label_node is None:
        return None

    current = label_node

    for _ in range(max_ancestor_levels):
        parent = current.parent

        if parent is None:
            break

        texts = [
            clean_text(node.get_text(" ", strip=True))
            for node in parent.find_all(
                ["span", "div", "dd", "td", "p"],
                recursive=False,
            )
        ]

        label_text = clean_text(label_node.get_text(" ", strip=True))

        for text in texts:
            if text and text.casefold() != label_text.casefold():
                if len(text) <= 100:
                    return text

        current = parent

    return None


def parse_card_visual(soup: BeautifulSoup, record: CardRecord) -> None:
    card = soup.select_one(".fc-card")

    if card is None:
        record.parse_warnings.append("fc-card element not found")
        return

    # Card name
    name_candidates = card.select(
        ".font-cruyff-bold, [class*='font-cruyff-bold']"
    )
    for candidate in name_candidates:
        text = clean_text(candidate.get_text(" ", strip=True))
        if text and not text.isdigit():
            record.name = text
            break

    # Overall / position: use the visually prominent card text values.
    card_texts = [
        clean_text(node.get_text(" ", strip=True))
        for node in card.find_all(["div", "span"])
    ]

    for index, text in enumerate(card_texts):
        if re.fullmatch(r"\d{2}", text):
            value = int(text)
            if 40 <= value <= 99:
                next_text = card_texts[index + 1] if index + 1 < len(card_texts) else ""
                if re.fullmatch(r"[A-Z]{1,4}", next_text):
                    record.rating = value
                    record.primary_position = next_text
                    break

    # Six outfield face stats.
    stat_labels = {
        "PAC": "pace",
        "SHO": "shooting",
        "PAS": "passing",
        "DRI": "dribbling",
        "DEF": "defending",
        "PHY": "physicality",
        "DIV": "diving",
        "HAN": "handling",
        "KIC": "kicking",
        "REF": "reflexes",
        "SPD": "speed",
        "POS": "positioning",
    }

    for label, field_name in stat_labels.items():
        label_node = card.find(
            lambda tag: clean_text(tag.get_text(" ", strip=True)) == label
        )

        if label_node is None:
            continue

        parent = label_node.parent
        if parent is None:
            continue

        numbers = [
            int(value)
            for value in re.findall(
                r"\b(\d{1,2})\b",
                clean_text(parent.get_text(" ", strip=True)),
            )
        ]

        numbers = [number for number in numbers if 0 <= number <= 99]

        if numbers:
            setattr(record, field_name, numbers[-1])

    # Images and source IDs.
    for image in card.find_all("img", src=True):
        src = urljoin(BASE_URL, image["src"])
        alt = clean_text(image.get("alt"))

        if "/player-item/" in src:
            record.player_image_url = src
        elif "/rarities-" in src:
            record.card_design_image_url = src
        elif alt.casefold() == "nation":
            record.nation_image_url = src
            record.nation_id = image_id_from_url(src, "nation")
        elif alt.casefold() == "league":
            record.league_image_url = src
            record.league_id = image_id_from_url(src, "league")
        elif alt.casefold() == "club":
            record.club_image_url = src
            record.club_id = image_id_from_url(src, "club")

    # Alternate-position pills on the right of the card.
    positions: list[str] = []
    for node in card.find_all(["div", "span"]):
        text = clean_text(node.get_text(" ", strip=True))
        if re.fullmatch(
            r"(?:GK|CB|LB|RB|LWB|RWB|CDM|CM|CAM|LM|RM|LW|RW|CF|ST)",
            text,
        ):
            positions.append(text)

    unique_positions = list(dict.fromkeys(positions))
    if record.primary_position in unique_positions:
        unique_positions.remove(record.primary_position)

    record.alternate_positions = unique_positions


def parse_title(record: CardRecord) -> None:
    title = record.page_title or ""

    # Example:
    # "Joshua King FUTTIES 95 OVR ST - EA FC 26 - FUT.GG"
    match = re.match(
        r"(?P<name>.+?)\s+"
        r"(?P<version>.+?)\s+"
        r"(?P<rating>\d{2})\s+OVR\s+"
        r"(?P<position>[A-Z]{1,4})\s+-\s+EA FC",
        title,
        re.IGNORECASE,
    )

    if not match:
        return

    record.full_name = clean_text(match.group("name"))
    record.rarity_or_version = clean_text(match.group("version"))

    if record.rating is None:
        record.rating = int(match.group("rating"))

    if record.primary_position is None:
        record.primary_position = match.group("position").upper()


def parse_misc_details(soup: BeautifulSoup, record: CardRecord) -> None:
    full_text = clean_text(soup.get_text(" ", strip=True))
    record.all_visible_text_excerpt = full_text[:1_500]

    # Foot
    foot_match = re.search(r"\bFoot\s+(Left|Right)\b", full_text, re.IGNORECASE)
    if foot_match:
        record.foot = foot_match.group(1).title()

    # Skill moves / weak foot. Capture common text arrangements.
    skill_patterns = [
        r"Skill Moves\s*(\d)",
        r"Skills\s*(\d)",
        r"(\d)\s*â\s*Skill",
    ]
    for pattern in skill_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            record.skill_moves = int(match.group(1))
            break

    weak_patterns = [
        r"Weak Foot\s*(\d)",
        r"(\d)\s*â\s*Weak Foot",
    ]
    for pattern in weak_patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            record.weak_foot = int(match.group(1))
            break

    # Card artwork usually displays paired stars as "5 â 4".
    if record.skill_moves is None or record.weak_foot is None:
        star_pair = re.search(r"\b([1-5])\s*â\s*([1-5])\b", full_text)
        if star_pair:
            record.skill_moves = record.skill_moves or int(star_pair.group(1))
            record.weak_foot = record.weak_foot or int(star_pair.group(2))

    height_match = re.search(
        r"\bHeight\s+([^|]{2,40}?)(?=\s+(?:Weight|Foot|Body Type|AcceleRATE|Skills|Weak Foot)\b|$)",
        full_text,
        re.IGNORECASE,
    )
    if height_match:
        record.height = clean_text(height_match.group(1))

    weight_match = re.search(
        r"\bWeight\s+([^|]{1,30}?)(?=\s+(?:Height|Foot|Body Type|AcceleRATE|Skills|Weak Foot)\b|$)",
        full_text,
        re.IGNORECASE,
    )
    if weight_match:
        record.weight = clean_text(weight_match.group(1))

    body_match = re.search(
        r"\bBody Type\s+(.{2,60}?)(?=\s+(?:Height|Weight|Foot|AcceleRATE|Skills|Weak Foot)\b|$)",
        full_text,
        re.IGNORECASE,
    )
    if body_match:
        record.body_type = clean_text(body_match.group(1))

    # Community vote controls.
    upvote = soup.select_one("button[aria-label='Upvote player']")
    downvote = soup.select_one("button[aria-label='Downvote player']")
    comments = soup.select_one("button[aria-label='Jump to comments']")

    if upvote:
        record.community_upvote_percent = parse_percent(upvote.get_text(" ", strip=True))
    if downvote:
        record.community_downvote_percent = parse_percent(downvote.get_text(" ", strip=True))
    if comments:
        record.comment_count = parse_int(comments.get_text(" ", strip=True))

    # SBC marker and amount.
    sbc_img = soup.find("img", alt="SBC")
    if sbc_img and sbc_img.parent:
        record.sbc_price = parse_int(sbc_img.parent.get_text(" ", strip=True))

    # Recommended PlayStyles: collect image alt/title text near that heading.
    heading = find_exact_heading(soup, "Recommended PlayStyles")
    if heading:
        container = heading.parent.parent if heading.parent and heading.parent.parent else heading.parent

        if container:
            names: list[str] = []
            for node in container.find_all(["img", "svg", "span", "div"]):
                value = (
                    node.get("alt")
                    or node.get("title")
                    or node.get("aria-label")
                    or ""
                )
                value = clean_text(value)

                if value and value.casefold() not in {
                    "recommended playstyles",
                    "playstyle",
                }:
                    names.append(value)

            record.recommended_playstyles = list(dict.fromkeys(names))[:30]


def parse_prices(soup: BeautifulSoup, record: CardRecord) -> None:
    overview = soup.select_one("#prices-overview")

    if overview is None:
        return

    heading = find_exact_heading(overview, "Lowest BIN")

    if heading:
        section = (
            heading.parent.parent
            if heading.parent and heading.parent.parent
            else heading.parent
        )

        if section:
            for candidate in section.select("span.tabular-nums"):
                value = parse_int(candidate.get_text(" ", strip=True))
                if value is not None:
                    record.lowest_bin = value
                    break

            time_node = section.find(
                string=re.compile(
                    r"\b\d+\s+(?:second|minute|hour|day)s?\s+ago\b",
                    re.IGNORECASE,
                )
            )
            if time_node:
                record.lowest_bin_age = clean_text(str(time_node))

    for node in overview.find_all(["div", "span"]):
        text = clean_text(node.get_text(" ", strip=True))
        match = re.fullmatch(
            r"(\d+(?:\.\d+)?[kKmM]?)\s*-\s*"
            r"(\d+(?:\.\d+)?[kKmM]?)",
            text,
        )

        if match:
            record.price_range_text = text
            record.price_range_low = parse_int(match.group(1))
            record.price_range_high = parse_int(match.group(2))
            break


def find_recent_sales_table(soup: BeautifulSoup):
    heading = find_exact_heading(soup, "Recent Sales")

    if heading is None:
        return None

    current = heading
    for _ in range(4):
        current = current.parent
        if current is None:
            return None

        table = current.find("table")
        if table is not None:
            return table

    return None


def parse_recent_sales(soup: BeautifulSoup, record: CardRecord) -> None:
    table = find_recent_sales_table(soup)

    if table is None:
        return

    tbody = table.find("tbody")
    if tbody is None:
        return

    captured_at = datetime.now(timezone.utc)
    rows: list[SaleRow] = []

    for row_number, row in enumerate(tbody.find_all("tr"), start=1):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        age_text = clean_text(cells[0].get_text(" ", strip=True))
        price = parse_int(cells[1].get_text(" ", strip=True))

        if price is None:
            continue

        approximate_time = relative_time_to_utc(age_text, captured_at)

        rows.append(
            SaleRow(
                age_text=age_text,
                price=price,
                approximate_sold_at_utc=(
                    approximate_time.isoformat()
                    if approximate_time is not None
                    else None
                ),
                row_number=row_number,
            )
        )

    record.recent_sales = rows
    record.recent_sales_count = len(rows)

    if rows:
        prices = [row.price for row in rows]
        record.recent_sales_latest = prices[0]
        record.recent_sales_low = min(prices)
        record.recent_sales_high = max(prices)
        record.recent_sales_average = round(mean(prices))


def parse_page(url: str, status: int | None, title: str, html: str) -> CardRecord:
    record = CardRecord(
        source_url=url,
        page_status=status,
        page_title=title,
        scraped_at_utc=datetime.now(timezone.utc).isoformat(),
    )

    path = urlparse(url).path
    match = CARD_URL_RE.match(path)

    if match:
        record.source_player_id = int(match.group("player_id"))
        record.source_card_id = int(match.group("card_id"))
        record.source_slug = match.group("slug")
        record.game_year = int(match.group("game_year"))
    else:
        record.parse_warnings.append("URL did not match expected FUT.GG card pattern")

    soup = BeautifulSoup(html, "html.parser")

    parse_card_visual(soup, record)
    parse_title(record)
    parse_misc_details(soup, record)
    parse_prices(soup, record)
    parse_recent_sales(soup, record)

    return record


async def dismiss_cookie_banner(page) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "#onetrust-reject-all-handler",
        "button:has-text('Accept all')",
        "button:has-text('Accept All')",
        "button:has-text('Reject all')",
        "button:has-text('Reject All')",
    ]

    for selector in selectors:
        locator = page.locator(selector).first
        try:
            if await locator.is_visible(timeout=600):
                await locator.click(timeout=2_000)
                return
        except Exception:
            pass


async def collect_card_urls(page, limit: int) -> list[str]:
    response = await page.goto(
        LISTING_URL,
        wait_until="domcontentloaded",
        timeout=TIMEOUT_MS,
    )

    status = response.status if response else None
    print(f"Listing status: {status}")

    if status != 200:
        raise RuntimeError(f"Listing page returned status {status}")

    await dismiss_cookie_banner(page)

    try:
        await page.locator("a[href*='/players/']").first.wait_for(
            state="attached",
            timeout=TIMEOUT_MS,
        )
    except PlaywrightTimeoutError as exc:
        raise RuntimeError("No player links appeared on the listing page") from exc

    await page.wait_for_timeout(1_500)

    hrefs = await page.locator("a[href]").evaluate_all(
        "(nodes) => nodes.map(node => node.getAttribute('href'))"
    )

    urls: list[str] = []
    seen: set[str] = set()

    for href in hrefs:
        if not href:
            continue

        parsed = urlparse(urljoin(BASE_URL, href))

        if not CARD_URL_RE.match(parsed.path):
            continue

        canonical = urljoin(BASE_URL, parsed.path)

        if canonical not in seen:
            seen.add(canonical)
            urls.append(canonical)

        if len(urls) >= limit:
            break

    return urls


async def scrape_one(page, url: str, index: int) -> CardRecord:
    print(f"\n[{index}/{LIMIT}] {url}")

    try:
        response = await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=TIMEOUT_MS,
        )

        status = response.status if response else None

        try:
            await page.locator(".fc-card").first.wait_for(
                state="attached",
                timeout=15_000,
            )
        except Exception:
            print("  Warning: card element did not appear.")

        # Prices and sales are rendered client-side; allow them to hydrate.
        try:
            await page.locator("#prices-overview").wait_for(
                state="attached",
                timeout=12_000,
            )
        except Exception:
            pass

        await page.wait_for_timeout(1_500)

        title = await page.title()
        html = await page.content()
        record = parse_page(url, status, title, html)

        print(
            "  "
            f"name={record.full_name or record.name!r} "
            f"rating={record.rating} "
            f"position={record.primary_position} "
            f"version={record.rarity_or_version!r} "
            f"BIN={record.lowest_bin} "
            f"sales={record.recent_sales_count}"
        )

        if SAVE_HTML:
            safe_name = f"{index:02d}_{record.source_card_id or 'unknown'}"
            (OUTPUT_DIR / f"{safe_name}.html").write_text(
                html,
                encoding="utf-8",
                errors="replace",
            )

        if SAVE_SCREENSHOTS:
            safe_name = f"{index:02d}_{record.source_card_id or 'unknown'}"
            await page.screenshot(
                path=str(OUTPUT_DIR / f"{safe_name}.png"),
                full_page=True,
            )

        return record

    except Exception as exc:
        print(f"  ERROR: {type(exc).__name__}: {exc}")
        return CardRecord(
            source_url=url,
            scraped_at_utc=datetime.now(timezone.utc).isoformat(),
            error=f"{type(exc).__name__}: {exc}",
        )


def record_to_flat_dict(record: CardRecord) -> dict[str, Any]:
    data = asdict(record)

    data["alternate_positions"] = "|".join(record.alternate_positions)
    data["recommended_playstyles"] = "|".join(record.recommended_playstyles)
    data["parse_warnings"] = "|".join(record.parse_warnings)
    data["recent_sales"] = json.dumps(
        [asdict(row) for row in record.recent_sales],
        ensure_ascii=False,
    )

    return data


def save_outputs(records: list[CardRecord]) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    json_path = OUTPUT_DIR / "futgg_10_cards.json"
    csv_path = OUTPUT_DIR / "futgg_10_cards.csv"

    json_path.write_text(
        json.dumps(
            [asdict(record) for record in records],
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    flattened = [record_to_flat_dict(record) for record in records]

    if flattened:
        fieldnames = list(flattened[0].keys())

        with csv_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(
                csv_file,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(flattened)

    print(f"\nSaved JSON: {json_path.resolve()}")
    print(f"Saved CSV:  {csv_path.resolve()}")


async def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    print("FUT.GG catalogue test")
    print(f"Listing:  {LISTING_URL}")
    print(f"Limit:    {LIMIT}")
    print(f"Headless: {HEADLESS}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-GB",
            viewport={"width": 1440, "height": 1000},
        )

        page = await context.new_page()

        try:
            urls = await collect_card_urls(page, LIMIT)
            print(f"Unique card URLs found: {len(urls)}")

            if not urls:
                raise RuntimeError("No FC 26 card URLs were found")

            records: list[CardRecord] = []

            for index, url in enumerate(urls, start=1):
                records.append(await scrape_one(page, url, index))

            save_outputs(records)

            successful = [record for record in records if not record.error]
            with_prices = [record for record in successful if record.lowest_bin is not None]
            with_sales = [record for record in successful if record.recent_sales_count > 0]

            print("\n=== SUMMARY ===")
            print(f"Requested:            {LIMIT}")
            print(f"URLs discovered:      {len(urls)}")
            print(f"Pages parsed:         {len(successful)}")
            print(f"Cards with BIN:       {len(with_prices)}")
            print(f"Cards with sales:     {len(with_sales)}")
            print(f"Errors:               {len(records) - len(successful)}")

        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())