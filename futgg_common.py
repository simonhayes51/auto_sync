from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

BASE_URL = "https://www.fut.gg"
CARD_URL_RE = re.compile(
    r"^/players/(?P<player_id>\d+)-(?P<slug>[^/]+)/"
    r"(?P<game_year>\d+)-(?P<card_id>\d+)/?$"
)
RELATIVE_TIME_RE = re.compile(
    r"^\s*(?P<amount>\d+)\s+(?P<unit>second|minute|hour|day)s?\s+ago\s*$",
    re.IGNORECASE,
)
POSITIONS = {
    "GK", "CB", "LB", "RB", "LWB", "RWB", "CDM", "CM", "CAM",
    "LM", "RM", "LW", "RW", "CF", "ST",
}


@dataclass
class SaleObservation:
    age_text: str
    age_seconds: int
    sold_price: int
    approximate_sold_at: datetime
    row_position: int
    occurrence_index: int
    fingerprint: str


@dataclass
class FutggCard:
    source_card_id: int
    source_player_id: int
    source_slug: str
    game_year: int
    source_url: str

    name: str | None = None
    rating: int | None = None
    primary_position: str | None = None
    alternate_positions: list[str] = field(default_factory=list)
    rarity: str | None = None
    squad: str | None = None

    club: str | None = None
    league: str | None = None
    nation: str | None = None

    height_cm: int | None = None
    weight_kg: int | None = None
    foot: str | None = None
    skill_moves: int | None = None
    weak_foot: int | None = None
    accelerate_type: str | None = None
    body_type: str | None = None
    real_face: bool | None = None
    shirt_number: int | None = None
    age: int | None = None

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

    player_image_url: str | None = None
    card_design_image_url: str | None = None
    club_image_url: str | None = None
    league_image_url: str | None = None
    nation_image_url: str | None = None
    club_source_id: int | None = None
    league_source_id: int | None = None
    nation_source_id: int | None = None

    lowest_bin: int | None = None
    lowest_bin_age: str | None = None
    price_range_low: int | None = None
    price_range_high: int | None = None
    recent_sales: list[SaleObservation] = field(default_factory=list)
    price_outcome: str = "no_active_market"

    parse_warnings: list[str] = field(default_factory=list)


def clean_text(value: str | None) -> str:
    return " ".join((value or "").split())


def parse_price(value: str | None) -> int | None:
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


def relative_age_seconds(value: str) -> int | None:
    match = RELATIVE_TIME_RE.fullmatch(value)
    if not match:
        return None
    amount = int(match.group("amount"))
    multiplier = {
        "second": 1,
        "minute": 60,
        "hour": 3600,
        "day": 86400,
    }[match.group("unit").lower()]
    return amount * multiplier


def _image_id(url: str | None, asset_type: str) -> int | None:
    if not url:
        return None
    match = re.search(rf"/{re.escape(asset_type)}/(\d+)[.\-]", url)
    return int(match.group(1)) if match else None


def _find_exact(soup: BeautifulSoup, text: str):
    wanted = text.casefold()
    return soup.find(
        lambda tag: tag.name in {"h1", "h2", "h3", "h4", "span", "div", "td", "dt"}
        and clean_text(tag.get_text(" ", strip=True)).casefold() == wanted
    )


def _find_label_value(soup: BeautifulSoup, label: str) -> tuple[str | None, str | None]:
    node = _find_exact(soup, label)
    if node is None:
        return None, None

    current = node
    label_text = clean_text(node.get_text(" ", strip=True))
    for _ in range(5):
        parent = current.parent
        if parent is None:
            break

        direct = parent.find_all(["div", "span", "td", "dd"], recursive=False)
        candidates: list[str] = []
        for child in direct:
            text = clean_text(child.get_text(" ", strip=True))
            if text and text.casefold() != label_text.casefold() and not text.casefold().startswith(label_text.casefold() + " "):
                candidates.append(text)
        if candidates:
            img = parent.find("img", src=True)
            return candidates[-1], urljoin(BASE_URL, img["src"]) if img else None

        full = clean_text(parent.get_text(" ", strip=True))
        if full.casefold().startswith(label_text.casefold() + " "):
            value = full[len(label_text):].strip()
            if value and len(value) <= 150:
                img = parent.find("img", src=True)
                return value, urljoin(BASE_URL, img["src"]) if img else None
        current = parent

    return None, None


def parse_card_url(url: str) -> tuple[int, int, str, int]:
    match = CARD_URL_RE.match(urlparse(url).path)
    if not match:
        raise ValueError(f"Unexpected FUT.GG card URL: {url}")
    return (
        int(match.group("card_id")),
        int(match.group("player_id")),
        match.group("slug"),
        int(match.group("game_year")),
    )


def _parse_int_field(value: str | None) -> int | None:
    if not value:
        return None
    match = re.search(r"\d+", value.replace(",", ""))
    return int(match.group(0)) if match else None


def _parse_bool(value: str | None) -> bool | None:
    text = clean_text(value).casefold()
    if text in {"yes", "true"}:
        return True
    if text in {"no", "false"}:
        return False
    return None


def _parse_card_visual(soup: BeautifulSoup, card: FutggCard) -> None:
    visual = soup.select_one(".fc-card")
    if visual is None:
        card.parse_warnings.append("fc-card not found")
        return

    for node in visual.select(".font-cruyff-bold, [class*='font-cruyff-bold']"):
        text = clean_text(node.get_text(" ", strip=True))
        if text and not text.isdigit():
            card.name = text
            break

    texts = [clean_text(node.get_text(" ", strip=True)) for node in visual.find_all(["div", "span"])]
    for index, text in enumerate(texts):
        if re.fullmatch(r"\d{2}", text) and 40 <= int(text) <= 99:
            following = texts[index + 1] if index + 1 < len(texts) else ""
            if following in POSITIONS:
                card.rating = int(text)
                card.primary_position = following
                break

    stat_map = {
        "PAC": "pace", "SHO": "shooting", "PAS": "passing",
        "DRI": "dribbling", "DEF": "defending", "PHY": "physicality",
        "DIV": "diving", "HAN": "handling", "KIC": "kicking",
        "REF": "reflexes", "SPD": "speed", "POS": "positioning",
    }
    for label, field_name in stat_map.items():
        node = visual.find(lambda tag: clean_text(tag.get_text(" ", strip=True)) == label)
        if node and node.parent:
            nums = [int(x) for x in re.findall(r"\b(\d{1,2})\b", clean_text(node.parent.get_text(" ", strip=True)))]
            nums = [x for x in nums if 0 <= x <= 99]
            if nums:
                setattr(card, field_name, nums[-1])

    positions: list[str] = []
    for node in visual.find_all(["div", "span"]):
        text = clean_text(node.get_text(" ", strip=True))
        if text in POSITIONS:
            positions.append(text)
    positions = list(dict.fromkeys(positions))
    if card.primary_position in positions:
        positions.remove(card.primary_position)
    card.alternate_positions = positions

    for image in visual.find_all("img", src=True):
        src = urljoin(BASE_URL, image["src"])
        alt = clean_text(image.get("alt")).casefold()
        if "/player-item/" in src:
            card.player_image_url = src
        elif "/rarities-" in src:
            card.card_design_image_url = src
        elif alt == "club":
            card.club_image_url = src
            card.club_source_id = _image_id(src, "club")
        elif alt == "league":
            card.league_image_url = src
            card.league_source_id = _image_id(src, "league")
        elif alt == "nation":
            card.nation_image_url = src
            card.nation_source_id = _image_id(src, "nation")


def _parse_player_information(soup: BeautifulSoup, card: FutggCard) -> None:
    fields = {
        "Name": "name", "Club": "club", "League": "league",
        "Nation": "nation", "Rarity": "rarity", "Squad": "squad",
        "Foot": "foot", "AcceleRATE": "accelerate_type", "Body Type": "body_type",
    }
    for label, attr in fields.items():
        value, image = _find_label_value(soup, label)
        if value:
            setattr(card, attr, value)
        if image:
            if label == "Club":
                card.club_image_url = image
                card.club_source_id = _image_id(image, "club")
            elif label == "League":
                card.league_image_url = image
                card.league_source_id = _image_id(image, "league")
            elif label == "Nation":
                card.nation_image_url = image
                card.nation_source_id = _image_id(image, "nation")

    height, _ = _find_label_value(soup, "Height")
    weight, _ = _find_label_value(soup, "Weight")
    skills, _ = _find_label_value(soup, "Skill Moves")
    weak_foot, _ = _find_label_value(soup, "Weak Foot")
    real_face, _ = _find_label_value(soup, "Real Face")
    shirt_number, _ = _find_label_value(soup, "Shirt Number")
    age, _ = _find_label_value(soup, "Age")

    card.height_cm = _parse_int_field(height)
    card.weight_kg = _parse_int_field(weight)
    card.skill_moves = _parse_int_field(skills)
    card.weak_foot = _parse_int_field(weak_foot)
    card.real_face = _parse_bool(real_face)
    card.shirt_number = _parse_int_field(shirt_number)
    card.age = _parse_int_field(age)


UNTRADEABLE_MARKERS = (
    "not tradeable",
    "untradeable",
    "this item cannot be sold",
    "sbc reward",
    "objective reward",
)


def detect_price_outcome(soup: BeautifulSoup, card: "FutggCard") -> str:
    """Classifies why a card page did/did not yield a price, so an SBC or
    objective-only card isn't treated the same as a genuine scrape
    failure (see futgg_price_sync.py's status handling)."""
    if card.lowest_bin is not None or card.recent_sales:
        return "success"

    page_text = clean_text(soup.get_text(" ", strip=True)).casefold()
    if any(marker in page_text for marker in UNTRADEABLE_MARKERS):
        return "untradeable"

    if soup.select_one("#prices-overview") is None:
        return "price_section_missing"

    return "no_active_market"


def _find_recent_sales_table(soup: BeautifulSoup):
    heading = _find_exact(soup, "Recent Sales")
    if heading is None:
        return None
    current = heading
    for _ in range(5):
        current = current.parent
        if current is None:
            return None
        table = current.find("table")
        if table is not None:
            return table
    return None


def _parse_prices(soup: BeautifulSoup, card: FutggCard, captured_at: datetime) -> None:
    overview = soup.select_one("#prices-overview")
    if overview is not None:
        heading = _find_exact(overview, "Lowest BIN")
        if heading:
            current = heading
            for _ in range(4):
                current = current.parent
                if current is None:
                    break
                for value_node in current.select("span.tabular-nums"):
                    price = parse_price(value_node.get_text(" ", strip=True))
                    if price is not None:
                        card.lowest_bin = price
                        break
                if card.lowest_bin is not None:
                    age_node = current.find(string=RELATIVE_TIME_RE)
                    if age_node:
                        card.lowest_bin_age = clean_text(str(age_node))
                    break

        for node in overview.find_all(["div", "span"]):
            text = clean_text(node.get_text(" ", strip=True))
            match = re.fullmatch(r"(\d+(?:\.\d+)?[kKmM]?)\s*-\s*(\d+(?:\.\d+)?[kKmM]?)", text)
            if match:
                card.price_range_low = parse_price(match.group(1))
                card.price_range_high = parse_price(match.group(2))
                break

    table = _find_recent_sales_table(soup)
    if table is None or table.find("tbody") is None:
        return

    occurrence_counts: dict[tuple[int, int], int] = {}
    for row_position, row in enumerate(table.find("tbody").find_all("tr"), start=1):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        age_text = clean_text(cells[0].get_text(" ", strip=True))
        age_seconds = relative_age_seconds(age_text)
        sold_price = parse_price(cells[1].get_text(" ", strip=True))
        if age_seconds is None or sold_price is None:
            continue

        approx = captured_at - timedelta(seconds=age_seconds)
        rounded = (approx + timedelta(seconds=30)).replace(second=0, microsecond=0)
        key = (int(rounded.timestamp()), sold_price)
        occurrence_index = occurrence_counts.get(key, 0) + 1
        occurrence_counts[key] = occurrence_index
        raw = f"{card.source_card_id}|{rounded.isoformat()}|{sold_price}|{occurrence_index}"
        fingerprint = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        card.recent_sales.append(
            SaleObservation(
                age_text=age_text,
                age_seconds=age_seconds,
                sold_price=sold_price,
                approximate_sold_at=rounded,
                row_position=row_position,
                occurrence_index=occurrence_index,
                fingerprint=fingerprint,
            )
        )


def parse_futgg_card(html: str, url: str, captured_at: datetime | None = None) -> FutggCard:
    captured_at = captured_at or datetime.now(timezone.utc)
    source_card_id, source_player_id, slug, game_year = parse_card_url(url)
    card = FutggCard(
        source_card_id=source_card_id,
        source_player_id=source_player_id,
        source_slug=slug,
        game_year=game_year,
        source_url=url,
    )
    soup = BeautifulSoup(html, "html.parser")
    _parse_card_visual(soup, card)
    _parse_player_information(soup, card)
    _parse_prices(soup, card, captured_at)
    card.price_outcome = detect_price_outcome(soup, card)
    return card


def classify_price_tier(card: FutggCard | dict[str, Any]) -> str:
    rating = card.rating if isinstance(card, FutggCard) else card.get("rating")
    rarity = card.rarity if isinstance(card, FutggCard) else card.get("rarity")
    rarity_text = clean_text(rarity).casefold()
    base_values = {
        "common", "rare", "gold", "silver", "bronze", "gold common", "gold rare",
        "silver common", "silver rare", "bronze common", "bronze rare",
    }
    if rarity_text and rarity_text not in base_values:
        return "special"
    if rating is not None and rating >= 75 and "rare" in rarity_text:
        return "gold_rare"
    if rating is not None and rating >= 75:
        return "gold_common"
    if rating is not None and rating >= 65:
        return "silver"
    return "bronze"
