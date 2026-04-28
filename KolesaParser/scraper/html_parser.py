import json
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from scraper import selectors
from scraper.config import BASE_URL, SOURCE_NAME
from scraper.utils import (
    canonicalize_url,
    clean_engine_volume,
    clean_mileage,
    clean_price,
    extract_listing_id,
    normalize_characteristic_key,
    normalize_currency,
    normalize_fuel_type,
    normalize_text,
    normalize_transmission,
    parse_brand_model_generation,
    parse_year_from_title,
    split_city_region,
)

logger = logging.getLogger("html_parser")


try:
    from selectolax.parser import HTMLParser
except ImportError:  # pragma: no cover - used only before dependencies are installed.
    HTMLParser = None


def _tree(html: str):
    if HTMLParser is not None:
        return HTMLParser(html)

    from bs4 import BeautifulSoup, FeatureNotFound

    try:
        return BeautifulSoup(html, "lxml")
    except FeatureNotFound:
        logger.warning("lxml parser not available; falling back to html.parser")
        return BeautifulSoup(html, "html.parser")


def _select(root, selector: str) -> list:
    try:
        if hasattr(root, "css"):
            return root.css(selector)
        return root.select(selector)
    except Exception:
        return []


def _first(root, selector_list: list[str]) -> Optional[object]:
    for selector in selector_list:
        matches = _select(root, selector)
        if matches:
            return matches[0]
    return None


def _text(node: Optional[object]) -> Optional[str]:
    if node is None:
        return None
    try:
        if hasattr(node, "get_text"):
            text = node.get_text(" ", strip=True)
        else:
            try:
                text = node.text(separator=" ", strip=True)
            except TypeError:
                text = node.text()
    except Exception:
        return None
    return normalize_text(text)


def _attr(node: object, name: str) -> Optional[str]:
    try:
        if hasattr(node, "attributes"):
            return node.attributes.get(name)
        return node.get(name)
    except Exception:
        return None


def extract_listing_urls(html: str) -> list[str]:
    root = _tree(html)
    urls: list[str] = []
    seen_ids: set[str] = set()

    for selector in selectors.LISTING_LINK_SELECTORS:
        for node in _select(root, selector):
            url = canonicalize_url(_attr(node, "href"), BASE_URL)
            listing_id = extract_listing_id(url)
            if url and listing_id and listing_id not in seen_ids:
                urls.append(url)
                seen_ids.add(listing_id)
        if urls:
            break

    return urls


def extract_brand_listing_cards(html: str, target_brand: str) -> list[dict]:
    """Extract listing links only from likely main search result cards."""
    from bs4 import BeautifulSoup, FeatureNotFound

    from scraper.brand_targets import guess_brand_from_text

    try:
        soup = BeautifulSoup(html or "", "lxml")
    except FeatureNotFound:
        soup = BeautifulSoup(html or "", "html.parser")

    for node in soup(["script", "style", "meta", "noscript", "svg"]):
        node.decompose()

    cards: list[dict] = []
    seen_ids: set[str] = set()

    card_selectors = [
        '[data-test*="advert"]',
        ".a-card",
        'div[class*="a-card"]',
        ".a-list__item",
        "article",
        'div[class*="listing"]',
        'div[class*="result"]',
        'div[class*="search"]',
    ]
    for selector in card_selectors:
        for node in soup.select(selector):
            _add_brand_card_from_node(node, cards, seen_ids, guess_brand_from_text)

    # Fallback: climb from each listing link to its nearest card-like ancestor.
    for link in soup.select('a[href*="/a/show/"]'):
        card_node = _closest_card_like_node(link)
        if card_node is not None:
            _add_brand_card_from_node(card_node, cards, seen_ids, guess_brand_from_text)

    return cards


def _add_brand_card_from_node(node, cards: list[dict], seen_ids: set[str], brand_guesser) -> None:
    if _node_is_excluded_from_brand_cards(node):
        return

    links = []
    for link in node.select('a[href*="/a/show/"]'):
        url = canonicalize_url(link.get("href"), BASE_URL)
        listing_id = extract_listing_id(url)
        if url and listing_id:
            links.append((url, listing_id, link))

    unique_links = []
    seen_in_node = set()
    for url, listing_id, link in links:
        if listing_id not in seen_in_node:
            unique_links.append((url, listing_id, link))
            seen_in_node.add(listing_id)

    # Broad containers such as full search sections are not cards.
    if len(unique_links) != 1:
        return

    url, listing_id, link = unique_links[0]
    if listing_id in seen_ids:
        return

    card_text = normalize_text(node.get_text(" ", strip=True)) or ""
    card_title = _brand_card_title(node, link, card_text)
    brand_guess = brand_guesser(" ".join(part for part in [card_title, card_text] if part))
    cards.append(
        {
            "url": url,
            "card_title": card_title,
            "card_text": card_text,
            "brand_guess": brand_guess,
        }
    )
    seen_ids.add(listing_id)


def _brand_card_title(node, link, card_text: str) -> str:
    title_selectors = [
        ".a-card__title",
        ".a-card__name",
        '[class*="title"]',
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
    ]
    for selector in title_selectors:
        title_node = node.select_one(selector)
        if title_node:
            title = normalize_text(title_node.get_text(" ", strip=True))
            if title:
                return title

    link_text = normalize_text(link.get_text(" ", strip=True))
    if link_text:
        return link_text
    return card_text[:160]


def _closest_card_like_node(link):
    card_hints = ("a-card", "card", "listing", "result", "item", "advert", "article")
    node = link
    for _ in range(8):
        node = node.parent
        if node is None:
            return None
        if getattr(node, "name", None) == "article":
            return node
        signature = _node_signature(node)
        if any(hint in signature for hint in card_hints):
            return node
    return None


def _node_is_excluded_from_brand_cards(node) -> bool:
    for current in [node, *list(node.parents)]:
        name = getattr(current, "name", "") or ""
        if name in {"footer", "aside"}:
            return True
        signature = _node_signature(current)
        if any(hint in signature for hint in _EXCLUDED_CARD_SIGNATURE_HINTS):
            return True
        if name == "body":
            break

    text = (normalize_text(node.get_text(" ", strip=True)) or "").lower()
    return any(hint in text for hint in _EXCLUDED_CARD_TEXT_HINTS)


def _node_signature(node) -> str:
    values = [getattr(node, "name", "") or ""]
    for key in ("id", "class", "data-test", "role", "aria-label"):
        value = node.attrs.get(key) if hasattr(node, "attrs") else None
        if isinstance(value, list):
            values.extend(str(item) for item in value)
        elif value:
            values.append(str(value))
    return " ".join(values).lower()


_EXCLUDED_CARD_SIGNATURE_HINTS = (
    "recommendation",
    "recommended",
    "similar",
    "banner",
    "footer",
    "sidebar",
    "recently",
    "viewed",
    "promo",
    "commercial",
    "advertising",
    "vip",
    "\u043f\u043e\u0445\u043e\u0436",
    "\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434",
)

_EXCLUDED_CARD_TEXT_HINTS = (
    "recommendations",
    "recommended",
    "similar",
    "recently viewed",
    "\u043f\u043e\u0445\u043e\u0436\u0438\u0435",
    "\u0440\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0443\u0435\u043c",
    "\u0441\u043c\u043e\u0442\u0440\u0438\u0442\u0435 \u0442\u0430\u043a\u0436\u0435",
)


def parse_listing_page(html: str, url: str) -> dict:
    root = _tree(html)
    title = _text(_first(root, selectors.TITLE_SELECTORS))
    price_text = _text(_first(root, selectors.PRICE_SELECTORS))
    characteristics = parse_characteristics(html)
    options = [_text(node) for node in _select(root, ", ".join(selectors.OPTION_SELECTORS))]
    options = [option for option in options if option]

    city, region = split_city_region(_characteristic(characteristics, "Город"))
    brand, model, generation = parse_brand_model_generation(title)
    generation = _characteristic(characteristics, "Поколение") or generation
    engine_volume, fuel_type = _parse_engine(_characteristic(characteristics, "Объем двигателя, л", "Двигатель"))

    car = {
        "listing_id": extract_listing_id(url),
        "url": url,
        "source": SOURCE_NAME,
        "title": title,
        "brand": brand,
        "model": model,
        "generation": generation,
        "year": parse_year_from_title(title),
        "price": clean_price(price_text),
        "currency": normalize_currency(price_text),
        "city": city,
        "region": region,
        "mileage_km": clean_mileage(_characteristic(characteristics, "Пробег")),
        "body_type": _characteristic(characteristics, "Кузов"),
        "engine_volume_l": engine_volume,
        "fuel_type": fuel_type,
        "transmission": normalize_transmission(_characteristic(characteristics, "Коробка передач", "Коробка")),
        "drive_type": _characteristic(characteristics, "Привод"),
        "steering_wheel": _characteristic(characteristics, "Руль"),
        "color": _characteristic(characteristics, "Цвет"),
        "condition": _characteristic(characteristics, "Состояние"),
        "customs_cleared": _characteristic(characteristics, "Растаможен в Казахстане", "Растаможен"),
        "description": _parse_description(root),
        "seller_type": _text(_first(root, selectors.SELLER_TYPE_SELECTORS)),
        "published_at": _parse_published_at(root),
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "photo_count": _parse_photo_count(root),
        "is_active": 1,
        "raw_characteristics": json.dumps(
            {"parameters": characteristics, "options": options},
            ensure_ascii=False,
        ),
    }
    car["generated_description"] = create_generated_description(car)
    return car


def parse_characteristics(html: str) -> dict:
    root = _tree(html)
    characteristics: dict[str, str] = {}

    for row in _select(root, selectors.PARAMETER_ROW_SELECTOR):
        label_nodes = _select(row, selectors.PARAMETER_LABEL_SELECTOR)
        value_nodes = _select(row, selectors.PARAMETER_VALUE_SELECTOR)
        label = _text(label_nodes[0]) if label_nodes else None
        value = _text(value_nodes[0]) if value_nodes else None
        if label and value:
            characteristics[label.rstrip(":")] = value

    return characteristics


def create_generated_description(car: dict) -> Optional[str]:
    brand_model = " ".join(
        str(value) for value in [car.get("brand"), car.get("model"), car.get("year")] if value
    )
    if not brand_model:
        brand_model = car.get("title")
    if not brand_model:
        return None

    parts = [brand_model]
    if car.get("city"):
        parts[-1] = f"{parts[-1]} in {car['city']}"
    parts[-1] = parts[-1] + "."

    if car.get("mileage_km") is not None:
        parts.append(f"Mileage: {car['mileage_km']} km.")

    engine = []
    if car.get("engine_volume_l") is not None:
        engine.append(f"{car['engine_volume_l']} L")
    if car.get("fuel_type"):
        engine.append(str(car["fuel_type"]))
    if engine:
        parts.append(f"Engine: {', '.join(engine)}.")

    if car.get("transmission"):
        parts.append(f"Transmission: {car['transmission']}.")

    if car.get("price") is not None:
        currency = f" {car['currency']}" if car.get("currency") else ""
        parts.append(f"Price: {car['price']}{currency}.")

    return " ".join(parts)


def _characteristic(characteristics: dict, *labels: str) -> Optional[str]:
    normalized = {normalize_characteristic_key(key): value for key, value in characteristics.items()}
    for label in labels:
        wanted = normalize_characteristic_key(label)
        if wanted in normalized:
            return normalized[wanted]
        for key, value in normalized.items():
            if wanted and wanted in key:
                return value
    return None


def _parse_engine(engine_text: Optional[str]) -> tuple[Optional[float], Optional[str]]:
    if not engine_text:
        return None, None
    volume = clean_engine_volume(engine_text)
    fuel_match = re.search(r"\(([^)]+)\)", engine_text)
    fuel_text = fuel_match.group(1) if fuel_match else engine_text
    return volume, normalize_fuel_type(fuel_text)


def _parse_description(root) -> Optional[str]:
    for selector in selectors.DESCRIPTION_SELECTORS:
        text = _text(_first(root, [selector]))
        if text and "loading" not in text.lower():
            return text
    return None


def _parse_photo_count(root) -> Optional[int]:
    counts = []
    for selector in selectors.PHOTO_SELECTORS:
        count = len(_select(root, selector))
        if count:
            counts.append(count)
    return max(counts) if counts else None


def _parse_published_at(root) -> Optional[str]:
    text = _text(_first(root, selectors.PUBLISHED_AT_SELECTORS))
    if not text:
        return None
    match = re.search(r"[cс]\s+(.+)$", text)
    return normalize_text(match.group(1) if match else text)
