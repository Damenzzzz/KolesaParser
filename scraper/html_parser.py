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

    from bs4 import BeautifulSoup

    return BeautifulSoup(html, "lxml")


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
