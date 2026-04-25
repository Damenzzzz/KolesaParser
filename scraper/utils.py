import re
from html import unescape
from typing import Optional
from urllib.parse import urlparse


def normalize_text(text: Optional[str]) -> Optional[str]:
    """Collapse whitespace and decode common HTML entities."""
    if text is None:
        return None
    cleaned = unescape(str(text)).replace("\xa0", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def clean_price(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def clean_mileage(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return int(digits) if digits else None


def clean_engine_volume(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    match = re.search(r"(\d+(?:[.,]\d+)?)", text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def extract_listing_id(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    parsed = urlparse(url)
    match = re.search(r"/a/show/(\d+)", parsed.path)
    return match.group(1) if match else None


def parse_year_from_title(title: Optional[str]) -> Optional[int]:
    if not title:
        return None
    match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
    if not match:
        return None
    year = int(match.group(1))
    return year if 1900 <= year <= 2100 else None


def split_city_region(text: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    text = normalize_text(text)
    if not text:
        return None, None
    parts = [part.strip() for part in text.split(",") if part.strip()]
    city = parts[0] if parts else text
    region = ", ".join(parts[1:]) if len(parts) > 1 else None
    return city, region


def canonicalize_url(url: Optional[str], base_url: str) -> Optional[str]:
    if not url:
        return None
    if url.startswith("//"):
        url = "https:" + url
    elif url.startswith("/"):
        url = base_url.rstrip("/") + url
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def parse_brand_model_generation(title: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Best-effort brand/model parsing from a title such as 'Toyota Camry 2020 г.'."""
    normalized = normalize_text(title)
    if not normalized:
        return None, None, None
    without_year = re.sub(r"\b(19\d{2}|20\d{2})\b\s*г?\.?", "", normalized, flags=re.I)
    without_year = normalize_text(without_year) or normalized
    parts = without_year.split()
    brand = parts[0] if parts else None
    model = " ".join(parts[1:]) if len(parts) > 1 else None
    return brand, model, None


def detect_currency(price_text: Optional[str]) -> Optional[str]:
    if not price_text:
        return None
    if "₸" in price_text or "тг" in price_text.lower():
        return "KZT"
    if "$" in price_text or "usd" in price_text.lower():
        return "USD"
    return None


def generated_description_from_car(car: dict) -> Optional[str]:
    brand_model = " ".join(
        str(part) for part in [car.get("brand"), car.get("model"), car.get("year")] if part
    )
    if not brand_model:
        brand_model = car.get("title")
    if not brand_model:
        return None

    chunks = [str(brand_model)]
    if car.get("city"):
        chunks[-1] = f"{chunks[-1]} in {car['city']}"
    chunks[-1] = chunks[-1] + "."

    if car.get("mileage_km") is not None:
        chunks.append(f"Mileage: {car['mileage_km']} km.")

    engine_parts = []
    if car.get("engine_volume_l") is not None:
        engine_parts.append(f"{car['engine_volume_l']} L")
    if car.get("fuel_type"):
        engine_parts.append(str(car["fuel_type"]).lower())
    if engine_parts:
        chunks.append(f"Engine: {', '.join(engine_parts)}.")

    if car.get("transmission"):
        chunks.append(f"Transmission: {str(car['transmission']).lower()}.")

    if car.get("price") is not None:
        currency = f" {car['currency']}" if car.get("currency") else ""
        chunks.append(f"Price: {car['price']}{currency}.")

    return " ".join(chunks)
