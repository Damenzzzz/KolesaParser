import re
from html import unescape
from typing import Optional
from urllib.parse import urlparse


def normalize_text(text: Optional[str]) -> Optional[str]:
    """Decode entities and collapse whitespace."""
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


def normalize_currency(text: Optional[str]) -> Optional[str]:
    text = normalize_text(text)
    if not text:
        return None
    lowered = text.lower()
    if "₸" in text or "тг" in lowered or "тенге" in lowered:
        return "KZT"
    if "$" in text or "usd" in lowered:
        return "USD"
    if "€" in text or "eur" in lowered:
        return "EUR"
    return None


def normalize_fuel_type(text: Optional[str]) -> Optional[str]:
    text = normalize_text(text)
    if not text:
        return None
    lowered = text.lower().replace("ё", "е")
    mapping = [
        ("газ-бензин", "petrol-gas"),
        ("бензин-газ", "petrol-gas"),
        ("бензин", "petrol"),
        ("дизель", "diesel"),
        ("газ", "gas"),
        ("электро", "electric"),
        ("гибрид", "hybrid"),
    ]
    for needle, normalized in mapping:
        if needle in lowered:
            return normalized
    return lowered


def normalize_transmission(text: Optional[str]) -> Optional[str]:
    text = normalize_text(text)
    if not text:
        return None
    lowered = text.lower().replace("ё", "е")
    if "автомат" in lowered:
        return "automatic"
    if "механ" in lowered:
        return "manual"
    if "вариатор" in lowered:
        return "cvt"
    if "робот" in lowered:
        return "robot"
    return lowered


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
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def parse_brand_model_generation(title: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Best-effort parsing from titles such as 'Toyota Camry 2020 г.'."""
    normalized = normalize_text(title)
    if not normalized:
        return None, None, None

    without_year = re.sub(
        r"\b(19\d{2}|20\d{2})\b\s*(?:г\.?)?",
        "",
        normalized,
        flags=re.IGNORECASE,
    )
    without_year = normalize_text(without_year) or normalized
    parts = without_year.split()
    brand = parts[0] if parts else None
    model = " ".join(parts[1:]) if len(parts) > 1 else None
    return brand, model, None


def normalize_characteristic_key(text: Optional[str]) -> str:
    text = normalize_text(text) or ""
    text = text.lower().replace("ё", "е")
    text = re.sub(r"[:\s]+$", "", text)
    return text


def looks_like_captcha_or_block_page(html: str) -> bool:
    """Detect obvious block/captcha pages without treating normal footer text as a captcha."""
    lowered = html[:20000].lower()
    indicators = [
        "captcha-form",
        "captcha__",
        "/captcha/",
        "подтвердите, что вы не робот",
        "докажите, что вы не робот",
        "too many requests",
        "access denied",
        "доступ ограничен",
    ]
    return any(indicator in lowered for indicator in indicators)
