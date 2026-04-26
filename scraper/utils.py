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


BLOCK_STATUS_CODES = {403, 429}
VISIBLE_BLOCK_PHRASES = [
    "captcha",
    "\u043a\u0430\u043f\u0447\u0430",
    "\u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u0431\u0435\u0437\u043e\u043f\u0430\u0441\u043d\u043e\u0441\u0442\u0438",
    "\u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0434\u0438\u0442\u0435, \u0447\u0442\u043e \u0432\u044b \u043d\u0435 \u0440\u043e\u0431\u043e\u0442",
    "\u0434\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d",
    "\u0441\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432",
    "access denied",
    "security check",
    "are you human",
    "verify you are human",
    "temporarily blocked",
]


def is_blocked_response(status_code: int, html: str) -> tuple[bool, str]:
    """Return true only for clear block/rate-limit/captcha responses."""
    if status_code in BLOCK_STATUS_CODES:
        return True, f"HTTP {status_code}"
    if status_code != 200:
        return False, ""

    title, visible_text = extract_visible_title_and_text(html)
    text_for_detection = f"{title}\n{visible_text}".lower()
    for phrase in VISIBLE_BLOCK_PHRASES:
        if phrase == "captcha":
            if re.search(r"\bcaptcha\b", text_for_detection):
                return True, f"visible challenge phrase: {phrase}"
            continue
        if phrase in text_for_detection:
            return True, f"visible challenge phrase: {phrase}"

    return False, ""


def looks_like_normal_listing_title(html: str) -> bool:
    title, _ = extract_visible_title_and_text(html)
    lowered = title.lower()
    if not lowered:
        return False

    has_sale_word = "\u043f\u0440\u043e\u0434\u0430\u0436\u0430" in lowered
    has_year_word = "\u0433\u043e\u0434" in lowered
    has_listing_context = any(
        word in lowered
        for word in (
            "\u043a\u0443\u043f\u0438\u0442\u044c",
            "\u0446\u0435\u043d\u0430",
            "\u043a\u043e\u043b\u0451\u0441\u0430",
            "\u043a\u043e\u043b\u0435\u0441\u0430",
        )
    )
    return has_sale_word and has_year_word and has_listing_context


def extract_visible_title_and_text(html: str) -> tuple[str, str]:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html or "", "lxml")
        title = normalize_text(soup.title.get_text(" ", strip=True) if soup.title else "") or ""
        for node in soup(["script", "style", "meta", "noscript", "svg"]):
            node.decompose()
        visible_text = normalize_text(soup.get_text(" ", strip=True)) or ""
        return title, visible_text
    except Exception:
        return _extract_visible_title_and_text_fallback(html)


def _extract_visible_title_and_text_fallback(html: str) -> tuple[str, str]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html or "", flags=re.IGNORECASE | re.DOTALL)
    title = normalize_text(re.sub(r"<[^>]+>", " ", title_match.group(1))) if title_match else ""

    stripped = re.sub(
        r"<(script|style|meta|noscript|svg)\b[^>]*>.*?</\1>",
        " ",
        html or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    stripped = re.sub(r"<meta\b[^>]*>", " ", stripped, flags=re.IGNORECASE | re.DOTALL)
    visible_text = normalize_text(re.sub(r"<[^>]+>", " ", stripped)) or ""
    return title or "", visible_text


def looks_like_captcha_or_block_page(html: str) -> bool:
    """Detect obvious block/captcha pages without treating normal footer text as a captcha."""
    blocked, _ = is_blocked_response(200, html)
    return blocked
    lowered = html[:20000].lower()
    indicators = [
        "captcha",
        "security check",
        "access denied",
        "forbidden",
        "too many requests",
        "blocked",
        "unavailable",
        "deny",
        "\u0441\u043b\u0438\u0448\u043a\u043e\u043c \u043c\u043d\u043e\u0433\u043e \u0437\u0430\u043f\u0440\u043e\u0441\u043e\u0432",
        "\u043f\u0440\u043e\u0432\u0435\u0440\u043a\u0430",
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
