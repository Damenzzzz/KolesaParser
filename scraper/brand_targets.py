import json
import re
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from scraper.config import BRAND_STATE_PATH
from scraper.utils import normalize_text


BRAND_ALIASES = {
    "toyota": "Toyota",
    "\u0442\u043e\u0439\u043e\u0442\u0430": "Toyota",
    "bmw": "BMW",
    "\u0431\u043c\u0432": "BMW",
    "hyundai": "Hyundai",
    "\u0445\u0435\u043d\u0434\u0430\u0439": "Hyundai",
    "\u0445\u0443\u043d\u0434\u0430\u0439": "Hyundai",
    "kia": "Kia",
    "\u043a\u0438\u0430": "Kia",
    "lexus": "Lexus",
    "\u043b\u0435\u043a\u0441\u0443\u0441": "Lexus",
    "mercedes": "Mercedes-Benz",
    "mercedes benz": "Mercedes-Benz",
    "\u043c\u0435\u0440\u0441\u0435\u0434\u0435\u0441": "Mercedes-Benz",
    "chevrolet": "Chevrolet",
    "\u0448\u0435\u0432\u0440\u043e\u043b\u0435": "Chevrolet",
    "vaz": "VAZ",
    "\u0432\u0430\u0437": "VAZ",
    "lada": "VAZ",
    "\u043b\u0430\u0434\u0430": "VAZ",
    "audi": "Audi",
    "nissan": "Nissan",
    "volkswagen": "Volkswagen",
    "mitsubishi": "Mitsubishi",
    "subaru": "Subaru",
    "geely": "Geely",
    "changan": "Changan",
    "gac": "GAC",
    "byd": "BYD",
    "li": "Li",
    "deepal": "Deepal",
    "daewoo": "Daewoo",
    "renault": "Renault",
    "skoda": "Skoda",
    "ford": "Ford",
    "honda": "Honda",
    "mazda": "Mazda",
    "porsche": "Porsche",
    "land rover": "Land Rover",
}

KNOWN_BRAND_ALIASES = sorted(BRAND_ALIASES.items(), key=lambda item: len(item[0]), reverse=True)


def normalize_brand_name(text: Optional[str]) -> str:
    if not text:
        return ""
    normalized = normalize_text(text) or ""
    normalized = normalized.lower().replace("\u0451", "\u0435")
    normalized = re.sub(r"[^0-9a-z\u0430-\u044f]+", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return BRAND_ALIASES.get(normalized, normalized.title())


def brand_matches(car: dict, brand: str) -> tuple[bool, str]:
    parsed_brand = normalize_brand_name(car.get("brand"))
    target_brand = normalize_brand_name(brand)
    if not parsed_brand:
        return True, "incomplete_brand_parse"
    if parsed_brand == target_brand:
        return True, ""
    return False, f"wrong brand parsed={parsed_brand} target={target_brand}"


def guess_brand_from_text(text: Optional[str]) -> Optional[str]:
    normalized = _normalized_search_text(text)
    if not normalized:
        return None

    for alias, canonical in KNOWN_BRAND_ALIASES:
        if re.search(rf"(?<!\w){re.escape(alias)}(?!\w)", normalized, flags=re.IGNORECASE):
            return canonical
    return None


def is_wrong_brand_guess(brand_guess: Optional[str], target_brand: str) -> bool:
    if not brand_guess:
        return False
    return normalize_brand_name(brand_guess) != normalize_brand_name(target_brand)


def build_brand_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    parsed = urlsplit(base_url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def load_brand_state(brand: str) -> int:
    state = _read_state()
    try:
        page = int(state.get(brand, {}).get("last_page", 1))
    except (TypeError, ValueError):
        page = 1
    return max(1, page)


def save_brand_state(brand: str, page: int) -> None:
    BRAND_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = _read_state()
    state[brand] = {"last_page": max(1, int(page))}
    temp_path = BRAND_STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(BRAND_STATE_PATH)


def _read_state() -> dict:
    if not BRAND_STATE_PATH.exists():
        return {}
    try:
        data = json.loads(BRAND_STATE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _normalized_search_text(text: Optional[str]) -> str:
    normalized = normalize_text(text) or ""
    normalized = normalized.lower().replace("\u0451", "\u0435")
    normalized = re.sub(r"[^0-9a-z\u0430-\u044f]+", " ", normalized, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", normalized).strip()
