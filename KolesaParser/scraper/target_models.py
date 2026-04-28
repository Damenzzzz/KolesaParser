import re
from typing import Optional
from urllib.parse import urlencode

from scraper.config import START_URL, TARGET_MODELS
from scraper.utils import normalize_text


def normalize_model_name(text: Optional[str]) -> str:
    if not text:
        return ""
    normalized = normalize_text(text) or ""
    normalized = normalized.lower().replace("\u0451", "\u0435")
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"[^0-9a-zа-я]+", " ", normalized, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", normalized).strip()


def matches_target_model(car: dict, target: dict) -> bool:
    brand = normalize_model_name(car.get("brand"))
    target_brand = normalize_model_name(target.get("brand"))
    if brand != target_brand:
        return False

    haystack = normalize_model_name(
        " ".join(
            str(value)
            for value in (
                car.get("model"),
                car.get("generation"),
                car.get("title"),
            )
            if value
        )
    )
    if not haystack:
        return False

    for alias in target.get("aliases", []):
        if _alias_matches(haystack, alias):
            return True

    return _alias_matches(haystack, target.get("model"))


def build_target_search_url(brand: str, model: str, page: int) -> str:
    query = urlencode({"q": f"{brand} {model}"})
    url = f"{START_URL}?{query}"
    if page > 1:
        url = f"{url}&page={page}"
    return url


def find_target(brand: str, model: str) -> Optional[dict]:
    normalized_brand = normalize_model_name(brand)
    normalized_model = normalize_model_name(model)
    for target in TARGET_MODELS:
        if normalize_model_name(target["brand"]) != normalized_brand:
            continue
        if normalize_model_name(target["model"]) == normalized_model:
            return target
    return None


def _alias_matches(haystack: str, alias: Optional[str]) -> bool:
    normalized_alias = normalize_model_name(alias)
    if not normalized_alias:
        return False
    if normalized_alias.isdigit():
        return re.search(rf"(?<!\w){re.escape(normalized_alias)}[a-zа-я]?(?!\w)", haystack) is not None
    if normalized_alias in {"rx", "es", "lx", "x5", "k5"}:
        return re.search(rf"(?<!\w){re.escape(normalized_alias)}[0-9a-zа-я]{{0,4}}(?!\w)", haystack) is not None
    return re.search(rf"(?<!\w){re.escape(normalized_alias)}(?!\w)", haystack) is not None
