import os
import re
from pathlib import Path
from typing import Any

from scraper.query_config import (
    normalize_body_type,
    normalize_brand,
    normalize_city,
    normalize_fuel_type,
    normalize_model,
    normalize_transmission,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ELASTICSEARCH_URL = "http://localhost:9200"
DEFAULT_ELASTICSEARCH_INDEX = "cars"

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", DEFAULT_ELASTICSEARCH_URL)
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", DEFAULT_ELASTICSEARCH_INDEX)

ELASTICSEARCH_UNAVAILABLE_MESSAGE = (
    f"Elasticsearch is not available. Start Elasticsearch on {ELASTICSEARCH_URL} and rerun indexing."
)

KEYWORD_FILTER_FIELDS = {
    "brand",
    "model",
    "city",
    "transmission",
    "fuel_type",
    "body_type",
}

BODY_TYPE_ALIASES = {
    "sedan": "sedan",
    "седан": "sedan",
    "station wagon": "station wagon",
    "универсал": "station wagon",
    "hatchback": "hatchback",
    "хэтчбек": "hatchback",
    "coupe": "coupe",
    "купе": "coupe",
    "convertible": "convertible",
    "кабриолет": "convertible",
    "limousine": "limousine",
    "лимузин": "limousine",
    "suv": "suv",
    "внедорожник": "suv",
    "crossover": "crossover",
    "кроссовер": "crossover",
    "minivan": "minivan",
    "минивэн": "minivan",
    "van": "van",
    "микроавтобус": "van",
    "pickup": "pickup",
    "пикап": "pickup",
}


def project_path(path: str | Path) -> Path:
    resolved = Path(path)
    if resolved.is_absolute():
        return resolved
    return PROJECT_ROOT / resolved


def normalize_keyword_value(field: str, value: Any) -> str | None:
    if value is None:
        return None

    if field == "brand":
        normalized = normalize_brand(value)
    elif field == "model":
        normalized = normalize_model(value)
    elif field == "city":
        normalized = normalize_city(value)
    elif field == "transmission":
        normalized = normalize_transmission(value)
    elif field == "fuel_type":
        normalized = normalize_fuel_type(value)
    elif field == "body_type":
        normalized = normalize_body_type(value)
        normalized = BODY_TYPE_ALIASES.get(normalized or "", normalized)
    else:
        normalized = str(value).strip()

    if normalized is None:
        return None
    text = str(normalized).strip()
    return text or None


def document_id_for_car(car: dict[str, Any]) -> str | None:
    listing_id = car.get("listing_id")
    if listing_id is not None and str(listing_id).strip():
        return str(listing_id).strip()
    url = car.get("url")
    if url is not None and str(url).strip():
        return str(url).strip()
    return None


def clean_query_stem(stem: str) -> str:
    slug = re.sub(r"[^0-9a-zA-Z_\-]+", "_", stem).strip("_")
    return slug or "query"

