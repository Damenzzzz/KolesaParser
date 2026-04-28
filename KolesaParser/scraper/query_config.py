import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from scraper.brand_targets import normalize_brand_name
from scraper.config import BASE_URL, PLAYWRIGHT_TIMEOUT_MS, PROJECT_ROOT
from scraper.utils import normalize_text


OPTIONAL_FIELDS = {
    "query_id",
    "city",
    "year_from",
    "year_to",
    "price_min",
    "price_max",
    "engine_volume_from",
    "engine_volume_to",
    "mileage_min",
    "mileage_max",
    "transmission",
    "fuel_type",
    "body_type",
    "parse_minutes",
    "max_results",
    "output_csv",
    "output_json",
}

FILTER_FIELDS = [
    "city",
    "year_from",
    "year_to",
    "price_min",
    "price_max",
    "engine_volume_from",
    "engine_volume_to",
    "mileage_min",
    "mileage_max",
    "transmission",
    "fuel_type",
    "body_type",
]

IGNORE_VALUES = {"", "any", "\u043b\u044e\u0431\u043e\u0439"}

BRAND_SLUGS = {
    "Toyota": "toyota",
    "BMW": "bmw",
    "Hyundai": "hyundai",
    "Kia": "kia",
    "Lexus": "lexus",
    "Mercedes-Benz": "mercedes-benz",
}

MODEL_SLUGS = {
    ("Toyota", "camry"): "camry",
    ("Toyota", "corolla"): "corolla",
    ("Toyota", "rav4"): "rav4",
    ("Toyota", "prado"): "prado",
    ("Toyota", "land cruiser 150"): "land-cruiser-150",
    ("BMW", "x5"): "x5",
    ("BMW", "5 series"): "5-series",
    ("BMW", "3 series"): "3-series",
    ("Hyundai", "tucson"): "tucson",
    ("Hyundai", "elantra"): "elantra",
    ("Hyundai", "sonata"): "sonata",
    ("Kia", "sportage"): "sportage",
    ("Kia", "k5"): "k5",
    ("Kia", "rio"): "rio",
    ("Lexus", "rx"): "rx",
    ("Lexus", "es"): "es",
    ("Lexus", "lx"): "lx",
    ("Mercedes-Benz", "e class"): "e-class",
    ("Mercedes-Benz", "c class"): "c-class",
    ("Mercedes-Benz", "s class"): "s-class",
    ("Mercedes-Benz", "glc"): "glc",
    ("Mercedes-Benz", "gle"): "gle",
}

CITY_SLUGS = {
    "\u0410\u043b\u043c\u0430\u0442\u044b": "almaty",
    "\u0430\u043b\u043c\u0430\u0442\u044b": "almaty",
    "almaty": "almaty",
    "\u0410\u0441\u0442\u0430\u043d\u0430": "astana",
    "\u0430\u0441\u0442\u0430\u043d\u0430": "astana",
    "astana": "astana",
    "\u0428\u044b\u043c\u043a\u0435\u043d\u0442": "shymkent",
    "\u0448\u044b\u043c\u043a\u0435\u043d\u0442": "shymkent",
    "shymkent": "shymkent",
    "\u041a\u0430\u0440\u0430\u0433\u0430\u043d\u0434\u0430": "karaganda",
    "\u043a\u0430\u0440\u0430\u0433\u0430\u043d\u0434\u0430": "karaganda",
    "karaganda": "karaganda",
}

SITE_NUMERIC_FILTERS = {
    "year_from": "year[from]",
    "year_to": "year[to]",
    "price_min": "price[from]",
    "price_max": "price[to]",
    "engine_volume_from": "auto-car-volume[from]",
    "engine_volume_to": "auto-car-volume[to]",
    "mileage_min": "auto-run[from]",
    "mileage_max": "auto-run[to]",
}

SITE_TRANSMISSION_VALUES = {
    "manual": "1",
    "automatic": "2345",
    "variator": "4",
    "robot": "5",
}

SITE_FUEL_VALUES = {
    "petrol": "1",
    "diesel": "2",
    "gas-petrol": "3",
    "gas": "4",
    "hybrid": "5",
    "electric": "6",
}

SITE_BODY_VALUES = {
    "sedan": "11",
    "\u0441\u0435\u0434\u0430\u043d": "11",
    "station wagon": "12",
    "\u0443\u043d\u0438\u0432\u0435\u0440\u0441\u0430\u043b": "12",
    "hatchback": "13",
    "\u0445\u044d\u0442\u0447\u0431\u0435\u043a": "13",
    "coupe": "14",
    "\u043a\u0443\u043f\u0435": "14",
    "convertible": "15",
    "\u043a\u0430\u0431\u0440\u0438\u043e\u043b\u0435\u0442": "15",
    "limousine": "16",
    "\u043b\u0438\u043c\u0443\u0437\u0438\u043d": "16",
    "suv": "21",
    "\u0432\u043d\u0435\u0434\u043e\u0440\u043e\u0436\u043d\u0438\u043a": "21",
    "crossover": "23",
    "\u043a\u0440\u043e\u0441\u0441\u043e\u0432\u0435\u0440": "23",
    "minivan": "31",
    "\u043c\u0438\u043d\u0438\u0432\u044d\u043d": "31",
    "van": "32",
    "\u043c\u0438\u043a\u0440\u043e\u0430\u0432\u0442\u043e\u0431\u0443\u0441": "32",
    "pickup": "22",
    "\u043f\u0438\u043a\u0430\u043f": "22",
}


def load_query_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.is_absolute():
        config_path = PROJECT_ROOT / config_path
    with config_path.open("r", encoding="utf-8") as config_file:
        data = json.load(config_file)
    if not isinstance(data, dict):
        raise ValueError("query config must be a JSON object")
    return data


def validate_query_config(config: dict[str, Any]) -> None:
    for field in ("brand", "model"):
        if _is_ignored(config.get(field)):
            raise ValueError(f"query config missing required field: {field}")


def normalize_query_config(config: dict[str, Any]) -> dict[str, Any]:
    validate_query_config(config)
    normalized: dict[str, Any] = {}
    normalized["brand"] = normalize_brand(config["brand"])
    normalized["model"] = str(config["model"]).strip()
    normalized["query_id"] = _clean_string(config.get("query_id")) or make_default_query_id(config)

    normalized["city"] = normalize_city(config.get("city")) or None
    normalized["year_from"] = _optional_int(config.get("year_from"), "year_from")
    normalized["year_to"] = _optional_int(config.get("year_to"), "year_to")
    normalized["price_min"] = _optional_int(config.get("price_min"), "price_min")
    normalized["price_max"] = _optional_int(config.get("price_max"), "price_max")
    normalized["engine_volume_from"] = _optional_float(config.get("engine_volume_from"), "engine_volume_from")
    normalized["engine_volume_to"] = _optional_float(config.get("engine_volume_to"), "engine_volume_to")
    normalized["mileage_min"] = _optional_int(config.get("mileage_min"), "mileage_min")
    normalized["mileage_max"] = _optional_int(config.get("mileage_max"), "mileage_max")
    normalized["transmission"] = normalize_transmission(config.get("transmission"))
    normalized["fuel_type"] = normalize_fuel_type(config.get("fuel_type"))
    normalized["body_type"] = normalize_body_type(config.get("body_type"))
    normalized["parse_minutes"] = _optional_float(config.get("parse_minutes"), "parse_minutes") or 10.0
    normalized["max_results"] = _optional_int(config.get("max_results"), "max_results") or 50
    normalized["output_csv"] = _clean_string(config.get("output_csv")) or str(
        Path("data") / "exports" / "queries" / f"{normalized['query_id']}.csv"
    )
    normalized["output_json"] = _clean_string(config.get("output_json")) or str(
        Path("data") / "exports" / "queries" / f"{normalized['query_id']}.json"
    )
    normalized["base_url"] = build_model_url(normalized["brand"], normalized["model"], normalized.get("city"))
    normalized["model_url_exact"] = _model_slug(normalized["brand"], normalized["model"]) is not None
    return normalized


def build_model_url(brand: str, model: str, city: str | None = None) -> str:
    canonical_brand = normalize_brand(brand)
    brand_slug = BRAND_SLUGS.get(canonical_brand)
    if not brand_slug:
        return f"{BASE_URL}/cars/"

    model_slug = _model_slug(canonical_brand, model)
    if not model_slug:
        return f"{BASE_URL}/cars/{brand_slug}/"

    parts = [BASE_URL.rstrip("/"), "cars", brand_slug, model_slug]
    city_slug = _city_slug(city)
    if city_slug:
        parts.append(city_slug)
    return "/".join(parts) + "/"


async def apply_site_filters(page, config: dict[str, Any]) -> dict[str, list[str]]:
    applied: list[str] = []
    not_applied: list[str] = []

    available_names = await _available_form_names(page)
    pending_params: dict[str, str] = {}
    pending_fields: list[str] = []

    for field, parameter_name in SITE_NUMERIC_FILTERS.items():
        value = config.get(field)
        if value is None:
            continue
        if parameter_name not in available_names:
            not_applied.append(field)
            continue
        pending_params[parameter_name] = _site_number(value)
        pending_fields.append(field)

    categorical_filters = [
        ("transmission", "auto-car-transm", normalize_transmission, SITE_TRANSMISSION_VALUES),
        ("fuel_type", "auto-fuel", normalize_fuel_type, SITE_FUEL_VALUES),
        ("body_type", "auto-car-body", normalize_body_type, SITE_BODY_VALUES),
    ]
    for field, parameter_name, normalizer, value_map in categorical_filters:
        value = normalizer(config.get(field))
        if not value:
            continue
        site_value = value_map.get(value)
        if not site_value or parameter_name not in available_names:
            not_applied.append(field)
            continue
        pending_params[parameter_name] = site_value
        pending_fields.append(field)

    if not pending_params:
        return {"applied": applied, "not_applied": not_applied}

    filtered_url = _url_with_query_params(page.url, pending_params)
    try:
        await page.goto(filtered_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
        applied.extend(pending_fields)
    except Exception:
        not_applied.extend(pending_fields)
    return {"applied": applied, "not_applied": not_applied}


def make_default_query_id(config: dict[str, Any]) -> str:
    parts = [
        normalize_brand(config.get("brand")),
        normalize_model(config.get("model")),
        normalize_city(config.get("city")),
        str(config.get("year_from") or ""),
        str(config.get("year_to") or ""),
        str(config.get("price_max") or ""),
    ]
    raw = "_".join(part for part in parts if part)
    slug = re.sub(r"[^0-9a-zA-Z]+", "_", raw.lower()).strip("_")
    return slug or "query"


def normalize_brand(value: Any) -> str:
    return normalize_brand_name(str(value).strip()) if not _is_ignored(value) else ""


def normalize_model(value: Any) -> str:
    if _is_ignored(value):
        return ""
    text = normalize_text(value)
    if not text:
        return ""
    text = text.lower().replace("\u0451", "\u0435")
    text = re.sub(r"[^0-9a-z\u0430-\u044f]+", " ", text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def normalize_city(value: Any) -> str:
    text = normalize_model(value)
    city_aliases = {
        "almaty": "\u0410\u043b\u043c\u0430\u0442\u044b",
        "\u0430\u043b\u043c\u0430\u0442\u044b": "\u0410\u043b\u043c\u0430\u0442\u044b",
        "astana": "\u0410\u0441\u0442\u0430\u043d\u0430",
        "\u0430\u0441\u0442\u0430\u043d\u0430": "\u0410\u0441\u0442\u0430\u043d\u0430",
        "shymkent": "\u0428\u044b\u043c\u043a\u0435\u043d\u0442",
        "\u0448\u044b\u043c\u043a\u0435\u043d\u0442": "\u0428\u044b\u043c\u043a\u0435\u043d\u0442",
        "karaganda": "\u041a\u0430\u0440\u0430\u0433\u0430\u043d\u0434\u0430",
        "\u043a\u0430\u0440\u0430\u0433\u0430\u043d\u0434\u0430": "\u041a\u0430\u0440\u0430\u0433\u0430\u043d\u0434\u0430",
    }
    return city_aliases.get(text, text)


def normalize_transmission(value: Any) -> str | None:
    if _is_ignored(value):
        return None
    text = normalize_model(value)
    if not text:
        return None
    mapping = {
        "automatic": "automatic",
        "auto": "automatic",
        "\u0430\u0432\u0442\u043e\u043c\u0430\u0442": "automatic",
        "\u0430\u043a\u043f\u043f": "automatic",
        "manual": "manual",
        "\u043c\u0435\u0445\u0430\u043d\u0438\u043a\u0430": "manual",
        "\u043c\u043a\u043f\u043f": "manual",
        "cvt": "variator",
        "variator": "variator",
        "\u0432\u0430\u0440\u0438\u0430\u0442\u043e\u0440": "variator",
        "robot": "robot",
        "\u0440\u043e\u0431\u043e\u0442": "robot",
    }
    return mapping.get(text, text)


def normalize_fuel_type(value: Any) -> str | None:
    if _is_ignored(value):
        return None
    text = normalize_model(value)
    if not text:
        return None
    mapping = {
        "petrol": "petrol",
        "gasoline": "petrol",
        "\u0431\u0435\u043d\u0437\u0438\u043d": "petrol",
        "diesel": "diesel",
        "\u0434\u0438\u0437\u0435\u043b\u044c": "diesel",
        "gas petrol": "gas-petrol",
        "petrol gas": "gas-petrol",
        "gas-petrol": "gas-petrol",
        "petrol-gas": "gas-petrol",
        "\u0433\u0430\u0437 \u0431\u0435\u043d\u0437\u0438\u043d": "gas-petrol",
        "\u0431\u0435\u043d\u0437\u0438\u043d \u0433\u0430\u0437": "gas-petrol",
        "gas": "gas",
        "\u0433\u0430\u0437": "gas",
        "electric": "electric",
        "\u044d\u043b\u0435\u043a\u0442\u0440\u043e": "electric",
        "\u044d\u043b\u0435\u043a\u0442\u0440\u0438\u0447\u0435\u0441\u0442\u0432\u043e": "electric",
        "electricity": "electric",
        "hybrid": "hybrid",
        "\u0433\u0438\u0431\u0440\u0438\u0434": "hybrid",
    }
    return mapping.get(text, text)


def normalize_body_type(value: Any) -> str | None:
    if _is_ignored(value):
        return None
    text = normalize_model(value)
    return text or None


def _clean_string(value: Any) -> str | None:
    if _is_ignored(value):
        return None
    text = normalize_text(value)
    return text.strip() if text else None


def _optional_int(value: Any, field: str) -> int | None:
    if _is_ignored(value):
        return None
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number") from exc


def _optional_float(value: Any, field: str) -> float | None:
    if _is_ignored(value):
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a number") from exc


def _is_ignored(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in IGNORE_VALUES
    return False


def _model_slug(brand: str, model: str) -> str | None:
    return MODEL_SLUGS.get((normalize_brand(brand), normalize_model(model)))


def _city_slug(city: str | None) -> str | None:
    normalized = normalize_city(city)
    return CITY_SLUGS.get(normalized) or CITY_SLUGS.get(normalized.lower())


async def _available_form_names(page) -> set[str]:
    try:
        names = await page.evaluate(
            """
            () => Array.from(document.querySelectorAll('input[name], select[name]'))
                .map((element) => element.getAttribute('name'))
                .filter(Boolean)
            """
        )
    except Exception:
        return set()
    return {str(name) for name in names}


def _site_number(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _url_with_query_params(url: str, params: dict[str, str]) -> str:
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.pop("page", None)
    for key, value in params.items():
        query[key] = value
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))
