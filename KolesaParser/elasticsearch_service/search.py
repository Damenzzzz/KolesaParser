from typing import Any

from elasticsearch_service.client import ElasticsearchUnavailable, ensure_elasticsearch_available, get_elasticsearch_client
from elasticsearch_service.config import ELASTICSEARCH_INDEX, normalize_keyword_value
from elasticsearch_service.indexer import CARS_FIELDS
from scraper.query_config import (
    make_default_query_id,
    normalize_body_type,
    normalize_brand,
    normalize_city,
    normalize_fuel_type,
    normalize_model,
    normalize_transmission,
)


RESULT_FIELDS = [
    "listing_id",
    "url",
    "brand",
    "model",
    "year",
    "price",
    "city",
    "mileage_km",
    "engine_volume_l",
    "fuel_type",
    "transmission",
    "body_type",
    "description",
    "generated_description",
]

RANGE_FILTERS = {
    "year_from": ("year", "gte"),
    "year_to": ("year", "lte"),
    "price_min": ("price", "gte"),
    "price_max": ("price", "lte"),
    "engine_volume_from": ("engine_volume_l", "gte"),
    "engine_volume_to": ("engine_volume_l", "lte"),
    "mileage_min": ("mileage_km", "gte"),
    "mileage_max": ("mileage_km", "lte"),
}

KEYWORD_FILTERS = [
    "brand",
    "model",
    "city",
    "transmission",
    "fuel_type",
    "body_type",
]


def search_cars_by_query(query_config: dict[str, Any], limit: int = 50) -> dict[str, Any]:
    config = normalize_search_query_config(query_config)
    limit = max(0, int(limit or 50))
    client = ensure_elasticsearch_available(get_elasticsearch_client())
    body = build_elasticsearch_query(config, limit)
    try:
        response = client.search(index=ELASTICSEARCH_INDEX, **body)
    except Exception as exc:
        if exc.__class__.__name__ == "NotFoundError" or getattr(exc, "status_code", None) == 404:
            raise ElasticsearchUnavailable(
                f"Elasticsearch index '{ELASTICSEARCH_INDEX}' not found. Run: python scripts/index_cars_to_elastic.py"
            ) from exc
        raise
    hits = response.get("hits", {}).get("hits", [])
    cars = [_result_car(hit.get("_source", {})) for hit in hits]
    return {
        "query_id": config["query_id"],
        "source": "elasticsearch",
        "count": len(cars),
        "cars": cars,
    }


def build_elasticsearch_query(config: dict[str, Any], limit: int) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []

    for field in KEYWORD_FILTERS:
        value = normalize_keyword_value(field, config.get(field))
        if value:
            filters.append({"term": {f"{field}_normalized": value}})

    range_groups: dict[str, dict[str, Any]] = {}
    for config_field, (es_field, operator) in RANGE_FILTERS.items():
        value = config.get(config_field)
        if value is None:
            continue
        range_groups.setdefault(es_field, {})[operator] = value

    for es_field, range_filter in range_groups.items():
        filters.append({"range": {es_field: range_filter}})

    query: dict[str, Any]
    if filters:
        query = {"bool": {"filter": filters}}
    else:
        query = {"match_all": {}}

    return {
        "query": query,
        "size": limit,
        "track_total_hits": True,
        "_source": CARS_FIELDS,
    }


def normalize_search_query_config(query_config: dict[str, Any]) -> dict[str, Any]:
    config = dict(query_config)
    normalized = {
        "query_id": _clean_string(config.get("query_id")) or make_default_query_id(config),
        "brand": normalize_brand(config.get("brand")),
        "model": normalize_model(config.get("model")),
        "city": normalize_city(config.get("city")) or None,
        "year_from": _optional_int(config.get("year_from")),
        "year_to": _optional_int(config.get("year_to")),
        "price_min": _optional_int(config.get("price_min")),
        "price_max": _optional_int(config.get("price_max")),
        "engine_volume_from": _optional_float(config.get("engine_volume_from")),
        "engine_volume_to": _optional_float(config.get("engine_volume_to")),
        "mileage_min": _optional_int(config.get("mileage_min")),
        "mileage_max": _optional_int(config.get("mileage_max")),
        "transmission": normalize_transmission(config.get("transmission")),
        "fuel_type": normalize_fuel_type(config.get("fuel_type")),
        "body_type": normalize_body_type(config.get("body_type")),
    }
    return normalized


def _result_car(source: dict[str, Any]) -> dict[str, Any]:
    return {field: source.get(field) for field in RESULT_FIELDS}


def _clean_string(value: Any) -> str | None:
    if _is_ignored(value):
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if _is_ignored(value):
        return None
    try:
        return int(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return None


def _optional_float(value: Any) -> float | None:
    if _is_ignored(value):
        return None
    try:
        return float(str(value).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _is_ignored(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"", "any", "любой"}
    return False
