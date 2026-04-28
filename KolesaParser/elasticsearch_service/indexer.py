import sqlite3
from pathlib import Path
from typing import Any

from elasticsearch_service.client import ensure_elasticsearch_available, get_elasticsearch_client
from elasticsearch_service.config import (
    ELASTICSEARCH_INDEX,
    KEYWORD_FILTER_FIELDS,
    document_id_for_car,
    normalize_keyword_value,
    project_path,
)
from elasticsearch_service.mappings import CARS_INDEX_MAPPING


CARS_FIELDS = [
    "listing_id",
    "url",
    "brand",
    "model",
    "city",
    "year",
    "price",
    "mileage_km",
    "body_type",
    "engine_volume_l",
    "fuel_type",
    "transmission",
    "drive_type",
    "steering_wheel",
    "color",
    "condition",
    "description",
    "generated_description",
    "scraped_at",
]


def create_cars_index() -> bool:
    client = ensure_elasticsearch_available(get_elasticsearch_client())
    if client.indices.exists(index=ELASTICSEARCH_INDEX):
        return False
    client.indices.create(index=ELASTICSEARCH_INDEX, mappings=CARS_INDEX_MAPPING["mappings"])
    return True


def delete_cars_index() -> bool:
    client = ensure_elasticsearch_available(get_elasticsearch_client())
    if not client.indices.exists(index=ELASTICSEARCH_INDEX):
        return False
    client.indices.delete(index=ELASTICSEARCH_INDEX)
    return True


def index_car(car: dict[str, Any]) -> bool:
    client = ensure_elasticsearch_available(get_elasticsearch_client())
    document = prepare_car_document(car)
    document_id = document_id_for_car(document)
    if not document_id:
        return False
    client.index(index=ELASTICSEARCH_INDEX, id=document_id, document=document)
    return True


def bulk_index_cars(cars: list[dict[str, Any]]) -> int:
    client = ensure_elasticsearch_available(get_elasticsearch_client())
    try:
        from elasticsearch import helpers
    except ImportError as exc:
        from elasticsearch_service.client import ElasticsearchUnavailable

        raise ElasticsearchUnavailable(
            "Python package 'elasticsearch' is not installed. Run: pip install -r requirements.txt"
        ) from exc

    actions = []
    for car in cars:
        document = prepare_car_document(car)
        document_id = document_id_for_car(document)
        if not document_id:
            continue
        actions.append(
            {
                "_op_type": "index",
                "_index": ELASTICSEARCH_INDEX,
                "_id": document_id,
                "_source": document,
            }
        )

    if not actions:
        return 0

    indexed_count, errors = helpers.bulk(client, actions, raise_on_error=False, refresh=True)
    if errors:
        # The caller still gets the successful count; failed rows are intentionally not fatal.
        return int(indexed_count)
    return int(indexed_count)


def load_cars_from_sqlite(db_path: str | Path = "data/cars.db") -> list[dict[str, Any]]:
    resolved_path = project_path(db_path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {resolved_path}")

    conn = sqlite3.connect(resolved_path)
    conn.row_factory = sqlite3.Row
    try:
        columns_sql = ", ".join(CARS_FIELDS)
        rows = conn.execute(f"SELECT {columns_sql} FROM cars ORDER BY id ASC").fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def index_sqlite_cars(db_path: str | Path = "data/cars.db") -> int:
    create_cars_index()
    cars = load_cars_from_sqlite(db_path)
    return bulk_index_cars(cars)


def prepare_car_document(car: dict[str, Any]) -> dict[str, Any]:
    document = {
        field: _coerce_field(field, car.get(field))
        for field in CARS_FIELDS
        if _has_value(car.get(field))
    }
    for field in KEYWORD_FILTER_FIELDS:
        normalized = normalize_keyword_value(field, car.get(field))
        if normalized:
            document[f"{field}_normalized"] = normalized
    return document


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def _coerce_field(field: str, value: Any) -> Any:
    if value is None:
        return None
    if field in {"year", "price", "mileage_km"}:
        try:
            return int(float(str(value).replace(",", ".")))
        except (TypeError, ValueError):
            return None
    if field == "engine_volume_l":
        try:
            return float(str(value).replace(",", "."))
        except (TypeError, ValueError):
            return None
    return value
