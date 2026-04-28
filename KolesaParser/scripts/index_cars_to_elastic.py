import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elasticsearch_service.client import ElasticsearchUnavailable
from elasticsearch_service.config import ELASTICSEARCH_INDEX, ELASTICSEARCH_URL, ELASTICSEARCH_UNAVAILABLE_MESSAGE
from elasticsearch_service.indexer import bulk_index_cars, create_cars_index, load_cars_from_sqlite


def main() -> None:
    try:
        create_cars_index()
        cars = load_cars_from_sqlite()
        indexed_count = bulk_index_cars(cars)
    except ElasticsearchUnavailable as exc:
        message = str(exc)
        print(message if "not installed" in message else ELASTICSEARCH_UNAVAILABLE_MESSAGE)
        return
    except Exception as exc:
        print(f"Elasticsearch indexing failed: {exc}")
        return

    print(f"Elasticsearch URL: {ELASTICSEARCH_URL}")
    print(f"Elasticsearch index: {ELASTICSEARCH_INDEX}")
    print(f"Loaded cars from SQLite: {len(cars)}")
    print(f"Indexed count: {indexed_count}")


if __name__ == "__main__":
    main()
