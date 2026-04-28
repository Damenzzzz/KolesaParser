import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from elasticsearch_service.client import ElasticsearchUnavailable
from elasticsearch_service.config import ELASTICSEARCH_UNAVAILABLE_MESSAGE, project_path
from elasticsearch_service.search import search_cars_by_query
from scraper.query_config import load_query_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a quick Elasticsearch search against indexed cars.")
    parser.add_argument("--config", default="data/queries/query_strict.json")
    parser.add_argument("--limit", type=int, default=10)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    try:
        query_config = load_query_config(project_path(args.config))
        result = search_cars_by_query(query_config, limit=args.limit)
    except ElasticsearchUnavailable as exc:
        message = str(exc)
        print(message if "not installed" in message else ELASTICSEARCH_UNAVAILABLE_MESSAGE)
        return
    except Exception as exc:
        print(f"Elasticsearch search failed: {exc}")
        return

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
