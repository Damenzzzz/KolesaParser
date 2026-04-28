from typing import Any

from elasticsearch_service.config import ELASTICSEARCH_URL, ELASTICSEARCH_UNAVAILABLE_MESSAGE


class ElasticsearchUnavailable(RuntimeError):
    """Raised when Elasticsearch cannot be reached or the client package is missing."""


def get_elasticsearch_client():
    try:
        from elasticsearch import Elasticsearch
    except ImportError as exc:
        raise ElasticsearchUnavailable(
            "Python package 'elasticsearch' is not installed. Run: pip install -r requirements.txt"
        ) from exc

    return Elasticsearch(
        ELASTICSEARCH_URL,
        request_timeout=5,
        max_retries=0,
        retry_on_timeout=False,
    )


def ensure_elasticsearch_available(client: Any | None = None) -> Any:
    client = client or get_elasticsearch_client()
    try:
        client.info()
    except ElasticsearchUnavailable:
        raise
    except Exception as exc:
        raise ElasticsearchUnavailable(ELASTICSEARCH_UNAVAILABLE_MESSAGE) from exc
    return client
