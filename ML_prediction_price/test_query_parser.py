from __future__ import annotations

import os
import logging
from contextlib import contextmanager
from collections.abc import Iterator

from LLM.query_parser import extract_car_info, extract_json_from_text


def test_extract_json_from_clean_json() -> None:
    result = extract_json_from_text('{"brand": "Toyota", "model": "Camry"}')
    assert result == {"brand": "Toyota", "model": "Camry"}


def test_extract_json_from_wrapped_text() -> None:
    result = extract_json_from_text('Вот JSON:\n{"brand": "BMW", "year": 2021}\nГотово.')
    assert result == {"brand": "BMW", "year": 2021}


def test_extract_car_info_fallback() -> None:
    with temporary_env(ML_PREDICTION_FORCE_QUERY_PARSER_FALLBACK="true"):
        result = extract_car_info("Toyota Camry 2021 3.5 до 20 млн пробег 10-55 тысяч")
    assert result["brand"] == "Toyota"
    assert result["model"] == "Camry"
    assert result["year"] == 2021
    assert result["mileage_km"] is None
    assert result["engine_volume_l"] == 3.5
    assert result["generation"] == "unknown"


def test_extract_car_info_real_llm_when_enabled() -> None:
    if os.getenv("ML_PREDICTION_USE_REAL_LLM", "").strip().lower() not in {"1", "true", "yes"}:
        print("real LLM query parser test skipped; ML_PREDICTION_USE_REAL_LLM is not true")
        return

    os.environ.setdefault("OPENAI_BASE_URL", "http://localhost:1234/v1")
    os.environ.setdefault("OPENAI_API_KEY", "lm-studio")
    os.environ.setdefault("OPENAI_MODEL", "google/gemma-4-e4b")
    os.environ.pop("ML_PREDICTION_FORCE_QUERY_PARSER_FALLBACK", None)

    print("real LLM query parser test enabled")
    result = extract_car_info("Toyota Camry 2021 3.5 до 20 млн пробег 10-55 тысяч")
    assert result["brand"] == "Toyota"
    assert result["model"] == "Camry"
    assert result["year"] == 2021
    assert result["engine_volume_l"] == 3.5


@contextmanager
def temporary_env(**updates: str) -> Iterator[None]:
    old_values = {key: os.environ.get(key) for key in updates}
    os.environ.update(updates)
    try:
        yield
    finally:
        for key, value in old_values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    test_extract_json_from_clean_json()
    test_extract_json_from_wrapped_text()
    test_extract_car_info_fallback()
    test_extract_car_info_real_llm_when_enabled()
    print("query parser tests passed")


if __name__ == "__main__":
    main()
