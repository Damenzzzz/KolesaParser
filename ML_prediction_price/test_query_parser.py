from __future__ import annotations

import os

from LLM.query_parser import extract_car_info, extract_json_from_text


def test_extract_json_from_clean_json() -> None:
    result = extract_json_from_text('{"brand": "Toyota", "model": "Camry"}')
    assert result == {"brand": "Toyota", "model": "Camry"}


def test_extract_json_from_wrapped_text() -> None:
    result = extract_json_from_text('Вот JSON:\n{"brand": "BMW", "year": 2021}\nГотово.')
    assert result == {"brand": "BMW", "year": 2021}


def test_extract_car_info_fallback() -> None:
    os.environ["ML_PREDICTION_FORCE_QUERY_PARSER_FALLBACK"] = "true"
    result = extract_car_info("Toyota Camry 2021 3.5 до 20 млн пробег 10-55 тысяч")
    assert result["brand"] == "Toyota"
    assert result["model"] == "Camry"
    assert result["year"] == 2021
    assert result["mileage_km"] is None
    assert result["engine_volume_l"] == 3.5
    assert result["generation"] == "unknown"


def main() -> None:
    test_extract_json_from_clean_json()
    test_extract_json_from_wrapped_text()
    test_extract_car_info_fallback()
    print("query parser tests passed")


if __name__ == "__main__":
    main()
