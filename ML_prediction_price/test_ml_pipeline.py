from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

try:
    from .LLM.main import explain_ranked_cars
    from .ML_for_predict.predict_price import predict_price
    from .ranker import rank_cars
    from .score_query_results import score_parser_payload, write_json
except ImportError:
    from LLM.main import explain_ranked_cars
    from ML_for_predict.predict_price import predict_price
    from ranker import rank_cars
    from score_query_results import score_parser_payload, write_json


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_ROOT / "outputs"
PARSER_SAMPLE = PROJECT_ROOT / "KolesaParser" / "data" / "outputs" / "live" / "live_query_test_camry_35.json"
LOCAL_SAMPLE = MODULE_ROOT / "test_data" / "sample_parser_output.json"


SAMPLE_CAR = {
    "brand": "Toyota",
    "model": "Camry",
    "year": 2021,
    "city": "Алматы",
    "mileage_km": 45000,
    "body_type": "sedan",
    "engine_volume_l": 3.5,
    "fuel_type": "petrol",
    "transmission": "automatic",
    "drive_type": "front",
    "steering_wheel": "left",
    "color": "white",
    "condition": "used",
    "price": 18500000,
}


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    single_prediction = predict_price(SAMPLE_CAR)

    input_path = PARSER_SAMPLE if PARSER_SAMPLE.exists() else LOCAL_SAMPLE
    with input_path.open("r", encoding="utf-8") as file:
        parser_payload = json.load(file)

    scored_payload = score_parser_payload(parser_payload, input_path)
    scored_path = OUTPUT_DIR / "test_scored_parser_output.json"
    write_json(scored_path, scored_payload)

    ranked = rank_cars(scored_payload["cars"], top_n=10)
    query = {"brand": "Toyota", "model": "Camry", "year_from": 2021}
    explanation = explain_ranked_cars(query, ranked)

    final_result = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "sample_prediction": single_prediction,
        "source_file": str(input_path),
        "scored_file": str(scored_path),
        "total_cars": scored_payload["count"],
        "scored_cars": scored_payload["scored_count"],
        "failed_cars": scored_payload["failed_count"],
        "top_cars": ranked,
        "llm_summary": explanation["summary"],
        "llm": explanation,
    }
    final_path = OUTPUT_DIR / "test_final_result.json"
    write_json(final_path, final_result)

    print(f"Single prediction status: {single_prediction.get('price_status')}")
    if single_prediction.get("ml_error"):
        print(f"Single prediction error: {single_prediction['ml_error']}")
    print(f"Source JSON: {input_path}")
    print(f"Scored cars: {scored_payload['scored_count']}")
    print(f"Failed cars: {scored_payload['failed_count']}")
    print(f"Final JSON: {final_path}")


if __name__ == "__main__":
    main()
