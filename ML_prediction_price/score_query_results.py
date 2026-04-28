from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from .ML_for_predict.predict_price import predict_price
except ImportError:
    from ML_for_predict.predict_price import predict_price


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = MODULE_ROOT / "outputs"


def score_cars(cars: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    scored_cars: list[dict[str, Any]] = []
    scored_count = 0
    failed_count = 0

    for car in cars:
        car_with_score = dict(car)
        prediction = predict_price(car)
        ml_error = prediction.get("ml_error")
        if ml_error:
            failed_count += 1
            car_with_score["ml_error"] = ml_error
        else:
            scored_count += 1

        for field in (
            "predicted_price",
            "listed_price",
            "price_difference",
            "price_difference_percent",
            "price_status",
        ):
            car_with_score[field] = prediction.get(field)
        scored_cars.append(car_with_score)

    return scored_cars, scored_count, failed_count


def score_parser_payload(payload: dict[str, Any], source_file: str | Path) -> dict[str, Any]:
    cars = payload.get("cars")
    if not isinstance(cars, list):
        raise ValueError("Input JSON must contain a cars array")

    scored_cars, scored_count, failed_count = score_cars(cars)
    return {
        "source_file": str(source_file),
        "count": len(cars),
        "scored_count": scored_count,
        "failed_count": failed_count,
        "cars": scored_cars,
    }


def score_query_results(input_path: str | Path, output_path: str | Path | None = None) -> dict[str, Any]:
    resolved_input = resolve_existing_path(input_path)
    with resolved_input.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    result = score_parser_payload(payload, resolved_input)
    resolved_output = resolve_output_path(output_path or default_output_path(resolved_input))
    write_json(resolved_output, result)
    result["output_file"] = str(resolved_output)
    return result


def default_output_path(input_path: Path) -> Path:
    return DEFAULT_OUTPUT_DIR / f"scored_{input_path.name}"


def resolve_existing_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    cwd_candidate = Path.cwd() / candidate
    if cwd_candidate.exists():
        return cwd_candidate
    root_candidate = PROJECT_ROOT / candidate
    if root_candidate.exists():
        return root_candidate
    return cwd_candidate


def resolve_output_path(path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    cwd_candidate = Path.cwd() / candidate
    if cwd_candidate.parent.exists():
        return cwd_candidate
    return PROJECT_ROOT / candidate


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score KolesaParser query result JSON with the CatBoost price model.")
    parser.add_argument("--input", required=True, help="Parser output JSON with a cars array.")
    parser.add_argument("--output", required=True, help="Output scored JSON path.")
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    result = score_query_results(args.input, args.output)
    print(f"Scored cars: {result['scored_count']}")
    print(f"Failed cars: {result['failed_count']}")
    print(f"Output JSON: {result['output_file']}")


if __name__ == "__main__":
    main()
