from __future__ import annotations

from typing import Any

try:
    from .ML_for_predict.feature_adapter import IMPORTANT_INPUT_FIELDS, adapt_car_to_ml_row, to_number
except ImportError:
    from ML_for_predict.feature_adapter import IMPORTANT_INPUT_FIELDS, adapt_car_to_ml_row, to_number


def rank_cars(cars: list[dict[str, Any]], top_n: int = 10) -> list[dict[str, Any]]:
    ranked: list[dict[str, Any]] = []
    for car in cars:
        enriched = dict(car)
        deal_score = _deal_score(enriched)
        risk_score = _risk_score(enriched)
        final_score = round(deal_score - risk_score, 2)
        enriched["deal_score"] = round(deal_score, 2)
        enriched["risk_score"] = round(risk_score, 2)
        enriched["final_score"] = final_score
        ranked.append(enriched)

    ranked.sort(key=lambda item: item.get("final_score", -9999), reverse=True)
    for index, car in enumerate(ranked[:top_n], 1):
        car["rank"] = index
    return ranked[:top_n]


def _deal_score(car: dict[str, Any]) -> float:
    percent = to_number(car.get("price_difference_percent"), float)
    score = 0.0
    if percent is not None:
        score += max(-30.0, min(35.0, -percent))

    mileage = to_number(car.get("mileage_km") or car.get("mileage"), float)
    if mileage is not None:
        if mileage <= 50_000:
            score += 5
        elif mileage <= 100_000:
            score += 3
        elif mileage <= 160_000:
            score += 1
        elif mileage > 250_000:
            score -= 3

    year = to_number(car.get("year"), int)
    if year is not None:
        if year >= 2023:
            score += 3
        elif year >= 2020:
            score += 2
        elif year >= 2016:
            score += 1
        elif year < 2010:
            score -= 2
    return score


def _risk_score(car: dict[str, Any]) -> float:
    score = 0.0
    if car.get("ml_error"):
        score += 20

    row = adapt_car_to_ml_row(car)
    missing = [field for field in IMPORTANT_INPUT_FIELDS if row.get(field) in (None, "", "unknown")]
    score += len(missing) * 3

    percent = to_number(car.get("price_difference_percent"), float)
    if percent is not None:
        if percent < -30:
            score += 14
        elif percent < -20:
            score += 8
        elif percent > 25:
            score += 5

    mileage = to_number(car.get("mileage_km") or car.get("mileage"), float)
    if mileage is None:
        score += 4
    elif mileage > 250_000:
        score += 6
    elif mileage > 180_000:
        score += 3

    year = to_number(car.get("year"), int)
    if year is None:
        score += 4
    elif year < 2008:
        score += 6
    elif year < 2013:
        score += 3

    if not car.get("url"):
        score += 2
    return score
