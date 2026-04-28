from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


logger = logging.getLogger(__name__)

MODULE_ROOT = Path(__file__).resolve().parents[1]
LLM_DIR = Path(__file__).resolve().parent


def explain_ranked_cars(query: dict[str, Any], cars: list[dict[str, Any]]) -> dict[str, Any]:
    """Explain ranked cars for a Telegram-friendly recommendation result."""

    load_dotenv(MODULE_ROOT / ".env", override=False)
    load_dotenv(LLM_DIR / ".env", override=False)

    api_key = os.getenv("OPENAI_API_KEY")
    use_real_llm = os.getenv("ML_PREDICTION_USE_REAL_LLM", "").strip().lower() in {"1", "true", "yes"}
    if not api_key:
        logger.warning("LLM API key not found, using fallback explanation")
        return fallback_explanation(query, cars)
    if not use_real_llm:
        logger.warning("LLM API key found but real LLM is not enabled, using fallback explanation")
        return fallback_explanation(query, cars)

    try:
        return openai_explanation(query, cars, api_key)
    except Exception as exc:
        logger.warning("LLM explanation failed, using fallback explanation: %s: %s", exc.__class__.__name__, exc)
        return fallback_explanation(query, cars)


def openai_explanation(query: dict[str, Any], cars: list[dict[str, Any]], api_key: str) -> dict[str, Any]:
    try:
        from openai import OpenAI
    except ImportError as exc:
        raise RuntimeError("openai package is not installed") from exc

    compact_cars = [compact_car_for_prompt(car) for car in cars[:10]]
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        temperature=0.2,
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You explain Kazakhstan used-car search results. "
                    "Return only JSON with keys summary and top_cars_explanation. "
                    "Be concise, practical, and mention price reason and risk."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({"query": query, "cars": compact_cars}, ensure_ascii=False),
            },
        ],
    )
    content = response.choices[0].message.content or "{}"
    parsed = json.loads(content)
    return normalize_explanation(parsed, query, cars)


def fallback_explanation(query: dict[str, Any], cars: list[dict[str, Any]]) -> dict[str, Any]:
    query_label = " ".join(
        str(part)
        for part in (query.get("brand"), query.get("model"), query.get("year_from") or query.get("year"))
        if part
    ).strip() or "selected cars"

    if not cars:
        return {
            "summary": f"No ranked cars are available for {query_label}.",
            "top_cars_explanation": [],
        }

    explanations = []
    for car in cars[:10]:
        explanations.append(
            {
                "rank": car.get("rank"),
                "title": car_title(car),
                "short_reason": short_reason(car),
                "risk_note": risk_note(car),
                "url": car.get("url"),
            }
        )

    best = explanations[0]["title"]
    summary = (
        f"Ranked {len(cars)} option(s) for {query_label}. "
        f"The current top option is {best}; ranking uses ML fair price, mileage, year, and risk flags."
    )
    return {"summary": summary, "top_cars_explanation": explanations}


def normalize_explanation(
    explanation: dict[str, Any],
    query: dict[str, Any],
    cars: list[dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(explanation, dict):
        return fallback_explanation(query, cars)
    summary = explanation.get("summary")
    top = explanation.get("top_cars_explanation")
    if not isinstance(summary, str) or not isinstance(top, list):
        return fallback_explanation(query, cars)
    return {
        "summary": summary,
        "top_cars_explanation": [
            {
                "rank": item.get("rank"),
                "title": item.get("title"),
                "short_reason": item.get("short_reason"),
                "risk_note": item.get("risk_note"),
                "url": item.get("url"),
            }
            for item in top
            if isinstance(item, dict)
        ],
    }


def compact_car_for_prompt(car: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "rank",
        "title",
        "brand",
        "model",
        "year",
        "price",
        "predicted_price",
        "price_difference",
        "price_difference_percent",
        "price_status",
        "mileage_km",
        "city",
        "deal_score",
        "risk_score",
        "final_score",
        "url",
    ]
    return {field: car.get(field) for field in fields if field in car}


def car_title(car: dict[str, Any]) -> str:
    title = car.get("title")
    if title:
        return str(title)
    parts = [car.get("brand"), car.get("model"), car.get("year")]
    return " ".join(str(part) for part in parts if part) or "Car listing"


def short_reason(car: dict[str, Any]) -> str:
    status = car.get("price_status")
    percent = car.get("price_difference_percent")
    predicted = car.get("predicted_price")
    listed = car.get("listed_price") or car.get("price")
    if car.get("ml_error"):
        return "ML price estimate is unavailable for this listing."
    if status == "below_market":
        return f"Listed about {abs(float(percent or 0)):.1f}% below the ML fair price."
    if status == "above_market":
        return f"Listed about {float(percent or 0):.1f}% above the ML fair price."
    if predicted and listed:
        return "Listed close to the ML fair price."
    return "Ranking is based on available mileage, year, and listing data."


def risk_note(car: dict[str, Any]) -> str:
    risk = float(car.get("risk_score") or 0)
    percent = car.get("price_difference_percent")
    if car.get("ml_error"):
        return "High risk: the ML model could not score this car."
    if percent is not None and float(percent) < -25:
        return "Check carefully: the listing is much cheaper than the predicted market price."
    if risk >= 12:
        return "Higher risk because important fields are missing or mileage/year look less favorable."
    if risk >= 6:
        return "Moderate risk; verify condition, documents, and service history."
    return "No major ML/ranking risk flags from the available fields."


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
    sample_query = {"brand": "Toyota", "model": "Camry", "year_from": 2021}
    sample_cars = [
        {
            "rank": 1,
            "brand": "Toyota",
            "model": "Camry",
            "year": 2021,
            "listed_price": 18_500_000,
            "predicted_price": 19_700_000,
            "price_difference_percent": -6.09,
            "price_status": "below_market",
            "risk_score": 3,
            "url": "https://kolesa.kz/",
        }
    ]
    print(json.dumps(explain_ranked_cars(sample_query, sample_cars), ensure_ascii=False, indent=2))
