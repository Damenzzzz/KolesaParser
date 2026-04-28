from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv


logger = logging.getLogger(__name__)

MODULE_ROOT = Path(__file__).resolve().parents[1]
LLM_DIR = Path(__file__).resolve().parent

try:
    from langchain.tools import tool
except ImportError:
    try:
        from langchain_core.tools import tool
    except ImportError:

        def tool(func: Callable | None = None, *args, **kwargs):
            if func is None:
                return lambda wrapped: wrapped
            return func


CAR_INFO_SYSTEM_PROMPT = """
Ты превращаешь текст пользователя в JSON с характеристиками автомобиля.

Верни ТОЛЬКО JSON. Без markdown, без объяснений.

Поля:
brand, model, year, mileage_km, engine_volume_l, fuel_type,
transmission, drive_type, steering_wheel, color, generation.

Правила:
- Если бренд написан по-русски, переведи в английское название: Тойота -> Toyota, БМВ -> BMW.
- Если модель Камри -> Camry.
- fuel_type используй: бензин, дизель, гибрид, электро, газ.
- transmission используй: Автомат, Механика, Вариатор, Робот.
- drive_type используй: Передний привод, Задний привод, Полный привод.
- steering_wheel используй: Слева или Справа.
- Если поле неизвестно, ставь null.
- generation если неизвестно, ставь "unknown".

Пример:
{
  "brand": "Toyota",
  "model": "Camry",
  "year": 2021,
  "mileage_km": 80000,
  "engine_volume_l": 2.5,
  "fuel_type": "бензин",
  "transmission": "Автомат",
  "drive_type": "Передний привод",
  "steering_wheel": "Слева",
  "color": "Серый",
  "generation": "XV70"
}
"""


def extract_json_from_text(text: str) -> dict:
    """
    Достаёт JSON из ответа LLM.
    Даже если модель случайно добавила текст вокруг JSON.
    """
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"LLM did not return JSON: {text}")

    return json.loads(match.group(0))


def extract_car_info(user_text: str) -> dict:
    llm = _build_llm()
    if llm is None:
        logger.warning("LLM API key not found, using fallback car-info extraction")
        return fallback_extract_car_info(user_text)

    response = llm.invoke(
        [
            {"role": "system", "content": CAR_INFO_SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ]
    )
    content = getattr(response, "content", str(response))
    return normalize_car_info(extract_json_from_text(content))


def prompt_to_query_json(user_text: str) -> dict:
    car_info = extract_car_info(user_text)
    query = dict(car_info)
    text = user_text.lower()

    price_max = _extract_price_max(text)
    if price_max is not None:
        query["price_max"] = price_max

    mileage_min, mileage_max = _extract_mileage_range(text)
    if mileage_min is not None:
        query["mileage_min"] = mileage_min
    if mileage_max is not None:
        query["mileage_max"] = mileage_max

    query.setdefault("query_id", _query_id_from_car_info(car_info))
    query.setdefault("max_results", 10)
    query.setdefault("parse_minutes", 3)
    return query


@tool
def extract_car_info_tool(user_text: str) -> str:
    """Convert a user car description into a structured JSON string."""

    return json.dumps(extract_car_info(user_text), ensure_ascii=False)


def _build_llm():
    load_dotenv(MODULE_ROOT / ".env", override=False)
    load_dotenv(LLM_DIR / ".env", override=False)

    if os.getenv("ML_PREDICTION_FORCE_QUERY_PARSER_FALLBACK", "").strip().lower() in {"1", "true", "yes"}:
        return None

    use_real_llm = os.getenv("ML_PREDICTION_USE_REAL_LLM", "").strip().lower() in {"1", "true", "yes"}
    base_url = os.getenv("LLM_BASE_URL") or os.getenv("OPENAI_BASE_URL")
    api_key = os.getenv("OPENAI_API_KEY")

    if not use_real_llm and not base_url:
        return None
    if not api_key:
        if base_url and ("localhost" in base_url or "127.0.0.1" in base_url):
            api_key = "lm-studio"
        else:
            return None

    try:
        from langchain_openai import ChatOpenAI
    except ImportError:
        logger.warning("langchain_openai is not installed, using fallback car-info extraction")
        return None

    return ChatOpenAI(
        base_url=base_url,
        api_key=api_key,
        model=os.getenv("QUERY_PARSER_MODEL") or os.getenv("OPENAI_MODEL") or "gpt-4o-mini",
        temperature=0,
    )


def fallback_extract_car_info(user_text: str) -> dict:
    text = user_text.lower()
    car_info = {
        "brand": _extract_brand(text),
        "model": _extract_model(text),
        "year": _extract_year(text),
        "mileage_km": _extract_single_mileage(text),
        "engine_volume_l": _extract_engine_volume(text),
        "fuel_type": _extract_fuel_type(text),
        "transmission": _extract_transmission(text),
        "drive_type": _extract_drive_type(text),
        "steering_wheel": _extract_steering_wheel(text),
        "color": _extract_color(text),
        "generation": _extract_generation(user_text) or "unknown",
    }
    return normalize_car_info(car_info)


def normalize_car_info(car_info: dict[str, Any]) -> dict[str, Any]:
    fields = [
        "brand",
        "model",
        "year",
        "mileage_km",
        "engine_volume_l",
        "fuel_type",
        "transmission",
        "drive_type",
        "steering_wheel",
        "color",
        "generation",
    ]
    normalized = {field: car_info.get(field) for field in fields}
    if normalized.get("generation") in (None, ""):
        normalized["generation"] = "unknown"
    return normalized


def _extract_brand(text: str) -> str | None:
    patterns = {
        "Toyota": ["toyota", "тойота"],
        "BMW": ["bmw", "бмв"],
        "Hyundai": ["hyundai", "хендай", "хундай"],
        "Kia": ["kia", "киа"],
        "Lexus": ["lexus", "лексус"],
        "Mercedes-Benz": ["mercedes", "mercedes-benz", "мерседес"],
    }
    return _first_match(text, patterns)


def _extract_model(text: str) -> str | None:
    patterns = {
        "Camry": ["camry", "камри"],
        "Corolla": ["corolla", "королла"],
        "RAV4": ["rav4", "rav 4", "рав4", "рав 4"],
        "Land Cruiser Prado": ["land cruiser prado", "prado", "прадо"],
        "Land Cruiser": ["land cruiser", "ленд крузер"],
        "X5": ["x5"],
        "X6": ["x6"],
        "X7": ["x7"],
        "K5": ["k5"],
        "Rio": ["rio", "рио"],
        "Sportage": ["sportage", "спортейдж"],
        "Sonata": ["sonata", "соната"],
        "Tucson": ["tucson", "туссан", "туксон"],
        "Elantra": ["elantra", "элантра"],
    }
    return _first_match(text, patterns)


def _first_match(text: str, patterns: dict[str, list[str]]) -> str | None:
    for normalized, aliases in patterns.items():
        if any(alias in text for alias in aliases):
            return normalized
    return None


def _extract_year(text: str) -> int | None:
    match = re.search(r"\b(19[9]\d|20[0-2]\d|2026)\b", text)
    return int(match.group(1)) if match else None


def _extract_engine_volume(text: str) -> float | None:
    match = re.search(r"\b([1-6](?:[.,]\d)?)\s*(?:л|литр|l|engine)?\b", text)
    if not match:
        return None
    return float(match.group(1).replace(",", "."))


def _extract_single_mileage(text: str) -> int | None:
    range_min, range_max = _extract_mileage_range(text)
    if range_min is not None and range_max is not None:
        return None
    match = re.search(r"(?:пробег|mileage)\D{0,10}(\d{1,3}(?:\s?\d{3})?|\d+)\s*(?:тыс|тысяч|км|km)?", text)
    if not match:
        return None
    value = _parse_int(match.group(1))
    if value is not None and re.search(r"(?:тыс|тысяч)", match.group(0)):
        value *= 1000
    return value


def _extract_mileage_range(text: str) -> tuple[int | None, int | None]:
    match = re.search(r"пробег\D{0,12}(\d{1,4})\s*[-–]\s*(\d{1,4})\s*(?:тыс|тысяч|км|km)?", text)
    if not match:
        return None, None
    start = _parse_int(match.group(1))
    end = _parse_int(match.group(2))
    if re.search(r"(?:тыс|тысяч)", match.group(0)):
        start = start * 1000 if start is not None else None
        end = end * 1000 if end is not None else None
    return start, end


def _extract_price_max(text: str) -> int | None:
    match = re.search(r"(?:до|under|max)\s*(\d+(?:[.,]\d+)?)\s*(?:млн|миллион|million|m)?", text)
    if not match:
        return None
    value = float(match.group(1).replace(",", "."))
    if re.search(r"(?:млн|миллион|million|m)", match.group(0)):
        value *= 1_000_000
    return int(value)


def _extract_fuel_type(text: str) -> str | None:
    if "бенз" in text or "petrol" in text or "gasoline" in text:
        return "бензин"
    if "диз" in text or "diesel" in text:
        return "дизель"
    if "гибрид" in text or "hybrid" in text:
        return "гибрид"
    if "электро" in text or "electric" in text:
        return "электро"
    if re.search(r"\bгаз\b", text):
        return "газ"
    return None


def _extract_transmission(text: str) -> str | None:
    if "автомат" in text or "automatic" in text:
        return "Автомат"
    if "механ" in text or "manual" in text:
        return "Механика"
    if "вариатор" in text or "cvt" in text:
        return "Вариатор"
    if "робот" in text or "robot" in text:
        return "Робот"
    return None


def _extract_drive_type(text: str) -> str | None:
    if "перед" in text or "front" in text or "fwd" in text:
        return "Передний привод"
    if "зад" in text or "rear" in text or "rwd" in text:
        return "Задний привод"
    if "полный" in text or "awd" in text or "4wd" in text:
        return "Полный привод"
    return None


def _extract_steering_wheel(text: str) -> str | None:
    if "слева" in text or "лев" in text or "left" in text:
        return "Слева"
    if "справа" in text or "прав" in text or "right" in text:
        return "Справа"
    return None


def _extract_color(text: str) -> str | None:
    colors = {
        "Серый": ["серый", "серая", "gray", "grey"],
        "Белый": ["белый", "белая", "white"],
        "Черный": ["черный", "черная", "black"],
        "Синий": ["синий", "синяя", "blue"],
        "Красный": ["красный", "красная", "red"],
        "Серебристый": ["серебристый", "silver"],
    }
    return _first_match(text, colors)


def _extract_generation(text: str) -> str | None:
    match = re.search(r"\b[A-ZА-Я]{1,4}\d{1,4}(?:/[A-ZА-Я]{1,4}\d{1,4})*\b", text.upper())
    return match.group(0) if match else None


def _parse_int(value: str) -> int | None:
    cleaned = re.sub(r"\D", "", value)
    return int(cleaned) if cleaned else None


def _query_id_from_car_info(car_info: dict[str, Any]) -> str:
    parts = [car_info.get("brand"), car_info.get("model"), car_info.get("year")]
    slug = "_".join(str(part).lower().replace(" ", "_") for part in parts if part)
    return slug or "car_query"
