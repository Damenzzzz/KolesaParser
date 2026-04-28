from __future__ import annotations

import re
from typing import Any

import numpy as np
import pandas as pd


CURRENT_YEAR = 2026

BASE_COLUMNS = [
    "brand",
    "model",
    "year",
    "price",
    "city",
    "mileage_km",
    "body_type",
    "engine_volume_l",
    "fuel_type",
    "transmission",
    "drive_type",
    "steering_wheel",
    "color",
    "generation",
]

CAT_COLUMNS = [
    "brand",
    "model",
    "city",
    "body_type",
    "fuel_type",
    "transmission",
    "drive_type",
    "steering_wheel",
    "color",
    "generation",
]

NUM_FEATURES = [
    "year",
    "mileage_km",
    "engine_volume_l",
    "car_age",
    "mileage_per_year",
    "engine_volume_sq",
    "age_times_volume",
    "age_sq",
    "mileage_missing",
    "generation_missing",
    "generation_digits",
    "is_new",
    "is_luxury",
]

IMPORTANT_INPUT_FIELDS = [
    "brand",
    "model",
    "year",
    "mileage_km",
    "engine_volume_l",
]

BRAND_MAP = {
    "toyota": "Toyota",
    "тойота": "Toyota",
    "bmw": "BMW",
    "бмв": "BMW",
    "hyundai": "Hyundai",
    "хундай": "Hyundai",
    "хендай": "Hyundai",
    "kia": "Kia",
    "киа": "Kia",
    "chevrolet": "Chevrolet",
    "шевроле": "Chevrolet",
    "daewoo": "Daewoo",
    "дэу": "Daewoo",
    "subaru": "Subaru",
    "volkswagen": "Volkswagen",
    "фольксваген": "Volkswagen",
    "gaz": "ГАЗ",
    "газ": "ГАЗ",
    "vaz": "ВАЗ",
    "ваз": "ВАЗ",
}

CITY_MAP = {
    "almaty": "Алматы",
    "алматы": "Алматы",
    "astana": "Астана",
    "астана": "Астана",
    "shymkent": "Шымкент",
    "шымкент": "Шымкент",
    "aktau": "Актау",
    "актау": "Актау",
    "aktobe": "Актобе",
    "актобе": "Актобе",
    "atyrau": "Атырау",
    "атырау": "Атырау",
    "karaganda": "Караганда",
    "караганда": "Караганда",
    "kostanay": "Костанай",
    "костанай": "Костанай",
    "pavlodar": "Павлодар",
    "павлодар": "Павлодар",
    "taraz": "Тараз",
    "тараз": "Тараз",
    "uralsk": "Уральск",
    "уральск": "Уральск",
}

BODY_TYPE_MAP = {
    "sedan": "Седан",
    "сeдан": "Седан",
    "седан": "Седан",
    "crossover": "Кроссовер",
    "кроссовер": "Кроссовер",
    "suv": "Внедорожник",
    "jeep": "Внедорожник",
    "внедорожник": "Внедорожник",
    "hatchback": "Хэтчбек",
    "хэтчбек": "Хэтчбек",
    "wagon": "Универсал",
    "универсал": "Универсал",
    "minivan": "Минивэн",
    "минивэн": "Минивэн",
    "pickup": "Пикап",
    "пикап": "Пикап",
    "van": "Фургон",
    "фургон": "Фургон",
}

FUEL_TYPE_MAP = {
    "petrol": "petrol",
    "gasoline": "petrol",
    "бензин": "бензин",
    "diesel": "diesel",
    "дизель": "diesel",
    "hybrid": "hybrid",
    "гибрид": "hybrid",
    "gas": "gas",
    "газ": "gas",
    "petrol-gas": "petrol-gas",
    "бензин-газ": "petrol-gas",
    "electric": "other",
    "электро": "other",
}

TRANSMISSION_MAP = {
    "automatic": "automatic",
    "auto": "automatic",
    "автомат": "automatic",
    "manual": "manual",
    "механика": "manual",
    "cvt": "cvt",
    "вариатор": "cvt",
    "robot": "robot",
    "робот": "robot",
}

DRIVE_TYPE_MAP = {
    "front": "Передний привод",
    "fwd": "Передний привод",
    "передний": "Передний привод",
    "передний привод": "Передний привод",
    "rear": "Задний привод",
    "rwd": "Задний привод",
    "задний": "Задний привод",
    "задний привод": "Задний привод",
    "all": "Полный привод",
    "awd": "Полный привод",
    "4wd": "Полный привод",
    "full": "Полный привод",
    "полный": "Полный привод",
    "полный привод": "Полный привод",
}

STEERING_WHEEL_MAP = {
    "left": "Слева",
    "слева": "Слева",
    "левый": "Слева",
    "right": "Справа",
    "справа": "Справа",
    "правый": "Справа",
}

COLOR_MAP = {
    "white": "белый",
    "белый": "белый",
    "black": "черный",
    "черный": "черный",
    "gray": "серый",
    "grey": "серый",
    "серый": "серый",
    "silver": "серебристый",
    "серебристый": "серебристый",
    "red": "красный",
    "красный": "красный",
    "blue": "синий",
    "синий": "синий",
    "green": "зеленый",
    "зеленый": "зеленый",
    "brown": "коричневый металлик",
    "gold": "золотистый металлик",
}


def adapt_car_to_ml_row(car: dict[str, Any]) -> dict[str, Any]:
    """Map parser/API field names into the feature names used by training."""

    row = {
        "brand": normalize_brand(car.get("brand")),
        "model": normalize_model(car.get("model")),
        "year": to_number(car.get("year"), int),
        "price": to_number(car.get("price") or car.get("listed_price"), int),
        "city": normalize_by_map(car.get("city"), CITY_MAP),
        "mileage_km": to_number(car.get("mileage_km") or car.get("mileage"), float),
        "body_type": normalize_by_map(car.get("body_type"), BODY_TYPE_MAP),
        "engine_volume_l": to_number(car.get("engine_volume_l") or car.get("engine_volume"), float),
        "fuel_type": normalize_by_map(car.get("fuel_type"), FUEL_TYPE_MAP),
        "transmission": normalize_by_map(car.get("transmission"), TRANSMISSION_MAP),
        "drive_type": normalize_by_map(car.get("drive_type") or car.get("drive"), DRIVE_TYPE_MAP),
        "steering_wheel": normalize_by_map(
            car.get("steering_wheel") or car.get("steering"),
            STEERING_WHEEL_MAP,
        ),
        "color": normalize_by_map(car.get("color"), COLOR_MAP),
        "generation": normalize_generation_code(car.get("generation")),
    }
    return {column: row.get(column) for column in BASE_COLUMNS}


def missing_important_fields(car: dict[str, Any]) -> list[str]:
    row = adapt_car_to_ml_row(car)
    return [field for field in IMPORTANT_INPUT_FIELDS if row.get(field) in (None, "", "unknown")]


def prepare_feature_frame(car: dict[str, Any], meta: dict[str, Any]) -> pd.DataFrame:
    row = adapt_car_to_ml_row(car)
    df = clean_base(pd.DataFrame([row]), is_train=False)
    return make_X(
        df,
        category_maps=meta.get("category_maps", {}),
        features=meta.get("features"),
        train_mileage_median=meta.get("train_mileage_median"),
        current_year=int(meta.get("current_year") or CURRENT_YEAR),
    )


def normalize_brand(value: Any) -> str:
    return normalize_by_map(value, BRAND_MAP)


def normalize_model(value: Any) -> str:
    text = clean_text(value)
    if text is None:
        return "unknown"
    aliases = {
        "camry": "Camry",
        "камри": "Camry",
        "corolla": "Corolla",
        "rav4": "RAV4",
        "rav 4": "RAV4",
        "land cruiser prado": "Land Cruiser Prado",
        "prado": "Land Cruiser Prado",
        "land cruiser": "Land Cruiser",
        "x5": "X5",
        "x6": "X6",
        "x7": "X7",
        "k5": "K5",
        "rio": "Rio",
        "sportage": "Sportage",
        "sonata": "Sonata",
        "tucson": "Tucson",
        "elantra": "Elantra",
    }
    lowered = text.lower()
    if lowered in aliases:
        return aliases[lowered]
    return text


def normalize_by_map(value: Any, mapping: dict[str, str]) -> str:
    text = clean_text(value)
    if text is None:
        return "unknown"
    return mapping.get(text.lower(), text)


def clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    text = repair_mojibake(text)
    if not text or text.lower() in {"none", "nan", "null", "unknown", "неизвестно"}:
        return None
    return re.sub(r"\s+", " ", text)


def repair_mojibake(text: str) -> str:
    if "Р" not in text and "Ð" not in text:
        return text
    try:
        repaired = text.encode("cp1251").decode("utf-8")
    except UnicodeError:
        return text
    return repaired if repaired else text


def to_number(value: Any, caster):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", ".")
        value = re.sub(r"[^\d.\-]", "", value)
        if value in {"", ".", "-", "-."}:
            return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not np.isfinite(number):
        return None
    try:
        return caster(number)
    except (TypeError, ValueError, OverflowError):
        return None


def normalize_generation_code(value: Any) -> str:
    text = clean_text(value)
    if text is None:
        return "unknown"
    upper = text.upper()
    codes = re.findall(r"\b[A-Z]{1,4}\d{1,4}(?:/[A-Z]{1,4}\d{1,4})*\b", upper)
    if codes:
        return codes[-1]
    return upper


def clean_base(df: pd.DataFrame, is_train: bool = True) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(columns=["condition"], errors="ignore")
    if "price" not in df.columns:
        df["price"] = 0

    for column in BASE_COLUMNS:
        if column not in df.columns:
            df[column] = np.nan

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["engine_volume_l"] = pd.to_numeric(df["engine_volume_l"], errors="coerce")
    df["mileage_km"] = pd.to_numeric(df["mileage_km"], errors="coerce")
    df["generation"] = df["generation"].apply(normalize_generation_code)

    if is_train:
        df = df.dropna(subset=["brand", "model", "price", "year", "engine_volume_l"])
        df = df[(df["price"] > 100_000) & (df["price"] < 200_000_000)]
        df = df[(df["year"] >= 1990) & (df["year"] <= CURRENT_YEAR)]
        df = df[df["engine_volume_l"] > 0]

    if len(df) == 0:
        raise ValueError("After cleaning dataframe has 0 rows. Check input data.")

    return df.reset_index(drop=True)


def add_features(
    df: pd.DataFrame,
    train_mileage_median: float | None = None,
    current_year: int = CURRENT_YEAR,
) -> pd.DataFrame:
    df = df.copy()
    df["mileage_missing"] = df["mileage_km"].isna().astype(int)

    if train_mileage_median is None:
        train_mileage_median = df["mileage_km"].median()
        if pd.isna(train_mileage_median):
            train_mileage_median = -1

    df["mileage_km"] = df["mileage_km"].fillna(train_mileage_median)
    df["car_age"] = (current_year - df["year"]).clip(lower=0)
    df["mileage_per_year"] = df["mileage_km"] / (df["car_age"] + 1)
    df["engine_volume_sq"] = df["engine_volume_l"] ** 2
    df["age_times_volume"] = df["car_age"] * df["engine_volume_l"]
    df["age_sq"] = df["car_age"] ** 2

    generation = df["generation"].fillna("unknown").astype(str)
    df["generation_missing"] = generation.str.lower().isin(["unknown", "nan", ""]).astype(int)
    df["generation_digits"] = pd.to_numeric(
        generation.str.extract(r"(\d+)")[0],
        errors="coerce",
    ).fillna(-1)
    df["is_new"] = ((df["year"] >= current_year - 1) | (df["mileage_km"] == 0)).astype(int)
    df["is_luxury"] = df["brand"].isin(["Porsche", "BMW", "Mercedes-Benz", "Mercedes", "Audi", "Lexus"]).astype(int)
    return df


def apply_category_maps(df: pd.DataFrame, category_maps: dict[str, set[str]]) -> pd.DataFrame:
    df = df.copy()
    for column, allowed in category_maps.items():
        if column not in df.columns:
            df[column] = "unknown"
        values = df[column].fillna("unknown").astype(str)
        df[column] = values.where(values.isin(allowed), "other")
    return df


def make_X(
    df: pd.DataFrame,
    category_maps: dict[str, set[str]],
    features: list[str] | None = None,
    train_mileage_median: float | None = None,
    current_year: int = CURRENT_YEAR,
) -> pd.DataFrame:
    df = add_features(df, train_mileage_median=train_mileage_median, current_year=current_year)
    df = apply_category_maps(df, category_maps)

    use_cat = [column for column in CAT_COLUMNS if column in df.columns]
    use_num = [column for column in NUM_FEATURES if column in df.columns]
    X = df[use_cat + use_num].copy()

    for column in use_cat:
        X[column] = X[column].fillna("unknown").astype(str)
    for column in use_num:
        X[column] = pd.to_numeric(X[column], errors="coerce").fillna(-1)

    if features is not None:
        X = X.reindex(columns=features, fill_value=-1)
        for column in use_cat:
            if column in X.columns:
                X[column] = X[column].fillna("unknown").astype(str)
    return X
