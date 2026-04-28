import re
import pandas as pd
import numpy as np


CURRENT_YEAR = 2026

MIN_MODEL_COUNT = 50
RARE_CATEGORY_MIN_COUNT = 10
POPULAR_BRANDS = None


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


def normalize_generation_code(x):
    """
    Приводит generation к коду.
    Если в колонке уже XV70/J150/F15 — оставит код.
    Если там полный текст — попробует вытащить код.
    """
    if pd.isna(x):
        return "unknown"

    s = str(x).strip().upper()
    if s in ["", "NAN", "NONE", "UNKNOWN", "0"]:
        return "unknown"

    # Ищем коды вида XV70, J150, F15, W212/S212, G05, etc.
    codes = re.findall(r"\b[A-ZА-Я]{1,4}\d{1,4}(?:/[A-ZА-Я]{1,4}\d{1,4})*\b", s)
    if codes:
        return codes[-1]

    return s


def clean_base(df, is_train=True, min_model_count=MIN_MODEL_COUNT, popular_brands=POPULAR_BRANDS):
   
    df = df.copy()

    # Удаляем condition полностью.
    df = df.drop(columns=["condition"], errors="ignore")

    if "price" not in df.columns:
        df["price"] = 0

    # Обязательные колонки для стабильности.
    required_cols = [
        "brand", "model", "year", "price", "city", "mileage_km",
        "body_type", "engine_volume_l", "fuel_type", "transmission",
        "drive_type", "steering_wheel", "color", "generation"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Типы.
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["engine_volume_l"] = pd.to_numeric(df["engine_volume_l"], errors="coerce")
    df["mileage_km"] = pd.to_numeric(df["mileage_km"], errors="coerce")

    # generation = только код.
    df["generation"] = df["generation"].apply(normalize_generation_code)

    if is_train:
        # Убираем плохие строки только при обучении.
        df = df.dropna(subset=["brand", "model", "price", "year", "engine_volume_l"])
        df = df[(df["price"] > 100_000) & (df["price"] < 200_000_000)]
        df = df[(df["year"] >= 1990) & (df["year"] <= 2026)]
        df = df[df["engine_volume_l"] > 0]

        if popular_brands is not None:
            df = df[df["brand"].isin(popular_brands)]

        # Фильтр популярных моделей.
        # НЕЛЬЗЯ применять на manual predict.
        if min_model_count and min_model_count > 1:
            counts = df["model"].value_counts()
            good_models = counts[counts >= min_model_count].index
            df = df[df["model"].isin(good_models)]

    if len(df) == 0:
        raise ValueError("After cleaning dataframe has 0 rows. Check filters / input data.")

    return df.reset_index(drop=True)

def add_features(df, train_mileage_median=None):
    df = df.copy()

    # Mileage.
    df["mileage_missing"] = df["mileage_km"].isna().astype(int)

    if train_mileage_median is None:
        train_mileage_median = df["mileage_km"].median()
        if pd.isna(train_mileage_median):
            train_mileage_median = -1

    df["mileage_km"] = df["mileage_km"].fillna(train_mileage_median)

    # Age.
    df["car_age"] = (CURRENT_YEAR - df["year"]).clip(lower=0)

    # Numerical interactions.
    df["mileage_per_year"] = df["mileage_km"] / (df["car_age"] + 1)
    df["engine_volume_sq"] = df["engine_volume_l"] ** 2
    df["age_times_volume"] = df["car_age"] * df["engine_volume_l"]
    df["age_sq"] = df["car_age"] ** 2

    # Generation features.
    gen = df["generation"].fillna("unknown").astype(str)
    df["generation_missing"] = gen.str.lower().isin(["unknown", "nan", ""]).astype(int)

    df["generation_digits"] = gen.str.extract(r"(\d+)")[0]
    df["generation_digits"] = pd.to_numeric(df["generation_digits"], errors="coerce").fillna(-1)

    # Flags.
    df["is_new"] = ((df["year"] >= CURRENT_YEAR - 1) | (df["mileage_km"] == 0)).astype(int)
    df["is_luxury"] = df["brand"].isin(["Porsche", "BMW", "Mercedes-Benz", "Mercedes", "Audi", "Lexus"]).astype(int)

    return df


def fit_category_maps(df, cat_columns=CAT_COLUMNS, min_count=RARE_CATEGORY_MIN_COUNT):
    """
    Сохраняем допустимые категории только на train.
    Редкие категории идут в 'other'.
    Новые категории на predict тоже идут в 'other'.
    """
    maps = {}

    for col in cat_columns:
        if col in df.columns:
            s = df[col].fillna("unknown").astype(str)
            counts = s.value_counts()
            allowed = set(counts[counts >= min_count].index)
            allowed.add("unknown")
            allowed.add("other")
            maps[col] = allowed

    return maps


def apply_category_maps(df, category_maps):
    df = df.copy()

    for col, allowed in category_maps.items():
        if col not in df.columns:
            df[col] = "unknown"

        s = df[col].fillna("unknown").astype(str)
        s = s.where(s.isin(allowed), "other")
        df[col] = s

    return df

def make_X(df, category_maps, features=None, train_mileage_median=None):
    """
    Делает X для CatBoost.
    Категории остаются строками — CatBoost сам их обработает.
    """
    df = add_features(df, train_mileage_median=train_mileage_median)
    df = apply_category_maps(df, category_maps)

    use_cat = [c for c in CAT_COLUMNS if c in df.columns]
    use_num = [c for c in NUM_FEATURES if c in df.columns]

    X = df[use_cat + use_num].copy()

    for col in use_cat:
        X[col] = X[col].fillna("unknown").astype(str)

    for col in use_num:
        X[col] = pd.to_numeric(X[col], errors="coerce").fillna(-1)

    if features is not None:
        X = X.reindex(columns=features, fill_value=-1)
        for col in use_cat:
            if col in X.columns:
                X[col] = X[col].fillna("unknown").astype(str)

    return X