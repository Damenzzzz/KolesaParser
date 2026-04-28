from __future__ import annotations

import logging
import pickle
from functools import lru_cache
from pathlib import Path
from typing import Any

import numpy as np

from .feature_adapter import adapt_car_to_ml_row, prepare_feature_frame, to_number


logger = logging.getLogger(__name__)

MODULE_DIR = Path(__file__).resolve().parent
MODEL_PATH = MODULE_DIR / "car_price_catboost.cbm"
PREPROCESSOR_PATH = MODULE_DIR / "car_price_preprocess.pkl"


class PredictionError(RuntimeError):
    """Raised when the CatBoost prediction pipeline cannot run."""


@lru_cache(maxsize=1)
def load_model_and_preprocessor():
    """Load the saved CatBoost model and preprocessing metadata."""

    if not MODEL_PATH.exists():
        raise PredictionError(f"CatBoost model file is missing: {MODEL_PATH}")
    if not PREPROCESSOR_PATH.exists():
        raise PredictionError(f"Preprocessor file is missing: {PREPROCESSOR_PATH}")

    try:
        from catboost import CatBoostRegressor
    except ImportError as exc:
        raise PredictionError("catboost is not installed. Run: python -m pip install -r ML_prediction_price/requirements.txt") from exc

    model = CatBoostRegressor()
    model.load_model(str(MODEL_PATH))

    with PREPROCESSOR_PATH.open("rb") as file:
        preprocessor = pickle.load(file)

    required_keys = {"features", "cat_features", "category_maps", "train_mileage_median"}
    missing_keys = sorted(required_keys - set(preprocessor))
    if missing_keys:
        raise PredictionError(f"Preprocessor metadata is missing keys: {', '.join(missing_keys)}")

    return model, preprocessor


def predict_price(car: dict[str, Any]) -> dict[str, Any]:
    """Predict fair market price for one car.

    The training notebook stores ``y_train = np.log1p(price)``, so model output
    is converted back to raw tenge with ``np.expm1``.
    """

    listed_price = _listed_price(car)
    try:
        model, preprocessor = load_model_and_preprocessor()
        features = prepare_feature_frame(car, preprocessor)
        prediction_log = float(model.predict(features)[0])
        predicted_price = int(round(max(0.0, float(np.expm1(prediction_log)))))
        return _build_success_result(predicted_price, listed_price)
    except Exception as exc:
        logger.exception("ML prediction failed")
        result = _empty_result(listed_price)
        result["price_status"] = "prediction_error"
        result["ml_error"] = f"{exc.__class__.__name__}: {exc}"
        return result


def _listed_price(car: dict[str, Any]) -> int | None:
    return to_number(car.get("price") or car.get("listed_price"), int)


def _build_success_result(predicted_price: int, listed_price: int | None) -> dict[str, Any]:
    result = {
        "predicted_price": predicted_price,
        "listed_price": listed_price,
        "price_difference": None,
        "price_difference_percent": None,
        "price_status": "unknown",
    }
    if listed_price is None or predicted_price <= 0:
        return result

    difference = int(listed_price - predicted_price)
    difference_percent = round((difference / predicted_price) * 100, 2)
    result.update(
        {
            "price_difference": difference,
            "price_difference_percent": difference_percent,
            "price_status": _price_status(difference_percent),
        }
    )
    return result


def _empty_result(listed_price: int | None) -> dict[str, Any]:
    return {
        "predicted_price": None,
        "listed_price": listed_price,
        "price_difference": None,
        "price_difference_percent": None,
        "price_status": "unknown",
    }


def _price_status(difference_percent: float) -> str:
    if difference_percent < -5:
        return "below_market"
    if difference_percent > 5:
        return "above_market"
    return "fair_market"


def adapted_features_for_debug(car: dict[str, Any]) -> dict[str, Any]:
    """Expose the adapted row for tests and debugging without loading CatBoost."""

    return adapt_car_to_ml_row(car)
