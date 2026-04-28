# ML Prediction Price

This folder scores car listings from `KolesaParser`, predicts a fair market price with the saved CatBoost model, ranks the best deals, and produces a Telegram-friendly explanation with an LLM fallback.

## Structure

- `ML_for_predict/`
  - `car_price_catboost.cbm`: saved CatBoost model.
  - `car_price_preprocess.pkl`: saved feature metadata.
  - `cars_ml_10k.csv`: training/export dataset used by the notebook.
  - `ML_last_variant.ipynb`: training notebook. It trains on `np.log1p(price)`, so runtime predictions are converted back with `np.expm1`.
  - `predict_price.py`: clean single-car prediction API.
  - `feature_adapter.py`: maps parser fields into model training fields.
- `LLM/`
  - `main.py`: `explain_ranked_cars(query, cars)` with deterministic fallback.
  - `proccessing.py`: older preprocessing helpers kept for compatibility.
- `catboost_info/`: CatBoost training logs.
- `outputs/`: generated scored/ranked pipeline outputs.

## Install

From `ML_prediction_price`:

```bash
cd ML_prediction_price
python -m pip install -r requirements.txt
```

## Run Local Test

From the `Total_project` root:

```bash
python ML_prediction_price/test_ml_pipeline.py
```

This writes:

```text
ML_prediction_price/outputs/test_scored_parser_output.json
ML_prediction_price/outputs/test_final_result.json
```

## Score Parser Output

From the `Total_project` root:

```bash
python ML_prediction_price/score_query_results.py --input KolesaParser/data/outputs/live/live_query_test_camry_35.json --output ML_prediction_price/outputs/scored_live_query_test_camry_35.json
```

The output adds:

- `predicted_price`
- `listed_price`
- `price_difference`
- `price_difference_percent`
- `price_status`
- `ml_error` when scoring fails

## Run ML + LLM Pipeline

From the `Total_project` root:

```bash
python ML_prediction_price/run_ml_llm_pipeline.py --query KolesaParser/data/queries/query_test_camry_35.json --input KolesaParser/data/outputs/live/live_query_test_camry_35.json --output ML_prediction_price/outputs/final_live_query_test_camry_35.json
```

This writes one final JSON containing the query, scored/ranked top cars, summary, and explanation data.

## LLM Behavior

`LLM/main.py` always provides deterministic fallback explanations, so tests and Telegram integration do not crash when API keys are missing.

To opt in to a real OpenAI call, set:

```text
OPENAI_API_KEY=...
ML_PREDICTION_USE_REAL_LLM=true
```

If the key is missing, the code logs:

```text
LLM API key not found, using fallback explanation
```

## Prompt to Car JSON

There are two separate LLM-facing jobs, and both are needed:

- `LLM/query_parser.py::extract_car_info(user_text)` converts a free-form user prompt into structured car JSON.
- `LLM/main.py::explain_ranked_cars(query, cars)` explains already scored and ranked cars for Telegram.

Prompt parsing also exposes:

- `extract_json_from_text(text)`: extracts JSON from an LLM response even when extra text surrounds it.
- `prompt_to_query_json(user_text)`: builds a query-like dict from parsed car info and simple constraints such as price or mileage ranges.
- `extract_car_info_tool(user_text)`: optional LangChain tool wrapper when LangChain is installed.

Local tests do not require an API key. If no LLM is configured, `extract_car_info` logs that fallback parsing is used and handles common prompts such as:

```text
Toyota Camry 2021 3.5 до 20 млн пробег 10-55 тысяч
```

Run the prompt parser check from the `Total_project` root:

```bash
python ML_prediction_price/test_query_parser.py
```

## Integration Plan

The intended flow is:

1. `KolesaParser` writes live query result JSON.
2. `score_query_results.py` or `run_ml_llm_pipeline.py` scores each car with CatBoost.
3. `ranker.py` ranks cars by deal quality minus risk.
4. `LLM/main.py` explains the ranked list for backend API or Telegram bot responses.
