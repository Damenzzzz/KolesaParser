# AI Car Search & Price Recommendation System

AI-powered car search and price evaluation system for Kolesa.kz.

The project lets a user search for cars from Telegram using natural language, converts the prompt into a structured query, searches Kolesa.kz data through both live parsing and Elasticsearch, scores listings with a CatBoost price model, and returns a ranked Telegram-friendly recommendation list.

## Short Description

This repository combines four services into one car recommendation pipeline:

- Searches car listings from Kolesa.kz.
- Uses a live parser and Elasticsearch over already collected local data.
- Uses an ML model to estimate a fair market price for each listing.
- Compares listed price against predicted price and ranks cars by deal quality and risk.
- Uses an LLM or fallback explanation layer to prepare concise top recommendations.
- Uses a Telegram bot as the user interface.

Example user prompt:

```text
BMW X5 от 10 до 20 млн пробег 50-100 тысяч
```

## Architecture

```text
Telegram User
    |
    v
telegram_bot_AI
    |
    v
backend_api
    |
    v
Query Parser / LLM
    |
    v
KolesaParser
   /        \
  v          v
Live Parser   Elasticsearch
  |            |
  v            v
live JSON    elastic JSON
   \          /
    v        v
ML_prediction_price
    |
    v
Top cars + explanation
    |
    v
Telegram bot response
```

Main flow:

1. The user sends a natural-language prompt in Telegram.
2. `telegram_bot_AI` sends the prompt to `backend_api`.
3. `backend_api` converts the prompt into query JSON using the query parser or LLM layer.
4. `KolesaParser` uses the query JSON for live parsing from Kolesa.kz and Elasticsearch search over collected local data.
5. The parser produces live result JSON and elastic result JSON.
6. `ML_prediction_price` predicts fair market price with CatBoost, compares listed price with predicted price, and calculates deal, risk, and final scores.
7. The LLM or fallback explanation layer prepares a Telegram-friendly top-10 answer.
8. The Telegram bot sends the ranked result back to the user.

## Folder Structure

```text
Total_project/
├── KolesaParser/
├── ML_prediction_price/
├── backend_api/
├── telegram_bot_AI/
├── docker-compose.yml
└── README.md
```

### `KolesaParser/`

- Kolesa.kz parser.
- Query JSON configuration files.
- Live and Elasticsearch output JSON files.
- Elasticsearch indexing and search service.
- SQLite database for local car storage.

### `ML_prediction_price/`

- CatBoost model and price prediction pipeline.
- Feature adapter for parser outputs.
- Fair price prediction.
- Deal, risk, and final ranking logic.
- LLM or fallback explanation generation.
- Prompt-to-query parsing utilities.

### `backend_api/`

- FastAPI backend.
- Orchestrates parser, Elasticsearch, ML scoring, and LLM/fallback explanation.
- Exposes synchronous search, background jobs, health, and popular cars endpoints.

### `telegram_bot_AI/`

- Telegram bot interface.
- Sends user prompts to the backend.
- Displays ranked results and recommendation explanations.

### `docker-compose.yml`

- Local Elasticsearch service for development.

## Main Features

- Telegram-based car search.
- Prompt-to-query JSON conversion.
- Live parsing from Kolesa.kz.
- Elasticsearch search over already collected cars.
- CatBoost fair price prediction.
- Deal, risk, and final ranking.
- Top-10 recommendation output.
- Background jobs API.
- Local development with Docker Elasticsearch.

## Requirements

- Python 3.11+ or compatible.
- Docker Desktop.
- Elasticsearch started through `docker-compose.yml`.
- Telegram Bot Token.
- Optional LM Studio or another OpenAI-compatible LLM server.
- Optional OpenAI API key.

## Environment Variables

Create `telegram_bot_AI/.env` for the Telegram bot:

```env
BOT_TOKEN=...
BACKEND_URL=http://127.0.0.1:8000
DEFAULT_MINUTES=1
DEFAULT_MODE=balanced
DEFAULT_HEADLESS=false
DEFAULT_USE_LIVE=false
DEFAULT_USE_ELASTIC=true
DEFAULT_TOP_N=10
```

Optional LLM variables:

```env
ML_PREDICTION_USE_REAL_LLM=true
OPENAI_BASE_URL=http://localhost:1234/v1
OPENAI_API_KEY=lm-studio
OPENAI_MODEL=google/gemma-4-e4b
```

Keep real tokens and API keys in `.env` files. Do not commit private credentials.

## Setup

Run these commands from the `Total_project` root.

### 1. Create and activate a virtual environment

```powershell
python -m venv venv
venv\Scripts\activate
```

### 2. Install dependencies

```powershell
python -m pip install -r KolesaParser\requirements.txt
python -m pip install -r ML_prediction_price\requirements.txt
python -m pip install -r backend_api\requirements.txt
python -m pip install -r telegram_bot_AI\requirements.txt
```

### 3. Start Elasticsearch

```powershell
docker compose up -d elasticsearch
```

### 4. Check Elasticsearch

```powershell
curl http://localhost:9200
```

### 5. Index cars into Elasticsearch

```powershell
cd KolesaParser
python scripts/index_cars_to_elastic.py
cd ..
```

### 6. Start the backend

```powershell
python -m uvicorn backend_api.main:app --reload --port 8000
```

### 7. Open Swagger

```text
http://127.0.0.1:8000/docs
```

### 8. Start the Telegram bot in a second terminal

```powershell
cd telegram_bot_AI
python bot.py
```

## API Endpoints

- `GET /health`
- `GET /popular`
- `POST /search/sync`
- `POST /jobs/search`
- `GET /jobs/{job_id}`

### Example `POST /search/sync` body with prompt

```json
{
  "prompt": "BMW X5 от 10 до 20 млн пробег 50-100 тысяч",
  "query_file": null,
  "minutes": 1,
  "mode": "balanced",
  "headless": false,
  "use_live": false,
  "use_elastic": true,
  "top_n": 10
}
```

### Example `POST /search/sync` body with query file

```json
{
  "prompt": null,
  "query_file": "KolesaParser/data/queries/query_test_camry_35.json",
  "minutes": 1,
  "mode": "balanced",
  "headless": false,
  "use_live": false,
  "use_elastic": true,
  "top_n": 10
}
```

## Parser Commands

Run from `KolesaParser`:

```powershell
cd KolesaParser
```

Live parser plus Elasticsearch search:

```powershell
python main.py dual-query --config data/queries/query_test_camry_35.json --engine playwright --balanced-mode --minutes 3 --headless false
```

Elasticsearch-only query:

```powershell
python main.py elastic-query --config data/queries/query_test_camry_35.json --limit 10
```

Index local cars into Elasticsearch:

```powershell
python scripts/index_cars_to_elastic.py
```

## ML Commands

Run from the `Total_project` root:

```powershell
python ML_prediction_price/test_query_parser.py
python ML_prediction_price/test_ml_pipeline.py
```

Run the ML and explanation pipeline on a parser output:

```powershell
python ML_prediction_price/run_ml_llm_pipeline.py --query KolesaParser/data/queries/query_test_camry_35.json --input KolesaParser/data/outputs/live/live_query_test_camry_35.json --output ML_prediction_price/outputs/final_live_query_test_camry_35.json
```

## Telegram Bot Usage

1. Start the backend.
2. Start the Telegram bot.
3. Open the bot in Telegram and send `/start`.
4. Press the Search button.
5. Send a prompt, for example:

```text
Toyota Camry 2023 3.5 до 30 млн пробег 50-100 тысяч
```

The bot returns ranked cars with ML fair price, final score, risks, and listing links.

## Development Notes

- For safe testing, use `DEFAULT_USE_LIVE=false`.
- Live parsing may be limited by website availability or rate limits.
- Elasticsearch only searches already indexed local data.
- Generated outputs, logs, database files, exports, and virtual environments should not be committed.
- Secrets should stay in `.env` files and should not be pushed to GitHub.

## Known Limitations

- Live parsing can be limited by Kolesa.kz availability and rate limits.
- ML model quality depends on the collected dataset.
- LLM usage is optional; fallback explanations are available.
- Telegram output quality depends on query parsing quality and available car data.

## GitHub Note

This repository contains source code and small model artifacts.

Large generated files such as databases, logs, parser outputs, and exports are ignored by `.gitignore`.
