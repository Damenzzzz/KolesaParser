# KolesaParser

KolesaParser collects public car listing data from Kolesa.kz into SQLite and exports CSV files. The default engine is the HTTP parser. Playwright remains available only as an optional fallback.

The crawler is intentionally conservative. If Kolesa.kz starts blocking, rate-limiting, timing out repeatedly, or showing captcha/security pages, the parser stops safely and keeps the data already saved.

## Safety Limits

- No captcha bypass.
- No ban or rate-limit bypass.
- No proxy rotation, residential proxies, or mobile proxies.
- No rotating user agents to imitate many devices.
- No login or authentication.
- No phone numbers, seller contacts, private personal data, or hidden data.
- Public listing information only.

If the parser prints this message, stop and try again later with safe mode or night mode:

```text
Possible temporary rate limit or block. Stopped safely. Try later with safe-mode, balanced-mode, or night-mode.
```

Do not increase concurrency when blocks or timeouts happen.

## Database And Resume

SQLite database:

```text
data/cars.db
```

Normal runs never delete or recreate `data/cars.db`. Every saved listing is committed immediately. If a run stops at 2000 cars, run the same command later and it will continue from the existing database, skip duplicates, and add only unique listings.

Duplicate protection is enforced by:

```text
listing_id TEXT UNIQUE
url TEXT UNIQUE
```

`--limit` means target total database size. If the database already has 2000 cars:

```bash
python main.py collect --limit 15000 --night-mode
```

continues until the database reaches 15000 cars, or until the parser stops safely.

`--add` means add new unique cars on top of the current database count:

```bash
python main.py collect --add 5000 --night-mode --max-runtime-hours 8
```

## Safer Collection Modes

The first search page request starts immediately and logs a `0.0s` pause. Later search pages and all listing detail pages use the configured random pauses.

Normal mode is available, but it is not recommended for large runs. Use it only for small local checks.

Safe mode is for careful testing:

- concurrency: 1
- detail page delay: random 8-20 seconds
- search page delay: random 45-120 seconds
- max consecutive errors: 3
- stops on 403, 429, captcha/security pages, access denied pages, or repeated timeouts

Balanced mode is the normal practical collection mode:

- concurrency: 1
- detail page delay: random 3-7 seconds
- normal search page delay: random 8-18 seconds
- empty or duplicate-only brand page delay: random 2-6 seconds
- after every 100 saved listings: sleep random 2-5 minutes
- after every 500 saved listings: sleep random 8-15 minutes
- supports `--max-runtime-hours`
- stops safely on 403, 429, real captcha/security challenge pages, or repeated timeouts

Night mode is for slow unattended collection:

- concurrency: 1
- detail page delay: random 15-35 seconds
- search page delay: random 2-5 minutes
- after every 50 saved listings: sleep random 5-15 minutes
- after every 200 saved listings: sleep random 20-45 minutes
- supports `--max-runtime-hours`
- stops safely if too many errors happen

Recommended test:

```bash
python main.py collect --add 100 --safe-mode
```

Practical collection:

```bash
python main.py collect --add 100 --balanced-mode
```

Larger balanced run:

```bash
python main.py collect --limit 15000 --balanced-mode --max-runtime-hours 8
```

Custom balanced delays:

```bash
python main.py collect --add 500 --balanced-mode --detail-delay-min 2 --detail-delay-max 5 --search-delay-min 15 --search-delay-max 30
```

Very slow unattended run:

```bash
python main.py collect --limit 15000 --night-mode --max-runtime-hours 8
```

Continue later:

```bash
python main.py collect --limit 15000 --night-mode --max-runtime-hours 8
```

Add new unique cars:

```bash
python main.py collect --add 5000 --night-mode --max-runtime-hours 8
```

## Brand Collection

`collect-brands` is the recommended collection mode now. It opens the configured Kolesa brand pages directly instead of scanning all cars and filtering by target model later. This is faster and cleaner because it starts from pages like:

```text
https://kolesa.kz/cars/toyota/
https://kolesa.kz/cars/bmw/
https://kolesa.kz/cars/hyundai/
https://kolesa.kz/cars/kia/
https://kolesa.kz/cars/lexus/
https://kolesa.kz/cars/mercedes-benz/
```

It collects up to 2000 unique public listings for each selected brand, for a total brand target of 12000 cars. It does not filter by model in this mode, and it does not use proxies, rotating user agents, captcha bypass, or ban bypass logic.

For each brand page, the parser extracts only likely main listing cards. It skips duplicates before opening detail pages, skips cards that clearly belong to another brand before opening detail pages, then verifies the brand again after parsing the detail page. If a brand already has enough rows in SQLite, that brand page is not opened.

Existing `data/cars.db` is never deleted. Running the same command later continues from the existing SQLite database, skips duplicate `listing_id` and `url` values, and resumes brand pagination from:

```text
data/brand_parser_state.json
```

Test Playwright brand collection:

```bash
python main.py collect-brands --engine playwright --safe-mode --headless false
```

Practical run:

```bash
python main.py collect-brands --engine playwright --balanced-mode --headless false
```

Single-brand run:

```bash
python main.py collect-brands --brand BMW --engine playwright --balanced-mode --headless false
python main.py collect-brands --brand Mercedes --engine playwright --balanced-mode --headless false
```

Start selected brand from page 1 instead of saved page state:

```bash
python main.py collect-brands --brand BMW --ignore-state --engine playwright --balanced-mode --headless false
```

Long run:

```bash
python main.py collect-brands --engine playwright --night-mode --headless false --max-runtime-hours 8
```

HTTP brand collection:

```bash
python main.py collect-brands --engine http --balanced-mode
```

Check progress:

```bash
python main.py brand-report
```

`brand-report` prints progress per selected brand and exports:

```text
data/exports/brand_report.csv
```

## JSON query collection

`query-collect` is a separate mode for future Telegram bot and LLM integration. An LLM or another module produces JSON, and the parser reads it, uses brand/model/city to build the most specific Kolesa URL, tries year and price filters through the website, then tries engine volume and mileage filters when the page exposes them.

Final validation always happens in Python after each detail page is parsed. Existing `collect-brands` commands still work unchanged. Query results are saved in the normal SQLite `cars` table, linked to the query with a separate `query_results` table, and exported to separate CSV and JSON result files for every query.

Create `data/queries/query.json`:

```json
{
  "query_id": "toyota_camry_2023_almaty_under_20m",
  "brand": "Toyota",
  "model": "Camry",
  "city": "Алматы",
  "year_from": 2023,
  "year_to": 2023,
  "price_min": null,
  "price_max": 20000000,
  "engine_volume_from": null,
  "engine_volume_to": null,
  "mileage_min": null,
  "mileage_max": null,
  "transmission": null,
  "fuel_type": null,
  "body_type": null,
  "parse_minutes": 10,
  "max_results": 50,
  "output_csv": "data/exports/queries/toyota_camry_2023_almaty_under_20m.csv",
  "output_json": "data/exports/queries/toyota_camry_2023_almaty_under_20m.json"
}
```

Run:

```bash
python main.py query-collect --config data/queries/query.json --engine playwright --headless false
```

Run with a manual time override:

```bash
python main.py query-collect --config data/queries/query.json --minutes 10 --engine playwright --headless false
```

Run with checkpoint export:

```bash
python main.py query-collect --config data/queries/query_strict.json --engine playwright --headless false --checkpoint-export-every 10
```

## Elasticsearch search and dual query

Elasticsearch search works only on cars that already exist in `data/cars.db` and have been indexed into Elasticsearch. It does not parse Kolesa.kz.

By default the Elasticsearch client uses:

```text
ELASTICSEARCH_URL=http://localhost:9200
ELASTICSEARCH_INDEX=cars
```

You can override either value with environment variables.

First index existing SQLite cars:

```bash
python scripts/index_cars_to_elastic.py
```

Search existing DB rows through Elasticsearch with the same query JSON filters:

```bash
python main.py elastic-query --config data/queries/query_strict.json --limit 50
```

This writes:

```text
data/outputs/elastic/elastic_query_strict.json
```

Run both live parsing and Elasticsearch search from one query file:

```bash
python main.py dual-query --config data/queries/query_strict.json --engine playwright --balanced-mode --minutes 10
```

This writes:

```text
data/outputs/live/live_query_strict.json
data/outputs/elastic/elastic_query_strict.json
```

The live output has `"source": "live_parser"`. The Elasticsearch output has `"source": "elasticsearch"`.

## Targeted Model Collection

`collect-targets` is still available as the older model-focused mode. It collects only selected brand/model pairs and post-filters every detail page before saving, so cars outside the configured target list are skipped.

Configured targets include Toyota Camry, Corolla, RAV4, Prado; Hyundai Tucson, Elantra, Sonata; Kia Sportage, K5, Rio; Lexus RX, ES, LX; BMW X5, 5-Series, and 3-Series. Existing `data/cars.db` is never deleted. Running the same command later continues from the current database and skips duplicates by `listing_id` and `url`.

Target collection test:

```bash
python main.py collect-targets --safe-mode
```

Practical target collection:

```bash
python main.py collect-targets --balanced-mode
```

Long slower run:

```bash
python main.py collect-targets --night-mode --max-runtime-hours 8
```

HTTP target collection:

```bash
python main.py collect-targets --engine http --balanced-mode
```

Optional Playwright fallback:

```bash
python main.py collect-targets --engine playwright --headless false --safe-mode
```

Target progress:

```bash
python main.py target-report
```

`target-report` prints progress per selected model and exports:

```text
data/exports/target_model_report.csv
```

## Balancing

Before saving a listing, the parser checks current database counts:

- `MAX_PER_MODEL = 700`
- `MAX_PER_BRAND = 5000`

If a brand or brand+model has already hit the configured limit, that listing is skipped. If some public fields cannot be parsed, the parser stores `None` where appropriate and continues.

## Installation

```bash
python -m venv venv
```

Windows:

```bash
venv\Scripts\activate
```

macOS/Linux:

```bash
source venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
playwright install
```

Playwright browsers are needed only if you run `--engine playwright`.

## Commands

Careful test:

```bash
python main.py collect --add 100 --safe-mode
```

Practical collection:

```bash
python main.py collect --add 100 --balanced-mode
```

Larger balanced run:

```bash
python main.py collect --limit 15000 --balanced-mode --max-runtime-hours 8
```

Custom delay override:

```bash
python main.py collect --add 500 --balanced-mode --detail-delay-min 2 --detail-delay-max 5 --search-delay-min 15 --search-delay-max 30
```

Very slow unattended run:

```bash
python main.py collect --limit 15000 --night-mode --max-runtime-hours 8
```

Continue the same target later:

```bash
python main.py collect --limit 15000 --night-mode --max-runtime-hours 8
```

Report:

```bash
python main.py report
```

Brand progress:

```bash
python main.py brand-report
```

Target progress:

```bash
python main.py target-report
```

Export:

```bash
python main.py export
```

Optional Playwright fallback:

```bash
python main.py collect --limit 100 --engine playwright
```

## Exports

Full export:

```text
data/exports/cars_full.csv
```

ML-friendly export:

```text
data/exports/cars_ml.csv
```

`cars_ml.csv` includes only:

```text
brand, model, year, price, city, mileage_km, body_type, engine_volume_l,
fuel_type, transmission, drive_type, steering_wheel, color, condition
```

It excludes URLs, listing IDs, timestamps, raw characteristics, descriptions, and generated descriptions.

The report command also writes:

```text
data/exports/model_report.csv
```

## Logs

Logs are written to:

```text
logs/parser.log
```

Logs include mode, engine, concurrency, current DB count, target total limit, current brand, brand URL, brand page number, current search page, listing URL, saved listings, duplicate skips, wrong-brand skips, model and brand limit skips, HTTP/navigation errors, timeout errors, block detection reasons, retry numbers, backoff duration, pause duration, stop reasons, and final DB count.
