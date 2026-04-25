# KolesaParser

Clean MVP Python parser for public car listings from Kolesa.kz. It uses Playwright to read public listing pages, stores structured data in SQLite, and exports CSV files for analysis or ML experiments.

## What It Does

- Starts from the public Kolesa.kz cars search pages.
- Collects public listing URLs.
- Opens each listing page with async Playwright and Chromium.
- Extracts public car information such as title, price, city, mileage, engine, body type, transmission, color, seller comment, and photo count when visible.
- Stores data in `data/cars.db`.
- Skips duplicates by `listing_id` and `url`, so collection can resume from an existing database.
- Exports full and ML-friendly CSV files.

The parser does not bypass captchas, does not log in, and does not collect phone numbers or private personal data.

## Why Playwright

Kolesa.kz listing pages load some public content dynamically. Playwright lets the parser wait for normal page rendering while still using a conservative, low-concurrency approach.

## Why SQLite

SQLite is built into Python, needs no separate server, and is enough for an MVP dataset. Unique constraints on `listing_id` and `url` make the parser resume-friendly.

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

## Test Collection

```bash
python main.py collect --limit 20 --headless false
```

Then run:

```bash
python main.py report
python main.py export
```

## CLI Commands

Collect a small test set:

```bash
python main.py collect --limit 20
```

Collect with a visible browser:

```bash
python main.py collect --limit 100 --headless false
```

Update from the first 3 search pages:

```bash
python main.py update --pages 3
```

Print dataset counts and export `data/exports/model_report.csv`:

```bash
python main.py report
```

Export CSV files:

```bash
python main.py export
```

Exports:

- `data/exports/cars_full.csv` contains all database columns.
- `data/exports/cars_ml.csv` contains ML-friendly feature columns only.

## Resume And Deduplication

The database is stored at `data/cars.db`. Before saving a listing, the parser checks whether the same `listing_id` or `url` already exists. SQLite also has `UNIQUE` constraints on both fields, so duplicates are skipped even if a listing appears on multiple search pages.

## Balancing

The MVP uses discovery mode from general search pages. Before saving, it checks the current database counts:

- `MAX_PER_MODEL = 700`
- `MAX_PER_BRAND = 5000`
- `TOTAL_LIMIT = 50000`

This helps prevent one common brand or model from dominating a larger dataset.

## Descriptions

`description` stores the original public seller comment from the listing page.

`generated_description` is a simple local template built from structured fields, for example:

```text
Toyota Camry 2020 in Almaty. Mileage: 85000 km. Engine: 2.5 L, petrol. Transmission: automatic. Price: 14500000 KZT.
```

The generated text never overwrites the original seller description.

## Ethical Limits

- No captcha bypass.
- No login or authentication scraping.
- No seller phone number scraping.
- No private personal data collection.
- Public listing data only.
- Polite scraping with low concurrency, random delays, retries, timeouts, and logs.

Logs are written to `logs/parser.log`.

## Selector Notes

Selectors are centralized in `scraper/selectors.py`. Kolesa.kz markup can change, so missing selectors are logged and the parser continues with `None` for fields that cannot be extracted reliably.
