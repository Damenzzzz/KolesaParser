# KolesaParser

KolesaParser is a resume-friendly Python project for collecting public car listing data from Kolesa.kz into SQLite and exporting CSV files.

The default parser uses async HTTP requests because it is much faster and lighter than opening every page in a browser. Playwright is still included as an optional fallback, but normal collection uses HTTP.

## Ethical Limits

- No captcha bypass.
- No ban or anti-bot bypass.
- No login or authentication.
- No phone numbers, seller contacts, private personal data, or hidden data.
- Public listing information only.
- If Kolesa.kz returns 403, 429, captcha/block pages, or too many errors, the parser slows down and stops safely instead of trying to evade protection.

## What Gets Stored

SQLite database:

```text
data/cars.db
```

The parser stores structured public listing fields such as brand, model, year, price, city, mileage, body type, engine, fuel type, transmission, color, seller description when publicly visible, and a generated template description.

`description` is the original seller text from the public listing page.

`generated_description` is our own simple template from structured fields. It is not made by ML or an LLM, and it never overwrites the original seller description.

## Resume And Duplicates

Normal runs never delete or recreate `data/cars.db`.

If you stop after 15k cars and run the parser later, it continues from the existing database. Duplicates are skipped by both `listing_id` and `url` using SQLite unique constraints.

Important: `--limit` means target total database size, not "collect this many more".

Examples:

```bash
python main.py collect --limit 15000
python main.py collect --limit 30000
python main.py collect --limit 50000
```

If the database already has 15k rows, `--limit 30000` collects about 15k more unique rows.

Use `--add` when you want to add a fixed number on top of the current database count:

```bash
python main.py collect --add 15000 --concurrency 3
```

`--limit` and `--add` cannot be used together.

## Balancing

Before saving a listing, the parser checks current database counts:

- `MAX_PER_MODEL = 700`
- `MAX_PER_BRAND = 5000`

If a brand or model has already hit the configured limit, that listing is skipped. This helps avoid a large dataset becoming dominated by a few common models.

If brand or model is missing but the listing has other useful public fields, the parser can still save it and logs the incomplete fields.

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

Test with conservative concurrency:

```bash
python main.py collect --limit 100 --concurrency 2
```

Collect 15k total cars:

```bash
python main.py collect --limit 15000 --concurrency 3
```

Continue from 15k to 30k:

```bash
python main.py collect --limit 30000 --concurrency 3
```

Add 15k more:

```bash
python main.py collect --add 15000 --concurrency 3
```

Use Playwright fallback:

```bash
python main.py collect --limit 100 --engine playwright
```

Update from the first 5 search pages:

```bash
python main.py update --pages 5
```

Report:

```bash
python main.py report
```

Export:

```bash
python main.py export
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

The logs include mode, engine, target limit, add target, starting database count, concurrency, search pages, listing URLs, saved listings, duplicate skips, balancing skips, HTTP errors, parsing errors, and current saved counts.
