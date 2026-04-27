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
- search page delay: random 20-45 seconds
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

## Targeted Model Collection

`collect-targets` collects only selected brand/model pairs. This is better for ML work because it builds repeated examples for the same models instead of a wide random mix. When Kolesa search filtering works through the query URL, it is faster and cleaner than collecting everything and filtering later. Every detail page is still post-filtered before saving, so cars outside the configured target list are skipped.

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

Logs include mode, engine, concurrency, current DB count, target total limit, current search page, listing URL, saved listings, duplicate skips, model and brand limit skips, HTTP status codes, exception class names, retry numbers, backoff duration, pause duration, stop reasons, and final DB count.
