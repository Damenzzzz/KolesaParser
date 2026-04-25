import argparse
import asyncio
import logging

from database.db import CarDatabase
from scraper.config import DEFAULT_HEADLESS, DEFAULT_TEST_LIMIT, LOG_FILE, LOGS_DIR


def setup_logging() -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def str_to_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.lower().strip()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("headless must be true or false")


async def run_collect(args: argparse.Namespace, db: CarDatabase) -> None:
    from scraper.kolesa_parser import KolesaParser

    parser = KolesaParser(db=db, headless=args.headless)
    saved = await parser.collect(limit=args.limit)
    print(f"Saved new listings: {saved}")


async def run_update(args: argparse.Namespace, db: CarDatabase) -> None:
    from scraper.kolesa_parser import KolesaParser

    parser = KolesaParser(db=db, headless=args.headless)
    saved = await parser.update(pages=args.pages)
    print(f"Saved new listings: {saved}")


def run_report(db: CarDatabase) -> None:
    total = db.count_total()
    model_report_path = db.export_model_report()

    print(f"Total cars: {total}")
    print("\nTop brands:")
    for row in db.top_brands():
        print(f"  {row['brand']}: {row['count']}")

    print("\nTop brand+model pairs:")
    for row in db.top_models():
        print(f"  {row['brand']} {row['model']}: {row['count']}")

    print(f"\nModel report exported to: {model_report_path}")


def run_export(db: CarDatabase) -> None:
    full_path = db.export_full()
    ml_path = db.export_ml()
    print(f"Full export: {full_path}")
    print(f"ML export: {ml_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse public Kolesa.kz car listings into SQLite.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Collect listings until the limit is reached.")
    collect.add_argument("--limit", type=int, default=DEFAULT_TEST_LIMIT)
    collect.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)

    update = subparsers.add_parser("update", help="Parse only the first N search pages.")
    update.add_argument("--pages", type=int, default=3)
    update.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)

    subparsers.add_parser("report", help="Print counts and export model report.")
    subparsers.add_parser("export", help="Export full and ML-friendly CSV files.")

    return parser


def main() -> None:
    setup_logging()
    args = build_arg_parser().parse_args()
    logging.getLogger("kolesa_parser").info("started mode %s", args.command)

    db = CarDatabase()
    try:
        if args.command == "collect":
            asyncio.run(run_collect(args, db))
        elif args.command == "update":
            asyncio.run(run_update(args, db))
        elif args.command == "report":
            run_report(db)
        elif args.command == "export":
            run_export(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
