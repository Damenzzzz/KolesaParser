import argparse
import asyncio
import logging

from database.db import CarDatabase
from scraper.config import (
    DEFAULT_ENGINE,
    DEFAULT_HEADLESS,
    DEFAULT_TEST_LIMIT,
    HTTP_CONCURRENCY,
    LOG_FILE,
    LOGS_DIR,
    TOTAL_LIMIT,
)


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
    raise argparse.ArgumentTypeError("value must be true or false")


async def run_collect(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    current_count = db.count_all_cars()
    target_total = current_count + args.add if args.add is not None else args.limit
    if target_total is None:
        target_total = DEFAULT_TEST_LIMIT
    if target_total > TOTAL_LIMIT:
        logger.warning("target %s is above TOTAL_LIMIT=%s; capping at TOTAL_LIMIT", target_total, TOTAL_LIMIT)
        target_total = TOTAL_LIMIT

    logger.info("selected mode collect")
    logger.info("selected engine %s", args.engine)
    logger.info("target limit %s", target_total)
    if args.add is not None:
        logger.info("add mode target: current %s + add %s = %s", current_count, args.add, target_total)
    logger.info("current database count at start: %s", current_count)
    logger.info("concurrency %s", args.concurrency)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(db=db, concurrency=args.concurrency)
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(db=db, headless=args.headless)

    saved = await parser.collect_until_total(target_total)
    print(f"Saved new listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


async def run_update(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    logger.info("selected mode update")
    logger.info("selected engine %s", args.engine)
    logger.info("current database count at start: %s", db.count_all_cars())
    logger.info("concurrency %s", args.concurrency)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(db=db, concurrency=args.concurrency)
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(db=db, headless=args.headless)

    saved = await parser.update(pages=args.pages)
    print(f"Saved new listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


def run_report(db: CarDatabase) -> None:
    total = db.count_all_cars()
    model_report_path = db.export_model_report()

    print(f"Total cars: {total}")
    print("\nTop brands:")
    for row in db.get_top_brands():
        print(f"  {row['brand']}: {row['count']}")

    print("\nTop brand+model pairs:")
    for row in db.get_top_models():
        print(f"  {row['brand']} {row['model']}: {row['count']}")

    print("\nMissing values:")
    for row in db.missing_value_stats():
        print(f"  {row['column']}: {row['missing']} / {row['total']}")

    print(f"\nModel report exported to: {model_report_path}")


def run_export(db: CarDatabase) -> None:
    full_path = db.export_full_csv()
    ml_path = db.export_ml_csv()
    print(f"Full export: {full_path}")
    print(f"ML export: {ml_path}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Parse public Kolesa.kz car listings into SQLite.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="Collect until the database reaches a target size.")
    limit_group = collect.add_mutually_exclusive_group()
    limit_group.add_argument("--limit", type=int, help="Target total database size.")
    limit_group.add_argument("--add", type=int, help="Add this many new unique cars.")
    collect.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    collect.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    collect.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)

    update = subparsers.add_parser("update", help="Parse only the first N search pages.")
    update.add_argument("--pages", type=int, default=5)
    update.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    update.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    update.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)

    subparsers.add_parser("report", help="Print counts and export model report.")
    subparsers.add_parser("export", help="Export full and ML-friendly CSV files.")

    return parser


def main() -> None:
    setup_logging()
    args = build_arg_parser().parse_args()

    db = CarDatabase()
    try:
        if args.command == "collect":
            asyncio.run(run_collect(args, db))
        elif args.command == "update":
            asyncio.run(run_update(args, db))
        elif args.command == "report":
            logging.getLogger("kolesa_parser").info("selected mode report")
            run_report(db)
        elif args.command == "export":
            logging.getLogger("kolesa_parser").info("selected mode export")
            run_export(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
