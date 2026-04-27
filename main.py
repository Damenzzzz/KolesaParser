import argparse
import asyncio
import logging
from dataclasses import replace

from database.db import CarDatabase
from scraper.config import (
    BLOCK_STOP_MESSAGE,
    CrawlModeSettings,
    DEFAULT_ENGINE,
    DEFAULT_HEADLESS,
    DEFAULT_TEST_LIMIT,
    HTTP_CONCURRENCY,
    LOG_FILE,
    LOGS_DIR,
    TOTAL_LIMIT,
    TARGET_MODELS,
    get_crawl_mode_settings,
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


def selected_crawl_mode(args: argparse.Namespace) -> str:
    if getattr(args, "night_mode", False):
        return "night"
    if getattr(args, "balanced_mode", False):
        return "balanced"
    if getattr(args, "safe_mode", False):
        return "safe"
    return "normal"


def build_mode_settings(args: argparse.Namespace, crawl_mode: str) -> CrawlModeSettings:
    settings = get_crawl_mode_settings(crawl_mode)
    detail_min = getattr(args, "detail_delay_min", None)
    detail_max = getattr(args, "detail_delay_max", None)
    search_min = getattr(args, "search_delay_min", None)
    search_max = getattr(args, "search_delay_max", None)

    detail_delay = (
        settings.detail_delay_seconds[0] if detail_min is None else detail_min,
        settings.detail_delay_seconds[1] if detail_max is None else detail_max,
    )
    search_delay = (
        settings.search_delay_seconds[0] if search_min is None else search_min,
        settings.search_delay_seconds[1] if search_max is None else search_max,
    )

    return replace(settings, detail_delay_seconds=detail_delay, search_delay_seconds=search_delay)


def validate_delay_settings(settings: CrawlModeSettings) -> None:
    for label, delay_range in (
        ("detail delay", settings.detail_delay_seconds),
        ("search delay", settings.search_delay_seconds),
    ):
        if delay_range[0] < 0 or delay_range[1] < 0:
            raise ValueError(f"{label} values must be non-negative")
        if delay_range[0] > delay_range[1]:
            raise ValueError(f"{label} min must be less than or equal to max")


async def run_collect(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    crawl_mode = selected_crawl_mode(args)
    settings = build_mode_settings(args, crawl_mode)
    validate_delay_settings(settings)
    current_count = db.count_all_cars()
    target_total = current_count + args.add if args.add is not None else args.limit
    if target_total is None:
        target_total = DEFAULT_TEST_LIMIT
    if target_total > TOTAL_LIMIT:
        logger.warning("target %s is above TOTAL_LIMIT=%s; capping at TOTAL_LIMIT", target_total, TOTAL_LIMIT)
        target_total = TOTAL_LIMIT

    concurrency = 1 if crawl_mode in {"safe", "balanced", "night"} else args.concurrency
    if concurrency != args.concurrency:
        logger.info("%s mode forces concurrency to 1", crawl_mode)

    logger.info("selected command collect")
    logger.info("mode: %s", crawl_mode)
    logger.info("engine: %s", args.engine)
    logger.info("target total limit: %s", target_total)
    if args.add is not None:
        logger.info("add mode target: current %s + add %s = %s", current_count, args.add, target_total)
    logger.info("current DB count: %s", current_count)
    logger.info("concurrency: %s", concurrency)
    logger.info("detail delay seconds: %s-%s", *settings.detail_delay_seconds)
    logger.info("search delay seconds: %s-%s", *settings.search_delay_seconds)
    logger.info("stop_on_block: %s", args.stop_on_block)
    logger.info("max runtime hours: %s", args.max_runtime_hours)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(
            db=db,
            concurrency=concurrency,
            mode=crawl_mode,
            max_runtime_hours=args.max_runtime_hours,
            stop_on_block=args.stop_on_block,
            settings=settings,
        )
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(db=db, headless=args.headless)

    saved = await parser.collect_until_total(target_total)
    if getattr(parser, "block_detected", False):
        print(BLOCK_STOP_MESSAGE)
    elif getattr(parser, "stop_reason", None):
        print(f"Stopped safely: {parser.stop_reason}")
    logger.info("final DB count: %s", db.count_all_cars())
    print(f"Saved new listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


async def run_update(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    crawl_mode = selected_crawl_mode(args)
    settings = build_mode_settings(args, crawl_mode)
    validate_delay_settings(settings)
    concurrency = 1 if crawl_mode in {"safe", "balanced", "night"} else args.concurrency

    logger.info("selected command update")
    logger.info("mode: %s", crawl_mode)
    logger.info("engine: %s", args.engine)
    logger.info("current DB count: %s", db.count_all_cars())
    logger.info("concurrency: %s", concurrency)
    logger.info("detail delay seconds: %s-%s", *settings.detail_delay_seconds)
    logger.info("search delay seconds: %s-%s", *settings.search_delay_seconds)
    logger.info("stop_on_block: %s", args.stop_on_block)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(
            db=db,
            concurrency=concurrency,
            mode=crawl_mode,
            stop_on_block=args.stop_on_block,
            settings=settings,
        )
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(db=db, headless=args.headless)

    saved = await parser.update(pages=args.pages)
    if getattr(parser, "block_detected", False):
        print(BLOCK_STOP_MESSAGE)
    elif getattr(parser, "stop_reason", None):
        print(f"Stopped safely: {parser.stop_reason}")
    logger.info("final DB count: %s", db.count_all_cars())
    print(f"Saved new listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


async def run_collect_targets(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    crawl_mode = selected_crawl_mode(args)
    settings = build_mode_settings(args, crawl_mode)
    validate_delay_settings(settings)
    concurrency = 1 if crawl_mode in {"safe", "balanced", "night"} else args.concurrency

    logger.info("selected command collect-targets")
    logger.info("mode: %s", crawl_mode)
    logger.info("engine: %s", args.engine)
    logger.info("current DB count: %s", db.count_all_cars())
    logger.info("target model count: %s", len(TARGET_MODELS))
    logger.info("concurrency: %s", concurrency)
    logger.info("detail delay seconds: %s-%s", *settings.detail_delay_seconds)
    logger.info("search delay seconds: %s-%s", *settings.search_delay_seconds)
    logger.info("stop_on_block: %s", args.stop_on_block)
    logger.info("max runtime hours: %s", args.max_runtime_hours)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(
            db=db,
            concurrency=concurrency,
            mode=crawl_mode,
            max_runtime_hours=args.max_runtime_hours,
            stop_on_block=args.stop_on_block,
            settings=settings,
        )
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(
            db=db,
            headless=args.headless,
            mode=crawl_mode,
            max_runtime_hours=args.max_runtime_hours,
            settings=settings,
        )

    saved = await parser.collect_targets()
    if getattr(parser, "block_detected", False):
        print(BLOCK_STOP_MESSAGE)
    elif getattr(parser, "stop_reason", None):
        print(f"Stopped safely: {parser.stop_reason}")
    logger.info("final DB count: %s", db.count_all_cars())
    print(f"Saved new target listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


def run_report(db: CarDatabase) -> None:
    total = db.count_all_cars()
    model_report_path = db.export_model_report()
    completed_targets, total_targets = db.completed_target_count()

    print(f"Total cars: {total}")
    print(f"Completed target models: {completed_targets} / {total_targets}")
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


def run_target_report(db: CarDatabase) -> None:
    rows = db.target_model_report_rows()
    path = db.export_target_model_report()
    for row in rows:
        print(
            f"{row['brand']} {row['model']}: "
            f"{row['current_count']} / {row['target_limit']}, remaining {row['remaining']}"
        )
    print(f"\nTarget model report exported to: {path}")


def run_export(db: CarDatabase) -> None:
    full_path = db.export_full_csv()
    ml_path = db.export_ml_csv()
    print(f"Full export: {full_path}")
    print(f"ML export: {ml_path}")


def add_safety_args(command_parser: argparse.ArgumentParser, include_runtime: bool = False) -> None:
    mode_group = command_parser.add_mutually_exclusive_group()
    mode_group.add_argument("--safe-mode", action="store_true", help="Very conservative HTTP collection mode.")
    mode_group.add_argument("--balanced-mode", action="store_true", help="Practical HTTP collection mode.")
    mode_group.add_argument("--night-mode", action="store_true", help="Slow unattended HTTP collection mode.")
    command_parser.add_argument("--detail-delay-min", type=float, default=None)
    command_parser.add_argument("--detail-delay-max", type=float, default=None)
    command_parser.add_argument("--search-delay-min", type=float, default=None)
    command_parser.add_argument("--search-delay-max", type=float, default=None)
    command_parser.add_argument(
        "--stop-on-block",
        type=str_to_bool,
        nargs="?",
        const=True,
        default=True,
        help="Stop safely on possible blocks, rate limits, captcha pages, or repeated timeouts.",
    )
    if include_runtime:
        command_parser.add_argument(
            "--max-runtime-hours",
            type=float,
            default=None,
            help="Stop gracefully after this many hours and keep saved data.",
        )


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
    add_safety_args(collect, include_runtime=True)

    collect_targets = subparsers.add_parser("collect-targets", help="Collect only configured target brand/model pairs.")
    collect_targets.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    collect_targets.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    collect_targets.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    add_safety_args(collect_targets, include_runtime=True)

    update = subparsers.add_parser("update", help="Parse only the first N search pages.")
    update.add_argument("--pages", type=int, default=5)
    update.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    update.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    update.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    add_safety_args(update)

    subparsers.add_parser("report", help="Print counts and export model report.")
    subparsers.add_parser("target-report", help="Print target model progress and export target report.")
    subparsers.add_parser("export", help="Export full and ML-friendly CSV files.")

    return parser


def main() -> None:
    setup_logging()
    args = build_arg_parser().parse_args()

    db = CarDatabase()
    try:
        if args.command == "collect":
            asyncio.run(run_collect(args, db))
        elif args.command == "collect-targets":
            asyncio.run(run_collect_targets(args, db))
        elif args.command == "update":
            asyncio.run(run_update(args, db))
        elif args.command == "report":
            logging.getLogger("kolesa_parser").info("selected mode report")
            run_report(db)
        elif args.command == "target-report":
            logging.getLogger("kolesa_parser").info("selected mode target-report")
            run_target_report(db)
        elif args.command == "export":
            logging.getLogger("kolesa_parser").info("selected mode export")
            run_export(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
