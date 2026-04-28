import argparse
import asyncio
import json
import logging
from dataclasses import replace
from pathlib import Path

from database.db import CarDatabase, QUERY_JSON_COLUMNS
from scraper.config import (
    BRAND_TARGETS,
    BLOCK_STOP_MESSAGE,
    CrawlModeSettings,
    DEFAULT_ENGINE,
    DEFAULT_HEADLESS,
    DEFAULT_TEST_LIMIT,
    HTTP_CONCURRENCY,
    LOG_FILE,
    LOGS_DIR,
    PROJECT_ROOT,
    TOTAL_LIMIT,
    TARGET_MODELS,
    get_crawl_mode_settings,
    VISIBLE_CHALLENGE_STOP_MESSAGE,
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
    logging.getLogger("elastic_transport").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.CRITICAL)


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


def selected_brand_targets(brand: str | None) -> list[dict]:
    if not brand:
        return BRAND_TARGETS

    from scraper.brand_targets import normalize_brand_name

    wanted = normalize_brand_name(brand)
    targets = [
        target
        for target in BRAND_TARGETS
        if any(normalize_brand_name(name) == wanted for name in brand_argument_values(target))
    ]
    if not targets:
        allowed = ", ".join(target["brand"] for target in BRAND_TARGETS)
        raise ValueError(f"unknown brand {brand!r}; choose one of: {allowed}")
    return targets


def brand_argument_values(target: dict) -> list[str]:
    return [target["brand"], *target.get("aliases", [])]


def brand_argument_choices() -> list[str]:
    choices = []
    seen = set()
    for target in BRAND_TARGETS:
        for value in brand_argument_values(target):
            if value not in seen:
                choices.append(value)
                seen.add(value)
    return choices


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
    if getattr(parser, "visible_challenge_detected", False):
        print(VISIBLE_CHALLENGE_STOP_MESSAGE)
    elif getattr(parser, "block_detected", False):
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
    if getattr(parser, "visible_challenge_detected", False):
        print(VISIBLE_CHALLENGE_STOP_MESSAGE)
    elif getattr(parser, "block_detected", False):
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
    if getattr(parser, "visible_challenge_detected", False):
        print(VISIBLE_CHALLENGE_STOP_MESSAGE)
    elif getattr(parser, "block_detected", False):
        print(BLOCK_STOP_MESSAGE)
    elif getattr(parser, "stop_reason", None):
        print(f"Stopped safely: {parser.stop_reason}")
    logger.info("final DB count: %s", db.count_all_cars())
    print(f"Saved new target listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


async def run_collect_brands(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    crawl_mode = selected_crawl_mode(args)
    settings = build_mode_settings(args, crawl_mode)
    validate_delay_settings(settings)
    if args.checkpoint_export_every < 0:
        raise ValueError("--checkpoint-export-every must be non-negative")
    concurrency = 1 if crawl_mode in {"safe", "balanced", "night"} else args.concurrency
    targets = selected_brand_targets(args.brand)
    all_brand_target_total = sum(int(target["limit"]) for target in BRAND_TARGETS)
    target_total = sum(int(target["limit"]) for target in targets)

    logger.info("selected command collect-brands")
    logger.info("mode: %s", crawl_mode)
    logger.info("engine: %s", args.engine)
    logger.info("current DB count: %s", db.count_all_cars())
    logger.info("brand target count: %s", len(targets))
    logger.info("all brand target total: %s", all_brand_target_total)
    logger.info("target total limit: %s", target_total)
    logger.info("selected brand: %s", args.brand or "all")
    logger.info("ignore_state: %s", args.ignore_state)
    logger.info("concurrency: %s", concurrency)
    logger.info("detail delay seconds: %s-%s", *settings.detail_delay_seconds)
    logger.info("search delay seconds: %s-%s", *settings.search_delay_seconds)
    logger.info("stop_on_block: %s", args.stop_on_block)
    logger.info("max runtime hours: %s", args.max_runtime_hours)
    logger.info("checkpoint export every: %s", args.checkpoint_export_every)

    if args.engine == "http":
        from scraper.kolesa_http_parser import KolesaHTTPParser

        parser = KolesaHTTPParser(
            db=db,
            concurrency=concurrency,
            mode=crawl_mode,
            max_runtime_hours=args.max_runtime_hours,
            stop_on_block=args.stop_on_block,
            settings=settings,
            checkpoint_export_every=args.checkpoint_export_every,
        )
    else:
        from scraper.kolesa_playwright_parser import KolesaPlaywrightParser

        parser = KolesaPlaywrightParser(
            db=db,
            headless=args.headless,
            mode=crawl_mode,
            max_runtime_hours=args.max_runtime_hours,
            settings=settings,
            checkpoint_export_every=args.checkpoint_export_every,
        )

    saved = await parser.collect_brands(targets=targets, ignore_state=args.ignore_state)
    if getattr(parser, "visible_challenge_detected", False):
        print(VISIBLE_CHALLENGE_STOP_MESSAGE)
    elif getattr(parser, "block_detected", False):
        print(BLOCK_STOP_MESSAGE)
    elif getattr(parser, "stop_reason", None):
        print(f"Stopped safely: {parser.stop_reason}")
    logger.info("final DB count: %s", db.count_all_cars())
    print(f"Saved new brand listings: {saved}")
    print(f"Current total cars: {db.count_all_cars()}")


async def run_query_collect(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    if args.checkpoint_export_every < 0:
        raise ValueError("--checkpoint-export-every must be non-negative")

    from scraper.query_collector import QueryCollector
    from scraper.query_config import load_query_config, normalize_query_config

    raw_config = load_query_config(args.config)
    config = normalize_query_config(raw_config)
    if args.minutes is not None:
        config["parse_minutes"] = args.minutes

    logger.info("selected command query-collect")
    logger.info("engine: %s", args.engine)
    logger.info("headless: %s", args.headless)
    logger.info("loaded config: %s", raw_config)
    logger.info("normalized config: %s", config)
    logger.info("checkpoint export every: %s", args.checkpoint_export_every)

    collector = QueryCollector(
        db=db,
        headless=args.headless,
        checkpoint_export_every=args.checkpoint_export_every,
    )
    result = None
    try:
        result = await collector.collect(config)
    finally:
        output_csv = db.export_query_results(config["query_id"], config["output_csv"])
        output_json = db.export_query_results_json(config["query_id"], config["output_json"])
        print(f"Current query matched count: {db.count_query_results(config['query_id'])}")
        print(f"Current total cars: {db.count_all_cars()}")
        print(f"Query CSV: {output_csv}")
        print(f"Query JSON: {output_json}")

    if result and result.get("stop_reason"):
        print(f"Stopped safely: {result['stop_reason']}")
    if result:
        print(f"Matched new query listings: {result['matched_this_run']}")


def live_output_path(config_path: str | Path) -> Path:
    from elasticsearch_service.config import clean_query_stem

    stem = clean_query_stem(Path(config_path).stem)
    return PROJECT_ROOT / "data" / "outputs" / "live" / f"live_{stem}.json"


def elastic_output_path(config_path: str | Path) -> Path:
    from elasticsearch_service.config import clean_query_stem

    stem = clean_query_stem(Path(config_path).stem)
    return PROJECT_ROOT / "data" / "outputs" / "elastic" / f"elastic_{stem}.json"


def write_json_payload(path: Path, payload: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.stem}.tmp{path.suffix}")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def live_query_payload(db: CarDatabase, query_id: str, error: str | None = None) -> dict:
    rows = db.query_result_rows(query_id)
    cars = [{column: row.get(column) for column in QUERY_JSON_COLUMNS} for row in rows]
    payload = {
        "query_id": query_id,
        "source": "live_parser",
        "count": len(cars),
        "cars": cars,
    }
    if error:
        payload["error"] = error
    return payload


def empty_elastic_payload(query_id: str, error: str | None = None) -> dict:
    payload = {
        "query_id": query_id,
        "source": "elasticsearch",
        "count": 0,
        "cars": [],
    }
    if error:
        payload["error"] = error
    return payload


def run_elastic_query(args: argparse.Namespace) -> dict:
    from elasticsearch_service.client import ElasticsearchUnavailable
    from elasticsearch_service.config import ELASTICSEARCH_UNAVAILABLE_MESSAGE
    from elasticsearch_service.search import normalize_search_query_config, search_cars_by_query
    from scraper.query_config import load_query_config

    raw_config = load_query_config(args.config)
    config = normalize_search_query_config(raw_config)
    output_json = elastic_output_path(args.config)
    try:
        result = search_cars_by_query(config, limit=args.limit)
    except ElasticsearchUnavailable as exc:
        message = str(exc) or ELASTICSEARCH_UNAVAILABLE_MESSAGE
        print(message)
        result = empty_elastic_payload(config["query_id"], message)
    write_json_payload(output_json, result)
    print(f"Elastic count: {result['count']}")
    print(f"Elastic JSON: {output_json}")
    return {"result": result, "output_json": output_json}


async def run_dual_query(args: argparse.Namespace, db: CarDatabase) -> None:
    logger = logging.getLogger("kolesa_parser")
    from elasticsearch_service.client import ElasticsearchUnavailable
    from elasticsearch_service.config import ELASTICSEARCH_UNAVAILABLE_MESSAGE
    from elasticsearch_service.search import search_cars_by_query
    from scraper.query_collector import QueryCollector
    from scraper.query_config import load_query_config, normalize_query_config

    raw_config = load_query_config(args.config)
    config = normalize_query_config(raw_config)
    if args.minutes is not None:
        config["parse_minutes"] = args.minutes
    config["output_json"] = str(live_output_path(args.config))

    crawl_mode = selected_crawl_mode(args)
    logger.info("selected command dual-query")
    logger.info("mode: %s", crawl_mode)
    logger.info("engine: %s", args.engine)
    logger.info("headless: %s", args.headless)
    logger.info("loaded config: %s", raw_config)
    logger.info("normalized config: %s", config)

    live_error = None
    collector = QueryCollector(db=db, headless=args.headless)
    try:
        live_result = await collector.collect(config)
        if live_result.get("stop_reason"):
            print(f"Stopped safely: {live_result['stop_reason']}")
    except Exception as exc:
        live_error = f"Live parser failed: {exc.__class__.__name__}: {exc}"
        logger.exception("dual-query live parser failed")
        print(live_error)

    live_json = live_output_path(args.config)
    live_payload = live_query_payload(db, config["query_id"], live_error)
    write_json_payload(live_json, live_payload)

    elastic_json = elastic_output_path(args.config)
    try:
        elastic_payload = search_cars_by_query(config, limit=config.get("max_results") or 50)
    except ElasticsearchUnavailable as exc:
        message = str(exc) or ELASTICSEARCH_UNAVAILABLE_MESSAGE
        print(message)
        elastic_payload = empty_elastic_payload(config["query_id"], message)
    write_json_payload(elastic_json, elastic_payload)

    print(f"Live count: {live_payload['count']}")
    print(f"Elastic count: {elastic_payload['count']}")
    print(f"Live JSON: {live_json}")
    print(f"Elastic JSON: {elastic_json}")


def run_report(db: CarDatabase) -> None:
    total = db.count_all_cars()
    model_report_path = db.export_model_report()
    brand_report_path = db.export_brand_report()
    completed_targets, total_targets = db.completed_target_count()
    completed_brands, total_brands = db.completed_brand_count()

    print(f"Total cars: {total}")
    print(f"Completed target models: {completed_targets} / {total_targets}")
    print(f"Completed brand targets: {completed_brands} / {total_brands}")

    print("\nBrand target progress:")
    for row in db.brand_report_rows():
        print(f"  {row['brand']}: {row['current_count']} / {row['target_limit']}, remaining {row['remaining']}")
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
    print(f"Brand report exported to: {brand_report_path}")


def run_target_report(db: CarDatabase) -> None:
    rows = db.target_model_report_rows()
    path = db.export_target_model_report()
    for row in rows:
        print(
            f"{row['brand']} {row['model']}: "
            f"{row['current_count']} / {row['target_limit']}, remaining {row['remaining']}"
        )
    print(f"\nTarget model report exported to: {path}")


def run_brand_report(db: CarDatabase) -> None:
    rows = db.brand_report_rows()
    path = db.export_brand_report()
    for row in rows:
        print(f"{row['brand']}: {row['current_count']} / {row['target_limit']}, remaining {row['remaining']}")
    print(f"\nBrand report exported to: {path}")


def run_export(db: CarDatabase) -> None:
    full_path = db.export_full_csv()
    ml_path = db.export_ml_csv()
    print(f"Full export: {full_path}")
    print(f"ML export: {ml_path}")


def print_stop_counts(db: CarDatabase, args: argparse.Namespace) -> None:
    if getattr(args, "command", None) in {"query-collect", "dual-query"} and getattr(args, "config", None):
        try:
            from scraper.query_config import load_query_config, normalize_query_config

            config = normalize_query_config(load_query_config(args.config))
            output_csv = db.export_query_results(config["query_id"], config["output_csv"])
            output_json = db.export_query_results_json(config["query_id"], config["output_json"])
            print(f"Current query matched count: {db.count_query_results(config['query_id'])}")
            print(f"Query CSV: {output_csv}")
            print(f"Query JSON: {output_json}")
        except Exception as exc:
            logging.getLogger("kolesa_parser").warning("could not print query stop counts: %s", exc)

    brand = getattr(args, "brand", None)
    if brand:
        try:
            targets = selected_brand_targets(brand)
        except ValueError:
            targets = []
        if len(targets) == 1:
            canonical_brand = targets[0]["brand"]
            print(f"Current {canonical_brand} count: {db.count_by_brand(canonical_brand)}")
    print(f"Current total cars: {db.count_all_cars()}")


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

    collect_brands = subparsers.add_parser("collect-brands", help="Collect directly from configured brand pages.")
    collect_brands.add_argument("--brand", choices=brand_argument_choices())
    collect_brands.add_argument("--ignore-state", action="store_true", help="Start each selected brand from page 1.")
    collect_brands.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    collect_brands.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    collect_brands.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    collect_brands.add_argument(
        "--checkpoint-export-every",
        type=int,
        default=0,
        help="Export selected brand checkpoint CSV after this many newly saved cars. 0 disables checkpoints.",
    )
    add_safety_args(collect_brands, include_runtime=True)

    query_collect = subparsers.add_parser("query-collect", help="Collect public listings from a JSON query config.")
    query_collect.add_argument("--config", required=True, help="Path to query JSON config.")
    query_collect.add_argument("--minutes", type=float, default=None, help="Override parse_minutes from the JSON config.")
    query_collect.add_argument("--engine", choices=["playwright"], default="playwright")
    query_collect.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    query_collect.add_argument(
        "--checkpoint-export-every",
        type=int,
        default=0,
        help="Export query checkpoint CSV after this many newly matched cars. 0 disables checkpoints.",
    )

    elastic_query = subparsers.add_parser("elastic-query", help="Search already indexed cars in Elasticsearch.")
    elastic_query.add_argument("--config", required=True, help="Path to query JSON config.")
    elastic_query.add_argument("--limit", type=int, default=50, help="Maximum Elasticsearch results to export.")

    dual_query = subparsers.add_parser("dual-query", help="Run live query collection and Elasticsearch search.")
    dual_query.add_argument("--config", required=True, help="Path to query JSON config.")
    dual_query.add_argument("--minutes", type=float, default=None, help="Override parse_minutes from the JSON config.")
    dual_query.add_argument("--engine", choices=["playwright"], default="playwright")
    dual_query.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    add_safety_args(dual_query)

    update = subparsers.add_parser("update", help="Parse only the first N search pages.")
    update.add_argument("--pages", type=int, default=5)
    update.add_argument("--engine", choices=["http", "playwright"], default=DEFAULT_ENGINE)
    update.add_argument("--concurrency", type=int, default=HTTP_CONCURRENCY)
    update.add_argument("--headless", type=str_to_bool, default=DEFAULT_HEADLESS)
    add_safety_args(update)

    subparsers.add_parser("report", help="Print counts and export model report.")
    subparsers.add_parser("target-report", help="Print target model progress and export target report.")
    subparsers.add_parser("brand-report", help="Print brand target progress and export brand report.")
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
        elif args.command == "collect-brands":
            asyncio.run(run_collect_brands(args, db))
        elif args.command == "query-collect":
            asyncio.run(run_query_collect(args, db))
        elif args.command == "elastic-query":
            logging.getLogger("kolesa_parser").info("selected mode elastic-query")
            run_elastic_query(args)
        elif args.command == "dual-query":
            asyncio.run(run_dual_query(args, db))
        elif args.command == "update":
            asyncio.run(run_update(args, db))
        elif args.command == "report":
            logging.getLogger("kolesa_parser").info("selected mode report")
            run_report(db)
        elif args.command == "target-report":
            logging.getLogger("kolesa_parser").info("selected mode target-report")
            run_target_report(db)
        elif args.command == "brand-report":
            logging.getLogger("kolesa_parser").info("selected mode brand-report")
            run_brand_report(db)
        elif args.command == "export":
            logging.getLogger("kolesa_parser").info("selected mode export")
            run_export(db)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logging.getLogger("kolesa_parser").warning("parser stopped by cancellation or KeyboardInterrupt")
        print("Stopped safely: manual stop or parser cancellation")
        print_stop_counts(db, args)
    finally:
        db.close()


if __name__ == "__main__":
    main()
