import asyncio
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from database.db import CarDatabase
from scraper.config import LOGS_DIR, PLAYWRIGHT_TIMEOUT_MS, USER_AGENT
from scraper.html_parser import extract_brand_listing_cards, extract_listing_urls, parse_listing_page
from scraper.query_config import (
    apply_site_filters,
    normalize_body_type,
    normalize_brand,
    normalize_city,
    normalize_fuel_type,
    normalize_model,
    normalize_transmission,
)
from scraper.utils import extract_listing_id, is_blocked_response, is_visible_challenge_text, normalize_text


logger = logging.getLogger("query_collector")


def car_matches_query(car: dict[str, Any], config: dict[str, Any]) -> bool:
    return query_mismatch_reason(car, config) is None


def query_mismatch_reason(car: dict[str, Any], config: dict[str, Any]) -> str | None:
    expected_brand = normalize_brand(config.get("brand"))
    parsed_brand = normalize_brand(car.get("brand"))
    if not parsed_brand:
        return "skipped because missing parsed field: brand"
    if parsed_brand != expected_brand:
        return f"wrong brand parsed={parsed_brand} expected={expected_brand}"

    expected_model = normalize_model(config.get("model"))
    parsed_model = normalize_model(car.get("model"))
    if not parsed_model:
        return "skipped because missing parsed field: model"
    if parsed_model != expected_model and not parsed_model.startswith(f"{expected_model} "):
        return f"wrong model parsed={parsed_model} expected={expected_model}"

    expected_city = normalize_city(config.get("city"))
    if expected_city:
        parsed_city = normalize_city(car.get("city"))
        if not parsed_city:
            return "skipped because missing parsed field: city"
        if parsed_city != expected_city:
            return f"wrong city parsed={parsed_city} expected={expected_city}"

    year = _number(car.get("year"), int)
    if config.get("year_from") is not None:
        if year is None:
            return "skipped because missing parsed field: year"
        if year < int(config["year_from"]):
            return f"year below range parsed={year} minimum={config['year_from']}"
    if config.get("year_to") is not None:
        if year is None:
            return "skipped because missing parsed field: year"
        if year > int(config["year_to"]):
            return f"year above range parsed={year} maximum={config['year_to']}"

    price = _number(car.get("price"), int)
    if config.get("price_min") is not None:
        if price is None:
            return "skipped because missing parsed field: price"
        if price < int(config["price_min"]):
            return f"price below range parsed={price} minimum={config['price_min']}"
    if config.get("price_max") is not None:
        if price is None:
            return "skipped because missing parsed field: price"
        if price > int(config["price_max"]):
            return f"price above range parsed={price} maximum={config['price_max']}"

    engine_volume = _number(car.get("engine_volume_l"), float)
    if config.get("engine_volume_from") is not None:
        if engine_volume is None:
            return "skipped because missing parsed field: engine_volume_l"
        if engine_volume < float(config["engine_volume_from"]):
            return f"engine volume below range parsed={engine_volume} minimum={config['engine_volume_from']}"
    if config.get("engine_volume_to") is not None:
        if engine_volume is None:
            return "skipped because missing parsed field: engine_volume_l"
        if engine_volume > float(config["engine_volume_to"]):
            return f"engine volume above range parsed={engine_volume} maximum={config['engine_volume_to']}"

    expected_transmission = normalize_transmission(config.get("transmission"))
    if expected_transmission:
        parsed_transmission = normalize_transmission(car.get("transmission"))
        if not parsed_transmission:
            return "skipped because missing parsed field: transmission"
        if parsed_transmission != expected_transmission:
            return f"wrong transmission parsed={parsed_transmission} expected={expected_transmission}"

    expected_fuel = normalize_fuel_type(config.get("fuel_type"))
    if expected_fuel:
        parsed_fuel = normalize_fuel_type(car.get("fuel_type"))
        if not parsed_fuel:
            return "skipped because missing parsed field: fuel_type"
        if parsed_fuel != expected_fuel:
            return f"wrong fuel_type parsed={parsed_fuel} expected={expected_fuel}"

    mileage = _number(car.get("mileage_km"), int)
    if config.get("mileage_min") is not None:
        if mileage is None:
            return "skipped because missing parsed field: mileage_km"
        if mileage < int(config["mileage_min"]):
            return f"mileage below range parsed={mileage} minimum={config['mileage_min']}"
    if config.get("mileage_max") is not None:
        if mileage is None:
            return "skipped because missing parsed field: mileage_km"
        if mileage > int(config["mileage_max"]):
            return f"mileage above range parsed={mileage} maximum={config['mileage_max']}"

    expected_body = normalize_body_type(config.get("body_type"))
    if expected_body:
        parsed_body = normalize_body_type(car.get("body_type"))
        if not parsed_body:
            return "skipped because missing parsed field: body_type"
        if parsed_body != expected_body:
            return f"wrong body_type parsed={parsed_body} expected={expected_body}"

    return None


def export_query_results(db: CarDatabase, query_id: str, output_csv: str | Path) -> Path:
    return db.export_query_results(query_id, output_csv)


def export_query_results_json(db: CarDatabase, query_id: str, output_json: str | Path) -> Path:
    return db.export_query_results_json(query_id, output_json)


async def is_normal_detail_page(page) -> bool:
    try:
        title = await page.title()
    except Exception:
        title = ""
    try:
        visible_text = await page.locator("body").inner_text(timeout=3000)
    except Exception:
        visible_text = ""

    haystack = normalize_text(f"{title} {visible_text}") or ""
    haystack = haystack.lower()
    has_listing_title = "\u043f\u0440\u043e\u0434\u0430\u0436\u0430" in haystack and (
        "\u043a\u043e\u043b\u0451\u0441\u0430" in haystack or "\u043a\u043e\u043b\u0435\u0441\u0430" in haystack
    )
    has_price = "\u20b8" in haystack
    markers = [
        "\u0433\u043e\u0440\u043e\u0434",
        "\u043a\u0443\u0437\u043e\u0432",
        "\u043e\u0431\u044a\u0435\u043c \u0434\u0432\u0438\u0433\u0430\u0442\u0435\u043b\u044f",
        "\u043a\u043e\u0440\u043e\u0431\u043a\u0430 \u043f\u0435\u0440\u0435\u0434\u0430\u0447",
        "\u043f\u0440\u0438\u0432\u043e\u0434",
        "\u0440\u0443\u043b\u044c",
    ]
    marker_count = sum(1 for marker in markers if marker in haystack)
    return has_listing_title or (has_price and marker_count >= 2)


def sanitize_visible_text(text: str) -> str:
    cleaned = normalize_text(text) or ""
    return re.sub(r"(?:\+?\d[\s\-()]*){7,}", "[PHONE_REDACTED]", cleaned)


class QueryCollector:
    def __init__(
        self,
        db: CarDatabase,
        headless: bool = True,
        checkpoint_export_every: int = 0,
    ) -> None:
        self.db = db
        self.headless = headless
        self.checkpoint_export_every = max(0, int(checkpoint_export_every))
        self.stop_requested = False
        self.stop_reason: str | None = None
        self.matched_this_run = 0
        self.logger = logger

    async def collect(self, config: dict[str, Any]) -> dict[str, Any]:
        from playwright.async_api import async_playwright

        query_id = str(config["query_id"])
        output_csv = config["output_csv"]
        output_json = config["output_json"]
        max_results = int(config.get("max_results") or 50)
        parse_minutes = float(config.get("parse_minutes") or 10.0)
        deadline = time.monotonic() + parse_minutes * 60.0

        self.logger.info("selected command query-collect")
        self.logger.info("command: query-collect")
        self.logger.info("query_id: %s", query_id)
        self.logger.info("normalized config: %s", config)
        self.logger.info("built base URL: %s", config["base_url"])
        if not config.get("model_url_exact"):
            self.logger.warning("model URL could not be built exactly; using fallback URL: %s", config["base_url"])

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale="ru-RU",
                viewport={"width": 1366, "height": 900},
            )
            context.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            search_page = await context.new_page()
            detail_page = await context.new_page()

            try:
                await search_page.goto(config["base_url"], wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
                filter_result = await apply_site_filters(search_page, config)
                for field in filter_result.get("applied", []):
                    self.logger.info("filter applied: %s", field)
                for field in filter_result.get("not_applied", []):
                    self.logger.info("filter not applied: %s", field)
                self.logger.info("final filtered URL: %s", search_page.url)

                page_number = 1
                base_filtered_url = search_page.url
                while self._can_continue(deadline) and self.db.count_query_results(query_id) < max_results:
                    current_url = _page_url(base_filtered_url, page_number)
                    self.logger.info("current page number: %s", page_number)
                    if page_number > 1:
                        await search_page.goto(current_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)

                    response_status = await _response_status(search_page)
                    html = await search_page.content()
                    if await self._page_is_blocked(search_page, response_status, html, "search"):
                        break

                    cards = extract_brand_listing_cards(html, config["brand"])
                    listing_urls = [card["url"] for card in cards if card.get("url")]
                    if not listing_urls:
                        listing_urls = extract_listing_urls(html)
                    listing_urls = _dedupe_urls(listing_urls)

                    if not listing_urls:
                        if page_number == 1:
                            self.stop_reason = "search page not recognized or no listing cards found"
                            await self._save_debug_stop(search_page, "search")
                        else:
                            self.stop_reason = "no more listing cards found"
                        break

                    for url in listing_urls:
                        if not self._can_continue(deadline) or self.db.count_query_results(query_id) >= max_results:
                            break
                        await self._process_listing(detail_page, url, config)

                    page_number += 1
            finally:
                final_csv = self.db.export_query_results(query_id, output_csv)
                final_json = self.db.export_query_results_json(query_id, output_json)
                self.logger.info("final query CSV exported: %s", final_csv)
                self.logger.info("final query JSON exported: %s", final_json)
                self.logger.info("final matched count: %s", self.db.count_query_results(query_id))
                self.logger.info("total DB count: %s", self.db.count_all_cars())
                await self._safe_close(detail_page, "detail page")
                await self._safe_close(search_page, "search page")
                await self._safe_close(context, "browser context")
                await self._safe_close(browser, "browser")

        return {
            "query_id": query_id,
            "matched_this_run": self.matched_this_run,
            "matched_total": self.db.count_query_results(query_id),
            "total_db_count": self.db.count_all_cars(),
            "output_csv": str(self.db.export_query_results(query_id, output_csv)),
            "output_json": str(self.db.export_query_results_json(query_id, output_json)),
            "stop_reason": self.stop_reason,
        }

    async def _process_listing(self, page, url: str, config: dict[str, Any]) -> None:
        query_id = str(config["query_id"])
        listing_id = extract_listing_id(url)
        self.logger.info("current listing URL: %s", url)

        if self.db.query_result_exists(query_id, listing_id, url):
            self.logger.info("skipped duplicate query result: %s", listing_id or url)
            return

        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            html = await page.content()
        except Exception as exc:
            self.logger.warning("listing navigation failed: %s %s: %s", url, exc.__class__.__name__, exc)
            return

        status_code = response.status if response else 0
        if await self._page_is_blocked(page, status_code, html, "detail"):
            return

        if not await is_normal_detail_page(page):
            self.stop_reason = "detail page not recognized"
            await self._save_debug_stop(page, "detail")
            self.stop_requested = True
            return

        try:
            car = parse_listing_page(html, url)
        except Exception:
            self.logger.exception("query detail parsing error for %s", url)
            return

        reason = query_mismatch_reason(car, config)
        if reason:
            if reason.startswith("skipped because missing parsed field"):
                self.logger.info(reason)
            else:
                self.logger.info("skipped because not matching query: %s; %s", url, reason)
            return

        inserted = self.db.insert_car(car)
        if not inserted:
            self.logger.info("skipped duplicate in cars table: %s", listing_id or url)

        linked = self.db.link_query_result(query_id, car.get("listing_id"), car.get("url"))
        if linked:
            self.matched_this_run += 1
            self.logger.info("linked listing to query_id: %s %s", query_id, car.get("listing_id") or car.get("url"))
            self.logger.info(
                "progress checkpoint: saved this run=%s; current query matched count=%s; total DB count=%s",
                self.matched_this_run,
                self.db.count_query_results(query_id),
                self.db.count_all_cars(),
            )
            if self.checkpoint_export_every and self.matched_this_run % self.checkpoint_export_every == 0:
                checkpoint_csv, checkpoint_json = self._checkpoint_paths(query_id)
                self.db.export_query_results(query_id, checkpoint_csv)
                self.db.export_query_results_json(query_id, checkpoint_json)
                self.logger.info("checkpoint export path: %s", checkpoint_csv)
                self.logger.info("checkpoint export path: %s", checkpoint_json)

    async def _page_is_blocked(self, page, status_code: int, html: str, page_type: str) -> bool:
        blocked, reason = is_blocked_response(status_code, html if status_code != 200 else "")
        if blocked:
            self.stop_requested = True
            self.stop_reason = reason
            await self._save_debug_stop(page, page_type)
            self.logger.error("page stop detected: %s", reason)
            return True

        try:
            visible_text = await page.locator("body").inner_text(timeout=3000)
        except Exception:
            visible_text = ""
        visible_blocked, visible_reason = is_visible_challenge_text(visible_text)
        if not visible_blocked:
            return False

        if page_type == "detail" and await is_normal_detail_page(page):
            self.logger.info("visible stop phrase found, but normal detail markers are present; continuing")
            return False
        if page_type == "search" and extract_listing_urls(html):
            self.logger.info("visible stop phrase found, but listing cards are present; continuing")
            return False

        self.stop_requested = True
        self.stop_reason = f"visible text phrase: {visible_reason}"
        await self._save_debug_stop(page, page_type)
        self.logger.error("page stop detected: %s", self.stop_reason)
        return True

    async def _save_debug_stop(self, page, page_type: str) -> None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        screenshot_path = LOGS_DIR / f"debug_stop_{timestamp}.png"
        try:
            title = await page.title()
        except Exception:
            title = ""
        try:
            visible_text = await page.locator("body").inner_text(timeout=3000)
        except Exception:
            visible_text = ""
        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
            screenshot_message = str(screenshot_path)
        except Exception as exc:
            screenshot_message = f"screenshot failed: {exc.__class__.__name__}: {exc}"

        self.logger.error(
            "debug stop snapshot; url=%s title=%r page_type=%s visible_text_snippet=%r screenshot=%s",
            page.url,
            title,
            page_type,
            sanitize_visible_text(visible_text[:1000]),
            screenshot_message,
        )

    async def _safe_close(self, resource, label: str) -> None:
        try:
            await resource.close()
        except Exception as exc:
            self.logger.warning("could not close %s cleanly: %s: %s", label, exc.__class__.__name__, exc)

    def _checkpoint_paths(self, query_id: str) -> tuple[Path, Path]:
        slug = re.sub(r"[^0-9a-zA-Z_\\-]+", "_", query_id).strip("_") or "query"
        base_path = Path("data") / "exports" / "queries" / "checkpoints"
        return base_path / f"{slug}_checkpoint.csv", base_path / f"{slug}_checkpoint.json"

    def _can_continue(self, deadline: float) -> bool:
        if self.stop_requested:
            return False
        if time.monotonic() >= deadline:
            self.stop_reason = "parse_minutes reached"
            return False
        return True


def _number(value: Any, caster):
    if value is None:
        return None
    try:
        return caster(float(str(value).replace(",", ".")))
    except (TypeError, ValueError):
        return None


def _dedupe_urls(urls: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for url in urls:
        listing_id = extract_listing_id(url) or url
        if listing_id in seen:
            continue
        seen.add(listing_id)
        deduped.append(url)
    return deduped


def _page_url(url: str, page_number: int) -> str:
    if page_number <= 1:
        return url
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page_number)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


async def _response_status(page) -> int:
    try:
        response = await page.evaluate("() => window.performance.getEntriesByType('navigation')[0]?.responseStatus || 200")
        return int(response or 200)
    except Exception:
        return 200
