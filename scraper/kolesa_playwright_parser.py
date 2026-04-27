import asyncio
import logging
import random
import time

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import (
    BLOCK_STOP_MESSAGE,
    CrawlModeSettings,
    DEFAULT_HEADLESS,
    MAX_DELAY_SECONDS,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    MIN_DELAY_SECONDS,
    PLAYWRIGHT_TIMEOUT_MS,
    START_URL,
    TARGET_MODELS,
    TOTAL_LIMIT,
    USER_AGENT,
    get_crawl_mode_settings,
)
from scraper.html_parser import extract_listing_urls, parse_listing_page
from scraper.target_models import build_target_search_url, matches_target_model
from scraper.utils import extract_listing_id, is_blocked_response


class KolesaPlaywrightParser:
    """Optional fallback parser. HTTP remains the default engine."""

    def __init__(
        self,
        db: CarDatabase,
        headless: bool = DEFAULT_HEADLESS,
        mode: str = "normal",
        max_runtime_hours: float | None = None,
        settings: CrawlModeSettings | None = None,
    ) -> None:
        self.db = db
        self.headless = headless
        self.mode = mode
        self.settings = settings or get_crawl_mode_settings(mode)
        self.max_runtime_seconds = max_runtime_hours * 3600 if max_runtime_hours is not None else None
        self.started_at: float | None = None
        self.stop_requested = False
        self.stop_reason: str | None = None
        self.block_detected = False
        self.logger = logging.getLogger("kolesa_playwright_parser")

    async def collect_until_total(self, target_total: int) -> int:
        from playwright.async_api import async_playwright

        target_total = min(target_total, TOTAL_LIMIT)
        start_count = self.db.count_all_cars()
        if start_count >= target_total:
            return 0

        saved = 0
        page_number = 1
        progress = tqdm(total=target_total - start_count, desc="Saved listings", unit="car")

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale="ru-RU",
                viewport={"width": 1366, "height": 900},
            )
            context.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            page = await context.new_page()

            try:
                while self.db.count_all_cars() < target_total:
                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
                    listing_urls = extract_listing_urls(await page.content())
                    if not listing_urls:
                        break

                    for url in listing_urls:
                        if self.db.count_all_cars() >= target_total:
                            break
                        if await self._parse_and_save(context, url):
                            saved += 1
                            progress.update(1)
                        await self._delay()

                    page_number += 1
            finally:
                progress.close()
                await context.close()
                await browser.close()

        return saved

    async def collect_targets(self, targets: list[dict] = TARGET_MODELS) -> int:
        from playwright.async_api import async_playwright

        self.started_at = time.monotonic()
        saved = 0
        first_search_request = True

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale="ru-RU",
                viewport={"width": 1366, "height": 900},
            )
            context.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            page = await context.new_page()

            try:
                for target in targets:
                    if not self._can_continue():
                        break

                    current_count = self.db.get_target_current_count(target["brand"], target["model"])
                    remaining = max(0, int(target["limit"]) - current_count)
                    self.logger.info(
                        "current target %s %s progress: %s/%s remaining=%s",
                        target["brand"],
                        target["model"],
                        current_count,
                        target["limit"],
                        remaining,
                    )
                    if remaining <= 0:
                        self.logger.info("skipped because target limit reached: %s %s", target["brand"], target["model"])
                        continue

                    progress = tqdm(total=remaining, desc=f"{target['brand']} {target['model']}", unit="car")
                    try:
                        page_number = 1
                        while self._can_continue():
                            current_count = self.db.get_target_current_count(target["brand"], target["model"])
                            if current_count >= int(target["limit"]):
                                break

                            search_url = build_target_search_url(target["brand"], target["model"], page_number)
                            self.logger.info("current search page %s: %s", page_number, search_url)
                            if first_search_request:
                                self.logger.info("pause duration 0.0s before search request: %s (first search request)", search_url)
                                first_search_request = False
                            else:
                                await self._delay("search")

                            response = await page.goto(search_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
                            html = await page.content()
                            if self._blocked_by_response(response.status if response else 0, html, search_url):
                                break

                            listing_urls = extract_listing_urls(html)
                            if not listing_urls:
                                self.logger.warning("no listing URLs found for target %s %s on page %s", target["brand"], target["model"], page_number)
                                break

                            for url in listing_urls:
                                if not self._can_continue():
                                    break
                                current_count = self.db.get_target_current_count(target["brand"], target["model"])
                                if current_count >= int(target["limit"]):
                                    break
                                if await self._parse_and_save_target(context, url, target):
                                    saved += 1
                                    progress.update(1)
                                await self._delay("detail")

                            page_number += 1
                    finally:
                        progress.close()
            finally:
                await page.close()
                await context.close()
                await browser.close()

        return saved

    async def update(self, pages: int) -> int:
        from playwright.async_api import async_playwright

        saved = 0
        progress = tqdm(desc="Saved listings", unit="car")

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await browser.new_context(user_agent=USER_AGENT, locale="ru-RU")
            context.set_default_timeout(PLAYWRIGHT_TIMEOUT_MS)
            page = await context.new_page()

            try:
                for page_number in range(1, pages + 1):
                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    await page.goto(search_url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
                    for url in extract_listing_urls(await page.content()):
                        if await self._parse_and_save(context, url):
                            saved += 1
                            progress.update(1)
                        await self._delay()
            finally:
                progress.close()
                await context.close()
                await browser.close()

        return saved

    async def _parse_and_save(self, context, url: str) -> bool:
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        page = await context.new_page()
        try:
            self.logger.info("listing URL %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            car = parse_listing_page(await page.content(), url)

            brand = car.get("brand")
            model = car.get("model")
            if brand and self.db.count_by_brand(brand) >= MAX_PER_BRAND:
                self.logger.info("skipped by brand limit: %s", brand)
                return False
            if brand and model and self.db.count_by_brand_model(brand, model) >= MAX_PER_MODEL:
                self.logger.info("skipped by model limit: %s %s", brand, model)
                return False

            saved = self.db.insert_car(car)
            if saved:
                self.logger.info("saved listing %s; current total saved count: %s", url, self.db.count_all_cars())
            return saved
        except Exception:
            self.logger.exception("Playwright parsing error for %s", url)
            return False
        finally:
            await page.close()

    async def _parse_and_save_target(self, context, url: str, target: dict) -> bool:
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        page = await context.new_page()
        try:
            self.logger.info("current listing URL %s", url)
            response = await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            html = await page.content()
            if self._blocked_by_response(response.status if response else 0, html, url):
                return False

            car = parse_listing_page(html, url)
            if not matches_target_model(car, target):
                self.logger.info("skipped because not target model: %s for target %s %s", url, target["brand"], target["model"])
                return False

            if self.db.get_target_current_count(target["brand"], target["model"]) >= int(target["limit"]):
                self.logger.info("skipped because target limit reached: %s %s", target["brand"], target["model"])
                return False

            saved = self.db.insert_car(car)
            if saved:
                self.logger.info("saved listing %s; current DB count: %s", url, self.db.count_all_cars())
            return saved
        except Exception:
            self.logger.exception("Playwright parsing error for %s", url)
            return False
        finally:
            await page.close()

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return START_URL
        separator = "&" if "?" in START_URL else "?"
        return f"{START_URL}{separator}page={page_number}"

    async def _delay(self, request_kind: str = "detail") -> None:
        if request_kind == "search":
            delay_range = self.settings.search_delay_seconds
        elif request_kind == "detail":
            delay_range = self.settings.detail_delay_seconds
        else:
            delay_range = (MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        delay = random.uniform(*delay_range)
        self.logger.info("pause duration %.1fs before %s request", delay, request_kind)
        await asyncio.sleep(delay)

    def _blocked_by_response(self, status_code: int, html: str, url: str) -> bool:
        blocked, reason = is_blocked_response(status_code, html)
        if blocked:
            self.stop_requested = True
            self.stop_reason = reason
            self.block_detected = True
            self.logger.error("%s Reason: %s URL: %s", BLOCK_STOP_MESSAGE, reason, url)
            return True
        return False

    def _can_continue(self) -> bool:
        if self.stop_requested:
            return False
        if self.max_runtime_seconds is not None and self.started_at is not None:
            if time.monotonic() - self.started_at >= self.max_runtime_seconds:
                self.stop_requested = True
                self.stop_reason = "max runtime reached"
                self.logger.error("reason for stopping: %s", self.stop_reason)
                return False
        return True
