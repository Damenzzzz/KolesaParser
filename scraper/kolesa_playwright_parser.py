import asyncio
import logging
import random
import time
from datetime import datetime

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import (
    BRAND_TARGETS,
    BLOCK_STOP_MESSAGE,
    CrawlModeSettings,
    DEFAULT_HEADLESS,
    FAST_SKIP_PAGE_DELAY_SECONDS,
    LOGS_DIR,
    MAX_DELAY_SECONDS,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    MIN_DELAY_SECONDS,
    PLAYWRIGHT_TIMEOUT_MS,
    START_URL,
    TARGET_MODELS,
    TOTAL_LIMIT,
    USER_AGENT,
    VISIBLE_CHALLENGE_STOP_MESSAGE,
    get_crawl_mode_settings,
)
from scraper.brand_targets import (
    brand_matches,
    build_brand_page_url,
    is_wrong_brand_guess,
    load_brand_state,
    save_brand_state,
)
from scraper.html_parser import extract_brand_listing_cards, extract_listing_urls, parse_listing_page
from scraper.target_models import build_target_search_url, matches_target_model
from scraper.utils import extract_listing_id, is_blocked_response, is_visible_challenge_text


class KolesaPlaywrightParser:
    """Optional fallback parser. HTTP remains the default engine."""

    def __init__(
        self,
        db: CarDatabase,
        headless: bool = DEFAULT_HEADLESS,
        mode: str = "normal",
        max_runtime_hours: float | None = None,
        settings: CrawlModeSettings | None = None,
        checkpoint_export_every: int = 0,
    ) -> None:
        self.db = db
        self.headless = headless
        self.mode = mode
        self.settings = settings or get_crawl_mode_settings(mode)
        self.checkpoint_export_every = max(0, int(checkpoint_export_every))
        self.max_runtime_seconds = max_runtime_hours * 3600 if max_runtime_hours is not None else None
        self.started_at: float | None = None
        self.stop_requested = False
        self.stop_reason: str | None = None
        self.block_detected = False
        self.visible_challenge_detected = False
        self.consecutive_errors = 0
        self.saved_this_run = 0
        self.last_printed_db_count = self.db.count_all_cars()
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
                await self._safe_close(context, "browser context")
                await self._safe_close(browser, "browser")

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
                await self._safe_close(page, "page")
                await self._safe_close(context, "browser context")
                await self._safe_close(browser, "browser")

        return saved

    async def collect_brands(self, targets: list[dict] = BRAND_TARGETS, ignore_state: bool = False) -> int:
        from playwright.async_api import async_playwright

        self._start_run("collect-brands", sum(int(target["limit"]) for target in targets))
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

                    current_count = self.db.count_by_brand(target["brand"])
                    remaining = max(0, int(target["limit"]) - current_count)
                    self.logger.info(
                        "current brand progress: %s %s/%s remaining=%s",
                        target["brand"],
                        current_count,
                        target["limit"],
                        remaining,
                    )
                    self.logger.info("current brand URL: %s", target["url"])
                    if remaining <= 0:
                        self.logger.info("brand completed: %s %s/%s", target["brand"], current_count, target["limit"])
                        self.logger.info("skipped because brand limit reached: %s", target["brand"])
                        continue

                    saved_for_brand, first_search_request = await self._collect_brand_target(
                        page,
                        target,
                        first_search_request,
                        ignore_state=ignore_state,
                    )
                    saved += saved_for_brand
            finally:
                await self._safe_close(page, "page")
                await self._safe_close(context, "browser context")
                await self._safe_close(browser, "browser")

        self.logger.info("reason for stopping: %s", self.stop_reason or "brand collection finished")
        self.logger.info("final DB count: %s", self.db.count_all_cars())
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
                await self._safe_close(context, "browser context")
                await self._safe_close(browser, "browser")

        return saved

    async def _collect_brand_target(
        self,
        page,
        target: dict,
        first_search_request: bool,
        ignore_state: bool = False,
    ) -> tuple[int, bool]:
        brand = target["brand"]
        limit = int(target["limit"])
        current_count = self.db.count_by_brand(brand)
        remaining = max(0, limit - current_count)
        saved = 0
        page_number = 1 if ignore_state else load_brand_state(brand)
        blank_pages = 0
        empty_saved_pages = 0
        use_fast_skip_delay = False
        progress = tqdm(total=remaining, desc=brand, unit="car")

        try:
            while self._can_continue():
                current_count = self.db.count_by_brand(brand)
                if current_count >= limit:
                    self.logger.info("brand completed: %s %s/%s", brand, current_count, limit)
                    self.logger.info("skipped because brand limit reached: %s", brand)
                    break

                brand_url = build_brand_page_url(target["url"], page_number)
                save_brand_state(brand, page_number)
                self.logger.info(
                    "current brand %s; current brand page number %s; current brand URL: %s",
                    brand,
                    page_number,
                    brand_url,
                )
                self.logger.info(
                    "current brand progress: %s %s/%s remaining=%s",
                    brand,
                    current_count,
                    limit,
                    max(0, limit - current_count),
                )

                if first_search_request:
                    self.logger.info("pause duration 0.0s before search request: %s (first search request)", brand_url)
                    first_search_request = False
                    use_fast_skip_delay = False
                else:
                    if use_fast_skip_delay:
                        await self._delay(
                            "search",
                            delay_range=FAST_SKIP_PAGE_DELAY_SECONDS,
                            delay_label="fast skip page delay",
                        )
                    else:
                        await self._delay("search", delay_label="normal search page delay")
                    use_fast_skip_delay = False
                if not self._can_continue():
                    break

                try:
                    response = await page.goto(
                        brand_url,
                        wait_until="domcontentloaded",
                        timeout=PLAYWRIGHT_TIMEOUT_MS,
                    )
                    html = await page.content()
                except Exception as exc:
                    self._register_navigation_error(brand_url, exc)
                    page_number += 1
                    continue

                status_code = response.status if response else 0
                self.logger.info("HTTP status code %s for %s", status_code, brand_url)
                if await self._blocked_by_playwright_page(status_code, html, page, brand_url):
                    break
                if not html or not html.strip():
                    blank_pages += 1
                    self.logger.warning(
                        "blank/error brand page for %s page %s; consecutive_blank_pages=%s/%s",
                        brand,
                        page_number,
                        blank_pages,
                        self.settings.max_consecutive_errors,
                    )
                    if blank_pages >= self.settings.max_consecutive_errors:
                        self._request_stop("too many blank/error brand pages")
                        break
                    page_number += 1
                    continue

                blank_pages = 0
                self.consecutive_errors = 0
                total_links_found = len(extract_listing_urls(html))
                cards = extract_brand_listing_cards(html, brand)
                page_stats = {
                    "brand": brand,
                    "page_number": page_number,
                    "total_links_found": total_links_found,
                    "unique_main_card_links": len(cards),
                    "duplicates_skipped_before_detail": 0,
                    "wrong_brand_skipped_before_detail": 0,
                    "detail_pages_opened": 0,
                    "saved_listings": 0,
                    "wrong_brand_after_detail": 0,
                }
                if not cards:
                    self.logger.warning("no main listing cards found for brand %s on page %s", brand, page_number)
                    self._log_brand_page_summary(page_stats)
                    empty_saved_pages += 1
                    if empty_saved_pages >= 5:
                        self.logger.info("stopping current brand %s after %s consecutive pages with zero saves", brand, empty_saved_pages)
                        break
                    use_fast_skip_delay = self.mode == "balanced"
                    save_brand_state(brand, page_number)
                    page_number += 1
                    continue

                for card in cards:
                    if not self._can_continue():
                        break
                    url = card["url"]
                    listing_id = extract_listing_id(url)
                    if self.db.car_exists(listing_id, url):
                        page_stats["duplicates_skipped_before_detail"] += 1
                        self.logger.info("skipped duplicate before detail request %s", url)
                        continue
                    if is_wrong_brand_guess(card.get("brand_guess"), brand):
                        page_stats["wrong_brand_skipped_before_detail"] += 1
                        self.logger.info(
                            "skipped wrong brand from card before detail request %s; brand_guess=%s target=%s title=%s",
                            url,
                            card.get("brand_guess"),
                            brand,
                            card.get("card_title"),
                        )
                        continue
                    current_count = self.db.count_by_brand(brand)
                    if current_count >= limit:
                        self.logger.info("brand completed: %s %s/%s", brand, current_count, limit)
                        self.logger.info("skipped because brand limit reached: %s", brand)
                        break
                    page_stats["detail_pages_opened"] += 1
                    result = await self._parse_and_save_brand(page, url, target)
                    if result == "saved":
                        saved += 1
                        page_stats["saved_listings"] += 1
                        progress.update(1)
                        await self._after_successful_save(brand)
                    elif result == "wrong_brand":
                        page_stats["wrong_brand_after_detail"] += 1

                self._log_brand_page_summary(page_stats)
                if page_stats["saved_listings"] == 0:
                    empty_saved_pages += 1
                    if empty_saved_pages >= 5:
                        self.logger.info("stopping current brand %s after %s consecutive pages with zero saves", brand, empty_saved_pages)
                        break
                    use_fast_skip_delay = self.mode == "balanced" and page_stats["detail_pages_opened"] == 0
                else:
                    empty_saved_pages = 0
                    use_fast_skip_delay = False
                save_brand_state(brand, page_number)
                page_number += 1
        finally:
            progress.close()

        return saved, first_search_request

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

    async def _parse_and_save_brand(self, page, url: str, target: dict) -> str:
        brand = target["brand"]
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate before detail request %s", url)
            return "duplicate"

        await self._delay("detail")
        if not self._can_continue():
            return "skipped"

        try:
            self.logger.info("current brand %s; current listing URL %s", brand, url)
            response = await page.goto(url, wait_until="domcontentloaded", timeout=PLAYWRIGHT_TIMEOUT_MS)
            html = await page.content()
        except Exception as exc:
            self._register_navigation_error(url, exc)
            return "skipped"

        status_code = response.status if response else 0
        self.logger.info("HTTP status code %s for %s", status_code, url)
        if await self._blocked_by_playwright_page(status_code, html, page, url):
            return "skipped"

        try:
            car = parse_listing_page(html, url)
        except Exception:
            self.logger.exception("Playwright parsing error for %s", url)
            return "skipped"

        matches, reason = brand_matches(car, brand)
        if not matches:
            self.logger.info("skipped because wrong brand: %s; %s", url, reason)
            return "wrong_brand"
        if reason == "incomplete_brand_parse":
            # The listing came from the public brand page, so keep it resumable under that brand.
            self.logger.info("incomplete_brand_parse for %s from brand page %s", url, brand)
            car["brand"] = brand

        if not self._has_minimum_public_data(car):
            self.logger.warning("skipped incomplete listing %s", url)
            return "skipped"

        if self.db.car_exists(car.get("listing_id"), car.get("url")):
            self.logger.info("skipped duplicate %s", url)
            return "duplicate"

        current_count = self.db.count_by_brand(brand)
        if current_count >= int(target["limit"]):
            self.logger.info("skipped because brand limit reached: %s", brand)
            return "limit"

        saved = self.db.insert_car(car)
        if saved:
            self.consecutive_errors = 0
            self.logger.info(
                "saved listing %s for brand %s; current brand progress: %s/%s; current DB count: %s",
                url,
                brand,
                current_count + 1,
                target["limit"],
                self.db.count_all_cars(),
            )
            return "saved"

        self.logger.info("skipped duplicate by SQLite constraint %s", url)
        return "duplicate"

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return START_URL
        separator = "&" if "?" in START_URL else "?"
        return f"{START_URL}{separator}page={page_number}"

    async def _delay(
        self,
        request_kind: str = "detail",
        delay_range: tuple[float, float] | None = None,
        delay_label: str | None = None,
    ) -> None:
        if delay_range is None:
            if request_kind == "search":
                delay_range = self.settings.search_delay_seconds
            elif request_kind == "detail":
                delay_range = self.settings.detail_delay_seconds
            else:
                delay_range = (MIN_DELAY_SECONDS, MAX_DELAY_SECONDS)
        if delay_label is None:
            delay_label = "normal search page delay" if request_kind == "search" else "detail delay"
        delay = random.uniform(*delay_range)
        if self.max_runtime_seconds is not None and self.started_at is not None:
            remaining = self.max_runtime_seconds - (time.monotonic() - self.started_at)
            if remaining <= 0:
                self._request_stop("max runtime reached")
                return
            delay = min(delay, remaining)
        self.logger.info("%s duration %.1fs before %s request", delay_label, delay, request_kind)
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

    async def _blocked_by_playwright_page(self, status_code: int, html: str, page, url: str) -> bool:
        blocked, reason = is_blocked_response(status_code, html if status_code != 200 else "")
        if blocked:
            self._request_stop(reason, block_detected=True)
            self.logger.error("%s Reason: %s URL: %s", BLOCK_STOP_MESSAGE, reason, url)
            return True

        visible_blocked, visible_reason = await self.detect_visible_challenge(page)
        if visible_blocked:
            await self._save_debug_stop_artifacts(page)
            self._request_stop(VISIBLE_CHALLENGE_STOP_MESSAGE, block_detected=True)
            self.visible_challenge_detected = True
            self.logger.error("%s Reason: %s URL: %s", VISIBLE_CHALLENGE_STOP_MESSAGE, visible_reason, url)
            return True
        return False

    async def detect_visible_challenge(self, page) -> tuple[bool, str]:
        try:
            visible_text = await page.locator("body").inner_text(timeout=3000)
        except Exception as exc:
            self.logger.warning(
                "could not read Playwright visible body text; url=%s exception_class=%s",
                page.url,
                exc.__class__.__name__,
            )
            visible_text = ""

        try:
            title = await page.title()
        except Exception as exc:
            self.logger.warning(
                "could not read Playwright page title; url=%s exception_class=%s",
                page.url,
                exc.__class__.__name__,
            )
            title = ""

        listing_card_count = await self._count_visible_listing_cards(page)
        blocked, reason = is_visible_challenge_text(visible_text)
        if not blocked:
            return False, ""

        if listing_card_count > 0:
            self.logger.info(
                (
                    "visible challenge phrase found, but normal listing cards are visible; "
                    "continuing; url=%s title=%r phrase=%s visible_listing_cards=%s"
                ),
                page.url,
                title,
                reason,
                listing_card_count,
            )
            return False, ""

        if self._looks_like_normal_listing_page(visible_text, title):
            self.logger.info(
                (
                    "visible challenge phrase found, but normal detail listing markers are visible; "
                    "continuing; url=%s title=%r phrase=%s"
                ),
                page.url,
                title,
                reason,
            )
            return False, ""

        self.logger.error(
            (
                "visible challenge detected; url=%s title=%r phrase=%s "
                "visible_listing_cards=%s visible_text_snippet=%r"
            ),
            page.url,
            title,
            reason,
            listing_card_count,
            " ".join(visible_text[:1000].split()),
        )
        return True, f"visible text phrase: {reason}"

    def _looks_like_normal_listing_page(self, visible_text: str, title: str) -> bool:
        haystack = f"{title} {visible_text}".lower()
        has_listing_title = "\u043f\u0440\u043e\u0434\u0430\u0436\u0430" in haystack and "\u043a\u043e\u043b\u0451\u0441\u0430" in haystack
        has_price = "\u20b8" in haystack
        field_markers = [
            "\u0433\u043e\u0440\u043e\u0434",
            "\u043a\u0443\u0437\u043e\u0432",
            "\u043e\u0431\u044a\u0435\u043c \u0434\u0432\u0438\u0433\u0430\u0442\u0435\u043b\u044f",
            "\u043a\u043e\u0440\u043e\u0431\u043a\u0430 \u043f\u0435\u0440\u0435\u0434\u0430\u0447",
            "\u043f\u0440\u0438\u0432\u043e\u0434",
            "\u0440\u0443\u043b\u044c",
        ]
        marker_count = sum(1 for marker in field_markers if marker in haystack)
        return has_listing_title or (has_price and marker_count >= 2)

    async def _count_visible_listing_cards(self, page) -> int:
        selectors = [
            '.a-card:has(a[href*="/a/show/"])',
            '[data-test*="advert"]:has(a[href*="/a/show/"])',
            '.a-list__item:has(a[href*="/a/show/"])',
            'article:has(a[href*="/a/show/"])',
            'div[class*="a-card"]:has(a[href*="/a/show/"])',
            'div[class*="listing"]:has(a[href*="/a/show/"])',
            'div[class*="result"]:has(a[href*="/a/show/"])',
        ]
        for selector in selectors:
            try:
                locator = page.locator(selector)
                count = await locator.count()
            except Exception:
                continue

            visible_count = 0
            for index in range(min(count, 50)):
                try:
                    if await locator.nth(index).is_visible(timeout=250):
                        visible_count += 1
                except Exception:
                    continue
            if visible_count:
                return visible_count
        return 0

    async def _save_debug_stop_artifacts(self, page) -> None:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        screenshot_path = LOGS_DIR / f"debug_stop_{timestamp}.png"

        try:
            visible_text = await page.locator("body").inner_text(timeout=3000)
        except Exception:
            visible_text = ""
        try:
            title = await page.title()
        except Exception:
            title = ""
        listing_card_count = await self._count_visible_listing_cards(page)

        try:
            await page.screenshot(path=str(screenshot_path), full_page=True)
            screenshot_message = str(screenshot_path)
        except Exception as exc:
            screenshot_message = f"screenshot failed: {exc.__class__.__name__}: {exc}"

        self.logger.error(
            (
                "debug stop snapshot; url=%s title=%r visible_listing_cards=%s "
                "visible_text_snippet=%r screenshot=%s"
            ),
            page.url,
            title,
            listing_card_count,
            " ".join(visible_text[:1000].split()),
            screenshot_message,
        )

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

    async def _after_successful_save(self) -> None:
        self.saved_this_run += 1
        self.logger.info("saved count this run: %s", self.saved_this_run)
        self._print_current_db_count()

        long_every = self.settings.long_pause_every
        short_every = self.settings.short_pause_every
        if (
            long_every
            and self.settings.long_pause_seconds
            and self.saved_this_run % long_every == 0
        ):
            await self._pause(self.settings.long_pause_seconds, f"{self.mode} long pause after saved batch")
        elif (
            short_every
            and self.settings.short_pause_seconds
            and self.saved_this_run % short_every == 0
        ):
            await self._pause(self.settings.short_pause_seconds, f"{self.mode} short pause after saved batch")

    async def _pause(self, delay_range: tuple[float, float], reason: str) -> None:
        delay = random.uniform(*delay_range)
        if self.max_runtime_seconds is not None and self.started_at is not None:
            remaining = self.max_runtime_seconds - (time.monotonic() - self.started_at)
            if remaining <= 0:
                self._request_stop("max runtime reached")
                return
            delay = min(delay, remaining)

        self.logger.info("pause duration %.1fs; reason=%s", delay, reason)
        await asyncio.sleep(delay)

    def _start_run(self, command: str, target_total: int) -> None:
        self.started_at = time.monotonic()
        self.stop_requested = False
        self.stop_reason = None
        self.block_detected = False
        self.visible_challenge_detected = False
        self.consecutive_errors = 0
        self.saved_this_run = 0
        self.last_printed_db_count = self.db.count_all_cars()

        self.logger.info("mode: %s", self.mode)
        self.logger.info("engine: playwright")
        self.logger.info("selected command: %s", command)
        self.logger.info("detail delay seconds: %s-%s", *self.settings.detail_delay_seconds)
        self.logger.info("search delay seconds: %s-%s", *self.settings.search_delay_seconds)
        self.logger.info("current DB count: %s", self.last_printed_db_count)
        self.logger.info("target total limit: %s", target_total)
        self.logger.info(
            "max runtime hours: %s",
            self.max_runtime_seconds / 3600 if self.max_runtime_seconds is not None else None,
        )
        self._print_current_db_count(force=True)

    def _log_brand_page_summary(self, stats: dict) -> None:
        self.logger.info(
            (
                "brand page summary: brand=%s page=%s total_links_found=%s "
                "unique_main_card_links=%s duplicates_on_page=%s wrong_brand_on_page=%s "
                "detail_pages_opened=%s saved_on_page=%s wrong_brand_after_detail=%s"
            ),
            stats["brand"],
            stats["page_number"],
            stats["total_links_found"],
            stats["unique_main_card_links"],
            stats["duplicates_skipped_before_detail"],
            stats["wrong_brand_skipped_before_detail"],
            stats["detail_pages_opened"],
            stats["saved_listings"],
            stats["wrong_brand_after_detail"],
        )

    def _request_stop(self, reason: str, block_detected: bool = False) -> None:
        if not self.stop_requested:
            self.stop_requested = True
            self.stop_reason = reason
            self.block_detected = block_detected
            message = BLOCK_STOP_MESSAGE if block_detected else f"Stopped safely: {reason}"
            self.logger.error("reason for stopping: %s", message)

    def _register_navigation_error(self, url: str, exc: Exception) -> None:
        self.consecutive_errors += 1
        self.logger.warning(
            "navigation error; url=%s exception_class=%s error=%s consecutive_errors=%s/%s",
            url,
            exc.__class__.__name__,
            exc,
            self.consecutive_errors,
            self.settings.max_consecutive_errors,
        )
        if self.consecutive_errors >= self.settings.max_consecutive_errors:
            self._request_stop("too many consecutive navigation errors or timeouts")

    def _print_current_db_count(self, force: bool = False) -> None:
        current_count = self.db.count_all_cars()
        if force or current_count - self.last_printed_db_count >= 10:
            print(f"Current total cars: {current_count}")
            self.logger.info("current DB count: %s", current_count)
            self.last_printed_db_count = current_count

    def _has_minimum_public_data(self, car: dict) -> bool:
        return bool(car.get("listing_id") and car.get("url") and (car.get("title") or car.get("price")))
