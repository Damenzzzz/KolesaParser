import asyncio
import logging
import random

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import (
    DEFAULT_HEADLESS,
    MAX_DELAY_SECONDS,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    MIN_DELAY_SECONDS,
    PLAYWRIGHT_TIMEOUT_MS,
    START_URL,
    TOTAL_LIMIT,
    USER_AGENT,
)
from scraper.html_parser import extract_listing_urls, parse_listing_page
from scraper.utils import extract_listing_id


class KolesaPlaywrightParser:
    """Optional fallback parser. HTTP remains the default engine."""

    def __init__(self, db: CarDatabase, headless: bool = DEFAULT_HEADLESS) -> None:
        self.db = db
        self.headless = headless
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

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return START_URL
        separator = "&" if "?" in START_URL else "?"
        return f"{START_URL}{separator}page={page_number}"

    async def _delay(self) -> None:
        await asyncio.sleep(random.uniform(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS))
