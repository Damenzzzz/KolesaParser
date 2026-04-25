import asyncio
import json
import logging
import random
import re
from datetime import datetime, timezone
from typing import Optional

from playwright.async_api import Browser, BrowserContext, Page, TimeoutError as PlaywrightTimeoutError, async_playwright
from tqdm import tqdm

from database.db import CarDatabase
from scraper import selectors
from scraper.config import (
    BASE_URL,
    DEFAULT_TIMEOUT_MS,
    DESCRIPTION_WAIT_MS,
    MAX_DELAY_SECONDS,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    MAX_RETRIES,
    MIN_DELAY_SECONDS,
    SEARCH_URL,
    SOURCE_NAME,
    TOTAL_LIMIT,
    USER_AGENT,
)
from scraper.utils import (
    canonicalize_url,
    clean_engine_volume,
    clean_mileage,
    clean_price,
    detect_currency,
    extract_listing_id,
    generated_description_from_car,
    normalize_text,
    parse_brand_model_generation,
    parse_year_from_title,
    split_city_region,
)


class KolesaParser:
    def __init__(
        self,
        db: CarDatabase,
        headless: bool = True,
        min_delay: int = MIN_DELAY_SECONDS,
        max_delay: int = MAX_DELAY_SECONDS,
    ) -> None:
        self.db = db
        self.headless = headless
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.logger = logging.getLogger("kolesa_parser")

    async def collect(self, limit: int, pages: Optional[int] = None) -> int:
        saved_count = 0
        seen_urls: set[str] = set()

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=self.headless)
            context = await self._new_context(browser)
            page = await context.new_page()

            page_number = 1
            progress = tqdm(total=limit, desc="Saved listings", unit="car")
            try:
                while saved_count < limit and self.db.count_total() < TOTAL_LIMIT:
                    if pages is not None and page_number > pages:
                        break

                    search_url = self._search_page_url(page_number)
                    self.logger.info("current page %s: %s", page_number, search_url)

                    ok = await self.safe_goto(page, search_url)
                    if not ok:
                        page_number += 1
                        continue

                    listing_urls = await self.collect_listing_urls(page)
                    if not listing_urls:
                        self.logger.warning("no listing URLs found on page %s", page_number)
                        break

                    for url in listing_urls:
                        if saved_count >= limit or self.db.count_total() >= TOTAL_LIMIT:
                            break
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)

                        result = await self.parse_and_save_listing(context, url)
                        if result:
                            saved_count += 1
                            progress.update(1)
                        await self.polite_delay()

                    page_number += 1
            finally:
                progress.close()
                await context.close()
                await browser.close()

        return saved_count

    async def update(self, pages: int) -> int:
        return await self.collect(limit=TOTAL_LIMIT, pages=pages)

    async def _new_context(self, browser: Browser) -> BrowserContext:
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="ru-RU",
            viewport={"width": 1366, "height": 900},
        )
        context.set_default_timeout(DEFAULT_TIMEOUT_MS)
        return context

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return SEARCH_URL
        return f"{SEARCH_URL}?page={page_number}"

    async def collect_listing_urls(self, page: Page) -> list[str]:
        urls: list[str] = []
        for selector in selectors.LISTING_LINK_SELECTORS:
            try:
                hrefs = await page.locator(selector).evaluate_all(
                    "(links) => links.map((link) => link.getAttribute('href')).filter(Boolean)"
                )
            except Exception as exc:
                self.logger.debug("selector failed for listing links %s: %s", selector, exc)
                continue
            for href in hrefs:
                url = canonicalize_url(href, BASE_URL)
                if url and extract_listing_id(url) and url not in urls:
                    urls.append(url)
            if urls:
                break
        return urls

    async def parse_and_save_listing(self, context: BrowserContext, url: str) -> bool:
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        page = await context.new_page()
        try:
            self.logger.info("listing URL %s", url)
            ok = await self.safe_goto(page, url)
            if not ok:
                return False

            car = await self.parse_listing_page(page, url)
            listing_id = car.get("listing_id")

            if self.db.car_exists(listing_id, car.get("url")):
                self.logger.info("skipped duplicate %s", url)
                return False

            brand = car.get("brand")
            model = car.get("model")
            if brand and self.db.count_by_brand(brand) >= MAX_PER_BRAND:
                self.logger.info("skipped because brand limit reached: %s", brand)
                return False
            if brand and model and self.db.count_by_brand_model(brand, model) >= MAX_PER_MODEL:
                self.logger.info("skipped because model limit reached: %s %s", brand, model)
                return False

            saved = self.db.insert_car(car)
            if saved:
                self.logger.info("saved listing %s", url)
                return True

            self.logger.info("skipped duplicate by SQLite constraint %s", url)
            return False
        except Exception:
            self.logger.exception("error while parsing listing %s", url)
            return False
        finally:
            await page.close()

    async def parse_listing_page(self, page: Page, url: str) -> dict:
        title = await self.first_text(page, selectors.TITLE_SELECTORS)
        price_text = await self.first_text(page, selectors.PRICE_SELECTORS)
        parameters = await self.parse_parameters(page)
        options = await self.get_all_texts(page, ", ".join(selectors.OPTION_SELECTORS))

        description = await self.extract_description(page)
        city, region = split_city_region(self.get_parameter(parameters, "Город"))
        brand, model, generation = self.extract_brand_model_generation(title, parameters)
        engine_volume, fuel_type = self.extract_engine(parameters)

        car = {
            "listing_id": extract_listing_id(url),
            "url": url,
            "source": SOURCE_NAME,
            "title": title,
            "brand": brand,
            "model": model,
            "generation": generation,
            "year": self.extract_year(title),
            "price": clean_price(price_text),
            "currency": detect_currency(price_text),
            "city": city,
            "region": region,
            "mileage_km": clean_mileage(self.get_parameter(parameters, "Пробег")),
            "body_type": self.get_parameter(parameters, "Кузов"),
            "engine_volume_l": engine_volume,
            "fuel_type": fuel_type,
            "transmission": self.get_parameter(parameters, "Коробка передач", "Коробка"),
            "drive_type": self.get_parameter(parameters, "Привод"),
            "steering_wheel": self.get_parameter(parameters, "Руль"),
            "color": self.get_parameter(parameters, "Цвет"),
            "condition": self.get_parameter(parameters, "Состояние"),
            "customs_cleared": self.get_parameter(parameters, "Растаможен в Казахстане", "Растаможен"),
            "description": description,
            "seller_type": await self.first_text(page, selectors.SELLER_TYPE_SELECTORS),
            "published_at": await self.extract_published_at(page),
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "photo_count": await self.extract_photo_count(page),
            "is_active": 1,
            "raw_characteristics": json.dumps(
                {"parameters": parameters, "options": options},
                ensure_ascii=False,
            ),
        }
        car["generated_description"] = generated_description_from_car(car)
        return car

    async def parse_parameters(self, page: Page) -> dict[str, str]:
        parameters: dict[str, str] = {}
        rows = page.locator(selectors.PARAMETER_ROW_SELECTOR)
        count = await rows.count()

        for index in range(count):
            row = rows.nth(index)
            try:
                label = await self.get_text_or_none(row, selectors.PARAMETER_LABEL_SELECTOR)
                value = await self.get_text_or_none(row, selectors.PARAMETER_VALUE_SELECTOR)
            except Exception as exc:
                self.logger.debug("failed to parse parameter row %s: %s", index, exc)
                continue
            label = normalize_text(label)
            value = normalize_text(value)
            if label and value:
                parameters[label.rstrip(":")] = value

        return parameters

    async def extract_description(self, page: Page) -> Optional[str]:
        try:
            await page.wait_for_selector(
                ".offer__description-seller:not(.is-loading), .js__description:not(.is-loading)",
                timeout=DESCRIPTION_WAIT_MS,
            )
        except PlaywrightTimeoutError:
            self.logger.debug("seller description did not finish loading")

        for selector in selectors.DESCRIPTION_SELECTORS:
            text = await self.get_text_or_none(page, selector)
            text = normalize_text(text)
            if text and "loading" not in text.lower():
                return text
        return None

    async def extract_photo_count(self, page: Page) -> Optional[int]:
        counts = []
        for selector in selectors.PHOTO_SELECTORS:
            try:
                count = await page.locator(selector).count()
            except Exception:
                continue
            if count:
                counts.append(count)
        return max(counts) if counts else None

    async def extract_published_at(self, page: Page) -> Optional[str]:
        text = await self.first_text(page, selectors.PUBLISHED_AT_SELECTORS)
        if not text:
            return None
        match = re.search(r"[cс]\s+(.+)$", text)
        return normalize_text(match.group(1) if match else text)

    def extract_brand_model_generation(
        self,
        title: Optional[str],
        parameters: dict[str, str],
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
        brand, model, generation = parse_brand_model_generation(title)
        generation = self.get_parameter(parameters, "Поколение") or generation
        return brand, model, generation

    def extract_year(self, title: Optional[str]) -> Optional[int]:
        return parse_year_from_title(title)

    def extract_engine(self, parameters: dict[str, str]) -> tuple[Optional[float], Optional[str]]:
        engine_text = self.get_parameter(parameters, "Объем двигателя, л", "Двигатель")
        if not engine_text:
            return None, None
        volume = clean_engine_volume(engine_text)
        fuel_match = re.search(r"\(([^)]+)\)", engine_text)
        fuel_type = normalize_text(fuel_match.group(1)) if fuel_match else None
        return volume, fuel_type

    def get_parameter(self, parameters: dict[str, str], *names: str) -> Optional[str]:
        normalized = {self._normalize_key(key): value for key, value in parameters.items()}
        for name in names:
            value = normalized.get(self._normalize_key(name))
            if value:
                return value
        return None

    def _normalize_key(self, key: str) -> str:
        return normalize_text(key).lower().replace("ё", "е") if normalize_text(key) else ""

    async def first_text(self, page: Page, selector_list: list[str]) -> Optional[str]:
        for selector in selector_list:
            text = await self.get_text_or_none(page, selector)
            if text:
                return text
        return None

    async def get_text_or_none(self, scope, selector: str) -> Optional[str]:
        try:
            locator = scope.locator(selector).first
            if await locator.count() == 0:
                return None
            return normalize_text(await locator.inner_text(timeout=3000))
        except Exception:
            return None

    async def get_all_texts(self, page: Page, selector: str) -> list[str]:
        try:
            texts = await page.locator(selector).all_inner_texts()
        except Exception:
            return []
        return [text for text in (normalize_text(item) for item in texts) if text]

    async def safe_goto(self, page: Page, url: str) -> bool:
        for attempt in range(1, MAX_RETRIES + 2):
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
                return True
            except PlaywrightTimeoutError:
                self.logger.warning("timeout opening %s on attempt %s", url, attempt)
            except Exception as exc:
                self.logger.warning("error opening %s on attempt %s: %s", url, attempt, exc)
            await asyncio.sleep(attempt * 2)
        return False

    async def polite_delay(self) -> None:
        delay = random.uniform(self.min_delay, self.max_delay)
        await asyncio.sleep(delay)
