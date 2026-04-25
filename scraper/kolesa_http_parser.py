import asyncio
import logging
from typing import Optional

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import HTTP_CONCURRENCY, MAX_PER_BRAND, MAX_PER_MODEL, START_URL, TOTAL_LIMIT
from scraper.html_parser import extract_listing_urls, parse_listing_page
from scraper.http_client import KolesaHTTPClient
from scraper.utils import extract_listing_id


class KolesaHTTPParser:
    """Fast default parser based on public HTTP pages."""

    def __init__(self, db: CarDatabase, concurrency: int = HTTP_CONCURRENCY) -> None:
        self.db = db
        self.concurrency = max(1, concurrency)
        self.logger = logging.getLogger("kolesa_http_parser")

    async def collect_until_total(self, target_total: int) -> int:
        target_total = min(target_total, TOTAL_LIMIT)
        start_count = self.db.count_all_cars()
        if start_count >= target_total:
            self.logger.info("database already has %s cars; target is %s", start_count, target_total)
            return 0

        saved_count = 0
        page_number = 1
        progress = tqdm(total=target_total - start_count, desc="Saved listings", unit="car")

        async with KolesaHTTPClient(concurrency=self.concurrency) as client:
            try:
                while self.db.count_all_cars() < target_total and not client.stop_requested:
                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    html = await client.fetch(search_url)
                    if client.stop_requested:
                        break
                    if not html:
                        page_number += 1
                        continue

                    listing_urls = extract_listing_urls(html)
                    if not listing_urls:
                        self.logger.warning("no listing URLs found on page %s", page_number)
                        break

                    saved_on_page = await self._process_listing_batch(client, listing_urls, target_total, progress)
                    saved_count += saved_on_page
                    self.logger.info("current total saved count: %s", self.db.count_all_cars())
                    page_number += 1
            finally:
                progress.close()

            if client.stop_requested:
                self.logger.error("collection stopped safely: %s", client.stop_reason)

        return saved_count

    async def update(self, pages: int) -> int:
        saved_count = 0
        progress = tqdm(desc="Saved listings", unit="car")

        async with KolesaHTTPClient(concurrency=self.concurrency) as client:
            try:
                for page_number in range(1, pages + 1):
                    if client.stop_requested:
                        break

                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    html = await client.fetch(search_url)
                    if not html:
                        continue

                    listing_urls = extract_listing_urls(html)
                    if not listing_urls:
                        self.logger.warning("no listing URLs found on page %s", page_number)
                        continue

                    saved_count += await self._process_listing_batch(client, listing_urls, TOTAL_LIMIT, progress)
                    self.logger.info("current total saved count: %s", self.db.count_all_cars())
            finally:
                progress.close()

            if client.stop_requested:
                self.logger.error("update stopped safely: %s", client.stop_reason)

        return saved_count

    async def _process_listing_batch(
        self,
        client: KolesaHTTPClient,
        listing_urls: list[str],
        target_total: int,
        progress: tqdm,
    ) -> int:
        saved = 0
        tasks = []

        for url in listing_urls:
            if self.db.count_all_cars() >= target_total or client.stop_requested:
                break
            listing_id = extract_listing_id(url)
            if self.db.car_exists(listing_id, url):
                self.logger.info("skipped duplicate %s", url)
                continue
            tasks.append(asyncio.create_task(self._fetch_parse_save(client, url, target_total)))

        try:
            for task in asyncio.as_completed(tasks):
                if self.db.count_all_cars() >= target_total:
                    break
                result = await task
                if result:
                    saved += 1
                    progress.update(1)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return saved

    async def _fetch_parse_save(self, client: KolesaHTTPClient, url: str, target_total: int) -> bool:
        if self.db.count_all_cars() >= target_total or client.stop_requested:
            return False

        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        self.logger.info("listing URL %s", url)
        html = await client.fetch(url)
        if not html:
            return False

        try:
            car = parse_listing_page(html, url)
        except Exception:
            self.logger.exception("parsing error for %s", url)
            return False

        if not self._has_minimum_public_data(car):
            self.logger.warning("skipped incomplete listing %s", url)
            return False

        if self.db.car_exists(car.get("listing_id"), car.get("url")):
            self.logger.info("skipped duplicate %s", url)
            return False

        brand = car.get("brand")
        model = car.get("model")
        if not brand or not model:
            self.logger.info("incomplete brand/model for %s: brand=%s model=%s", url, brand, model)

        if brand and self.db.count_by_brand(brand) >= MAX_PER_BRAND:
            self.logger.info("skipped by brand limit: %s", brand)
            return False

        if brand and model and self.db.count_by_brand_model(brand, model) >= MAX_PER_MODEL:
            self.logger.info("skipped by model limit: %s %s", brand, model)
            return False

        if self.db.count_all_cars() >= target_total:
            return False

        saved = self.db.insert_car(car)
        if saved:
            self.logger.info("saved listing %s; current total saved count: %s", url, self.db.count_all_cars())
            return True

        self.logger.info("skipped duplicate by SQLite constraint %s", url)
        return False

    def _has_minimum_public_data(self, car: dict) -> bool:
        return bool(car.get("listing_id") and car.get("url") and (car.get("title") or car.get("price")))

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return START_URL
        separator = "&" if "?" in START_URL else "?"
        return f"{START_URL}{separator}page={page_number}"
