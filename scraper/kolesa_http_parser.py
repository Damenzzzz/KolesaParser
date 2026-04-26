import asyncio
import logging
import random
import time

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import (
    BLOCK_STOP_MESSAGE,
    HTTP_CONCURRENCY,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    START_URL,
    TOTAL_LIMIT,
    get_crawl_mode_settings,
    CrawlModeSettings,
)
from scraper.html_parser import extract_listing_urls, parse_listing_page
from scraper.http_client import KolesaHTTPClient
from scraper.utils import extract_listing_id


class KolesaHTTPParser:
    """Default parser for public Kolesa.kz pages, tuned for slow resumable runs."""

    def __init__(
        self,
        db: CarDatabase,
        concurrency: int = HTTP_CONCURRENCY,
        mode: str = "normal",
        max_runtime_hours: float | None = None,
        stop_on_block: bool = True,
        settings: CrawlModeSettings | None = None,
    ) -> None:
        self.db = db
        self.mode = mode
        self.settings = settings or get_crawl_mode_settings(mode)
        self.concurrency = 1 if mode in {"safe", "balanced", "night"} else max(1, concurrency)
        self.max_runtime_seconds = max_runtime_hours * 3600 if max_runtime_hours else None
        self.stop_on_block = stop_on_block
        self.started_at: float | None = None
        self.stop_requested = False
        self.stop_reason: str | None = None
        self.block_detected = False
        self.saved_this_run = 0
        self.last_printed_db_count = self.db.count_all_cars()
        self.logger = logging.getLogger("kolesa_http_parser")

    async def collect_until_total(self, target_total: int) -> int:
        target_total = min(target_total, TOTAL_LIMIT)
        start_count = self.db.count_all_cars()
        self._start_run("collect", target_total)
        if start_count >= target_total:
            self.logger.info("database already has %s cars; target is %s", start_count, target_total)
            return 0

        saved_count = 0
        page_number = 1
        progress = tqdm(total=target_total - start_count, desc="Saved listings", unit="car")

        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            first_search_request = True
            try:
                while self._should_continue(target_total, client):
                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    html = await client.fetch(
                        search_url,
                        request_kind="search",
                        skip_delay=first_search_request,
                    )
                    first_search_request = False
                    if not self._should_continue(target_total, client):
                        break
                    if not html:
                        page_number += 1
                        continue

                    listing_urls = extract_listing_urls(html)
                    if not listing_urls:
                        self.logger.warning("no listing URLs found on page %s", page_number)
                        break

                    saved_on_page = await self._process_listing_batch(
                        client,
                        listing_urls,
                        target_total,
                        progress,
                    )
                    saved_count += saved_on_page
                    self._print_current_db_count(force=True)
                    page_number += 1
            finally:
                progress.close()

            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)

        self.logger.info("reason for stopping: %s", self.stop_reason or "target reached or no more listings")
        self.logger.info("final DB count: %s", self.db.count_all_cars())
        return saved_count

    async def update(self, pages: int) -> int:
        self._start_run("update", TOTAL_LIMIT)
        saved_count = 0
        progress = tqdm(desc="Saved listings", unit="car")

        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            first_search_request = True
            try:
                for page_number in range(1, pages + 1):
                    if not self._should_continue(TOTAL_LIMIT, client):
                        break

                    search_url = self._search_page_url(page_number)
                    self.logger.info("current search page %s: %s", page_number, search_url)
                    html = await client.fetch(
                        search_url,
                        request_kind="search",
                        skip_delay=first_search_request,
                    )
                    first_search_request = False
                    if not self._should_continue(TOTAL_LIMIT, client):
                        break
                    if not html:
                        continue

                    listing_urls = extract_listing_urls(html)
                    if not listing_urls:
                        self.logger.warning("no listing URLs found on page %s", page_number)
                        continue

                    saved_count += await self._process_listing_batch(
                        client,
                        listing_urls,
                        TOTAL_LIMIT,
                        progress,
                    )
                    self._print_current_db_count(force=True)
            finally:
                progress.close()

            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)

        self.logger.info("reason for stopping: %s", self.stop_reason or "update finished")
        self.logger.info("final DB count: %s", self.db.count_all_cars())
        return saved_count

    async def _process_listing_batch(
        self,
        client: KolesaHTTPClient,
        listing_urls: list[str],
        target_total: int,
        progress: tqdm,
    ) -> int:
        if self.concurrency <= 1:
            return await self._process_listing_batch_sequential(client, listing_urls, target_total, progress)
        return await self._process_listing_batch_concurrent(client, listing_urls, target_total, progress)

    async def _process_listing_batch_sequential(
        self,
        client: KolesaHTTPClient,
        listing_urls: list[str],
        target_total: int,
        progress: tqdm,
    ) -> int:
        saved = 0
        for url in listing_urls:
            if not self._should_continue(target_total, client):
                break
            if await self._fetch_parse_save(client, url, target_total):
                saved += 1
                progress.update(1)
                await self._after_successful_save(client, target_total)
        return saved

    async def _process_listing_batch_concurrent(
        self,
        client: KolesaHTTPClient,
        listing_urls: list[str],
        target_total: int,
        progress: tqdm,
    ) -> int:
        saved = 0
        tasks = []

        for url in listing_urls:
            if not self._should_continue(target_total, client):
                break
            listing_id = extract_listing_id(url)
            if self.db.car_exists(listing_id, url):
                self.logger.info("skipped duplicate %s", url)
                continue
            tasks.append(asyncio.create_task(self._fetch_parse_save(client, url, target_total)))

        try:
            for task in asyncio.as_completed(tasks):
                if not self._should_continue(target_total, client):
                    break
                result = await task
                if result:
                    saved += 1
                    progress.update(1)
                    await self._after_successful_save(client, target_total)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        return saved

    async def _fetch_parse_save(self, client: KolesaHTTPClient, url: str, target_total: int) -> bool:
        if not self._should_continue(target_total, client):
            return False

        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        self.logger.info("current listing URL %s", url)
        html = await client.fetch(url, request_kind="detail")
        if not self._should_continue(target_total, client) or not html:
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

        if not self._should_continue(target_total, client):
            return False

        saved = self.db.insert_car(car)
        if saved:
            self.logger.info("saved listing %s; current DB count: %s", url, self.db.count_all_cars())
            return True

        self.logger.info("skipped duplicate by SQLite constraint %s", url)
        return False

    async def _after_successful_save(self, client: KolesaHTTPClient, target_total: int) -> None:
        self.saved_this_run += 1
        self.logger.info("saved count this run: %s", self.saved_this_run)
        self._print_current_db_count()

        if not self.settings.short_pause_every and not self.settings.long_pause_every:
            return
        if not self._should_continue(target_total, client):
            return

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
        if self.max_runtime_seconds and self.started_at is not None:
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
        self.saved_this_run = 0
        self.last_printed_db_count = self.db.count_all_cars()

        self.logger.info("mode: %s", self.mode)
        self.logger.info("engine: http")
        self.logger.info("selected command: %s", command)
        self.logger.info("concurrency: %s", self.concurrency)
        self.logger.info("detail delay seconds: %s-%s", *self.settings.detail_delay_seconds)
        self.logger.info("search delay seconds: %s-%s", *self.settings.search_delay_seconds)
        self.logger.info("current DB count: %s", self.last_printed_db_count)
        self.logger.info("target total limit: %s", target_total)
        self.logger.info("max runtime hours: %s", self.max_runtime_seconds / 3600 if self.max_runtime_seconds else None)
        self._print_current_db_count(force=True)

    def _should_continue(self, target_total: int, client: KolesaHTTPClient) -> bool:
        if self.stop_requested or client.stop_requested:
            return False
        if self.db.count_all_cars() >= target_total:
            return False
        if self.max_runtime_seconds and self.started_at is not None:
            elapsed = time.monotonic() - self.started_at
            if elapsed >= self.max_runtime_seconds:
                self._request_stop("max runtime reached")
                return False
        return True

    def _request_stop(self, reason: str, block_detected: bool = False) -> None:
        if not self.stop_requested:
            self.stop_requested = True
            self.stop_reason = reason
            self.block_detected = block_detected
            message = BLOCK_STOP_MESSAGE if block_detected else f"Stopped safely: {reason}"
            self.logger.error("reason for stopping: %s", message)

    def _print_current_db_count(self, force: bool = False) -> None:
        current_count = self.db.count_all_cars()
        if force or current_count - self.last_printed_db_count >= 10:
            print(f"Current total cars: {current_count}")
            self.logger.info("current DB count: %s", current_count)
            self.last_printed_db_count = current_count

    def _has_minimum_public_data(self, car: dict) -> bool:
        return bool(car.get("listing_id") and car.get("url") and (car.get("title") or car.get("price")))

    def _search_page_url(self, page_number: int) -> str:
        if page_number <= 1:
            return START_URL
        separator = "&" if "?" in START_URL else "?"
        return f"{START_URL}{separator}page={page_number}"
