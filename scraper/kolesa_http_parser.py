import asyncio
import logging
import random
import time

from tqdm import tqdm

from database.db import CarDatabase
from scraper.config import (
    BRAND_TARGETS,
    BLOCK_STOP_MESSAGE,
    FAST_SKIP_PAGE_DELAY_SECONDS,
    HTTP_CONCURRENCY,
    MAX_PER_BRAND,
    MAX_PER_MODEL,
    START_URL,
    TARGET_MODELS,
    TOTAL_LIMIT,
    get_crawl_mode_settings,
    CrawlModeSettings,
)
from scraper.brand_targets import (
    brand_matches,
    build_brand_page_url,
    is_wrong_brand_guess,
    load_brand_state,
    save_brand_state,
)
from scraper.html_parser import extract_brand_listing_cards, extract_listing_urls, parse_listing_page
from scraper.http_client import KolesaHTTPClient
from scraper.target_models import build_target_search_url, matches_target_model
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
        self.max_runtime_seconds = max_runtime_hours * 3600 if max_runtime_hours is not None else None
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

    async def collect_targets(self, targets: list[dict] = TARGET_MODELS) -> int:
        self._start_run("collect-targets", sum(int(target["limit"]) for target in targets))
        saved_count = 0

        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            first_search_request = True
            for target in targets:
                if not self._run_can_continue(client):
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

                saved_for_target, first_search_request = await self._collect_target_model(
                    client,
                    target,
                    remaining,
                    first_search_request,
                )
                saved_count += saved_for_target

            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)

        self.logger.info("reason for stopping: %s", self.stop_reason or "target collection finished")
        self.logger.info("final DB count: %s", self.db.count_all_cars())
        return saved_count

    async def collect_target_model(self, target: dict, remaining: int) -> int:
        self._start_run("collect-target-model", self.db.count_all_cars() + remaining)
        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            saved, _ = await self._collect_target_model(client, target, remaining, True)
            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)
            return saved

    async def collect_brands(self, targets: list[dict] = BRAND_TARGETS, ignore_state: bool = False) -> int:
        self._start_run("collect-brands", sum(int(target["limit"]) for target in targets))
        saved_count = 0

        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            first_search_request = True
            for target in targets:
                if not self._run_can_continue(client):
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
                    client,
                    target,
                    first_search_request,
                    ignore_state=ignore_state,
                )
                saved_count += saved_for_brand

            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)

        self.logger.info("reason for stopping: %s", self.stop_reason or "brand collection finished")
        self.logger.info("final DB count: %s", self.db.count_all_cars())
        return saved_count

    async def collect_brand_target(self, target: dict, ignore_state: bool = False) -> int:
        self._start_run("collect-brand-target", int(target["limit"]))
        async with KolesaHTTPClient(
            concurrency=self.concurrency,
            mode=self.mode,
            stop_on_block=self.stop_on_block,
            settings=self.settings,
        ) as client:
            saved, _ = await self._collect_brand_target(client, target, True, ignore_state=ignore_state)
            if client.stop_requested:
                self._request_stop(client.stop_reason or "HTTP client requested stop", block_detected=True)
            return saved

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

    async def _collect_target_model(
        self,
        client: KolesaHTTPClient,
        target: dict,
        remaining: int,
        first_search_request: bool,
    ) -> tuple[int, bool]:
        saved = 0
        page_number = 1
        progress = tqdm(total=remaining, desc=f"{target['brand']} {target['model']}", unit="car")

        try:
            while self._run_can_continue(client):
                current_count = self.db.get_target_current_count(target["brand"], target["model"])
                if current_count >= int(target["limit"]):
                    self.logger.info("skipped because target limit reached: %s %s", target["brand"], target["model"])
                    break

                search_url = build_target_search_url(target["brand"], target["model"], page_number)
                self.logger.info(
                    "current target %s %s; current search page %s: %s",
                    target["brand"],
                    target["model"],
                    page_number,
                    search_url,
                )
                html = await client.fetch(
                    search_url,
                    request_kind="search",
                    skip_delay=first_search_request,
                )
                first_search_request = False
                if not self._run_can_continue(client):
                    break
                if not html:
                    page_number += 1
                    continue

                listing_urls = extract_listing_urls(html)
                if not listing_urls:
                    self.logger.warning("no listing URLs found for target %s %s on page %s", target["brand"], target["model"], page_number)
                    break

                for url in listing_urls:
                    if not self._run_can_continue(client):
                        break
                    current_count = self.db.get_target_current_count(target["brand"], target["model"])
                    if current_count >= int(target["limit"]):
                        self.logger.info("skipped because target limit reached: %s %s", target["brand"], target["model"])
                        break
                    if await self._fetch_parse_save_target(client, url, target):
                        saved += 1
                        progress.update(1)
                        await self._after_successful_save(client, TOTAL_LIMIT)

                page_number += 1
        finally:
            progress.close()

        return saved, first_search_request

    async def _collect_brand_target(
        self,
        client: KolesaHTTPClient,
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
            while self._run_can_continue(client):
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

                html = await client.fetch(
                    brand_url,
                    request_kind="search",
                    skip_delay=first_search_request,
                    delay_range_override=FAST_SKIP_PAGE_DELAY_SECONDS if use_fast_skip_delay else None,
                    delay_label="fast skip page delay" if use_fast_skip_delay else "normal search page delay",
                )
                first_search_request = False
                use_fast_skip_delay = False
                if not self._run_can_continue(client):
                    break
                if not html:
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
                    if not self._run_can_continue(client):
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
                    result = await self._fetch_parse_save_brand(client, url, target)
                    if result == "saved":
                        saved += 1
                        page_stats["saved_listings"] += 1
                        progress.update(1)
                        await self._after_successful_save(client, TOTAL_LIMIT)
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

    async def _fetch_parse_save_target(self, client: KolesaHTTPClient, url: str, target: dict) -> bool:
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate %s", url)
            return False

        self.logger.info("current listing URL %s", url)
        html = await client.fetch(url, request_kind="detail")
        if not self._run_can_continue(client) or not html:
            return False

        try:
            car = parse_listing_page(html, url)
        except Exception:
            self.logger.exception("parsing error for %s", url)
            return False

        if not matches_target_model(car, target):
            self.logger.info("skipped because not target model: %s for target %s %s", url, target["brand"], target["model"])
            return False

        if not self._has_minimum_public_data(car):
            self.logger.warning("skipped incomplete listing %s", url)
            return False

        if self.db.car_exists(car.get("listing_id"), car.get("url")):
            self.logger.info("skipped duplicate %s", url)
            return False

        current_count = self.db.get_target_current_count(target["brand"], target["model"])
        if current_count >= int(target["limit"]):
            self.logger.info("skipped because target limit reached: %s %s", target["brand"], target["model"])
            return False

        saved = self.db.insert_car(car)
        if saved:
            self.logger.info(
                "saved listing %s for target %s %s; current target progress: %s/%s; current DB count: %s",
                url,
                target["brand"],
                target["model"],
                current_count + 1,
                target["limit"],
                self.db.count_all_cars(),
            )
            return True

        self.logger.info("skipped duplicate by SQLite constraint %s", url)
        return False

    async def _fetch_parse_save_brand(self, client: KolesaHTTPClient, url: str, target: dict) -> str:
        brand = target["brand"]
        listing_id = extract_listing_id(url)
        if self.db.car_exists(listing_id, url):
            self.logger.info("skipped duplicate before detail request %s", url)
            return "duplicate"

        self.logger.info("current brand %s; current listing URL %s", brand, url)
        html = await client.fetch(url, request_kind="detail")
        if not self._run_can_continue(client) or not html:
            return "skipped"

        try:
            car = parse_listing_page(html, url)
        except Exception:
            self.logger.exception("parsing error for %s", url)
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
        self.logger.info(
            "max runtime hours: %s",
            self.max_runtime_seconds / 3600 if self.max_runtime_seconds is not None else None,
        )
        self._print_current_db_count(force=True)

    def _should_continue(self, target_total: int, client: KolesaHTTPClient) -> bool:
        if not self._run_can_continue(client):
            return False
        if self.db.count_all_cars() >= target_total:
            return False
        return True

    def _run_can_continue(self, client: KolesaHTTPClient) -> bool:
        if self.stop_requested or client.stop_requested:
            return False
        if self.max_runtime_seconds is not None and self.started_at is not None:
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
