import asyncio
import logging
import random
from typing import Optional

import httpx

from scraper.config import (
    ACCEPT_LANGUAGE,
    ERROR_STOP_THRESHOLD,
    HTTP_CONCURRENCY,
    MAX_DELAY_SECONDS,
    MAX_RETRIES,
    MIN_DELAY_SECONDS,
    REQUEST_TIMEOUT_SECONDS,
    USER_AGENT,
)
from scraper.utils import looks_like_captcha_or_block_page


RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}


class KolesaHTTPClient:
    """Small polite HTTP client with bounded concurrency and backoff."""

    def __init__(
        self,
        concurrency: int = HTTP_CONCURRENCY,
        min_delay: float = MIN_DELAY_SECONDS,
        max_delay: float = MAX_DELAY_SECONDS,
        max_retries: int = MAX_RETRIES,
        error_stop_threshold: int = ERROR_STOP_THRESHOLD,
    ) -> None:
        self.concurrency = max(1, concurrency)
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.error_stop_threshold = error_stop_threshold
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.consecutive_errors = 0
        self.stop_requested = False
        self.stop_reason: Optional[str] = None
        self.client: Optional[httpx.AsyncClient] = None
        self.logger = logging.getLogger("kolesa_http_client")

    async def __aenter__(self) -> "KolesaHTTPClient":
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": ACCEPT_LANGUAGE,
            "Cache-Control": "no-cache",
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client:
            await self.client.aclose()

    async def fetch(self, url: str) -> Optional[str]:
        if self.stop_requested:
            return None
        if not self.client:
            raise RuntimeError("KolesaHTTPClient must be used as an async context manager")

        async with self.semaphore:
            await asyncio.sleep(random.uniform(self.min_delay, self.max_delay))

            for attempt in range(1, self.max_retries + 1):
                if self.stop_requested:
                    return None

                try:
                    response = await self.client.get(url)
                except httpx.HTTPError as exc:
                    self._register_error(f"HTTP error for {url}: {exc}")
                    await self._backoff(attempt)
                    continue

                if response.status_code == 200:
                    html = response.text
                    if looks_like_captcha_or_block_page(html):
                        self._request_stop(f"captcha or block page detected at {url}")
                        return None
                    self.consecutive_errors = 0
                    return html

                if response.status_code in RETRYABLE_STATUS_CODES:
                    self._register_error(f"status {response.status_code} for {url}")
                    await self._backoff(attempt)
                    continue

                self.logger.warning("non-retryable status %s for %s", response.status_code, url)
                return None

            return None

    def _register_error(self, message: str) -> None:
        self.consecutive_errors += 1
        self.logger.warning("%s; consecutive errors=%s", message, self.consecutive_errors)
        if self.consecutive_errors >= self.error_stop_threshold:
            self._request_stop("too many consecutive HTTP errors")

    def _request_stop(self, reason: str) -> None:
        self.stop_requested = True
        self.stop_reason = reason
        self.logger.error("stopping HTTP collection safely: %s", reason)

    async def _backoff(self, attempt: int) -> None:
        delay = min(60.0, (2 ** attempt) + random.uniform(self.min_delay, self.max_delay))
        await asyncio.sleep(delay)
