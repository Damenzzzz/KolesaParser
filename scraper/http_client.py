import asyncio
import logging
import random
from typing import Literal, Optional

import httpx

from scraper.config import (
    ACCEPT_LANGUAGE,
    BLOCK_STOP_MESSAGE,
    HTTP_CONCURRENCY,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_SECONDS,
    USER_AGENT,
    CrawlModeSettings,
    get_crawl_mode_settings,
)
from scraper.utils import is_blocked_response, looks_like_normal_listing_title


RequestKind = Literal["search", "detail"]

RETRYABLE_STATUS_CODES = {500, 502, 503, 504}


class KolesaHTTPClient:
    """Conservative HTTP client with bounded concurrency and slow retries."""

    def __init__(
        self,
        concurrency: int = HTTP_CONCURRENCY,
        mode: str = "normal",
        max_retries: int = MAX_RETRIES,
        stop_on_block: bool = True,
        settings: CrawlModeSettings | None = None,
    ) -> None:
        self.settings = settings or get_crawl_mode_settings(mode)
        self.mode = self.settings.name
        self.concurrency = max(1, concurrency)
        self.max_retries = max(0, max_retries)
        self.stop_on_block = stop_on_block
        self.max_consecutive_errors = self.settings.max_consecutive_errors
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.consecutive_errors = 0
        self.stop_requested = False
        self.stop_reason: Optional[str] = None
        self.client: Optional[httpx.AsyncClient] = None
        self.logger = logging.getLogger("kolesa_http_client")

    async def __aenter__(self) -> "KolesaHTTPClient":
        headers = {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": ACCEPT_LANGUAGE,
            "Upgrade-Insecure-Requests": "1",
        }
        self.client = httpx.AsyncClient(
            headers=headers,
            timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
            follow_redirects=True,
            limits=httpx.Limits(
                max_connections=self.concurrency,
                max_keepalive_connections=self.concurrency,
            ),
        )
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self.client:
            await self.client.aclose()

    async def fetch(
        self,
        url: str,
        request_kind: RequestKind = "detail",
        skip_delay: bool = False,
    ) -> Optional[str]:
        if self.stop_requested:
            return None
        if not self.client:
            raise RuntimeError("KolesaHTTPClient must be used as an async context manager")

        async with self.semaphore:
            for retry_index in range(self.max_retries + 1):
                if self.stop_requested:
                    return None

                if skip_delay and retry_index == 0:
                    self._log_skipped_delay(request_kind, url)
                else:
                    await self._delay_before_request(request_kind, url)

                try:
                    response = await self.client.get(url)
                except httpx.TimeoutException as exc:
                    self._register_network_error(url, exc)
                    if not await self._sleep_before_retry(url, retry_index):
                        return None
                    continue
                except httpx.HTTPError as exc:
                    self._register_network_error(url, exc)
                    if not await self._sleep_before_retry(url, retry_index):
                        return None
                    continue

                status_code = response.status_code
                snippet = self._response_snippet(response)
                self.logger.info("HTTP status code %s for %s", status_code, url)

                blocked, block_reason = is_blocked_response(status_code, response.text)
                if blocked:
                    self._register_block_response(
                        url=url,
                        status_code=status_code,
                        snippet=snippet,
                        reason=block_reason,
                    )
                    return None

                if status_code == 200:
                    if looks_like_normal_listing_title(response.text):
                        self.logger.info("normal listing page detected")
                    self.consecutive_errors = 0
                    return response.text

                if status_code in RETRYABLE_STATUS_CODES:
                    self._register_response_error(
                        url=url,
                        status_code=status_code,
                        snippet=snippet,
                    )
                    if not await self._sleep_before_retry(url, retry_index):
                        return None
                    continue

                self.logger.warning(
                    "non-retryable status code %s for %s; response snippet=%r",
                    status_code,
                    url,
                    snippet,
                )
                return None

        return None

    async def _delay_before_request(self, request_kind: RequestKind, url: str) -> None:
        delay_range = (
            self.settings.search_delay_seconds
            if request_kind == "search"
            else self.settings.detail_delay_seconds
        )
        delay = random.uniform(*delay_range)
        self.logger.info(
            "pause duration %.1fs before %s request: %s",
            delay,
            request_kind,
            url,
        )
        await asyncio.sleep(delay)

    def _log_skipped_delay(self, request_kind: RequestKind, url: str) -> None:
        self.logger.info(
            "pause duration 0.0s before %s request: %s (first search request)",
            request_kind,
            url,
        )

    async def _sleep_before_retry(self, url: str, retry_index: int) -> bool:
        if self.stop_requested or retry_index >= self.max_retries:
            return False

        retry_number = retry_index + 1
        if retry_index < len(RETRY_BACKOFF_SECONDS):
            backoff = RETRY_BACKOFF_SECONDS[retry_index]
        else:
            backoff = RETRY_BACKOFF_SECONDS[-1] * (2 ** (retry_index - len(RETRY_BACKOFF_SECONDS) + 1))

        self.logger.warning(
            "retry number %s for %s; backoff duration %.1fs",
            retry_number,
            url,
            backoff,
        )
        await asyncio.sleep(backoff)
        return not self.stop_requested

    def _register_block_response(
        self,
        url: str,
        status_code: int,
        snippet: str,
        reason: str,
    ) -> None:
        self.consecutive_errors += 1
        self.logger.error(
            "possible block detected; url=%s status=%s reason=%s response snippet=%r consecutive_errors=%s/%s",
            url,
            status_code,
            reason,
            snippet,
            self.consecutive_errors,
            self.max_consecutive_errors,
        )
        if self.stop_on_block:
            self._request_stop(reason)
        elif self.consecutive_errors >= self.max_consecutive_errors:
            self._request_stop("too many consecutive HTTP errors")

    def _register_response_error(self, url: str, status_code: int, snippet: str) -> None:
        self.consecutive_errors += 1
        self.logger.warning(
            "retryable HTTP status; url=%s status=%s response snippet=%r consecutive_errors=%s/%s",
            url,
            status_code,
            snippet,
            self.consecutive_errors,
            self.max_consecutive_errors,
        )
        if self.consecutive_errors >= self.max_consecutive_errors:
            self._request_stop("too many consecutive HTTP errors")

    def _register_network_error(self, url: str, exc: httpx.HTTPError) -> None:
        self.consecutive_errors += 1
        self.logger.warning(
            "network error; url=%s exception_class=%s error=%s consecutive_errors=%s/%s",
            url,
            exc.__class__.__name__,
            exc,
            self.consecutive_errors,
            self.max_consecutive_errors,
        )
        if self.consecutive_errors >= self.max_consecutive_errors:
            self._request_stop("too many consecutive network errors or timeouts")

    def _request_stop(self, reason: str) -> None:
        self.stop_requested = True
        self.stop_reason = reason
        self.logger.error("%s Reason: %s", BLOCK_STOP_MESSAGE, reason)

    def _response_snippet(self, response: httpx.Response) -> str:
        try:
            return " ".join(response.text[:500].split())
        except Exception as exc:
            self.logger.warning(
                "could not read response snippet; status=%s exception_class=%s",
                response.status_code,
                exc.__class__.__name__,
            )
            return ""
