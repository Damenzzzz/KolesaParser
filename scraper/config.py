from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BASE_URL = "https://kolesa.kz"
START_URL = f"{BASE_URL}/cars/"
SEARCH_URL = START_URL
SOURCE_NAME = "kolesa.kz"
DEFAULT_ENGINE = "http"

DATA_DIR = PROJECT_ROOT / "data"
EXPORTS_DIR = DATA_DIR / "exports"
LOGS_DIR = PROJECT_ROOT / "logs"
DB_PATH = DATA_DIR / "cars.db"
LOG_FILE = LOGS_DIR / "parser.log"

TOTAL_LIMIT = 50000
MAX_PER_MODEL = 700
MAX_PER_BRAND = 5000
DEFAULT_TEST_LIMIT = 100

HTTP_CONCURRENCY = 1
MIN_DELAY_SECONDS = 5.0
MAX_DELAY_SECONDS = 12.0
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 2
ERROR_STOP_THRESHOLD = 5
RETRY_BACKOFF_SECONDS = (20.0, 180.0)
BLOCK_STOP_MESSAGE = (
    "Possible temporary rate limit or block. Stopped safely. "
    "Try later with safe-mode or night-mode."
)

DEFAULT_HEADLESS = True
DESCRIPTION_WAIT_MS = 5000
PLAYWRIGHT_TIMEOUT_MS = 30000

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
ACCEPT_LANGUAGE = "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7"


@dataclass(frozen=True)
class CrawlModeSettings:
    name: str
    detail_delay_seconds: tuple[float, float]
    search_delay_seconds: tuple[float, float]
    max_consecutive_errors: int
    short_pause_every: int | None = None
    short_pause_seconds: tuple[float, float] | None = None
    long_pause_every: int | None = None
    long_pause_seconds: tuple[float, float] | None = None


CRAWL_MODE_SETTINGS = {
    "normal": CrawlModeSettings(
        name="normal",
        detail_delay_seconds=(MIN_DELAY_SECONDS, MAX_DELAY_SECONDS),
        search_delay_seconds=(15.0, 45.0),
        max_consecutive_errors=ERROR_STOP_THRESHOLD,
    ),
    "safe": CrawlModeSettings(
        name="safe",
        detail_delay_seconds=(5.0, 12.0),
        search_delay_seconds=(20.0, 45.0),
        max_consecutive_errors=3,
    ),
    "balanced": CrawlModeSettings(
        name="balanced",
        detail_delay_seconds=(3.0, 7.0),
        search_delay_seconds=(20.0, 45.0),
        max_consecutive_errors=3,
        short_pause_every=100,
        short_pause_seconds=(120.0, 300.0),
        long_pause_every=500,
        long_pause_seconds=(480.0, 900.0),
    ),
    "night": CrawlModeSettings(
        name="night",
        detail_delay_seconds=(8.0, 18.0),
        search_delay_seconds=(45.0, 90.0),
        max_consecutive_errors=3,
        short_pause_every=100,
        short_pause_seconds=(180.0, 480.0),
        long_pause_every=500,
        long_pause_seconds=(600.0, 1200.0),
    ),
}


def get_crawl_mode_settings(mode: str) -> CrawlModeSettings:
    return CRAWL_MODE_SETTINGS.get(mode, CRAWL_MODE_SETTINGS["normal"])
