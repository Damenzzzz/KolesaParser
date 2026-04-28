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
BRAND_STATE_PATH = DATA_DIR / "brand_parser_state.json"

TOTAL_LIMIT = 50_000
MAX_PER_MODEL = 700
MAX_PER_BRAND = TOTAL_LIMIT // 10
DEFAULT_TEST_LIMIT = 100

HTTP_CONCURRENCY = 1
MIN_DELAY_SECONDS = 5.0
MAX_DELAY_SECONDS = 12.0
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 2
ERROR_STOP_THRESHOLD = 5
RETRY_BACKOFF_SECONDS = (20.0, 180.0)
FAST_SKIP_PAGE_DELAY_SECONDS = (2.0, 6.0)
BLOCK_STOP_MESSAGE = (
    "Possible temporary rate limit or block. Stopped safely. "
    "Try later with safe-mode, balanced-mode, or night-mode."
)
VISIBLE_CHALLENGE_STOP_MESSAGE = "Captcha or visible challenge detected. Stop parser and retry later."

DEFAULT_HEADLESS = True
DESCRIPTION_WAIT_MS = 5_000
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
        detail_delay_seconds=(8.0, 20.0),
        search_delay_seconds=(45.0, 120.0),
        max_consecutive_errors=3,
    ),
    "balanced": CrawlModeSettings(
        name="balanced",
        detail_delay_seconds=(3.0, 7.0),
        search_delay_seconds=(8.0, 18.0),
        max_consecutive_errors=3,
        short_pause_every=100,
        short_pause_seconds=(120.0, 300.0),
        long_pause_every=500,
        long_pause_seconds=(480.0, 900.0),
    ),
    "night": CrawlModeSettings(
        name="night",
        detail_delay_seconds=(15.0, 35.0),
        search_delay_seconds=(120.0, 300.0),
        max_consecutive_errors=3,
        short_pause_every=50,
        short_pause_seconds=(300.0, 900.0),
        long_pause_every=200,
        long_pause_seconds=(1200.0, 2700.0),
    ),
}


def get_crawl_mode_settings(mode: str) -> CrawlModeSettings:
    return CRAWL_MODE_SETTINGS.get(mode, CRAWL_MODE_SETTINGS["normal"])


TARGET_MODELS = [
    {"brand": "Toyota", "model": "Camry", "limit": 1200, "aliases": ["Camry", "\u041a\u0430\u043c\u0440\u0438"]},
    {"brand": "Toyota", "model": "Corolla", "limit": 1000, "aliases": ["Corolla", "\u041a\u043e\u0440\u043e\u043b\u043b\u0430"]},
    {"brand": "Toyota", "model": "RAV4", "limit": 1000, "aliases": ["RAV4", "Rav 4", "RAV 4", "\u0420\u0410\u04124"]},
    {"brand": "Toyota", "model": "Prado", "limit": 900, "aliases": ["Prado", "Land Cruiser Prado", "LC Prado"]},
    {"brand": "Hyundai", "model": "Tucson", "limit": 1000, "aliases": ["Tucson", "\u0422\u0443\u0441\u0441\u0430\u043d"]},
    {"brand": "Hyundai", "model": "Elantra", "limit": 900, "aliases": ["Elantra", "\u042d\u043b\u0430\u043d\u0442\u0440\u0430"]},
    {"brand": "Hyundai", "model": "Sonata", "limit": 1000, "aliases": ["Sonata", "\u0421\u043e\u043d\u0430\u0442\u0430"]},
    {"brand": "Kia", "model": "Sportage", "limit": 1000, "aliases": ["Sportage", "\u0421\u043f\u043e\u0440\u0442\u0435\u0439\u0434\u0436"]},
    {"brand": "Kia", "model": "K5", "limit": 1000, "aliases": ["K5"]},
    {"brand": "Kia", "model": "Rio", "limit": 900, "aliases": ["Rio", "\u0420\u0438\u043e"]},
    {"brand": "Lexus", "model": "RX", "limit": 1000, "aliases": ["RX", "RX-Series", "RX Series"]},
    {"brand": "Lexus", "model": "ES", "limit": 800, "aliases": ["ES", "ES-Series", "ES Series"]},
    {"brand": "Lexus", "model": "LX", "limit": 700, "aliases": ["LX", "LX-Series", "LX Series"]},
    {"brand": "BMW", "model": "X5", "limit": 1000, "aliases": ["X5"]},
    {"brand": "Toyota", "model": "Land Cruiser 150", "limit": 700, "aliases": ["Land Cruiser 150", "LC 150"]},
    {
        "brand": "BMW",
        "model": "5-Series",
        "limit": 900,
        "aliases": ["5-Series", "5 Series", "5 \u0441\u0435\u0440\u0438\u044f", "520", "523", "525", "528", "530", "535", "540"],
    },
    {
        "brand": "BMW",
        "model": "3-Series",
        "limit": 700,
        "aliases": ["3-Series", "3 Series", "3 \u0441\u0435\u0440\u0438\u044f", "316", "318", "320", "325", "328", "330"],
    },
]


BRAND_TARGETS = [
    {
        "brand": "Toyota",
        "url": "https://kolesa.kz/cars/toyota/",
        "limit": 3500,
        "aliases": ["Toyota", "toyota", "\u0422\u043e\u0439\u043e\u0442\u0430"],
    },
    {"brand": "BMW", "url": "https://kolesa.kz/cars/bmw/", "limit": 2000},
    {"brand": "Hyundai", "url": "https://kolesa.kz/cars/hyundai/", "limit": 2000},
    {"brand": "Kia", "url": "https://kolesa.kz/cars/kia/", "limit": 2000},
    {"brand": "Lexus", "url": "https://kolesa.kz/cars/lexus/", "limit": 2000},
    {
        "brand": "Mercedes-Benz",
        "url": "https://kolesa.kz/cars/mercedes-benz/",
        "limit": 2000,
        "aliases": ["Mercedes", "Mercedes-Benz", "Mercedes Benz", "\u041c\u0435\u0440\u0441\u0435\u0434\u0435\u0441"],
    },
]
