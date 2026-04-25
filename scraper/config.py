from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BASE_URL = "https://kolesa.kz"
SEARCH_URL = f"{BASE_URL}/cars/"
SOURCE_NAME = "kolesa.kz"

DATA_DIR = PROJECT_ROOT / "data"
EXPORTS_DIR = DATA_DIR / "exports"
LOGS_DIR = PROJECT_ROOT / "logs"
DB_PATH = DATA_DIR / "cars.db"
LOG_FILE = LOGS_DIR / "parser.log"

TOTAL_LIMIT = 50000
MAX_PER_MODEL = 700
MAX_PER_BRAND = 5000
DEFAULT_TEST_LIMIT = 20
MIN_DELAY_SECONDS = 3
MAX_DELAY_SECONDS = 8

DEFAULT_HEADLESS = True
DEFAULT_TIMEOUT_MS = 30000
DESCRIPTION_WAIT_MS = 5000
MAX_RETRIES = 2

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
