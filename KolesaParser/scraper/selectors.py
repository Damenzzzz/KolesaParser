"""CSS selectors used by the Kolesa parser.

Kolesa.kz can change markup over time. Keep selectors centralized here so the
parser can be repaired without touching the extraction logic.
"""

LISTING_LINK_SELECTORS = [
    'a[href*="/a/show/"]',
    '.a-card a[href*="/a/show/"]',
    '.a-list__item a[href*="/a/show/"]',
    '[data-test*="advert"] a[href*="/a/show/"]',
]

TITLE_SELECTORS = [
    '[data-test="offer-title"]',
    ".offer__title",
    "h1",
]

PRICE_SELECTORS = [
    '[data-test="offer-price"]',
    ".offer__price",
    '[itemprop="price"]',
]

PARAMETERS_CONTAINER_SELECTORS = [
    '[data-test="offer-parameters"]',
    ".offer__parameters",
]

PARAMETER_ROW_SELECTOR = '[data-test="offer-parameters"] dl, .offer__parameters dl'
PARAMETER_LABEL_SELECTOR = "dt"
PARAMETER_VALUE_SELECTOR = "dd"

DESCRIPTION_SELECTORS = [
    ".offer__description-seller:not(.is-loading)",
    ".offer__description-seller",
    ".js__description",
    '[data-test="description"]',
]

PHOTO_SELECTORS = [
    ".offer__gallery img",
    ".gallery img",
    ".gallery button",
    ".gallery__thumbs button",
    'picture img[src*="kolesa"]',
]

PUBLISHED_AT_SELECTORS = [
    ".offer__info-views",
    '[data-test="offer-date"]',
    ".offer__date",
]

SELLER_TYPE_SELECTORS = [
    '[data-test="seller-contacts-title"]',
    ".offer__contacts-title",
]

OPTION_SELECTORS = [
    ".offer__option-label",
    '[data-test="spec_block"] .offer__option-label',
]
