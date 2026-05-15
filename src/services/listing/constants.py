"""Константы модуля парсинга карточек объявлений."""

# URL внутреннего API для получения цен и занятости
API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Количество дней в одном пакетном запросе к API
API_BATCH_SIZE: int = 5

# Пауза между пакетами запросов (секунды) — защита от rate-limit
API_BATCH_DELAY: float = 0.5

# Максимальное количество попыток загрузки страницы карточки
MAX_GOTO_RETRIES: int = 3

# Пауза между повторными попытками загрузки (секунды)
GOTO_RETRY_DELAY: float = 5.0

# Таймаут остановки одного прокси-браузера (секунды)
WORKER_STOP_TIMEOUT: float = 15.0

# Таймаут мягкого ожидания networkidle (мс)
NETWORKIDLE_SOFT_TIMEOUT_MS: int = 10000

# Селекторы, подтверждающие что карточка загрузилась
PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]

# Таймаут ожидания готовности страницы (мс)
PAGE_READY_TIMEOUT_MS: int = 15000

# Количество гостей по умолчанию (используется в API-запросе)
DEFAULT_GUESTS: int = 2

# Максимальное количество retry при полном провале
MAX_API_RETRIES: int = 2

# Пауза перед повторной попыткой после перезагрузки страницы (секунды)
RELOAD_WAIT_SECONDS: float = 10.0

# Варианты min_nights для адаптивного запроса (по возрастанию).
# Расширен до 30 — встречаются объекты с min_nights=10, 14, 30.
MIN_NIGHTS_VARIANTS: list[int] = [2, 3, 4, 5, 6, 7, 10, 14, 30]

# Количество дней для анализа
DAYS_COUNT: int = 60

# Порог ошибок, после которого скользящее окно считается провалившимся
ERROR_THRESHOLD: int = 30

# Ключевые слова в ответе API, указывающие на ограничение min_nights
MIN_NIGHTS_ERROR_KEYWORDS: list[str] = [
    "min_nights",
    "minimum_nights",
    "минимальный срок",
    "минимальное количество суток",
    "минимальное количество",
    "минимум",
    "суток",
    "сут.",
    "nights_min",
    "min_duration",
    "minimalnoe_kolichestvo",
    "minimum_stay",
    "min_stay",
]

# Типы записей detail[], содержащие базовую цену за сутки.
# API возвращает type="season_price" (сезонные цены с диапазонами дат)
# или type=1 (числовой тип — единая базовая цена без дат).
# Типы "interval" (скидки за длительность), "dop_persons" (доплата за гостей),
# "sale" (акции) НЕ являются базовой ценой и игнорируются.
BASE_PRICE_TYPE_INT: int = 1
SEASON_PRICE_TYPE: str = "season_price"

# ── Добавлено для совместимости с listing_parser.py и listing_service.py ──

SUTOCHNO_BASE_URL: str = "https://sutochno.ru"

LISTING_URL_TEMPLATE: str = f"{SUTOCHNO_BASE_URL}/{{object_id}}"

MAX_TOKEN_RETRIES: int = 3

MAX_TABS: int = 5

TAB_DELAY_SECONDS: float = 1.0