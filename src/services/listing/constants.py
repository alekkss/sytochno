"""Константы и вспомогательные функции модуля парсинга карточек объявлений."""

import asyncio

from src.config.logger import get_logger

logger = get_logger("listing")

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

# ── Дополнительные константы для совместимости ──

SUTOCHNO_BASE_URL: str = "https://sutochno.ru"

LISTING_URL_TEMPLATE: str = f"{SUTOCHNO_BASE_URL}/{{object_id}}"

MAX_TOKEN_RETRIES: int = 3

MAX_TABS: int = 5

TAB_DELAY_SECONDS: float = 1.0


# ── Вспомогательные функции ──


def format_duration(seconds: float) -> str:
    """Форматирует длительность в секундах в человекочитаемый вид.

    Args:
        seconds: Длительность в секундах.

    Returns:
        Строка вида «Xм Yс» или «Yс» если менее минуты.
    """
    total_seconds = int(seconds)
    minutes = total_seconds // 60
    secs = total_seconds % 60

    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


async def safe_stop_browser(browser_service: "any", worker_idx: int) -> None:  # type: ignore[name-defined]
    """Безопасно останавливает прокси-браузер с таймаутом.

    Args:
        browser_service: Экземпляр BrowserService для остановки.
        worker_idx: Номер воркера (для логов).
    """
    try:
        await asyncio.wait_for(
            browser_service.stop(),
            timeout=WORKER_STOP_TIMEOUT,
        )
        logger.info(
            "воркер_браузер_остановлен",
            step=f"воркер={worker_idx}",
        )
    except asyncio.TimeoutError:
        logger.warning(
            "воркер_браузер_таймаут_остановки",
            step=f"воркер={worker_idx}, лимит={WORKER_STOP_TIMEOUT}с",
        )
    except Exception as e:
        logger.warning(
            "воркер_ошибка_остановки_браузера",
            error=str(e),
            error_type=type(e).__name__,
            step=f"воркер={worker_idx}",
        )
