"""Сервис парсинга карточки объявления — оркестратор.

Делегирует всю логику модулям src/services/listing/:
- PageLoader — загрузка страницы и перехват токена.
- TokenManager — валидация и перезагрузка токена.
- ApiClient — низкоуровневые запросы к API.
- HybridStrategy — гибридная стратегия (bulk + скользящее окно).
- EnrichStrategies — параллельная обработка через вкладки и прокси.
"""

import time
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.services.browser_service import BrowserService
from src.services.listing.api_client import ApiClient
from src.services.listing.constants import DAYS_COUNT, DEFAULT_GUESTS, format_duration
from src.services.listing.enrich_strategies import EnrichStrategies
from src.services.listing.hybrid_strategy import HybridStrategy
from src.services.listing.page_loader import PageLoader
from src.services.listing.price_parser import PriceParser
from src.services.listing.token_manager import TokenManager

if TYPE_CHECKING:
    from src.models.proxy import ProxyConfig

logger = get_logger("listing")


class ListingService:
    """Оркестратор обогащения карточек объявлений данными о ценах и занятости.

    Публичный API полностью сохранён для обратной совместимости:
    - enrich_listing(listing, page=None)
    - enrich_listings(listings)
    - enrich_listings_tabbed(listings)
    - enrich_listings_parallel(settings, listings, proxies) — статический
    """

    def __init__(self, settings: Settings, browser_service: BrowserService) -> None:
        """Инициализирует сервис и все вложенные компоненты.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
        """
        self._settings = settings
        self._browser = browser_service

        self._page_loader = PageLoader()
        self._token_manager = TokenManager(
            page_loader=self._page_loader,
            browser_service=self._browser,
        )
        self._api_client = ApiClient(price_parser=PriceParser())
        self._strategy = HybridStrategy(
            api_client=self._api_client,
            token_manager=self._token_manager,
            guests=DEFAULT_GUESTS,
        )
        self._enrich_strategies = EnrichStrategies(
            listing_service=self,
            browser_service=self._browser,
            settings=self._settings,
        )

    async def enrich_listing(
        self, listing: RawListing, page: Page | None = None
    ) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Args:
            listing: Объявление с базовыми данными из каталога.
            page: Вкладка для работы. Если None — используется основная страница браузера.

        Returns:
            Объявление с заполненными calendar_60_days и prices_60_days.
        """
        active_page = page if page is not None else self._browser.page
        start_time = time.perf_counter()

        logger.info(
            "парсинг_карточки",
            path=listing.url,
            step=f"id={listing.external_id}",
        )

        try:
            loaded, token = await self._page_loader.goto_and_capture_token(
                active_page, listing.url
            )

            if not loaded:
                logger.warning(
                    "страница_не_загрузилась",
                    step=f"id={listing.external_id}",
                )
                return listing

            if not token:
                logger.warning(
                    "токен_не_получен_пропуск_карточки",
                    step=f"id={listing.external_id}",
                )
                return listing

            await self._browser.random_delay()

            calendar, prices = await self._strategy.fetch_calendar_and_prices(
                active_page, listing.external_id, token, listing.url
            )

            listing.calendar_60_days = calendar
            listing.prices_60_days = prices

            logger.info(
                "карточка_обогащена",
                step=f"id={listing.external_id}",
                total=f"свободных={sum(1 for c in calendar if c == 0)}, "
                      f"занятых={sum(1 for c in calendar if c == 1)}, "
                      f"цен={sum(1 for p in prices if p > 0)}",
            )

        except Exception as e:
            logger.warning(
                "ошибка_парсинга_карточки",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}",
            )

        elapsed = time.perf_counter() - start_time
        logger.info(
            "карточка_завершена",
            step=f"id={listing.external_id}",
            total=f"{format_duration(elapsed)}",
        )

        return listing

    async def enrich_listings(self, listings: list[RawListing]) -> list[RawListing]:
        """Обогащает список объявлений последовательно.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days и prices_60_days.
        """
        total = len(listings)
        for idx, listing in enumerate(listings, start=1):
            logger.info(
                "обработка_карточки",
                current=idx,
                total=total,
            )
            await self.enrich_listing(listing)
            await self._browser.random_delay()

        return listings

    async def enrich_listings_tabbed(
        self, listings: list[RawListing]
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько вкладок.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days и prices_60_days.
        """
        return await self._enrich_strategies.enrich_listings_tabbed(listings)

    @staticmethod
    async def enrich_listings_parallel(
        settings: Settings,
        listings: list[RawListing],
        proxies: list["ProxyConfig"],
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.

        Returns:
            Список обогащённых карточек.
        """
        return await EnrichStrategies.enrich_listings_parallel(
            settings=settings,
            listings=listings,
            proxies=proxies,
        )
