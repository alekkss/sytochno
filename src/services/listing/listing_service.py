"""Сервис парсинга карточки объявления — оркестратор.

Делегирует всю логику модулям src/services/listing/:
- PageLoader — загрузка страницы и перехват токена.
- TokenManager — валидация и перезагрузка токена.
- ApiClient — низкоуровневые запросы к API.
- HybridStrategy — гибридная стратегия (bulk + скользящее окно).
- EnrichStrategies — параллельная обработка через вкладки и прокси.
"""

import asyncio
import time
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.services.browser_service import BrowserService
from src.services.listing.api_client import ApiClient
from src.services.listing.connection_monitor import ConnectionMonitor
from src.services.listing.constants import DAYS_COUNT, DEFAULT_GUESTS, format_duration
from src.services.listing.enrich_strategies import EnrichStrategies
from src.services.listing.hybrid_strategy import HybridStrategy
from src.services.listing.page_loader import PageLoader
from src.services.listing.price_parser import PriceParser
from src.services.listing.token_manager import TokenManager

if TYPE_CHECKING:
    from src.models.proxy import ProxyConfig
    from src.services.proxy_service import ProxyService

logger = get_logger("listing")

# Максимальное количество попыток обогащения одной карточки
_MAX_ENRICH_ATTEMPTS = 3

# Пауза перед повторной попыткой (секунды)
_RETRY_DELAY_SECONDS = 3.0


class ListingService:
    """Оркестратор обогащения карточек объявлений данными о ценах и занятости.

    Публичный API полностью сохранён для обратной совместимости:
    - enrich_listing(listing, page=None)
    - enrich_listings(listings)
    - enrich_listings_tabbed(listings)
    - enrich_listings_parallel(settings, listings, proxies, proxy_service) — статический
    """

    def __init__(
        self,
        settings: Settings,
        browser_service: BrowserService,
        monitor: ConnectionMonitor | None = None,
        proxy_service: "ProxyService | None" = None,
    ) -> None:
        """Инициализирует сервис и все вложенные компоненты.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
            monitor: Монитор здоровья соединения (опциональный).
                Если передан — используется для раннего обнаружения
                массовых сбоев и остановки бесполезных попыток.
            proxy_service: Сервис прокси с заполненным пулом (опциональный).
                Передаётся в EnrichStrategies для проверки/замены прокси
                при перезапуске браузера.
        """
        self._settings = settings
        self._browser = browser_service
        self._monitor = monitor
        self._proxy_service = proxy_service

        self._page_loader = PageLoader(monitor=monitor)
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
            proxy_service=self._proxy_service,
        )

    @property
    def monitor(self) -> ConnectionMonitor | None:
        """Возвращает текущий монитор соединения.

        Returns:
            Экземпляр ConnectionMonitor или None.
        """
        return self._monitor

    @monitor.setter
    def monitor(self, value: ConnectionMonitor | None) -> None:
        """Устанавливает монитор соединения.

        Обновляет монитор как в сервисе, так и в PageLoader.

        Args:
            value: Новый монитор или None для отключения.
        """
        self._monitor = value
        self._page_loader.monitor = value

    async def enrich_listing(
        self, listing: RawListing, page: Page | None = None
    ) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Выполняет до _MAX_ENRICH_ATTEMPTS попыток. Повторная попытка
        запускается при трёх сценариях сбоя:
        - страница не загрузилась;
        - токен API не перехвачен (частая проблема при параллельных вкладках);
        - hybrid_strategy вернула нулевой sentinel ([0]*60, [0]*60).

        Если стратегия вернула skip_reason (фатальная ошибка) — повторные
        попытки не запускаются, карточка сразу помечается как необогащаемая.

        Если монитор соединения сигнализирует о необходимости перезапуска
        браузера — обработка прерывается досрочно без траты попыток.

        Нулевой sentinel отличается от реально свободного объявления тем,
        что у свободного объявления цены > 0, а у sentinel все цены = 0.

        asyncio.CancelledError намеренно пробрасывается выше — это штатная
        отмена задачи, а не сбой обработки. Перед пробросом фиксируется лог,
        чтобы пустая карточка не оставалась без следов в логах.

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

        for attempt in range(1, _MAX_ENRICH_ATTEMPTS + 1):
            # Проверяем монитор перед каждой попыткой — если перезапуск
            # браузера уже требуется, не тратим время на бесполезные загрузки
            if self._monitor and self._monitor.should_skip():
                logger.debug(
                    "карточка_пропущена_перезапуск_требуется",
                    step=f"id={listing.external_id}, попытка={attempt}",
                )
                break

            try:
                # Повторные попытки: пауза перед загрузкой страницы
                if attempt > 1:
                    logger.info(
                        "повтор_карточки",
                        step=f"id={listing.external_id}, попытка={attempt}/{_MAX_ENRICH_ATTEMPTS}",
                    )
                    await asyncio.sleep(_RETRY_DELAY_SECONDS)

                loaded, token = await self._page_loader.goto_and_capture_token(
                    active_page, listing.url, object_id=listing.external_id
                )

                if not loaded:
                    logger.warning(
                        "страница_не_загрузилась",
                        step=f"id={listing.external_id}, попытка={attempt}",
                    )
                    # Если монитор сработал из-за этого сбоя — прерываем сразу
                    if self._monitor and self._monitor.should_skip():
                        logger.debug(
                            "прерывание_после_сбоя_загрузки",
                            step=f"id={listing.external_id}",
                        )
                        break
                    continue

                if not token:
                    logger.warning(
                        "токен_не_получен_повтор",
                        step=f"id={listing.external_id}, попытка={attempt}/{_MAX_ENRICH_ATTEMPTS}",
                    )
                    continue

                await self._browser.random_delay()

                calendar, prices, skip_reason = await self._strategy.fetch_calendar_and_prices(
                    active_page, listing.external_id, token, listing.url
                )

                # ── Фатальная ошибка — повторные попытки бессмысленны ──
                if skip_reason is not None:
                    listing.enrichment_skip_reason = skip_reason
                    logger.info(
                        "карточка_необогащаема",
                        step=f"id={listing.external_id}, причина={skip_reason}",
                    )
                    break

                # Проверяем: не получили ли мы нулевой sentinel вместо данных.
                # Реально свободное объявление (busy=unbusy) имеет prices > 0.
                # Нулевой sentinel возвращается при полном провале стратегии.
                if self._is_failure_sentinel(calendar, prices):
                    logger.warning(
                        "нулевой_результат_повтор",
                        step=f"id={listing.external_id}, попытка={attempt}/{_MAX_ENRICH_ATTEMPTS}",
                    )
                    continue

                # Успех — сохраняем результат и выходим из цикла
                listing.calendar_60_days = calendar
                listing.prices_60_days = prices

                logger.info(
                    "карточка_обогащена",
                    step=f"id={listing.external_id}",
                    total=f"свободных={sum(1 for c in calendar if c == 0)}, "
                          f"занятых={sum(1 for c in calendar if c == 1)}, "
                          f"цен={sum(1 for p in prices if p > 0)}"
                          + (f", попытка={attempt}" if attempt > 1 else ""),
                )
                break

            except asyncio.CancelledError:
                # CancelledError — штатная отмена задачи (например, при сбое
                # другой вкладки в asyncio.gather). Логируем факт отмены,
                # чтобы пустая карточка не оставалась без следов, и пробрасываем
                # выше — подавлять отмену нельзя.
                logger.warning(
                    "карточка_отменена",
                    step=f"id={listing.external_id}, попытка={attempt}",
                )
                raise

            except Exception as e:
                logger.warning(
                    "ошибка_парсинга_карточки",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"id={listing.external_id}, попытка={attempt}",
                )
                # Исключение — пробуем ещё раз если есть попытки
                if attempt < _MAX_ENRICH_ATTEMPTS:
                    continue

        else:
            # Все попытки исчерпаны — карточка остаётся пустой
            logger.warning(
                "карточка_не_обогащена_все_попытки_исчерпаны",
                step=f"id={listing.external_id}, попыток={_MAX_ENRICH_ATTEMPTS}",
            )

        elapsed = time.perf_counter() - start_time
        logger.info(
            "карточка_завершена",
            step=f"id={listing.external_id}",
            total=f"{format_duration(elapsed)}",
        )

        return listing

    @staticmethod
    def _is_failure_sentinel(calendar: list[int], prices: list[int]) -> bool:
        """Определяет, является ли результат нулевым sentinel'ом сбоя.

        HybridStrategy возвращает [0]*60, [0]*60 при полном провале.
        Реально свободное объявление (busy=unbusy) всегда имеет prices > 0.
        Реально занятое объявление (busy=busy) имеет calendar с единицами.

        Args:
            calendar: Список занятости на 60 дней.
            prices: Список цен на 60 дней.

        Returns:
            True если результат является sentinel'ом сбоя.
        """
        if len(calendar) != DAYS_COUNT or len(prices) != DAYS_COUNT:
            return True

        # Все нули в обоих списках — признак сбоя, а не реальных данных
        all_calendar_zero = all(c == 0 for c in calendar)
        all_prices_zero = all(p == 0 for p in prices)

        return all_calendar_zero and all_prices_zero

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
        proxy_service: "ProxyService | None" = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.
            proxy_service: Сервис прокси с заполненным пулом (опциональный).
                Передаётся в воркеры для проверки/замены при перезапуске.

        Returns:
            Список обогащённых карточек.
        """
        return await EnrichStrategies.enrich_listings_parallel(
            settings=settings,
            listings=listings,
            proxies=proxies,
            proxy_service=proxy_service,
        )
