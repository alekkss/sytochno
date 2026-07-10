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
from src.services.listing.concurrency_controller import ConcurrencyController
from src.services.listing.connection_monitor import ConnectionMonitor
from src.services.listing.constants import (
    DAYS_COUNT,
    DEFAULT_GUESTS,
    build_enrichment_url,
    format_duration,
)
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

# Минимальный остаток времени, при котором имеет смысл запускать
# тяжёлую операцию (fetch_calendar_and_prices). Если осталось меньше —
# лучше прервать сразу, чем запускать операцию, которая гарантированно
# не успеет завершиться.
_MIN_REMAINING_SECONDS = 10.0

# Фатальная причина: объявление удалено или заблокировано на сайте.
# API возвращает пустой ответ (no_objects). Поскольку парсер теперь
# загружает front/searchapp/detail/{id} напрямую (без редиректа),
# ложные срабатывания из-за сбоя редиректа исключены — если API
# ответил no_objects, объявление действительно удалено.
_OBJECT_NOT_FOUND_REASON = "object_not_found"

# Фатальная причина: невозможно получить сессионный токен API.
# Без токена никакие API-запросы невозможны — карточка полностью
# необрабатываема в этой сессии. Ошибка воспроизводится стабильно
# при перманентно битой странице или изменении механизма авторизации.
_PAGE_ELEMENTS_NOT_FOUND_REASON = "page_elements_not_found"


class ListingService:
    """Оркестратор обогащения карточек объявлений данными о ценах и занятости.

    Публичный API полностью сохранён для обратной совместимости:
    - enrich_listing(listing, page=None)
    - enrich_listings(listings)
    - enrich_listings_tabbed(listings)
    - enrich_listings_parallel(settings, listings, proxies, proxy_service, controller) — статический
    """

    def __init__(
        self,
        settings: Settings,
        browser_service: BrowserService,
        monitor: ConnectionMonitor | None = None,
        proxy_service: "ProxyService | None" = None,
        concurrency_controller: ConcurrencyController | None = None,
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
            concurrency_controller: Глобальный контроллер параллелизма
                (опциональный). Пробрасывается в PageLoader, HybridStrategy
                и EnrichStrategies для адаптивного управления нагрузкой.
        """
        self._settings = settings
        self._browser = browser_service
        self._monitor = monitor
        self._proxy_service = proxy_service
        self._controller = concurrency_controller

        # Таймаут обработки одной карточки (все попытки суммарно).
        # 0 = отключён (без ограничения времени).
        self._enrich_timeout_seconds: float = float(settings.enrich_timeout_seconds)

        # PageLoader получает navigation_timeout из settings — пользователь
        # управляет таймаутом навигации через NAVIGATION_TIMEOUT в .env.
        # Если settings.navigation_timeout = 60000 (дефолт), page.goto
        # будет ждать 60 секунд вместо прежних 30. Для медленных прокси
        # рекомендуется 45000–60000 мс.
        self._page_loader = PageLoader(
            monitor=monitor,
            concurrency_controller=concurrency_controller,
            navigation_timeout_ms=settings.navigation_timeout,
        )
        self._token_manager = TokenManager(
            page_loader=self._page_loader,
            browser_service=self._browser,
        )
        self._api_client = ApiClient(price_parser=PriceParser())
        self._strategy = HybridStrategy(
            api_client=self._api_client,
            token_manager=self._token_manager,
            guests=DEFAULT_GUESTS,
            concurrency_controller=concurrency_controller,
        )
        self._enrich_strategies = EnrichStrategies(
            listing_service=self,
            browser_service=self._browser,
            settings=self._settings,
            proxy_service=self._proxy_service,
            concurrency_controller=concurrency_controller,
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

    @property
    def concurrency_controller(self) -> ConcurrencyController | None:
        """Возвращает текущий контроллер параллелизма.

        Returns:
            Экземпляр ConcurrencyController или None.
        """
        return self._controller

    @concurrency_controller.setter
    def concurrency_controller(self, value: ConcurrencyController | None) -> None:
        """Устанавливает контроллер параллелизма.

        Обновляет контроллер во всех вложенных компонентах.

        Args:
            value: Новый контроллер или None для отключения.
        """
        self._controller = value
        self._page_loader.concurrency_controller = value
        self._strategy.concurrency_controller = value

    async def enrich_listing(
        self, listing: RawListing, page: Page | None = None
    ) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Загружает страницу карточки через прямой URL фронтенда
        (front/searchapp/detail/{id}), минуя редирект через публичный
        URL. Это исключает сбои редиректа, проблемы с региональными
        поддоменами (spb.sutochno.ru) и ускоряет навигацию.

        Выполняет до _MAX_ENRICH_ATTEMPTS попыток. Повторная попытка
        запускается при трёх сценариях сбоя:
        - страница не загрузилась (сетевая ошибка);
        - токен API не перехвачен;
        - hybrid_strategy вернула нулевой sentinel ([0]*60, [0]*60).

        Если токен не получен после всех попыток — карточка помечается
        фатально page_elements_not_found.

        Если стратегия вернула skip_reason (object_not_found,
        min_nights_exceeded) — карточка сразу помечается как
        необогащаемая, повторные попытки не запускаются. Поскольку
        парсер загружает front/searchapp/detail/{id} напрямую,
        ложные object_not_found из-за сбоя редиректа исключены.

        Если монитор соединения сигнализирует о необходимости перезапуска
        браузера — обработка прерывается досрочно без траты попыток.

        Если суммарное время обработки превысило enrich_timeout_seconds —
        обработка прерывается принудительно через asyncio.wait_for().

        Публичное поле listing.url не меняется — в Excel-отчёте
        остаётся https://sutochno.ru/{id}.

        Args:
            listing: Объявление с базовыми данными из каталога.
            page: Вкладка для работы. Если None — используется основная страница браузера.

        Returns:
            Объявление с заполненными calendar_60_days и prices_60_days.
        """
        active_page = page if page is not None else self._browser.page
        start_time = time.perf_counter()

        # Рабочий URL для парсинга — front/searchapp/detail/{id}.
        # Загружается напрямую, минуя редирект через публичный URL.
        enrichment_url = build_enrichment_url(listing.external_id)

        logger.info(
            "парсинг_карточки",
            path=enrichment_url,
            step=f"id={listing.external_id}",
        )

        for attempt in range(1, _MAX_ENRICH_ATTEMPTS + 1):
            # ── Проверка таймаута перед каждой попыткой ──
            if self._is_timeout_exceeded(start_time, listing.external_id):
                break

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

                loaded, token, elements_not_found = (
                    await self._page_loader.goto_and_capture_token(
                        active_page, enrichment_url, object_id=listing.external_id
                    )
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
                    # Токен не перехвачен — пробуем на следующей попытке.
                    # Если все попытки исчерпаны — карточка будет помечена
                    # фатально page_elements_not_found после цикла.
                    logger.warning(
                        "токен_не_получен_повтор",
                        step=f"id={listing.external_id}, попытка={attempt}/{_MAX_ENRICH_ATTEMPTS}",
                    )
                    # На последней попытке помечаем фатально
                    if attempt == _MAX_ENRICH_ATTEMPTS:
                        listing.enrichment_skip_reason = (
                            _PAGE_ELEMENTS_NOT_FOUND_REASON
                        )
                        logger.info(
                            "карточка_необогащаема",
                            step=f"id={listing.external_id}, "
                                 f"причина={_PAGE_ELEMENTS_NOT_FOUND_REASON}, "
                                 f"попытка={attempt}",
                        )
                        break
                    continue

                # Информационный лог: элементы не найдены, но токен есть.
                # Это не ошибка — обработка продолжается через API.
                if elements_not_found:
                    logger.info(
                        "элементы_не_найдены_но_токен_есть_продолжаем",
                        step=f"id={listing.external_id}, попытка={attempt}",
                    )

                await self._browser.random_delay()

                # ── Вызов стратегии с принудительным таймаутом ──
                remaining = self._remaining_timeout(start_time)
                if remaining is not None and remaining < _MIN_REMAINING_SECONDS:
                    logger.warning(
                        "карточка_прервана_по_таймауту",
                        step=f"id={listing.external_id}, "
                             f"осталось={format_duration(remaining)}, "
                             f"минимум={format_duration(_MIN_REMAINING_SECONDS)}",
                    )
                    break

                try:
                    if remaining is not None:
                        calendar, prices, skip_reason = await asyncio.wait_for(
                            self._strategy.fetch_calendar_and_prices(
                                active_page, listing.external_id, token,
                                enrichment_url,
                            ),
                            timeout=remaining,
                        )
                    else:
                        # Таймаут отключён — вызов без ограничения
                        calendar, prices, skip_reason = (
                            await self._strategy.fetch_calendar_and_prices(
                                active_page, listing.external_id, token,
                                enrichment_url,
                            )
                        )
                except asyncio.TimeoutError:
                    elapsed = time.perf_counter() - start_time
                    logger.warning(
                        "карточка_прервана_по_таймауту",
                        step=f"id={listing.external_id}, "
                             f"время={format_duration(elapsed)}, "
                             f"лимит={format_duration(self._enrich_timeout_seconds)}",
                    )
                    break

                # ── Фатальная ошибка — повторные попытки бессмысленны ──
                # Карточка помечается как необогащаемая и не будет повторяться
                # ни в текущем цикле retry, ни в _retry_empty_listings.
                # Поскольку парсер загружает front/searchapp/detail/{id}
                # напрямую, ложные object_not_found из-за сбоя редиректа
                # исключены — fallback не нужен.
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

    def _remaining_timeout(self, start_time: float) -> float | None:
        """Вычисляет оставшееся время до таймаута обработки карточки.

        Args:
            start_time: Время начала обработки (perf_counter).

        Returns:
            Оставшееся время в секундах или None если таймаут отключён.
        """
        if self._enrich_timeout_seconds <= 0:
            return None

        elapsed = time.perf_counter() - start_time
        remaining = self._enrich_timeout_seconds - elapsed

        # Не возвращаем отрицательное значение — 0.0 означает «время вышло»
        return max(remaining, 0.0)

    def _is_timeout_exceeded(self, start_time: float, object_id: str) -> bool:
        """Проверяет, превышен ли таймаут обработки карточки.

        Если enrich_timeout_seconds = 0 — таймаут отключён, всегда False.
        Если время с момента start_time превысило порог — логирует
        предупреждение и возвращает True.

        Карточка при этом НЕ помечается как фатальная (без skip_reason) —
        она остаётся «пустой» и попадёт в retry-раунды как обычная
        необогащённая. На следующем раунде (или следующем запуске)
        проблема может разрешиться сама (блокировка прошла, прокси сменилась).

        Args:
            start_time: Время начала обработки (perf_counter).
            object_id: ID объявления (для логов).

        Returns:
            True если таймаут превышен и обработку следует прервать.
        """
        if self._enrich_timeout_seconds <= 0:
            return False

        elapsed = time.perf_counter() - start_time

        if elapsed >= self._enrich_timeout_seconds:
            logger.warning(
                "карточка_прервана_по_таймауту",
                step=f"id={object_id}, "
                     f"время={format_duration(elapsed)}, "
                     f"лимит={format_duration(self._enrich_timeout_seconds)}",
            )
            return True

        return False

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
        concurrency_controller: ConcurrencyController | None = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.
            proxy_service: Сервис прокси с заполненным пулом (опциональный).
                Передаётся в воркеры для проверки/замены при перезапуске.
            concurrency_controller: Глобальный контроллер параллелизма
                (опциональный). Если не передан — создаётся автоматически
                внутри EnrichStrategies с параметрами из settings.

        Returns:
            Список обогащённых карточек.
        """
        return await EnrichStrategies.enrich_listings_parallel(
            settings=settings,
            listings=listings,
            proxies=proxies,
            proxy_service=proxy_service,
            concurrency_controller=concurrency_controller,
        )
