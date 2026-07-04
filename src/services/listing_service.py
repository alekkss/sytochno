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
    build_alt_url,
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

# Фатальная причина, при которой запускается fallback через альтернативный URL.
# Другие причины (например, min_nights_exceeded) не требуют fallback —
# они устанавливаются на основе логики API, а не структуры страницы.
_OBJECT_NOT_FOUND_REASON = "object_not_found"


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

        self._page_loader = PageLoader(
            monitor=monitor,
            concurrency_controller=concurrency_controller,
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

        Выполняет до _MAX_ENRICH_ATTEMPTS попыток. Повторная попытка
        запускается при трёх сценариях сбоя:
        - страница не загрузилась;
        - токен API не перехвачен (частая проблема при параллельных вкладках);
        - hybrid_strategy вернула нулевой sentinel ([0]*60, [0]*60).

        Если стратегия вернула skip_reason (фатальная ошибка) — обработка
        зависит от типа причины:
        - "object_not_found" на основном URL → запускается fallback через
          альтернативный URL (https://sutochno.ru/front/searchapp/detail/{id}).
          Основной URL — старая версия страницы, которая перенаправляет на
          этот внутренний путь; иногда редирект даёт сбой и основной URL
          возвращает пустую страницу. Если и альтернативный URL вернул
          object_not_found — карточка окончательно помечается как удалённая.
          Fallback выполняется не более одного раза за вызов enrich_listing.
        - "min_nights_exceeded" или другие причины → карточка сразу
          помечается как необогащаемая, повторные попытки не запускаются.

        Если монитор соединения сигнализирует о необходимости перезапуска
        браузера — обработка прерывается досрочно без траты попыток.

        Если суммарное время обработки превысило enrich_timeout_seconds —
        обработка прерывается принудительно через asyncio.wait_for().
        Карточка остаётся необогащённой (без enrichment_skip_reason) и
        попадёт в retry-раунды как обычная.

        Нулевой sentinel отличается от реально свободного объявления тем,
        что у свободного объявления цены > 0, а у sentinel все цены = 0.

        asyncio.CancelledError намеренно пробрасывается выше — это штатная
        отмена задачи, а не сбой обработки. Перед пробросом фиксируется лог,
        чтобы пустая карточка не оставалась без следов в логах.

        Публичное поле listing.url НЕ меняется даже при успешном fallback —
        альтернативный URL используется только внутри как рабочий адрес.
        В Excel-отчёте всегда остаётся публичная ссылка.

        Args:
            listing: Объявление с базовыми данными из каталога.
            page: Вкладка для работы. Если None — используется основная страница браузера.

        Returns:
            Объявление с заполненными calendar_60_days и prices_60_days.
        """
        active_page = page if page is not None else self._browser.page
        start_time = time.perf_counter()

        # Флаг «fallback через альтернативный URL уже использован».
        # Гарантирует, что fallback выполняется не более одного раза
        # за один вызов enrich_listing — даже если в разных попытках
        # мы снова получим object_not_found.
        alt_url_tried = False

        logger.info(
            "парсинг_карточки",
            path=listing.url,
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

                # ── Вызов стратегии с принудительным таймаутом ──
                # asyncio.wait_for прерывает операцию ровно по лимиту,
                # а не ждёт завершения текущего шага внутри стратегии.
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
                                active_page, listing.external_id, token, listing.url
                            ),
                            timeout=remaining,
                        )
                    else:
                        # Таймаут отключён — вызов без ограничения
                        calendar, prices, skip_reason = (
                            await self._strategy.fetch_calendar_and_prices(
                                active_page, listing.external_id, token, listing.url
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

                # ── Fallback для object_not_found через альтернативный URL ──
                # Основной URL (https://sutochno.ru/{id}) — старая версия
                # страницы, которая перенаправляет на внутренний путь.
                # Иногда редирект даёт сбой → основной URL возвращает пустоту
                # → API отвечает no_objects → ложное object_not_found.
                # Пробуем альтернативный URL один раз за вызов enrich_listing.
                if (
                    skip_reason == _OBJECT_NOT_FOUND_REASON
                    and not alt_url_tried
                ):
                    alt_url_tried = True
                    fallback_result = await self._try_alt_url_fallback(
                        active_page, listing, start_time, attempt
                    )
                    if fallback_result is not None:
                        # Fallback дал определённый результат — используем его
                        calendar, prices, skip_reason = fallback_result
                    # Если fallback вернул None — прерываемся (таймаут/монитор).
                    # Не запускаем ретрай — за пределами fallback уже нет времени.
                    else:
                        break

                # ── Фатальная ошибка — повторные попытки бессмысленны ──
                # Карточка помечается как необогащаемая и не будет повторяться
                # ни в текущем цикле retry, ни в _retry_empty_listings.
                if skip_reason is not None:
                    listing.enrichment_skip_reason = skip_reason
                    logger.info(
                        "карточка_необогащаема",
                        step=f"id={listing.external_id}, причина={skip_reason}"
                             + (", fallback_проверен=да" if alt_url_tried
                                and skip_reason == _OBJECT_NOT_FOUND_REASON
                                else ""),
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
                          + (f", попытка={attempt}" if attempt > 1 else "")
                          + (", через=alt_url" if alt_url_tried else ""),
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

    async def _try_alt_url_fallback(
        self,
        active_page: Page,
        listing: RawListing,
        start_time: float,
        attempt: int,
    ) -> tuple[list[int], list[int], str | None] | None:
        """Пытается обогатить карточку через альтернативный URL.

        Выполняет полный цикл обогащения на альтернативном URL
        (https://sutochno.ru/front/searchapp/detail/{id}):
        1. Проверяет остаток времени и монитор.
        2. Загружает альтернативную страницу и перехватывает токен.
        3. Вызывает стратегию с альтернативным URL.

        Публичное поле listing.url НЕ меняется — альтернативный URL
        используется только внутри этого метода как временный
        рабочий адрес для загрузки страницы.

        Args:
            active_page: Активная вкладка браузера.
            listing: Объявление (используется его external_id).
            start_time: Время начала обработки карточки (для таймаута).
            attempt: Номер текущей попытки (для логов).

        Returns:
            Кортеж (calendar, prices, skip_reason) — результат fallback.
            None если fallback был прерван (таймаут, монитор, ошибка загрузки).
        """
        alt_url = build_alt_url(listing.external_id)

        logger.info(
            "fallback_alt_url_старт",
            path=alt_url,
            step=f"id={listing.external_id}, попытка={attempt}",
        )

        # Проверка монитора — вдруг за это время что-то сломалось
        if self._monitor and self._monitor.should_skip():
            logger.debug(
                "fallback_прерван_монитор",
                step=f"id={listing.external_id}",
            )
            return None

        # Проверка остатка времени — не запускаем тяжёлую операцию,
        # если времени осталось меньше минимума
        remaining = self._remaining_timeout(start_time)
        if remaining is not None and remaining < _MIN_REMAINING_SECONDS:
            logger.warning(
                "fallback_прерван_таймаут",
                step=f"id={listing.external_id}, "
                     f"осталось={format_duration(remaining)}",
            )
            return None

        # Загружаем альтернативную страницу и перехватываем токен
        try:
            alt_loaded, alt_token = await self._page_loader.goto_and_capture_token(
                active_page, alt_url, object_id=listing.external_id
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                "fallback_ошибка_загрузки",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}, url={alt_url}",
            )
            return None

        if not alt_loaded:
            logger.warning(
                "fallback_страница_не_загрузилась",
                step=f"id={listing.external_id}",
            )
            return None

        if not alt_token:
            logger.warning(
                "fallback_токен_не_получен",
                step=f"id={listing.external_id}",
            )
            return None

        # Пересчитываем остаток времени после загрузки страницы
        remaining_after_load = self._remaining_timeout(start_time)
        if (
            remaining_after_load is not None
            and remaining_after_load < _MIN_REMAINING_SECONDS
        ):
            logger.warning(
                "fallback_прерван_таймаут_после_загрузки",
                step=f"id={listing.external_id}, "
                     f"осталось={format_duration(remaining_after_load)}",
            )
            return None

        # Вызываем стратегию с альтернативным URL
        try:
            if remaining_after_load is not None:
                result = await asyncio.wait_for(
                    self._strategy.fetch_calendar_and_prices(
                        active_page, listing.external_id, alt_token, alt_url
                    ),
                    timeout=remaining_after_load,
                )
            else:
                result = await self._strategy.fetch_calendar_and_prices(
                    active_page, listing.external_id, alt_token, alt_url
                )
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - start_time
            logger.warning(
                "fallback_таймаут_стратегии",
                step=f"id={listing.external_id}, "
                     f"время={format_duration(elapsed)}",
            )
            return None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                "fallback_ошибка_стратегии",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}",
            )
            return None

        alt_calendar, alt_prices, alt_skip_reason = result

        # Логируем результат fallback — для диагностики нужно понимать,
        # что дал альтернативный маршрут
        if alt_skip_reason == _OBJECT_NOT_FOUND_REASON:
            logger.info(
                "fallback_подтвердил_object_not_found",
                step=f"id={listing.external_id}, "
                     f"карточка_окончательно_удалена=да",
            )
        elif alt_skip_reason is not None:
            logger.info(
                "fallback_вернул_другую_причину",
                step=f"id={listing.external_id}, причина={alt_skip_reason}",
            )
        elif not self._is_failure_sentinel(alt_calendar, alt_prices):
            logger.info(
                "fallback_успешно_обогатил",
                step=f"id={listing.external_id}, "
                     f"свободных={sum(1 for c in alt_calendar if c == 0)}, "
                     f"занятых={sum(1 for c in alt_calendar if c == 1)}",
            )
        else:
            logger.info(
                "fallback_вернул_пустой_результат",
                step=f"id={listing.external_id}",
            )

        return alt_calendar, alt_prices, alt_skip_reason

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
