"""Стратегии обогащения списка карточек — вкладки, прокси, воркеры."""

import asyncio
import time
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService
from src.services.listing.connection_monitor import ConnectionMonitor
from src.services.listing.constants import (
    format_duration,
    safe_stop_browser,
)

if TYPE_CHECKING:
    from src.services.listing_service import ListingService
    from src.services.proxy_service import ProxyService

logger = get_logger("enrich_strategies")

# Пауза после перезапуска браузера перед возобновлением обработки (секунды)
_RESTART_COOLDOWN_SECONDS: float = 5.0

# Максимальное количество перезапусков браузера за один прогон воркера
_MAX_RESTARTS_PER_WORKER: int = 3


class EnrichStrategies:
    """Параллельные стратегии обогащения карточек.

    Инкапсулирует:
    - enrich_listings_tabbed: параллельная обработка через вкладки.
    - enrich_listings_parallel: параллельная обработка через прокси-браузеры.
    - _worker: воркер для одного прокси-браузера.
    """

    def __init__(
        self,
        listing_service: "ListingService",
        browser_service: BrowserService,
        settings: "any",  # type: ignore[name-defined]
        proxy_service: "ProxyService | None" = None,
    ) -> None:
        """Инициализирует стратегии.

        Args:
            listing_service: Основной сервис карточки (для enrich_listing).
            browser_service: Сервис браузера (для create_page/close_page).
            settings: Настройки приложения.
            proxy_service: Сервис прокси с заполненным списком рабочих прокси
                (опциональный). Если передан — используется при перезапуске
                браузера для проверки и замены прокси.
        """
        self._listing_service = listing_service
        self._browser = browser_service
        self._settings = settings
        self._proxy_service = proxy_service

    async def enrich_listings_tabbed(
        self, listings: list[RawListing]
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько вкладок.

        При срабатывании монитора соединения (2+ сбоя подряд):
        1. Все вкладки прекращают обработку.
        2. Браузер перезапускается (с проверкой/заменой прокси).
        3. Необработанные карточки отправляются на повторную обработку.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days и prices_60_days.
        """
        max_tabs = self._settings.max_tabs
        tab_delay_ms = self._settings.tab_delay_ms
        total = len(listings)

        logger.info(
            "запуск_параллельных_вкладок",
            step=f"вкладок={max_tabs}",
            total=total,
        )

        # Определяем необработанные карточки — те, у которых нет данных
        remaining = [l for l in listings if not self._is_enriched(l)]
        restart_count = 0

        while remaining and restart_count <= _MAX_RESTARTS_PER_WORKER:
            monitor = self._listing_service.monitor
            # Если монитор не был установлен извне — создаём локальный
            if monitor is None:
                monitor = ConnectionMonitor()
                self._listing_service.monitor = monitor

            # Сбрасываем монитор перед новой итерацией
            await monitor.reset()

            await self._process_batch_with_tabs(
                remaining, max_tabs, tab_delay_ms, monitor
            )

            # Проверяем — сработал ли монитор (массовый сбой соединения)
            if monitor.restart_needed:
                restart_count += 1

                if restart_count > _MAX_RESTARTS_PER_WORKER:
                    logger.warning(
                        "лимит_перезапусков_исчерпан",
                        step=f"перезапусков={restart_count - 1}, "
                             f"лимит={_MAX_RESTARTS_PER_WORKER}",
                    )
                    break

                logger.warning(
                    "перезапуск_браузера_из_за_сбоев",
                    step=f"перезапуск={restart_count}/{_MAX_RESTARTS_PER_WORKER}",
                )

                # Перезапускаем браузер с проверкой прокси
                success = await self._restart_browser_with_proxy_check()

                if not success:
                    logger.error(
                        "перезапуск_браузера_не_удался",
                        step="обработка_прекращена",
                    )
                    break

                # Пауза после перезапуска — даём сети стабилизироваться
                await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                # Пересчитываем необработанные карточки
                remaining = [l for l in listings if not self._is_enriched(l)]

                logger.info(
                    "продолжение_после_перезапуска",
                    step=f"осталось={len(remaining)}, всего={total}",
                )
            else:
                # Монитор не сработал — обработка завершена штатно
                break

        enriched_count = sum(1 for l in listings if self._is_enriched(l))
        logger.info(
            "параллельные_вкладки_завершены",
            total=total,
            step=f"обогащено={enriched_count}, перезапусков={restart_count}",
        )

        return listings

    async def _process_batch_with_tabs(
        self,
        listings: list[RawListing],
        max_tabs: int,
        tab_delay_ms: int,
        monitor: ConnectionMonitor,
    ) -> list[RawListing]:
        """Обрабатывает пачку карточек параллельными вкладками.

        Вкладки проверяют monitor.should_skip() — при срабатывании
        монитора новые загрузки не начинаются.

        Args:
            listings: Карточки для обработки.
            max_tabs: Максимум параллельных вкладок.
            tab_delay_ms: Задержка между запуском вкладок (мс).
            monitor: Монитор здоровья соединения.

        Returns:
            Список карточек (модифицированных in-place).
        """
        total = len(listings)
        semaphore = asyncio.Semaphore(max_tabs)
        navigation_lock = asyncio.Lock()
        processed_count = 0
        count_lock = asyncio.Lock()

        async def _process_one(listing: RawListing) -> None:
            """Обрабатывает одну карточку в отдельной вкладке."""
            nonlocal processed_count
            page: Page | None = None

            async with semaphore:
                # Проверяем монитор перед стартом — если перезапуск уже
                # требуется, не тратим ресурсы на открытие вкладки
                if monitor.should_skip():
                    logger.debug(
                        "вкладка_пропущена_перезапуск",
                        step=f"id={listing.external_id}",
                    )
                    return

                # Задержка при старте вкладки
                async with navigation_lock:
                    await asyncio.sleep(tab_delay_ms / 1000.0)

                # Повторная проверка после ожидания задержки
                if monitor.should_skip():
                    return

                try:
                    page = await self._browser.create_page()
                    await self._listing_service.enrich_listing(listing, page)
                finally:
                    if page is not None:
                        await self._browser.close_page(page)

                async with count_lock:
                    processed_count += 1
                    current = processed_count

                if not monitor.should_skip():
                    logger.info(
                        "прогресс_вкладок",
                        current=current,
                        total=total,
                    )

        tasks = [_process_one(listing) for listing in listings]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        error_count = sum(1 for r in results if isinstance(r, BaseException))
        if error_count > 0:
            for idx, result in enumerate(results):
                if isinstance(result, BaseException):
                    logger.warning(
                        "ошибка_в_задаче_вкладки",
                        error=str(result),
                        error_type=type(result).__name__,
                        step=f"карточка={idx + 1}",
                    )

        return listings

    async def _restart_browser_with_proxy_check(self) -> bool:
        """Перезапускает браузер с проверкой и возможной заменой прокси.

        Логика:
        1. Останавливаем текущий браузер.
        2. Если есть текущая прокси — проверяем только её (один запрос).
        3. Если прокси работает — перезапускаем с ней.
        4. Если прокси не работает — ищем замену из уже проверенного пула.
        5. Если замена найдена — перезапускаем с новой прокси.
        6. Если замены нет — перезапускаем без прокси.

        Returns:
            True если браузер успешно перезапущен, False — если не удалось.
        """
        current_proxy = self._browser._proxy  # noqa: SLF001

        # Шаг 1: Останавливаем текущий браузер
        logger.info(
            "остановка_браузера_для_перезапуска",
            step=str(current_proxy) if current_proxy else "без_прокси",
        )

        try:
            await self._browser.stop()
        except Exception as e:
            logger.warning(
                "ошибка_при_остановке_браузера",
                error=str(e),
                error_type=type(e).__name__,
            )

        # Шаг 2: Проверяем текущую прокси (если есть)
        active_proxy = current_proxy

        if current_proxy is not None and self._proxy_service is not None:
            logger.info(
                "проверка_текущей_прокси",
                step=str(current_proxy),
            )

            is_current_working = await self._proxy_service.check_single_proxy(
                current_proxy
            )

            if is_current_working:
                logger.info(
                    "текущая_прокси_работает_перезапуск_браузера",
                    step=str(current_proxy),
                )
                active_proxy = current_proxy
            else:
                # Текущая прокси мертва — ищем замену из уже проверенного пула
                logger.warning(
                    "текущая_прокси_не_работает_ищем_замену",
                    step=str(current_proxy),
                )

                replacement = await self._proxy_service.get_replacement_proxy(
                    current_proxy=current_proxy,
                    in_use_proxies=[],
                )

                if replacement is not None:
                    logger.info(
                        "замена_прокси_применена",
                        step=f"старая={current_proxy}, новая={replacement}",
                    )
                    active_proxy = replacement
                else:
                    logger.warning(
                        "замена_не_найдена_запуск_без_прокси",
                    )
                    active_proxy = None

        # Шаг 3: Перезапускаем браузер
        try:
            await self._browser.start(proxy=active_proxy)

            # Прогрев нового браузера
            await self._browser.navigate("https://sutochno.ru")
            await self._browser.scroll_page()
            await asyncio.sleep(5)

            logger.info(
                "браузер_перезапущен",
                step=str(active_proxy) if active_proxy else "без_прокси",
            )
            return True

        except Exception as e:
            logger.error(
                "не_удалось_перезапустить_браузер",
                error=str(e),
                error_type=type(e).__name__,
            )
            return False

    @staticmethod
    def _is_enriched(listing: RawListing) -> bool:
        """Проверяет, обогащена ли карточка данными.

        Карточка считается обогащённой, если хотя бы одна цена > 0
        или хотя бы один день занят (calendar = 1).

        Args:
            listing: Объявление для проверки.

        Returns:
            True если карточка содержит данные календаря/цен.
        """
        if listing.calendar_60_days and any(c == 1 for c in listing.calendar_60_days):
            return True
        if listing.prices_60_days and any(p > 0 for p in listing.prices_60_days):
            return True
        return False

    @staticmethod
    async def enrich_listings_parallel(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.
            proxy_service: Сервис прокси с заполненным пулом рабочих прокси
                (опциональный). Передаётся в воркеры для проверки/замены.

        Returns:
            Список обогащённых карточек.
        """
        from src.services.proxy_service import ProxyService as ProxyServiceClass

        chunks = ProxyServiceClass.distribute_listings(listings, len(proxies))

        logger.info(
            "параллельная_обработка",
            total=len(listings),
            step=f"прокси={len(proxies)}, вкладок_на_прокси={settings.max_tabs}",
        )

        parallel_start = time.perf_counter()

        tasks = [
            EnrichStrategies._worker(
                settings, chunk, proxy, worker_idx, proxies, proxy_service
            )
            for worker_idx, (chunk, proxy) in enumerate(zip(chunks, proxies), start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        parallel_elapsed = time.perf_counter() - parallel_start

        all_enriched: list[RawListing] = []
        worker_stats: list[tuple[int, int, float]] = []
        browsers_to_stop: list[tuple[BrowserService, int]] = []

        for worker_idx, result in enumerate(results, start=1):
            if isinstance(result, BaseException):
                logger.warning(
                    "воркер_завершился_с_ошибкой",
                    error=str(result),
                    error_type=type(result).__name__,
                    step=f"воркер={worker_idx}",
                )
            elif isinstance(result, tuple) and len(result) == 3:
                enriched_list, duration, browser_svc = result
                all_enriched.extend(enriched_list)
                worker_stats.append((worker_idx, len(enriched_list), duration))
                browsers_to_stop.append((browser_svc, worker_idx))

        if browsers_to_stop:
            logger.info("остановка_прокси_браузеров", total=len(browsers_to_stop))
            for browser_svc, w_idx in browsers_to_stop:
                await safe_stop_browser(browser_svc, w_idx)
            logger.info("все_прокси_браузеры_остановлены")

        if worker_stats:
            logger.info("─" * 50)
            logger.info("сводка_по_воркерам", total=len(worker_stats))

            for w_idx, w_cards, w_duration in worker_stats:
                avg_per_card = w_duration / w_cards if w_cards > 0 else 0.0
                logger.info(
                    "время_воркера",
                    step=f"воркер={w_idx}",
                    total=f"карточек={w_cards}, время={format_duration(w_duration)}, "
                          f"среднее={format_duration(avg_per_card)}/карточка",
                )

            fastest = min(worker_stats, key=lambda x: x[2])
            slowest = max(worker_stats, key=lambda x: x[2])
            total_cards = sum(c for _, c, _ in worker_stats)

            logger.info(
                "итого_параллельная_обработка",
                step=f"карточек={total_cards}, воркеров={len(worker_stats)}",
                total=f"общее_время={format_duration(parallel_elapsed)}, "
                      f"быстрейший=воркер_{fastest[0]}({format_duration(fastest[2])}), "
                      f"медленнейший=воркер_{slowest[0]}({format_duration(slowest[2])})",
            )
            logger.info("─" * 50)

        logger.info(
            "параллельная_обработка_завершена",
            total=len(all_enriched),
        )

        return all_enriched

    @staticmethod
    async def _worker(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxy: ProxyConfig,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
    ) -> tuple[list[RawListing], float, BrowserService]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер.

        При срабатывании монитора соединения выполняет перезапуск браузера
        с проверкой текущей прокси. Если текущая мертва — ищет замену
        из пула proxy_service (уже проверенных на старте прокси).

        Args:
            settings: Настройки приложения.
            listings: Порция карточек для этого воркера.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).
            all_proxies: Полный список прокси всех воркеров (для исключения занятых).
            proxy_service: Сервис прокси с заполненным пулом (опциональный).

        Returns:
            Кортеж (список карточек, время работы, browser_service).
        """
        if not listings:
            return ([], 0.0, BrowserService(settings=settings))

        worker_start = time.perf_counter()
        browser_service = BrowserService(settings=settings)
        monitor = ConnectionMonitor()
        current_proxy: ProxyConfig | None = proxy

        # Прокси, занятые другими воркерами (исключаем из замены)
        in_use_by_others = [p for p in all_proxies if p != proxy]

        try:
            await browser_service.start(proxy=current_proxy)

            logger.info(
                "воркер_запущен",
                step=f"воркер={worker_idx}",
                total=len(listings),
            )

            await browser_service.navigate("https://sutochno.ru")
            await browser_service.scroll_page()
            await asyncio.sleep(10)

            logger.info("воркер_прогрет", step=f"воркер={worker_idx}")

            from src.services.listing_service import ListingService

            listing_service = ListingService(
                settings=settings,
                browser_service=browser_service,
                monitor=monitor,
            )

            # Обработка с возможностью перезапуска при массовых сбоях
            remaining = list(listings)
            restart_count = 0

            while remaining and restart_count <= _MAX_RESTARTS_PER_WORKER:
                await monitor.reset()
                await listing_service.enrich_listings_tabbed(remaining)

                if monitor.restart_needed:
                    restart_count += 1

                    if restart_count > _MAX_RESTARTS_PER_WORKER:
                        logger.warning(
                            "воркер_лимит_перезапусков",
                            step=f"воркер={worker_idx}, "
                                 f"перезапусков={restart_count - 1}",
                        )
                        break

                    logger.warning(
                        "воркер_перезапуск_браузера",
                        step=f"воркер={worker_idx}, "
                             f"перезапуск={restart_count}/{_MAX_RESTARTS_PER_WORKER}",
                    )

                    # Останавливаем браузер
                    try:
                        await browser_service.stop()
                    except Exception as e:
                        logger.warning(
                            "воркер_ошибка_остановки",
                            error=str(e),
                            step=f"воркер={worker_idx}",
                        )

                    # Проверяем текущую прокси (один запрос)
                    if current_proxy is not None and proxy_service is not None:
                        is_current_ok = await proxy_service.check_single_proxy(
                            current_proxy
                        )

                        if is_current_ok:
                            logger.info(
                                "воркер_прокси_работает",
                                step=f"воркер={worker_idx}, прокси={current_proxy}",
                            )
                        else:
                            # Ищем замену из уже проверенного пула
                            replacement = await proxy_service.get_replacement_proxy(
                                current_proxy=current_proxy,
                                in_use_proxies=in_use_by_others,
                            )

                            if replacement is not None:
                                logger.info(
                                    "воркер_замена_прокси",
                                    step=f"воркер={worker_idx}, "
                                         f"старая={current_proxy}, "
                                         f"новая={replacement}",
                                )
                                current_proxy = replacement
                                # Обновляем список исключений
                                in_use_by_others = [
                                    p for p in all_proxies if p != current_proxy
                                ]
                            else:
                                logger.warning(
                                    "воркер_замена_не_найдена",
                                    step=f"воркер={worker_idx}, "
                                         f"пробуем_без_прокси",
                                )
                                current_proxy = None

                    # Перезапускаем браузер
                    try:
                        browser_service = BrowserService(settings=settings)
                        await browser_service.start(proxy=current_proxy)
                        await browser_service.navigate("https://sutochno.ru")
                        await browser_service.scroll_page()
                        await asyncio.sleep(5)

                        # Пересоздаём ListingService с новым браузером и монитором
                        monitor = ConnectionMonitor()
                        listing_service = ListingService(
                            settings=settings,
                            browser_service=browser_service,
                            monitor=monitor,
                        )

                        logger.info(
                            "воркер_браузер_перезапущен",
                            step=f"воркер={worker_idx}, "
                                 f"прокси={current_proxy or 'без_прокси'}",
                        )

                    except Exception as e:
                        logger.error(
                            "воркер_перезапуск_не_удался",
                            error=str(e),
                            step=f"воркер={worker_idx}",
                        )
                        break

                    # Пауза после перезапуска
                    await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                    # Пересчитываем необработанные
                    remaining = [
                        l for l in listings
                        if not EnrichStrategies._is_enriched(l)
                    ]

                    logger.info(
                        "воркер_продолжение",
                        step=f"воркер={worker_idx}, осталось={len(remaining)}",
                    )
                else:
                    # Штатное завершение — выходим из while
                    break

            worker_elapsed = time.perf_counter() - worker_start

            logger.info(
                "воркер_завершил_обработку",
                step=f"воркер={worker_idx}",
                total=f"карточек={len(listings)}, время={format_duration(worker_elapsed)}",
            )

            return (listings, worker_elapsed, browser_service)

        except Exception as e:
            worker_elapsed = time.perf_counter() - worker_start
            logger.warning(
                "ошибка_воркера",
                error=str(e),
                error_type=type(e).__name__,
                step=f"воркер={worker_idx}",
            )
            return (listings, worker_elapsed, browser_service)
