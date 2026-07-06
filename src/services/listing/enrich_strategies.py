"""Стратегии обогащения списка карточек — вкладки, прокси, воркеры."""

import asyncio
import time
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService
from src.services.listing.concurrency_controller import ConcurrencyController
from src.services.listing.connection_monitor import ConnectionMonitor
from src.services.listing.constants import (
    format_duration,
    safe_stop_browser,
)
from src.services.memory_monitor import MemoryMonitor

if TYPE_CHECKING:
    from src.services.listing_service import ListingService
    from src.services.proxy_service import ProxyService

logger = get_logger("enrich_strategies")

# Пауза после перезапуска браузера перед возобновлением обработки (секунды)
_RESTART_COOLDOWN_SECONDS: float = 5.0

# Максимальное количество перезапусков браузера за один прогон воркера
_MAX_RESTARTS_PER_WORKER: int = 2

# Максимальное количество попыток прогрева браузера (навигация на главную).
# При каждой неудачной попытке выполняется пауза и проверка/замена прокси.
_MAX_WARMUP_ATTEMPTS: int = 2

# Пауза между попытками прогрева (секунды)
_WARMUP_RETRY_DELAY_SECONDS: float = 5.0

# Задержка между стартом воркеров (секунды).
# Предотвращает одновременный вызов playwright.chromium.launch()
# и параллельный прогрев на всех прокси сразу.
_WORKER_START_DELAY_SECONDS: float = 2.0

# Максимальное количество retry-раундов для упавших воркеров.
# После каждого раунда необработанные карточки перераспределяются
# между оставшимися рабочими прокси и запускаются повторно.
_MAX_PARALLEL_RETRY_ROUNDS: int = 1

# Интервал логирования статистики контроллера (секунды).
# Каждые N секунд в лог выводится текущее состояние адаптации.
_STATS_LOG_INTERVAL_SECONDS: float = 60.0

# Общий таймаут этапа остановки всех прокси-браузеров (секунды).
# После завершения всех воркеров браузеры останавливаются параллельно
# через asyncio.gather. Каждый браузер имеет собственный таймаут
# WORKER_STOP_TIMEOUT (20 сек в constants.py) — но если общая
# картина зависает (например, event loop заблокирован CDP-сессиями),
# этот таймаут гарантирует, что программа перейдёт к следующему этапу
# pipeline (снимки → сравнение → экспорт) не позднее чем через 100 сек.
# 100 сек — избыточно для параллельной остановки: даже при 85 воркерах
# все браузеры должны отчитаться за 20-30 секунд. Резерв в 3-5× —
# защита от патологических случаев.
_STOP_BROWSERS_GLOBAL_TIMEOUT: float = 100.0


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
        concurrency_controller: ConcurrencyController | None = None,
    ) -> None:
        """Инициализирует стратегии.

        Args:
            listing_service: Основной сервис карточки (для enrich_listing).
            browser_service: Сервис браузера (для create_page/close_page).
            settings: Настройки приложения.
            proxy_service: Сервис прокси с заполненным списком рабочих прокси
                (опциональный). Если передан — используется при перезапуске
                браузера для проверки и замены прокси.
            concurrency_controller: Глобальный контроллер параллелизма (опциональный).
                Если передан — вкладки запрашивают разрешение перед каждой карточкой.
        """
        self._listing_service = listing_service
        self._browser = browser_service
        self._settings = settings
        self._proxy_service = proxy_service
        self._controller = concurrency_controller

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
        # и не установлена фатальная причина пропуска
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

                # Пересчитываем необработанные карточки — карточки
                # с фатальной причиной (skip_reason) сюда не попадают
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
        """Обрабатывает пачку карточек порциями по max_tabs вкладок.

        Вместо создания всех корутин одновременно через asyncio.gather
        (что приводит к созданию сотен замыканий в памяти), карточки
        обрабатываются порциями. После каждой порции дополнительные
        вкладки закрываются — это освобождает ~50-150 МБ на вкладку.

        Вкладки проверяют monitor.should_skip() — при срабатывании
        монитора новые загрузки не начинаются.

        Если передан concurrency_controller — каждая вкладка дополнительно
        запрашивает разрешение перед обработкой карточки.

        Args:
            listings: Карточки для обработки.
            max_tabs: Максимум параллельных вкладок.
            tab_delay_ms: Задержка между запуском вкладок (мс).
            monitor: Монитор здоровья соединения.

        Returns:
            Список карточек (модифицированных in-place).
        """
        total = len(listings)
        processed_count = 0

        # Разбиваем на порции по max_tabs карточек
        for chunk_start in range(0, total, max_tabs):
            # Проверяем монитор перед каждой порцией
            if monitor.should_skip():
                logger.debug(
                    "порция_пропущена_перезапуск",
                    step=f"с_позиции={chunk_start}",
                )
                break

            chunk = listings[chunk_start : chunk_start + max_tabs]

            # Создаём задачи только для текущей порции
            tasks = [
                self._process_one_tab(
                    listing, tab_delay_ms, monitor, idx
                )
                for idx, listing in enumerate(chunk)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Логируем ошибки из текущей порции
            for idx, result in enumerate(results):
                if isinstance(result, BaseException):
                    listing_id = chunk[idx].external_id if idx < len(chunk) else "?"
                    logger.warning(
                        "ошибка_в_задаче_вкладки",
                        error=str(result),
                        error_type=type(result).__name__,
                        step=f"id={listing_id}",
                    )

            processed_count += len(chunk)

            # Закрываем все дополнительные вкладки после каждой порции —
            # освобождаем память Chromium перед следующей порцией
            await self._browser.close_all_pages()

            if not monitor.should_skip():
                logger.info(
                    "прогресс_вкладок",
                    current=min(processed_count, total),
                    total=total,
                )

        return listings

    async def _process_one_tab(
        self,
        listing: RawListing,
        tab_delay_ms: int,
        monitor: ConnectionMonitor,
        tab_index: int,
    ) -> None:
        """Обрабатывает одну карточку в отдельной вкладке.

        Если передан concurrency_controller — запрашивает разрешение
        перед началом обработки и освобождает после завершения.

        Args:
            listing: Объявление для обогащения.
            tab_delay_ms: Задержка перед стартом (мс).
            monitor: Монитор здоровья соединения.
            tab_index: Порядковый номер вкладки в порции (для задержки).
        """
        page: Page | None = None

        # Проверяем монитор перед стартом — если перезапуск уже
        # требуется, не тратим ресурсы на открытие вкладки
        if monitor.should_skip():
            logger.debug(
                "вкладка_пропущена_перезапуск",
                step=f"id={listing.external_id}",
            )
            return

        # Задержка между стартом вкладок внутри порции —
        # первая вкладка стартует сразу, остальные с задержкой
        if tab_index > 0:
            await asyncio.sleep(tab_delay_ms / 1000.0)

        # Повторная проверка после ожидания задержки
        if monitor.should_skip():
            return

        # Если есть контроллер — ждём разрешения (глобальный троттлинг)
        if self._controller is not None:
            await self._controller.acquire()

        try:
            page = await self._browser.create_page()
            await self._listing_service.enrich_listing(listing, page)
        finally:
            if self._controller is not None:
                self._controller.release()
            if page is not None:
                await self._browser.close_page(page)

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
        """Проверяет, завершена ли обработка карточки (обогащена или необогащаема).

        Карточка считается «завершённой» (не нуждающейся в retry) если
        выполнено любое из условий:

        1. Обогащена успешно — хотя бы одна цена > 0 или хотя бы один
           день занят (calendar = 1). Это означает, что данные получены.

        2. Установлена фатальная причина пропуска (enrichment_skip_reason).
           Возможные значения:
           - "object_not_found" — объявление удалено или заблокировано.
           - "min_nights_exceeded" — min_nights объекта превышает 60 дней.
           - "page_elements_not_found" — страница загрузилась без ключевых
             DOM-элементов (битая вёрстка или антибот-заглушка).
           Такие карточки принципиально невозможно обогатить, повторные
           попытки — чистая потеря времени и лишние запросы к API.

        Обе ситуации семантически эквивалентны для retry-цикла:
        «трогать эту карточку больше не нужно».

        Args:
            listing: Объявление для проверки.

        Returns:
            True если карточка обогащена ИЛИ необогащаема (skip_reason установлен).
        """
        # Приоритетная проверка — фатальная причина исключает карточку
        # мгновенно, без осмотра данных календаря/цен
        if listing.enrichment_skip_reason is not None:
            return True

        if listing.calendar_60_days and any(c == 1 for c in listing.calendar_60_days):
            return True
        if listing.prices_60_days and any(p > 0 for p in listing.prices_60_days):
            return True
        return False

    @staticmethod
    async def _warmup_browser(
        browser_service: BrowserService,
        proxy: ProxyConfig | None,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
    ) -> tuple[bool, ProxyConfig | None]:
        """Прогревает браузер с retry и возможной заменой прокси.

        Выполняет до _MAX_WARMUP_ATTEMPTS попыток навигации на главную
        страницу sutochno.ru. При каждой неудачной попытке:
        1. Ждёт _WARMUP_RETRY_DELAY_SECONDS.
        2. Если есть proxy_service — проверяет текущую прокси.
        3. Если прокси мертва — ищет замену, останавливает старый браузер
           и запускает новый с новой прокси.

        Args:
            browser_service: Экземпляр браузера (уже запущенный).
            proxy: Текущая прокси воркера (может быть None).
            worker_idx: Номер воркера (для логов).
            all_proxies: Полный список прокси всех воркеров (для исключения).
            proxy_service: Сервис прокси (опциональный).

        Returns:
            Кортеж (success, active_proxy):
            - success=True если прогрев удался.
            - active_proxy — прокси, с которой браузер в итоге работает
              (может отличаться от исходной, если была замена).
        """
        current_proxy = proxy
        in_use_by_others = [p for p in all_proxies if p != proxy]

        for attempt in range(1, _MAX_WARMUP_ATTEMPTS + 1):
            try:
                logger.debug(
                    "прогрев_попытка",
                    step=f"воркер={worker_idx}, попытка={attempt}/{_MAX_WARMUP_ATTEMPTS}, "
                         f"прокси={current_proxy or 'без_прокси'}",
                )

                await browser_service.navigate("https://sutochno.ru")
                await browser_service.scroll_page()
                await asyncio.sleep(10)

                logger.info(
                    "воркер_прогрет",
                    step=f"воркер={worker_idx}"
                         + (f", попытка={attempt}" if attempt > 1 else ""),
                )
                return (True, current_proxy)

            except Exception as e:
                logger.warning(
                    "прогрев_не_удался",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"воркер={worker_idx}, попытка={attempt}/{_MAX_WARMUP_ATTEMPTS}",
                )

                # Последняя попытка — не тратим время на замену
                if attempt >= _MAX_WARMUP_ATTEMPTS:
                    break

                # Пауза перед retry
                await asyncio.sleep(_WARMUP_RETRY_DELAY_SECONDS)

                # Проверяем/заменяем прокси если есть proxy_service
                if current_proxy is not None and proxy_service is not None:
                    is_current_ok = await proxy_service.check_single_proxy(
                        current_proxy
                    )

                    if not is_current_ok:
                        logger.warning(
                            "прогрев_прокси_не_работает_ищем_замену",
                            step=f"воркер={worker_idx}, прокси={current_proxy}",
                        )

                        replacement = await proxy_service.get_replacement_proxy(
                            current_proxy=current_proxy,
                            in_use_proxies=in_use_by_others,
                        )

                        if replacement is not None:
                            logger.info(
                                "прогрев_замена_прокси",
                                step=f"воркер={worker_idx}, "
                                     f"старая={current_proxy}, "
                                     f"новая={replacement}",
                            )

                            # Останавливаем текущий браузер и запускаем новый
                            try:
                                await browser_service.stop()
                            except Exception as stop_err:
                                logger.warning(
                                    "прогрев_ошибка_остановки",
                                    error=str(stop_err),
                                    step=f"воркер={worker_idx}",
                                )

                            current_proxy = replacement
                            in_use_by_others = [
                                p for p in all_proxies if p != current_proxy
                            ]

                            try:
                                await browser_service.start(proxy=current_proxy)
                            except Exception as start_err:
                                logger.error(
                                    "прогрев_ошибка_запуска_нового_браузера",
                                    error=str(start_err),
                                    step=f"воркер={worker_idx}",
                                )
                                return (False, current_proxy)

                        else:
                            logger.warning(
                                "прогрев_замена_не_найдена",
                                step=f"воркер={worker_idx}, "
                                     f"пробуем_повторно_с_текущей",
                            )

        logger.error(
            "прогрев_все_попытки_исчерпаны",
            step=f"воркер={worker_idx}, попыток={_MAX_WARMUP_ATTEMPTS}",
        )
        return (False, current_proxy)

    @staticmethod
    async def _stats_logger(controller: ConcurrencyController) -> None:
        """Фоновая задача — периодическое логирование статистики контроллера.

        Выводит текущее состояние адаптации каждые _STATS_LOG_INTERVAL_SECONDS.
        Завершается при отмене задачи (CancelledError).

        Args:
            controller: Глобальный контроллер параллелизма.
        """
        try:
            while True:
                await asyncio.sleep(_STATS_LOG_INTERVAL_SECONDS)
                controller.log_stats()
        except asyncio.CancelledError:
            # Финальный вывод статистики при завершении
            controller.log_stats()

    @staticmethod
    async def enrich_listings_parallel(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
        concurrency_controller: ConcurrencyController | None = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Все воркеры стартуют с задержкой _WORKER_START_DELAY_SECONDS
        между ними, но работают параллельно. Реальный параллелизм
        контролируется ConcurrencyController — воркеры запрашивают
        разрешение (acquire) перед каждой карточкой.

        При падении воркера с исключением его карточки считаются
        необработанными. После основного раунда необработанные карточки
        перераспределяются между оставшимися рабочими прокси и запускаются
        повторно (до _MAX_PARALLEL_RETRY_ROUNDS раундов).

        Карточки с установленным enrichment_skip_reason (object_not_found,
        min_nights_exceeded, page_elements_not_found) мгновенно исключаются
        из retry-раундов — это экономит время и запросы на объявлениях,
        которые принципиально невозможно обогатить.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.
            proxy_service: Сервис прокси с заполненным пулом рабочих прокси
                (опциональный). Передаётся в воркеры для проверки/замены.
            concurrency_controller: Глобальный контроллер параллелизма
                (опциональный). Если не передан — создаётся внутри.

        Returns:
            Список обогащённых карточек.
        """
        from src.services.proxy_service import ProxyService as ProxyServiceClass

        # ── Статический расчёт безопасного количества воркеров ──
        memory_monitor = MemoryMonitor(
            memory_limit_mb=settings.memory_limit_mb,
            max_tabs=settings.max_tabs,
        )

        requested_workers = len(proxies)
        safe_workers = memory_monitor.calculate_safe_workers(requested_workers)

        # Если безопасное количество меньше запрошенного — сокращаем
        active_proxies = proxies[:safe_workers]

        chunks = ProxyServiceClass.distribute_listings(listings, len(active_proxies))

        # ── Создание или использование контроллера параллелизма ──
        controller = concurrency_controller
        if controller is None:
            # Автоматический расчёт ceiling и start
            ceiling = settings.concurrency_max if settings.concurrency_max > 0 else (
                len(active_proxies) * settings.max_tabs
            )
            floor = settings.concurrency_min
            start = settings.concurrency_start if settings.concurrency_start > 0 else None

            controller = ConcurrencyController(
                floor=floor,
                ceiling=ceiling,
                start=start,
            )

        logger.info(
            "параллельная_обработка",
            total=len(listings),
            step=f"прокси={len(active_proxies)}"
                 f"{f' (ограничено_по_ram из {requested_workers})' if safe_workers < requested_workers else ''}"
                 f", вкладок_на_прокси={settings.max_tabs}"
                 f", контроллер: floor={controller.floor}"
                 f", ceiling={controller.ceiling}"
                 f", start={controller.current_limit}",
        )

        # ── Запуск фонового мониторинга RAM ──
        await memory_monitor.start_monitoring()

        # ── Запуск фонового логирования статистики контроллера ──
        stats_task = asyncio.create_task(
            EnrichStrategies._stats_logger(controller),
            name="stats-logger",
        )

        parallel_start = time.perf_counter()

        # Отслеживаем прокси, на которых воркеры упали (исключаем из retry)
        failed_proxies: set[str] = set()

        try:
            # ── Основной раунд: запуск всех воркеров с задержкой ──
            worker_configs: list[tuple[int, list[RawListing], ProxyConfig]] = [
                (idx, chunk, proxy)
                for idx, (chunk, proxy) in enumerate(
                    zip(chunks, active_proxies), start=1
                )
            ]

            all_tasks = await EnrichStrategies._launch_workers(
                worker_configs=worker_configs,
                settings=settings,
                active_proxies=active_proxies,
                proxy_service=proxy_service,
                memory_monitor=memory_monitor,
                controller=controller,
            )

            # Ожидаем завершения ВСЕХ воркеров параллельно
            results = await asyncio.gather(*all_tasks, return_exceptions=True)

            # Обрабатываем результаты основного раунда
            all_enriched, worker_stats, browsers_to_stop, failed_proxies = (
                EnrichStrategies._process_worker_results(
                    results, worker_configs, active_proxies
                )
            )

            # Останавливаем браузеры основного раунда
            await EnrichStrategies._stop_browsers(browsers_to_stop)

            # ── Retry-раунды для необработанных карточек ──
            for retry_round in range(1, _MAX_PARALLEL_RETRY_ROUNDS + 1):
                # Собираем необработанные карточки из всех чанков.
                # Метод _is_enriched теперь исключает карточки с
                # enrichment_skip_reason — они не попадают в retry.
                unenriched = [
                    l for l in listings
                    if not EnrichStrategies._is_enriched(l)
                ]

                # Диагностика: сколько карточек мгновенно пропущено
                # по фатальной причине (object_not_found, min_nights_exceeded).
                # Это даёт видимость экономии времени и запросов к API.
                skipped_by_reason = sum(
                    1 for l in listings
                    if l.enrichment_skip_reason is not None
                )

                if skipped_by_reason > 0:
                    # Разбивка по причинам для более информативного лога
                    reasons_breakdown: dict[str, int] = {}
                    for l in listings:
                        if l.enrichment_skip_reason is not None:
                            reason = l.enrichment_skip_reason
                            reasons_breakdown[reason] = (
                                reasons_breakdown.get(reason, 0) + 1
                            )
                    breakdown_str = ", ".join(
                        f"{k}={v}" for k, v in sorted(reasons_breakdown.items())
                    )
                    logger.info(
                        "retry_карточки_исключены_по_skip_reason",
                        step=f"раунд={retry_round}, пропущено={skipped_by_reason} "
                             f"({breakdown_str})",
                    )

                if not unenriched:
                    logger.info(
                        "все_карточки_обогащены_retry_не_нужен",
                    )
                    break

                # Отбираем прокси, которые НЕ упали в предыдущих раундах
                retry_proxies = [
                    p for p in active_proxies
                    if str(p) not in failed_proxies
                ]

                if not retry_proxies:
                    logger.warning(
                        "нет_рабочих_прокси_для_retry",
                        step=f"упавших_прокси={len(failed_proxies)}, "
                             f"необработано={len(unenriched)}",
                    )
                    break

                logger.info(
                    "retry_раунд_упавших_воркеров",
                    step=f"раунд={retry_round}/{_MAX_PARALLEL_RETRY_ROUNDS}, "
                         f"необработано={len(unenriched)}, "
                         f"прокси_для_retry={len(retry_proxies)}",
                )

                # Перераспределяем необработанные карточки
                retry_chunks = ProxyServiceClass.distribute_listings(
                    unenriched, len(retry_proxies)
                )

                retry_configs: list[tuple[int, list[RawListing], ProxyConfig]] = [
                    (100 * retry_round + idx, chunk, proxy)
                    for idx, (chunk, proxy) in enumerate(
                        zip(retry_chunks, retry_proxies), start=1
                    )
                ]

                # Пауза перед retry — даём сети стабилизироваться
                await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                retry_tasks = await EnrichStrategies._launch_workers(
                    worker_configs=retry_configs,
                    settings=settings,
                    active_proxies=retry_proxies,
                    proxy_service=proxy_service,
                    memory_monitor=memory_monitor,
                    controller=controller,
                )

                retry_results = await asyncio.gather(
                    *retry_tasks, return_exceptions=True
                )

                # Обрабатываем результаты retry
                retry_enriched, retry_stats, retry_browsers, retry_failed = (
                    EnrichStrategies._process_worker_results(
                        retry_results, retry_configs, retry_proxies
                    )
                )

                all_enriched.extend(retry_enriched)
                worker_stats.extend(retry_stats)
                failed_proxies.update(retry_failed)

                # Останавливаем браузеры retry-раунда
                await EnrichStrategies._stop_browsers(retry_browsers)

                logger.info(
                    "retry_раунд_завершён",
                    step=f"раунд={retry_round}, "
                         f"дообогащено={len(retry_enriched)}, "
                         f"упало_прокси={len(retry_failed)}",
                )

        finally:
            # ── ВАЖНО: сначала останавливаем stats_task ──
            # stats_task работает в бесконечном цикле. Если _stop_browsers
            # зависнет — stats_task будет печатать статистику вечно,
            # создавая иллюзию «программа работает». Отменяем ДО остановки
            # браузеров — так при зависании остановки программа хотя бы
            # замолчит и станет очевидно, что она зависла.
            stats_task.cancel()
            try:
                await stats_task
            except asyncio.CancelledError:
                pass

            # Гарантированная остановка мониторинга RAM — даже при исключениях
            await memory_monitor.stop_monitoring()

        parallel_elapsed = time.perf_counter() - parallel_start

        # ── Итоговая сводка ──
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

        # Финальная статистика контроллера
        controller.log_stats()

        logger.info(
            "параллельная_обработка_завершена",
            total=len(all_enriched),
        )

        return all_enriched

    @staticmethod
    async def _launch_workers(
        worker_configs: list[tuple[int, list[RawListing], ProxyConfig]],
        settings: "any",  # type: ignore[name-defined]
        active_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None",
        memory_monitor: MemoryMonitor,
        controller: ConcurrencyController,
    ) -> list[asyncio.Task]:
        """Создаёт и запускает все воркеры с задержкой между стартами.

        Все воркеры стартуют последовательно с задержкой
        _WORKER_START_DELAY_SECONDS. После создания — работают параллельно.
        Реальный параллелизм контролируется ConcurrencyController.

        Args:
            worker_configs: Список (worker_idx, chunk, proxy) для каждого воркера.
            settings: Настройки приложения.
            active_proxies: Полный список активных прокси (для исключения занятых).
            proxy_service: Сервис прокси (опциональный).
            memory_monitor: Монитор RAM.
            controller: Глобальный контроллер параллелизма.

        Returns:
            Список запущенных asyncio.Task.
        """
        all_tasks: list[asyncio.Task] = []

        logger.info(
            "запуск_воркеров",
            step=f"всего={len(worker_configs)}, "
                 f"задержка_между_стартами={_WORKER_START_DELAY_SECONDS}с",
        )

        for i, (worker_idx, chunk, proxy) in enumerate(worker_configs):
            # Задержка перед запуском каждого воркера (кроме первого)
            if i > 0:
                await asyncio.sleep(_WORKER_START_DELAY_SECONDS)

            task = asyncio.create_task(
                EnrichStrategies._worker(
                    settings, chunk, proxy, worker_idx,
                    active_proxies, proxy_service, memory_monitor,
                    controller,
                ),
                name=f"worker-{worker_idx}",
            )
            all_tasks.append(task)

        logger.info(
            "все_воркеры_запущены",
            step=f"всего_задач={len(all_tasks)}",
        )

        return all_tasks

    @staticmethod
    def _process_worker_results(
        results: list,
        worker_configs: list[tuple[int, list[RawListing], ProxyConfig]],
        active_proxies: list[ProxyConfig],
    ) -> tuple[
        list[RawListing],
        list[tuple[int, int, float]],
        list[tuple[BrowserService, int]],
        set[str],
    ]:
        """Обрабатывает результаты завершённых воркеров.

        Разделяет успешные и упавшие воркеры. Для упавших — запоминает
        прокси, чтобы исключить их из retry-раундов.

        Args:
            results: Результаты asyncio.gather (tuple или BaseException).
            worker_configs: Конфигурации воркеров (для определения прокси упавших).
            active_proxies: Список активных прокси.

        Returns:
            Кортеж:
            - all_enriched: карточки из успешных воркеров.
            - worker_stats: статистика (worker_idx, cards, duration).
            - browsers_to_stop: браузеры для остановки.
            - failed_proxies: прокси упавших воркеров (строковое представление).
        """
        all_enriched: list[RawListing] = []
        worker_stats: list[tuple[int, int, float]] = []
        browsers_to_stop: list[tuple[BrowserService, int]] = []
        failed_proxies: set[str] = set()

        for idx, result in enumerate(results):
            worker_idx = worker_configs[idx][0] if idx < len(worker_configs) else idx + 1
            worker_proxy = worker_configs[idx][2] if idx < len(worker_configs) else None

            if isinstance(result, BaseException):
                logger.warning(
                    "воркер_завершился_с_ошибкой",
                    error=str(result),
                    error_type=type(result).__name__,
                    step=f"воркер={worker_idx}",
                )
                # Помечаем прокси этого воркера как упавшую
                if worker_proxy is not None:
                    failed_proxies.add(str(worker_proxy))

            elif isinstance(result, tuple) and len(result) == 3:
                enriched_list, duration, browser_svc = result
                all_enriched.extend(enriched_list)
                worker_stats.append((worker_idx, len(enriched_list), duration))
                browsers_to_stop.append((browser_svc, worker_idx))

        return all_enriched, worker_stats, browsers_to_stop, failed_proxies

    @staticmethod
    async def _stop_browsers(
        browsers_to_stop: list[tuple[BrowserService, int]],
    ) -> None:
        """Останавливает все браузеры из списка параллельно с жёстким таймаутом.

        Использует asyncio.wait вместо asyncio.wait_for + asyncio.gather.
        Причина: Playwright browser.close() может зависнуть навечно на
        мёртвой прокси (CDP-сессия не отвечает). asyncio.wait_for отменяет
        внутреннюю задачу, но CancelledError не проходит через native-код
        Playwright — программа зависает навсегда.

        asyncio.wait с таймаутом корректно разделяет задачи на завершённые
        (done) и зависшие (pending). Зависшие задачи отменяются и
        игнорируются — процесс Chromium завершится вместе с интерпретатором.

        Args:
            browsers_to_stop: Список (browser_service, worker_idx).
        """
        if not browsers_to_stop:
            return

        total = len(browsers_to_stop)
        logger.info(
            "остановка_прокси_браузеров",
            total=total,
            step=f"параллельно, глобальный_таймаут={_STOP_BROWSERS_GLOBAL_TIMEOUT}с",
        )

        stop_start = time.perf_counter()

        # Создаём реальные Task-объекты — asyncio.wait требует задачи, не корутины
        stop_tasks: list[asyncio.Task] = [
            asyncio.create_task(
                safe_stop_browser(browser_svc, w_idx),
                name=f"stop-browser-{w_idx}",
            )
            for browser_svc, w_idx in browsers_to_stop
        ]

        # asyncio.wait возвращает (done, pending) и НЕ блокируется навечно
        done, pending = await asyncio.wait(
            stop_tasks,
            timeout=_STOP_BROWSERS_GLOBAL_TIMEOUT,
        )

        elapsed = time.perf_counter() - stop_start

        if not pending:
            # Все браузеры остановились в срок
            logger.info(
                "все_прокси_браузеры_остановлены",
                total=total,
                step=f"время={format_duration(elapsed)}",
            )
        else:
            # Часть браузеров зависла — отменяем и продолжаем
            logger.warning(
                "остановка_браузеров_превысила_таймаут",
                step=f"завершено={len(done)}, зависло={len(pending)}, "
                     f"время={format_duration(elapsed)}, "
                     f"лимит={_STOP_BROWSERS_GLOBAL_TIMEOUT}с, "
                     f"продолжаем_дальше=да",
            )

            # Отменяем зависшие задачи — это реальные Task, cancel() сработает
            for task in pending:
                task.cancel()

            # Даём короткое время на обработку отмены (не ждём долго)
            if pending:
                await asyncio.wait(pending, timeout=3.0)

    @staticmethod
    async def _worker(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxy: ProxyConfig,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
        memory_monitor: MemoryMonitor | None = None,
        controller: ConcurrencyController | None = None,
    ) -> tuple[list[RawListing], float, BrowserService]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер.

        Перед каждой карточкой запрашивает разрешение у контроллера
        параллелизма (acquire). Это обеспечивает глобальное ограничение
        нагрузки на сайт — если контроллер снизил лимит, воркер ждёт.

        Этап прогрева защищён retry-циклом (_warmup_browser): при ошибках
        навигации на главную страницу выполняется до _MAX_WARMUP_ATTEMPTS
        попыток с возможной заменой прокси. Если прогрев не удался —
        воркер завершается с пустым результатом (карточки подхватит retry-раунд).

        При срабатывании монитора соединения (во время обработки карточек)
        выполняет перезапуск браузера с проверкой текущей прокси. Если текущая
        мертва — ищет замену из пула proxy_service.

        При срабатывании монитора памяти (should_reduce_workers) воркер
        досрочно завершает работу, возвращая уже обработанные карточки.

        Args:
            settings: Настройки приложения.
            listings: Порция карточек для этого воркера.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).
            all_proxies: Полный список прокси всех воркеров (для исключения занятых).
            proxy_service: Сервис прокси с заполненным пулом (опциональный).
            memory_monitor: Монитор RAM (опциональный). Общий для всех воркеров.
            controller: Глобальный контроллер параллелизма (опциональный).

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

            # ── Прогрев с retry и возможной заменой прокси ──
            warmup_ok, current_proxy = await EnrichStrategies._warmup_browser(
                browser_service=browser_service,
                proxy=current_proxy,
                worker_idx=worker_idx,
                all_proxies=all_proxies,
                proxy_service=proxy_service,
            )

            if not warmup_ok:
                # Прогрев не удался после всех попыток — воркер завершается.
                # Карточки остаются необработанными → подхватит retry-раунд.
                worker_elapsed = time.perf_counter() - worker_start
                logger.warning(
                    "воркер_не_прогрелся_завершение",
                    step=f"воркер={worker_idx}, время={format_duration(worker_elapsed)}",
                )
                return (listings, worker_elapsed, browser_service)

            # Обновляем список исключений после возможной замены прокси
            in_use_by_others = [p for p in all_proxies if p != current_proxy]

            from src.services.listing_service import ListingService

            # ── ИСПРАВЛЕНИЕ: пробрасываем controller в ListingService ──
            # Без этого PageLoader и HybridStrategy внутри ListingService
            # не получают контроллер и не вызывают report_success/failure.
            listing_service = ListingService(
                settings=settings,
                browser_service=browser_service,
                monitor=monitor,
                concurrency_controller=controller,
            )

            # ── Обработка карточек по одной с контролем параллелизма ──
            # Фильтруем на входе — если карточка уже помечена как
            # необогащаемая (например, в предыдущем раунде), она сюда
            # не попадёт. Это дополнительная защита на уровне воркера.
            remaining = [
                l for l in listings
                if not EnrichStrategies._is_enriched(l)
            ]
            restart_count = 0

            while remaining and restart_count <= _MAX_RESTARTS_PER_WORKER:
                # ── Проверка монитора памяти перед каждой итерацией ──
                if memory_monitor is not None and memory_monitor.should_reduce_workers:
                    enriched_in_worker = sum(
                        1 for l in listings if EnrichStrategies._is_enriched(l)
                    )
                    logger.warning(
                        "воркер_остановлен_по_ram",
                        step=f"воркер={worker_idx}, "
                             f"обогащено={enriched_in_worker}, "
                             f"осталось={len(remaining)}",
                    )
                    break

                await monitor.reset()

                # Обрабатываем карточки порциями по max_tabs вкладок.
                # Перед каждой карточкой — acquire() у контроллера.
                await EnrichStrategies._process_worker_cards(
                    listings=remaining,
                    listing_service=listing_service,
                    browser_service=browser_service,
                    monitor=monitor,
                    controller=controller,
                    settings=settings,
                    worker_idx=worker_idx,
                )

                # ── Проверка памяти после обработки порции ──
                if memory_monitor is not None and memory_monitor.should_reduce_workers:
                    enriched_in_worker = sum(
                        1 for l in listings if EnrichStrategies._is_enriched(l)
                    )
                    logger.warning(
                        "воркер_остановлен_по_ram_после_порции",
                        step=f"воркер={worker_idx}, "
                             f"обогащено={enriched_in_worker}, "
                             f"осталось={len(remaining)}",
                    )
                    break

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

                    # Гарантированная остановка старого браузера
                    old_browser = browser_service
                    try:
                        await old_browser.stop()
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

                    # Создаём НОВЫЙ browser_service
                    try:
                        browser_service = BrowserService(settings=settings)
                        await browser_service.start(proxy=current_proxy)

                        warmup_ok, current_proxy = (
                            await EnrichStrategies._warmup_browser(
                                browser_service=browser_service,
                                proxy=current_proxy,
                                worker_idx=worker_idx,
                                all_proxies=all_proxies,
                                proxy_service=proxy_service,
                            )
                        )

                        if not warmup_ok:
                            logger.error(
                                "воркер_перезапуск_прогрев_не_удался",
                                step=f"воркер={worker_idx}",
                            )
                            break

                        in_use_by_others = [
                            p for p in all_proxies if p != current_proxy
                        ]

                        # ── ИСПРАВЛЕНИЕ: пробрасываем controller при пересоздании ──
                        monitor = ConnectionMonitor()
                        listing_service = ListingService(
                            settings=settings,
                            browser_service=browser_service,
                            monitor=monitor,
                            concurrency_controller=controller,
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

                    # Пересчитываем необработанные — карточки с
                    # skip_reason сюда не попадают
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

    @staticmethod
    async def _process_worker_cards(
        listings: list[RawListing],
        listing_service: "ListingService",
        browser_service: BrowserService,
        monitor: ConnectionMonitor,
        controller: ConcurrencyController | None,
        settings: "any",  # type: ignore[name-defined]
        worker_idx: int,
    ) -> None:
        """Обрабатывает карточки воркера порциями по max_tabs вкладок.

        Каждая вкладка перед обработкой карточки запрашивает разрешение
        у контроллера (acquire). Это обеспечивает глобальный контроль:
        даже если у воркера max_tabs=5, реально одновременно будет
        работать только столько вкладок, сколько разрешает контроллер.

        Args:
            listings: Карточки для обработки (in-place модификация).
            listing_service: Сервис обогащения.
            browser_service: Сервис браузера.
            monitor: Локальный монитор соединения.
            controller: Глобальный контроллер параллелизма.
            settings: Настройки приложения.
            worker_idx: Номер воркера (для логов).
        """
        max_tabs = settings.max_tabs
        tab_delay_ms = settings.tab_delay_ms
        total = len(listings)
        processed_count = 0

        for chunk_start in range(0, total, max_tabs):
            # Проверяем монитор перед каждой порцией
            if monitor.should_skip():
                logger.debug(
                    "воркер_порция_пропущена",
                    step=f"воркер={worker_idx}, позиция={chunk_start}",
                )
                break

            chunk = listings[chunk_start : chunk_start + max_tabs]

            # Создаём задачи для текущей порции
            tasks = [
                EnrichStrategies._process_worker_one_tab(
                    listing=listing,
                    listing_service=listing_service,
                    browser_service=browser_service,
                    monitor=monitor,
                    controller=controller,
                    tab_delay_ms=tab_delay_ms,
                    tab_index=idx,
                    worker_idx=worker_idx,
                )
                for idx, listing in enumerate(chunk)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Логируем ошибки
            for idx, result in enumerate(results):
                if isinstance(result, BaseException):
                    listing_id = chunk[idx].external_id if idx < len(chunk) else "?"
                    logger.warning(
                        "воркер_ошибка_вкладки",
                        error=str(result),
                        error_type=type(result).__name__,
                        step=f"воркер={worker_idx}, id={listing_id}",
                    )

            processed_count += len(chunk)

            # Закрываем вкладки после порции
            await browser_service.close_all_pages()

            # Логируем прогресс воркера после каждой порции
            if not monitor.should_skip():
                logger.info(
                    "прогресс_воркера",
                    current=min(processed_count, total),
                    total=total,
                    step=f"воркер={worker_idx}",
                )

    @staticmethod
    async def _process_worker_one_tab(
        listing: RawListing,
        listing_service: "ListingService",
        browser_service: BrowserService,
        monitor: ConnectionMonitor,
        controller: ConcurrencyController | None,
        tab_delay_ms: int,
        tab_index: int,
        worker_idx: int,
    ) -> None:
        """Обрабатывает одну карточку в вкладке воркера с контролем параллелизма.

        Args:
            listing: Объявление для обогащения.
            listing_service: Сервис обогащения.
            browser_service: Сервис браузера.
            monitor: Локальный монитор соединения.
            controller: Глобальный контроллер параллелизма.
            tab_delay_ms: Задержка между вкладками (мс).
            tab_index: Порядковый номер вкладки в порции.
            worker_idx: Номер воркера (для логов).
        """
        page: Page | None = None

        if monitor.should_skip():
            return

        # Задержка между стартом вкладок внутри порции
        if tab_index > 0:
            await asyncio.sleep(tab_delay_ms / 1000.0)

        if monitor.should_skip():
            return

        # ── Глобальный контроль: ждём разрешения от контроллера ──
        if controller is not None:
            await controller.acquire()

        try:
            page = await browser_service.create_page()
            await listing_service.enrich_listing(listing, page)
        finally:
            if controller is not None:
                controller.release()
            if page is not None:
                await browser_service.close_page(page)
