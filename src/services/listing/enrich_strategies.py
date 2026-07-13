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

# Максимальное количество попыток прогрева браузера.
# Прогрев теперь проверяет только что браузер жив (is_alive),
# без навигации на главную страницу.
_MAX_WARMUP_ATTEMPTS: int = 2

# Пауза между попытками прогрева (секунды)
_WARMUP_RETRY_DELAY_SECONDS: float = 5.0

# Задержка между стартом воркеров (секунды).
# Предотвращает одновременный вызов playwright.chromium.launch()
# и параллельный прогрев на всех прокси сразу.
_WORKER_START_DELAY_SECONDS: float = 2.0

# Максимальное количество retry-раундов для упавших воркеров.
_MAX_PARALLEL_RETRY_ROUNDS: int = 1

# Интервал логирования статистики контроллера (секунды).
_STATS_LOG_INTERVAL_SECONDS: float = 60.0

# Общий таймаут этапа остановки всех прокси-браузеров (секунды).
_STOP_BROWSERS_GLOBAL_TIMEOUT: float = 100.0

# ── Детектор стагнации (watchdog) ──

# Максимальное время без прогресса обогащения (секунды).
# Увеличено до 10 минут — при множестве «тяжёлых» карточек (busy=busy)
# скользящее окно (60 запросов × 0.5с пауза) занимает до 5 минут.
# Если 73 воркера одновременно обрабатывают такие карточки, прогресс
# по завершённым = 0 на протяжении всего окна обработки.
# Watchdog теперь дополнительно учитывает активность контроллера —
# если API-запросы идут, стагнации нет даже при нулевом прогрессе.
_STAGNATION_TIMEOUT_SECONDS: float = 600.0

# Интервал проверки прогресса watchdog'ом (секунды).
_STAGNATION_CHECK_INTERVAL_SECONDS: float = 60.0

# Время ожидания завершения задач после сигнала watchdog'а (секунды).
# После отмены задач watchdog'ом основной код ждёт ещё столько секунд,
# чтобы задачи, уже находящиеся в процессе graceful shutdown, успели
# корректно завершиться. По истечении — продолжает pipeline с тем, что есть.
# Это предотвращает бесконечное зависание на некансeлируемых операциях
# Playwright (page.evaluate, page.goto), которые игнорируют asyncio cancel
# и завершаются только по собственному таймауту (до 300 секунд).
_WATCHDOG_GRACE_PERIOD_SECONDS: float = 30.0

# Максимальное количество одновременных проверок упавших прокси при retry.
# Ограничиваем, чтобы не перегружать сервер запуском 66 Chromium сразу.
_MAX_CONCURRENT_PROXY_CHECKS: int = 5


class EnrichStrategies:
    """Параллельные стратегии обогащения карточек.

    Инкапсулирует:
    - enrich_listings_tabbed: параллельная обработка через вкладки.
    - enrich_listings_parallel: параллельная обработка через прокси-браузеры.
    - _worker: воркер для одного прокси-браузера.
    - _stagnation_watchdog: фоновый детектор зависания.
    - _await_tasks_with_watchdog: защита от бесконечного зависания.
    - _revive_failed_proxies: проверка и воскрешение упавших прокси.
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

        remaining = [l for l in listings if not self._is_enriched(l)]
        restart_count = 0

        while remaining and restart_count <= _MAX_RESTARTS_PER_WORKER:
            monitor = self._listing_service.monitor
            if monitor is None:
                monitor = ConnectionMonitor()
                self._listing_service.monitor = monitor

            await monitor.reset()

            await self._process_batch_with_tabs(
                remaining, max_tabs, tab_delay_ms, monitor
            )

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

                success = await self._restart_browser_with_proxy_check()

                if not success:
                    logger.error(
                        "перезапуск_браузера_не_удался",
                        step="обработка_прекращена",
                    )
                    break

                await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                remaining = [l for l in listings if not self._is_enriched(l)]

                logger.info(
                    "продолжение_после_перезапуска",
                    step=f"осталось={len(remaining)}, всего={total}",
                )
            else:
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
        """Обрабатывает пачку карточек порциями по max_tabs вкладок."""
        total = len(listings)
        processed_count = 0

        for chunk_start in range(0, total, max_tabs):
            if monitor.should_skip():
                logger.debug(
                    "порция_пропущена_перезапуск",
                    step=f"с_позиции={chunk_start}",
                )
                break

            chunk = listings[chunk_start : chunk_start + max_tabs]

            tasks = [
                self._process_one_tab(
                    listing, tab_delay_ms, monitor, idx
                )
                for idx, listing in enumerate(chunk)
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

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
        """Обрабатывает одну карточку в отдельной вкладке."""
        page: Page | None = None

        if monitor.should_skip():
            logger.debug(
                "вкладка_пропущена_перезапуск",
                step=f"id={listing.external_id}",
            )
            return

        if tab_index > 0:
            await asyncio.sleep(tab_delay_ms / 1000.0)

        if monitor.should_skip():
            return

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

        После перезапуска выполняет навигацию на главную для установки
        сессионных cookies — без них фронтенд карточки не отправляет
        API-запросы и токен не перехватывается.

        Returns:
            True если браузер успешно перезапущен, False — если не удалось.
        """
        current_proxy = self._browser._proxy  # noqa: SLF001

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

        active_proxy = current_proxy

        if current_proxy is not None and self._proxy_service is not None:
            logger.info(
                "проверка_текущей_прокси",
                step=str(current_proxy),
            )

            is_current_working = await self._proxy_service.check_single_proxy(
                current_proxy, fast=True
            )

            if is_current_working:
                logger.info(
                    "текущая_прокси_работает_перезапуск_браузера",
                    step=str(current_proxy),
                )
                active_proxy = current_proxy
            else:
                logger.warning(
                    "текущая_прокси_не_работает_ищем_замену",
                    step=str(current_proxy),
                )

                replacement = await self._proxy_service.get_replacement_proxy(
                    current_proxy=current_proxy,
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

        try:
            await self._browser.start(proxy=active_proxy)

            # Навигация на главную — устанавливает сессионные cookies.
            # Без этого фронтенд карточки не инициализирует API-запросы.
            await self._browser.navigate("https://sutochno.ru")
            await asyncio.sleep(2)

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

        Args:
            listing: Объявление для проверки.

        Returns:
            True если карточка обогащена ИЛИ необогащаема (skip_reason установлен).
        """
        if listing.enrichment_skip_reason is not None:
            return True

        if listing.calendar_60_days and any(c == 1 for c in listing.calendar_60_days):
            return True
        if listing.prices_60_days and any(p > 0 for p in listing.prices_60_days):
            return True
        return False

    @staticmethod
    def _count_enriched(listings: list[RawListing]) -> int:
        """Подсчитывает количество обогащённых карточек в списке."""
        return sum(1 for l in listings if EnrichStrategies._is_enriched(l))

    @staticmethod
    async def _stagnation_watchdog(
        all_listings: list[RawListing],
        worker_tasks: list[asyncio.Task],
        stagnation_event: asyncio.Event,
        label: str = "основной",
        controller: ConcurrencyController | None = None,
    ) -> None:
        """Фоновый детектор зависания — отменяет воркеры при отсутствии прогресса.

        Стагнация определяется как отсутствие ОДНОВРЕМЕННО:
        - прогресса по завершённым карточкам (_count_enriched не растёт);
        - активности контроллера (total_successes + total_failures не растёт).

        Если хотя бы один из индикаторов показывает рост — воркеры активны,
        таймер стагнации сбрасывается. Это предотвращает ложные срабатывания
        при обработке «тяжёлых» карточек (busy=busy → скользящее окно 60 запросов),
        когда ни одна карточка не завершилась, но API-запросы идут активно.

        При обнаружении реальной стагнации:
        1. Отменяет все незавершённые задачи.
        2. Устанавливает stagnation_event — сигнал основному коду,
           что ожидание задач нужно прервать принудительно.

        Args:
            all_listings: Все карточки (для подсчёта прогресса).
            worker_tasks: Задачи воркеров для отмены.
            stagnation_event: Event для сигнализации основному коду.
            label: Метка раунда (для логов).
            controller: Контроллер параллелизма (для проверки активности).
        """
        last_enriched_count = EnrichStrategies._count_enriched(all_listings)
        last_progress_time = time.monotonic()

        # Начальное значение активности контроллера
        last_controller_activity: int = 0
        if controller is not None:
            stats = controller.stats
            last_controller_activity = (
                stats["total_successes"] + stats["total_failures"]
            )

        try:
            while True:
                await asyncio.sleep(_STAGNATION_CHECK_INTERVAL_SECONDS)

                # ── Проверка 1: прогресс по завершённым карточкам ──
                current_count = EnrichStrategies._count_enriched(all_listings)
                enrichment_progressed = current_count > last_enriched_count

                # ── Проверка 2: активность контроллера (API-запросы идут) ──
                controller_progressed = False
                current_controller_activity: int = 0
                if controller is not None:
                    stats = controller.stats
                    current_controller_activity = (
                        stats["total_successes"] + stats["total_failures"]
                    )
                    controller_progressed = (
                        current_controller_activity > last_controller_activity
                    )

                # ── Решение: есть ли прогресс? ──
                has_progress = enrichment_progressed or controller_progressed

                if has_progress:
                    # Прогресс есть — сбрасываем таймер стагнации
                    last_enriched_count = current_count
                    last_controller_activity = current_controller_activity
                    last_progress_time = time.monotonic()

                    logger.debug(
                        "watchdog_прогресс",
                        step=f"раунд={label}, обогащено={current_count}, "
                             f"всего={len(all_listings)}, "
                             f"активность_контроллера="
                             f"{'да' if controller_progressed else 'нет'}, "
                             f"новых_обогащений="
                             f"{'да' if enrichment_progressed else 'нет'}",
                    )
                else:
                    # Прогресса нет — считаем длительность стагнации
                    stagnation_duration = time.monotonic() - last_progress_time

                    logger.debug(
                        "watchdog_нет_прогресса",
                        step=f"раунд={label}, обогащено={current_count}, "
                             f"стагнация={format_duration(stagnation_duration)}, "
                             f"лимит={format_duration(_STAGNATION_TIMEOUT_SECONDS)}, "
                             f"активность_контроллера=нет",
                    )

                    if stagnation_duration >= _STAGNATION_TIMEOUT_SECONDS:
                        pending_tasks = [
                            t for t in worker_tasks if not t.done()
                        ]

                        logger.warning(
                            "watchdog_стагнация_обнаружена",
                            step=f"раунд={label}, "
                                 f"без_прогресса={format_duration(stagnation_duration)}, "
                                 f"обогащено={current_count}/{len(all_listings)}, "
                                 f"зависших_задач={len(pending_tasks)}",
                        )

                        for task in pending_tasks:
                            task.cancel()

                        if pending_tasks:
                            await asyncio.wait(pending_tasks, timeout=15.0)

                        logger.info(
                            "watchdog_воркеры_отменены",
                            step=f"раунд={label}, отменено={len(pending_tasks)}",
                        )

                        # Сигнализируем основному коду: стагнация обнаружена,
                        # не ждать завершения задач бесконечно.
                        stagnation_event.set()
                        return

        except asyncio.CancelledError:
            logger.debug(
                "watchdog_отменён_штатно",
                step=f"раунд={label}",
            )

    @staticmethod
    async def _await_tasks_with_watchdog(
        all_tasks: list[asyncio.Task],
        stagnation_event: asyncio.Event,
    ) -> None:
        """Ожидает завершения задач с защитой от бесконечного зависания.

        Если watchdog обнаружил стагнацию и установил stagnation_event —
        даёт задачам ограниченное время (_WATCHDOG_GRACE_PERIOD_SECONDS)
        на завершение, после чего прекращает ожидание и возвращает
        управление вызывающему коду.

        Args:
            all_tasks: Список задач воркеров.
            stagnation_event: Event от watchdog'а (сигнал стагнации).
        """
        if not all_tasks:
            return

        # Создаём задачу-наблюдатель за Event от watchdog'а
        async def _wait_for_stagnation() -> None:
            await stagnation_event.wait()

        stagnation_waiter = asyncio.create_task(
            _wait_for_stagnation(),
            name="stagnation-waiter",
        )

        # Создаём задачу ожидания всех воркеров
        async def _wait_all_tasks() -> None:
            await asyncio.wait(all_tasks)

        tasks_waiter = asyncio.create_task(
            _wait_all_tasks(),
            name="tasks-waiter",
        )

        # Ожидаем первое из двух событий:
        # - Все воркеры завершились штатно (tasks_waiter)
        # - Watchdog обнаружил стагнацию (stagnation_waiter)
        done, _pending = await asyncio.wait(
            [tasks_waiter, stagnation_waiter],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stagnation_waiter in done:
            # ── Watchdog сработал — задачи зависли ──
            logger.warning(
                "watchdog_grace_period_начат",
                step=f"ожидание={format_duration(_WATCHDOG_GRACE_PERIOD_SECONDS)}, "
                     f"задач={len(all_tasks)}",
            )

            # Даём задачам grace period для мягкого завершения
            still_pending = [t for t in all_tasks if not t.done()]
            if still_pending:
                await asyncio.wait(
                    still_pending, timeout=_WATCHDOG_GRACE_PERIOD_SECONDS
                )

            # Проверяем сколько задач так и не завершилось
            final_pending = [t for t in all_tasks if not t.done()]
            if final_pending:
                logger.warning(
                    "watchdog_grace_period_завершён_есть_зависшие",
                    step=f"зависших={len(final_pending)}, "
                         f"завершённых={len(all_tasks) - len(final_pending)}, "
                         f"продолжаем_pipeline=да",
                )
            else:
                logger.info(
                    "watchdog_grace_period_все_задачи_завершились",
                    step=f"задач={len(all_tasks)}",
                )

            # Отменяем tasks_waiter
            if not tasks_waiter.done():
                tasks_waiter.cancel()
                try:
                    await tasks_waiter
                except (asyncio.CancelledError, Exception):
                    pass
        else:
            # ── Все задачи завершились штатно — watchdog не сработал ──
            stagnation_waiter.cancel()
            try:
                await stagnation_waiter
            except asyncio.CancelledError:
                pass

    @staticmethod
    async def _warmup_browser(
        browser_service: BrowserService,
        proxy: ProxyConfig | None,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
    ) -> tuple[bool, ProxyConfig | None]:
        """Прогревает браузер навигацией на главную для установки cookies.

        Загружает sutochno.ru один раз — это устанавливает сессионные
        cookies и антибот-токены, без которых фронтенд карточки не
        инициализирует API-запросы (и токен не перехватывается).

        Без прокрутки и длительного ожидания — только навигация + 2 сек.
        Экономия ~8 секунд по сравнению с полным прогревом (scroll + 10 сек).

        Args:
            browser_service: Экземпляр браузера (уже запущенный).
            proxy: Текущая прокси воркера (может быть None).
            worker_idx: Номер воркера (для логов).
            all_proxies: Устаревший параметр, больше не используется —
                занятость прокси теперь централизованно отслеживается
                внутри ProxyService (claim_proxy/get_replacement_proxy)
                под блокировкой. Раньше каждый воркер вычислял "занятые"
                прокси локально из этого списка, и несколько воркеров
                могли одновременно выбрать один и тот же "свободный"
                прокси — это и вызывало каскадные сбои. Параметр оставлен
                для обратной совместимости вызовов.
            proxy_service: Сервис прокси (опциональный).

        Returns:
            Кортеж (success, active_proxy).
        """
        current_proxy = proxy

        for attempt in range(1, _MAX_WARMUP_ATTEMPTS + 1):
            try:
                logger.debug(
                    "прогрев_попытка",
                    step=f"воркер={worker_idx}, попытка={attempt}/{_MAX_WARMUP_ATTEMPTS}, "
                         f"прокси={current_proxy or 'без_прокси'}",
                )

                # Навигация на главную — устанавливает сессионные cookies.
                # Без этого фронтенд карточки не отправляет API-запросы
                # и токен не перехватывается через route interception.
                await browser_service.navigate("https://sutochno.ru")
                await asyncio.sleep(2)

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

                if attempt >= _MAX_WARMUP_ATTEMPTS:
                    break

                await asyncio.sleep(_WARMUP_RETRY_DELAY_SECONDS)

                if current_proxy is not None and proxy_service is not None:
                    is_current_ok = await proxy_service.check_single_proxy(
                        current_proxy, fast=True
                    )

                    if not is_current_ok:
                        logger.warning(
                            "прогрев_прокси_не_работает_ищем_замену",
                            step=f"воркер={worker_idx}, прокси={current_proxy}",
                        )

                        # get_replacement_proxy теперь сам атомарно резервирует
                        # кандидата (под блокировкой в ProxyService) и снимает
                        # current_proxy с пула рабочих — передавать список
                        # "занятых" прокси вручную больше не нужно.
                        replacement = await proxy_service.get_replacement_proxy(
                            current_proxy=current_proxy,
                        )

                        if replacement is not None:
                            logger.info(
                                "прогрев_замена_прокси",
                                step=f"воркер={worker_idx}, "
                                     f"старая={current_proxy}, "
                                     f"новая={replacement}",
                            )

                            try:
                                await browser_service.stop()
                            except Exception as stop_err:
                                logger.warning(
                                    "прогрев_ошибка_остановки",
                                    error=str(stop_err),
                                    step=f"воркер={worker_idx}",
                                )

                            current_proxy = replacement

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

        # Прогрев так и не удался с current_proxy — снимаем резерв и
        # удаляем её из пула рабочих, иначе она останется "занятой" в
        # ProxyService, но никем реально не используемой, и просто
        # выпадет из оборота до конца прогона.
        if current_proxy is not None and proxy_service is not None:
            await proxy_service.mark_dead(current_proxy)

        return (False, current_proxy)

    @staticmethod
    async def _stats_logger(controller: ConcurrencyController) -> None:
        """Фоновая задача — периодическое логирование статистики контроллера."""
        try:
            while True:
                await asyncio.sleep(_STATS_LOG_INTERVAL_SECONDS)
                controller.log_stats()
        except asyncio.CancelledError:
            controller.log_stats()

    @staticmethod
    async def _revive_failed_proxies(
        failed_proxy_configs: list[ProxyConfig],
        live_proxy_strings: set[str],
        proxy_service: "ProxyService | None",
        settings: "any",  # type: ignore[name-defined]
        all_proxies: list[ProxyConfig],
        retry_round: int,
    ) -> list[tuple[int, BrowserService, ProxyConfig]]:
        """Проверяет упавшие прокси и запускает браузеры для ожившых.

        Прокси, которые были забанены в основном раунде, могли
        разблокироваться к моменту retry. Эта функция проверяет их
        через proxy_service и для рабочих — запускает новые браузеры.

        Проверка выполняется через proxy_service.check_single_proxy()
        с ограничением параллелизма (_MAX_CONCURRENT_PROXY_CHECKS).

        Args:
            failed_proxy_configs: Прокси, на которых воркеры упали.
            live_proxy_strings: Строковые представления живых прокси
                (чтобы не проверять уже работающие).
            proxy_service: Сервис прокси (если None — возвращает пустой список).
            settings: Настройки приложения.
            all_proxies: Полный список прокси (для warmup).
            retry_round: Номер retry-раунда (для нумерации воркеров).

        Returns:
            Список кортежей (worker_idx, browser_service, proxy) для
            ожившых прокси с запущенными браузерами.
        """
        if not proxy_service or not failed_proxy_configs:
            return []

        # Исключаем прокси, которые уже живы (на случай дублей)
        to_check = [
            p for p in failed_proxy_configs
            if str(p) not in live_proxy_strings
        ]

        if not to_check:
            return []

        logger.info(
            "проверка_упавших_прокси",
            step=f"раунд={retry_round}, к_проверке={len(to_check)}",
        )

        # ── Проверяем прокси с ограничением параллелизма ──
        semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PROXY_CHECKS)

        async def _check_one(proxy: ProxyConfig) -> tuple[ProxyConfig, bool]:
            async with semaphore:
                is_ok = await proxy_service.check_single_proxy(proxy, fast=True)
                return (proxy, is_ok)

        check_tasks = [_check_one(p) for p in to_check]
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)

        revived_proxies: list[ProxyConfig] = []
        for result in check_results:
            if isinstance(result, tuple) and len(result) == 2:
                proxy, is_ok = result
                if is_ok:
                    revived_proxies.append(proxy)

        if not revived_proxies:
            logger.info(
                "упавшие_прокси_не_ожили",
                step=f"раунд={retry_round}, проверено={len(to_check)}",
            )
            return []

        logger.info(
            "упавшие_прокси_ожили",
            step=f"раунд={retry_round}, "
                 f"ожило={len(revived_proxies)} из {len(to_check)}",
        )

        # ── Запускаем браузеры для ожившых прокси ──
        revived_browsers: list[tuple[int, BrowserService, ProxyConfig]] = []

        for i, proxy in enumerate(revived_proxies):
            worker_idx = 200 * retry_round + i + 1

            # Резервируем прокси сразу после успешной проверки — до того,
            # как для неё поднимется браузер. Иначе параллельный
            # get_replacement_proxy в другом воркере мог бы схватить
            # ту же самую только что ожившую прокси.
            if proxy_service is not None:
                await proxy_service.claim_proxy(proxy)

            browser_service = BrowserService(settings=settings)

            try:
                await browser_service.start(proxy=proxy)

                warmup_ok, active_proxy = await EnrichStrategies._warmup_browser(
                    browser_service=browser_service,
                    proxy=proxy,
                    worker_idx=worker_idx,
                    all_proxies=all_proxies,
                    proxy_service=proxy_service,
                )

                if warmup_ok:
                    revived_browsers.append(
                        (worker_idx, browser_service, active_proxy or proxy)
                    )
                    logger.info(
                        "ожившая_прокси_браузер_готов",
                        step=f"воркер={worker_idx}, прокси={active_proxy or proxy}",
                    )
                else:
                    # Прогрев не удался — останавливаем, снимаем резерв
                    # и удаляем прокси из пула рабочих.
                    await safe_stop_browser(browser_service, worker_idx)
                    if proxy_service is not None:
                        await proxy_service.mark_dead(proxy)

            except Exception as e:
                logger.warning(
                    "ожившая_прокси_ошибка_запуска",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"воркер={worker_idx}, прокси={proxy}",
                )
                if proxy_service is not None:
                    await proxy_service.mark_dead(proxy)
                try:
                    await browser_service.stop()
                except Exception:
                    pass

            # Задержка между запусками браузеров
            if i < len(revived_proxies) - 1:
                await asyncio.sleep(_WORKER_START_DELAY_SECONDS)

        logger.info(
            "ожившие_прокси_итого",
            step=f"раунд={retry_round}, "
                 f"запущено_браузеров={len(revived_browsers)} "
                 f"из {len(revived_proxies)} ожившых",
        )

        return revived_browsers

    @staticmethod
    async def enrich_listings_parallel(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
        concurrency_controller: ConcurrencyController | None = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Переиспользование браузеров и воскрешение упавших прокси:
        После основного раунда браузеры успешных воркеров НЕ останавливаются.
        Упавшие прокси проверяются — те, что ожили (бан снят), получают
        новые браузеры. Необработанные карточки перераспределяются между
        всем объединённым пулом (живые + ожившие). Все браузеры
        останавливаются только после завершения всех retry-раундов.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек.
            proxies: Список рабочих прокси.
            proxy_service: Сервис прокси (опциональный).
            concurrency_controller: Глобальный контроллер параллелизма (опциональный).

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

        active_proxies = proxies[:safe_workers]

        # ── Резервируем стартовые прокси ДО запуска конкурентных воркеров ──
        # Без этого шага параллельный поиск замены (get_replacement_proxy),
        # вызываемый из разных воркеров сразу после старта, мог бы выбрать
        # прокси, которая на самом деле уже отдана другому воркеру статически
        # (просто ещё не успела провалить проверку).
        if proxy_service is not None:
            for p in active_proxies:
                await proxy_service.claim_proxy(p)

        chunks = ProxyServiceClass.distribute_listings(listings, len(active_proxies))

        # ── Создание или использование контроллера параллелизма ──
        controller = concurrency_controller
        if controller is None:
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
                 f", start={controller.current_limit}"
                 f", watchdog={format_duration(_STAGNATION_TIMEOUT_SECONDS)}"
                 f", grace_period={format_duration(_WATCHDOG_GRACE_PERIOD_SECONDS)}",
        )

        # ── Запуск фонового мониторинга RAM ──
        await memory_monitor.start_monitoring()

        # ── Запуск фонового логирования статистики контроллера ──
        stats_task = asyncio.create_task(
            EnrichStrategies._stats_logger(controller),
            name="stats-logger",
        )

        parallel_start = time.perf_counter()

        # Отслеживаем упавшие прокси: строковое представление → ProxyConfig
        failed_proxies: set[str] = set()
        failed_proxy_configs: list[ProxyConfig] = []

        # Фоновые задачи, которые нужно отменить в finally
        background_tasks: list[asyncio.Task] = [stats_task]

        # Все живые браузеры: worker_idx → (BrowserService, ProxyConfig | None).
        all_live_browsers: dict[int, tuple[BrowserService, ProxyConfig | None]] = {}

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

            # ── Собираем плоский список всех карточек для watchdog ──
            all_listings_flat: list[RawListing] = []
            for _, chunk, _ in worker_configs:
                all_listings_flat.extend(chunk)

            # ── Event для сигнализации watchdog → основной код ──
            stagnation_event = asyncio.Event()

            # ── Запуск watchdog'а — детектор стагнации ──
            watchdog_task = asyncio.create_task(
                EnrichStrategies._stagnation_watchdog(
                    all_listings=all_listings_flat,
                    worker_tasks=all_tasks,
                    stagnation_event=stagnation_event,
                    label="основной",
                    controller=controller,
                ),
                name="stagnation-watchdog",
            )
            background_tasks.append(watchdog_task)

            # ── Ожидаем завершения ВСЕХ воркеров (с защитой от зависания) ──
            await EnrichStrategies._await_tasks_with_watchdog(
                all_tasks, stagnation_event
            )

            # ── Отменяем watchdog ──
            if not watchdog_task.done():
                watchdog_task.cancel()
                try:
                    await watchdog_task
                except asyncio.CancelledError:
                    pass

            # Собираем результаты из завершённых задач
            results: list = []
            for task in all_tasks:
                if task.done():
                    exc = task.exception() if not task.cancelled() else None
                    if exc is not None:
                        results.append(exc)
                    elif task.cancelled():
                        results.append(asyncio.CancelledError())
                    else:
                        results.append(task.result())
                else:
                    # Задача всё ещё pending (не успела за grace period).
                    # Трактуем как отменённую — результатов от неё нет.
                    results.append(asyncio.CancelledError())

            # Обрабатываем результаты основного раунда
            (
                all_enriched, worker_stats, live_browsers,
                dead_browsers, failed_proxies, failed_proxy_configs,
            ) = EnrichStrategies._process_worker_results(
                results, worker_configs, active_proxies
            )

            # Сохраняем живые браузеры для переиспользования
            all_live_browsers.update(live_browsers)

            # Останавливаем браузеры упавших воркеров
            if dead_browsers:
                logger.info(
                    "остановка_браузеров_упавших_воркеров",
                    total=len(dead_browsers),
                )
                await EnrichStrategies._stop_browsers(dead_browsers)

            # ── Retry-раунды для необработанных карточек ──
            for retry_round in range(1, _MAX_PARALLEL_RETRY_ROUNDS + 1):
                unenriched = [
                    l for l in listings
                    if not EnrichStrategies._is_enriched(l)
                ]

                # Диагностика skip_reason
                skipped_by_reason = sum(
                    1 for l in listings
                    if l.enrichment_skip_reason is not None
                )

                if skipped_by_reason > 0:
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

                # ── Шаг 1: Собираем живые (переиспользуемые) браузеры ──
                reusable_browsers: list[tuple[int, BrowserService, ProxyConfig | None]] = [
                    (w_idx, bsvc, bproxy)
                    for w_idx, (bsvc, bproxy) in all_live_browsers.items()
                    if str(bproxy) not in failed_proxies
                ]

                live_proxy_strings = {
                    str(bproxy) for _, (_, bproxy) in all_live_browsers.items()
                    if bproxy is not None
                }

                # ── Шаг 2: Проверяем упавшие прокси — какие ожили ──
                revived_browsers: list[tuple[int, BrowserService, ProxyConfig]] = []

                if failed_proxy_configs and proxy_service:
                    logger.info(
                        "retry_проверка_упавших_прокси",
                        step=f"раунд={retry_round}, "
                             f"упавших={len(failed_proxy_configs)}, "
                             f"живых={len(reusable_browsers)}",
                    )

                    # Пауза перед проверкой — дать антибот-защите «остыть»
                    await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                    revived_browsers = await EnrichStrategies._revive_failed_proxies(
                        failed_proxy_configs=failed_proxy_configs,
                        live_proxy_strings=live_proxy_strings,
                        proxy_service=proxy_service,
                        settings=settings,
                        all_proxies=active_proxies,
                        retry_round=retry_round,
                    )

                    # Добавляем ожившие браузеры в общий пул живых
                    for r_w_idx, r_bsvc, r_proxy in revived_browsers:
                        all_live_browsers[r_w_idx] = (r_bsvc, r_proxy)

                    # Убираем ожившие прокси из списка упавших
                    revived_proxy_strings = {str(p) for _, _, p in revived_browsers}
                    failed_proxies -= revived_proxy_strings
                    failed_proxy_configs = [
                        p for p in failed_proxy_configs
                        if str(p) not in revived_proxy_strings
                    ]

                # ── Шаг 3: Объединяем все доступные браузеры ──
                all_available_browsers: list[tuple[int, BrowserService, ProxyConfig | None]] = [
                    *reusable_browsers,
                    *revived_browsers,
                ]

                if not all_available_browsers:
                    logger.warning(
                        "нет_браузеров_для_retry",
                        step=f"упавших_прокси={len(failed_proxy_configs)}, "
                             f"необработано={len(unenriched)}",
                    )
                    break

                logger.info(
                    "retry_раунд_объединённый_пул",
                    step=f"раунд={retry_round}/{_MAX_PARALLEL_RETRY_ROUNDS}, "
                         f"необработано={len(unenriched)}, "
                         f"переиспользуемых={len(reusable_browsers)}, "
                         f"ожившых={len(revived_browsers)}, "
                         f"всего_браузеров={len(all_available_browsers)}",
                )

                # ── Шаг 4: Перераспределяем карточки между всеми браузерами ──
                retry_chunks = ProxyServiceClass.distribute_listings(
                    unenriched, len(all_available_browsers)
                )

                retry_configs: list[tuple[int, list[RawListing], ProxyConfig | None]] = []
                retry_existing_browsers: dict[int, BrowserService] = {}

                for i, (chunk, (orig_w_idx, bsvc, bproxy)) in enumerate(
                    zip(retry_chunks, all_available_browsers)
                ):
                    retry_w_idx = 100 * retry_round + orig_w_idx
                    retry_configs.append((retry_w_idx, chunk, bproxy))
                    retry_existing_browsers[retry_w_idx] = bsvc

                # Пауза перед retry (если не было паузы при проверке прокси)
                if not revived_browsers:
                    await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                # Собираем список прокси для retry
                retry_proxies_list = [
                    bproxy for _, bproxy in all_live_browsers.items()
                    if str(bproxy) not in failed_proxies and bproxy is not None
                ]

                retry_tasks = await EnrichStrategies._launch_workers(
                    worker_configs=retry_configs,
                    settings=settings,
                    active_proxies=retry_proxies_list or active_proxies,
                    proxy_service=proxy_service,
                    memory_monitor=memory_monitor,
                    controller=controller,
                    existing_browsers=retry_existing_browsers,
                )

                # ── Собираем плоский список для retry watchdog ──
                retry_listings_flat: list[RawListing] = []
                for _, chunk, _ in retry_configs:
                    retry_listings_flat.extend(chunk)

                # ── Event для retry watchdog ──
                retry_stagnation_event = asyncio.Event()

                # ── Запуск watchdog для retry-раунда ──
                retry_watchdog_task = asyncio.create_task(
                    EnrichStrategies._stagnation_watchdog(
                        all_listings=retry_listings_flat,
                        worker_tasks=retry_tasks,
                        stagnation_event=retry_stagnation_event,
                        label=f"retry_{retry_round}",
                        controller=controller,
                    ),
                    name=f"stagnation-watchdog-retry-{retry_round}",
                )

                # ── Ожидаем завершения retry-воркеров (с защитой от зависания) ──
                await EnrichStrategies._await_tasks_with_watchdog(
                    retry_tasks, retry_stagnation_event
                )

                if not retry_watchdog_task.done():
                    retry_watchdog_task.cancel()
                    try:
                        await retry_watchdog_task
                    except asyncio.CancelledError:
                        pass

                # Собираем результаты retry
                retry_results: list = []
                for task in retry_tasks:
                    if task.done():
                        exc = task.exception() if not task.cancelled() else None
                        if exc is not None:
                            retry_results.append(exc)
                        elif task.cancelled():
                            retry_results.append(asyncio.CancelledError())
                        else:
                            retry_results.append(task.result())
                    else:
                        retry_results.append(asyncio.CancelledError())

                # Обрабатываем результаты retry
                (
                    retry_enriched, retry_stats, retry_live,
                    retry_dead, retry_failed, retry_failed_configs,
                ) = EnrichStrategies._process_worker_results(
                    retry_results, retry_configs,
                    retry_proxies_list or active_proxies,
                )

                all_enriched.extend(retry_enriched)
                worker_stats.extend(retry_stats)
                failed_proxies.update(retry_failed)
                failed_proxy_configs.extend(retry_failed_configs)

                # Обновляем живые браузеры
                for i, (orig_w_idx, _, _) in enumerate(all_available_browsers):
                    retry_w_idx = 100 * retry_round + orig_w_idx
                    if retry_w_idx in retry_live:
                        all_live_browsers[orig_w_idx] = retry_live[retry_w_idx]
                    elif retry_w_idx in {
                        w_idx for (bsvc, w_idx) in retry_dead
                    }:
                        all_live_browsers.pop(orig_w_idx, None)

                # Останавливаем браузеры упавших retry-воркеров
                if retry_dead:
                    await EnrichStrategies._stop_browsers(retry_dead)

                logger.info(
                    "retry_раунд_завершён",
                    step=f"раунд={retry_round}, "
                         f"дообогащено={len(retry_enriched)}, "
                         f"упало_прокси={len(retry_failed)}",
                )

        finally:
            # ── Останавливаем фоновые задачи ──
            for bg_task in background_tasks:
                if not bg_task.done():
                    bg_task.cancel()

            for bg_task in background_tasks:
                try:
                    await bg_task
                except asyncio.CancelledError:
                    pass

            await memory_monitor.stop_monitoring()

            # ── Остановка ВСЕХ оставшихся живых браузеров ──
            remaining_browsers: list[tuple[BrowserService, int]] = [
                (bsvc, w_idx) for w_idx, (bsvc, _) in all_live_browsers.items()
            ]
            if remaining_browsers:
                logger.info(
                    "остановка_всех_живых_браузеров",
                    total=len(remaining_browsers),
                )
                await EnrichStrategies._stop_browsers(remaining_browsers)

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

        controller.log_stats()

        logger.info(
            "параллельная_обработка_завершена",
            total=len(all_enriched),
        )

        return all_enriched

    @staticmethod
    async def _launch_workers(
        worker_configs: list[tuple[int, list[RawListing], ProxyConfig | None]],
        settings: "any",  # type: ignore[name-defined]
        active_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None",
        memory_monitor: MemoryMonitor,
        controller: ConcurrencyController,
        existing_browsers: dict[int, BrowserService] | None = None,
    ) -> list[asyncio.Task]:
        """Создаёт и запускает все воркеры с задержкой между стартами."""
        all_tasks: list[asyncio.Task] = []
        browsers_map = existing_browsers or {}

        reuse_count = sum(
            1 for w_idx, _, _ in worker_configs if w_idx in browsers_map
        )
        new_count = len(worker_configs) - reuse_count

        logger.info(
            "запуск_воркеров",
            step=f"всего={len(worker_configs)}, "
                 f"переиспользуемых={reuse_count}, "
                 f"новых={new_count}, "
                 f"задержка_между_стартами={_WORKER_START_DELAY_SECONDS}с",
        )

        for i, (worker_idx, chunk, proxy) in enumerate(worker_configs):
            if i > 0 and worker_idx not in browsers_map:
                await asyncio.sleep(_WORKER_START_DELAY_SECONDS)

            existing_browser = browsers_map.get(worker_idx)

            task = asyncio.create_task(
                EnrichStrategies._worker(
                    settings, chunk, proxy, worker_idx,
                    active_proxies, proxy_service, memory_monitor,
                    controller,
                    existing_browser=existing_browser,
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
        worker_configs: list[tuple[int, list[RawListing], ProxyConfig | None]],
        active_proxies: list[ProxyConfig],
    ) -> tuple[
        list[RawListing],
        list[tuple[int, int, float]],
        dict[int, tuple[BrowserService, ProxyConfig | None]],
        list[tuple[BrowserService, int]],
        set[str],
        list[ProxyConfig],
    ]:
        """Обрабатывает результаты завершённых воркеров.

        Returns:
            Кортеж из шести элементов:
            - all_enriched: карточки из успешных воркеров.
            - worker_stats: статистика (worker_idx, cards, duration).
            - live_browsers: живые браузеры {worker_idx: (BrowserService, proxy)}.
            - dead_browsers: браузеры упавших воркеров [(BrowserService, worker_idx)].
            - failed_proxies: строковые представления прокси упавших воркеров.
            - failed_proxy_configs: объекты ProxyConfig упавших прокси
              (для последующей проверки и воскрешения в retry-раунде).
        """
        all_enriched: list[RawListing] = []
        worker_stats: list[tuple[int, int, float]] = []
        live_browsers: dict[int, tuple[BrowserService, ProxyConfig | None]] = {}
        dead_browsers: list[tuple[BrowserService, int]] = []
        failed_proxies: set[str] = set()
        failed_proxy_configs: list[ProxyConfig] = []

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
                if worker_proxy is not None:
                    failed_proxies.add(str(worker_proxy))
                    failed_proxy_configs.append(worker_proxy)

            elif isinstance(result, tuple) and len(result) == 3:
                enriched_list, duration, browser_svc = result
                all_enriched.extend(enriched_list)
                worker_stats.append((worker_idx, len(enriched_list), duration))

                live_browsers[worker_idx] = (browser_svc, worker_proxy)

        return (
            all_enriched, worker_stats, live_browsers,
            dead_browsers, failed_proxies, failed_proxy_configs,
        )

    @staticmethod
    async def _stop_browsers(
        browsers_to_stop: list[tuple[BrowserService, int]],
    ) -> None:
        """Останавливает все браузеры из списка параллельно с жёстким таймаутом."""
        if not browsers_to_stop:
            return

        total = len(browsers_to_stop)
        logger.info(
            "остановка_прокси_браузеров",
            total=total,
            step=f"параллельно, глобальный_таймаут={_STOP_BROWSERS_GLOBAL_TIMEOUT}с",
        )

        stop_start = time.perf_counter()

        stop_tasks: list[asyncio.Task] = [
            asyncio.create_task(
                safe_stop_browser(browser_svc, w_idx),
                name=f"stop-browser-{w_idx}",
            )
            for browser_svc, w_idx in browsers_to_stop
        ]

        done, pending = await asyncio.wait(
            stop_tasks,
            timeout=_STOP_BROWSERS_GLOBAL_TIMEOUT,
        )

        elapsed = time.perf_counter() - stop_start

        if not pending:
            logger.info(
                "все_прокси_браузеры_остановлены",
                total=total,
                step=f"время={format_duration(elapsed)}",
            )
        else:
            logger.warning(
                "остановка_браузеров_превысила_таймаут",
                step=f"завершено={len(done)}, зависло={len(pending)}, "
                     f"время={format_duration(elapsed)}, "
                     f"лимит={_STOP_BROWSERS_GLOBAL_TIMEOUT}с, "
                     f"продолжаем_дальше=да",
            )

            for task in pending:
                task.cancel()

            if pending:
                await asyncio.wait(pending, timeout=3.0)

    @staticmethod
    async def _worker(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxy: ProxyConfig | None,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
        memory_monitor: MemoryMonitor | None = None,
        controller: ConcurrencyController | None = None,
        existing_browser: BrowserService | None = None,
    ) -> tuple[list[RawListing], float, BrowserService]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер."""
        if not listings:
            fallback_browser = existing_browser or BrowserService(settings=settings)
            return ([], 0.0, fallback_browser)

        worker_start = time.perf_counter()
        monitor = ConnectionMonitor()
        current_proxy: ProxyConfig | None = proxy

        # Занятость прокси больше не считается локально по снимку
        # all_proxies — это и было источником гонки, когда несколько
        # воркеров одновременно выбирали один и тот же "свободный" прокси.
        # Теперь резервирование прокси централизовано и защищено
        # блокировкой внутри ProxyService (claim_proxy/get_replacement_proxy).

        # ── Определяем, нужно ли создавать браузер ──
        if existing_browser is not None:
            browser_service = existing_browser
            logger.info(
                "воркер_переиспользует_браузер",
                step=f"воркер={worker_idx}, прокси={current_proxy or 'без_прокси'}",
                total=len(listings),
            )
        else:
            browser_service = BrowserService(settings=settings)

            try:
                await browser_service.start(proxy=current_proxy)

                logger.info(
                    "воркер_запущен",
                    step=f"воркер={worker_idx}",
                    total=len(listings),
                )

                warmup_ok, current_proxy = await EnrichStrategies._warmup_browser(
                    browser_service=browser_service,
                    proxy=current_proxy,
                    worker_idx=worker_idx,
                    all_proxies=all_proxies,
                    proxy_service=proxy_service,
                )

                if not warmup_ok:
                    worker_elapsed = time.perf_counter() - worker_start
                    logger.warning(
                        "воркер_не_прогрелся_завершение",
                        step=f"воркер={worker_idx}, время={format_duration(worker_elapsed)}",
                    )
                    return (listings, worker_elapsed, browser_service)

            except Exception as e:
                worker_elapsed = time.perf_counter() - worker_start
                logger.warning(
                    "ошибка_воркера_при_запуске",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"воркер={worker_idx}",
                )
                return (listings, worker_elapsed, browser_service)

        try:
            from src.services.listing_service import ListingService

            listing_service = ListingService(
                settings=settings,
                browser_service=browser_service,
                monitor=monitor,
                concurrency_controller=controller,
            )

            remaining = [
                l for l in listings
                if not EnrichStrategies._is_enriched(l)
            ]
            restart_count = 0

            while remaining and restart_count <= _MAX_RESTARTS_PER_WORKER:
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

                await EnrichStrategies._process_worker_cards(
                    listings=remaining,
                    listing_service=listing_service,
                    browser_service=browser_service,
                    monitor=monitor,
                    controller=controller,
                    settings=settings,
                    worker_idx=worker_idx,
                )

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

                    old_browser = browser_service
                    try:
                        await old_browser.stop()
                    except Exception as e:
                        logger.warning(
                            "воркер_ошибка_остановки",
                            error=str(e),
                            step=f"воркер={worker_idx}",
                        )

                    if current_proxy is not None and proxy_service is not None:
                        is_current_ok = await proxy_service.check_single_proxy(
                            current_proxy, fast=True
                        )

                        if is_current_ok:
                            logger.info(
                                "воркер_прокси_работает",
                                step=f"воркер={worker_idx}, прокси={current_proxy}",
                            )
                        else:
                            # get_replacement_proxy сам атомарно резервирует
                            # кандидата под блокировкой и снимает current_proxy
                            # с пула рабочих — вручную передавать "занятые"
                            # прокси больше не нужно.
                            replacement = await proxy_service.get_replacement_proxy(
                                current_proxy=current_proxy,
                            )

                            if replacement is not None:
                                logger.info(
                                    "воркер_замена_прокси",
                                    step=f"воркер={worker_idx}, "
                                         f"старая={current_proxy}, "
                                         f"новая={replacement}",
                                )
                                current_proxy = replacement
                            else:
                                logger.warning(
                                    "воркер_замена_не_найдена",
                                    step=f"воркер={worker_idx}, "
                                         f"пробуем_без_прокси",
                                )
                                current_proxy = None

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

                    await asyncio.sleep(_RESTART_COOLDOWN_SECONDS)

                    remaining = [
                        l for l in listings
                        if not EnrichStrategies._is_enriched(l)
                    ]

                    logger.info(
                        "воркер_продолжение",
                        step=f"воркер={worker_idx}, осталось={len(remaining)}",
                    )
                else:
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
        """Обрабатывает карточки воркера порциями по max_tabs вкладок."""
        max_tabs = settings.max_tabs
        tab_delay_ms = settings.tab_delay_ms
        total = len(listings)
        processed_count = 0

        for chunk_start in range(0, total, max_tabs):
            if monitor.should_skip():
                logger.debug(
                    "воркер_порция_пропущена",
                    step=f"воркер={worker_idx}, позиция={chunk_start}",
                )
                break

            chunk = listings[chunk_start : chunk_start + max_tabs]

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

            await browser_service.close_all_pages()

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
        """Обрабатывает одну карточку в вкладке воркера с контролем параллелизма."""
        page: Page | None = None

        if monitor.should_skip():
            return

        if tab_index > 0:
            await asyncio.sleep(tab_delay_ms / 1000.0)

        if monitor.should_skip():
            return

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
