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
from src.services.memory_monitor import MemoryMonitor

if TYPE_CHECKING:
    from src.services.listing_service import ListingService
    from src.services.proxy_service import ProxyService

logger = get_logger("enrich_strategies")

# Пауза после перезапуска браузера перед возобновлением обработки (секунды)
_RESTART_COOLDOWN_SECONDS: float = 5.0

# Максимальное количество перезапусков браузера за один прогон воркера
_MAX_RESTARTS_PER_WORKER: int = 3

# ── Staggered start: поэтапный запуск воркеров ──

# Количество воркеров в одной пачке запуска.
# 10 одновременных Chromium — безопасный предел для большинства прокси-серверов.
# При 45 прокси запуск разобьётся на 5 пачек по 10 (последняя — 5 воркеров).
_WORKER_BATCH_SIZE: int = 10

# Пауза между запуском пачек воркеров (секунды).
# 15 секунд — достаточно, чтобы предыдущая пачка успела пройти прогрев
# и освободить пиковую нагрузку на сеть. Прокси-серверы восстанавливают
# пул соединений за 5-10 секунд после пика.
_BATCH_DELAY_SECONDS: float = 15.0

# Задержка между запуском отдельных воркеров внутри пачки (секунды).
# 2 секунды между стартом каждого Chromium — предотвращает одновременный
# вызов playwright.chromium.launch() и параллельный прогрев на одних прокси.
_WORKER_START_DELAY_SECONDS: float = 2.0

# Максимальное количество retry-раундов для упавших воркеров.
# После каждого раунда необработанные карточки перераспределяются
# между оставшимися рабочими прокси и запускаются повторно.
_MAX_PARALLEL_RETRY_ROUNDS: int = 2


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
        """Обрабатывает пачку карточек порциями по max_tabs вкладок.

        Вместо создания всех корутин одновременно через asyncio.gather
        (что приводит к созданию сотен замыканий в памяти), карточки
        обрабатываются порциями. После каждой порции дополнительные
        вкладки закрываются — это освобождает ~50-150 МБ на вкладку.

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

        try:
            page = await self._browser.create_page()
            await self._listing_service.enrich_listing(listing, page)
        finally:
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
    async def _launch_workers_staggered(
        worker_configs: list[tuple[int, list[RawListing], ProxyConfig]],
        settings: "any",  # type: ignore[name-defined]
        active_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None",
        memory_monitor: MemoryMonitor,
    ) -> list[asyncio.Task]:
        """Создаёт и запускает все воркеры поэтапно (staggered start).

        Воркеры создаются как asyncio.Task пачками по _WORKER_BATCH_SIZE.
        Между пачками — пауза _BATCH_DELAY_SECONDS.
        Внутри пачки — задержка _WORKER_START_DELAY_SECONDS между стартами.

        Все Task'и возвращаются сразу после создания — они уже запущены
        и работают параллельно. Вызывающий код должен выполнить
        await asyncio.gather(*tasks) для ожидания завершения всех.

        Args:
            worker_configs: Список (worker_idx, chunk, proxy) для каждого воркера.
            settings: Настройки приложения.
            active_proxies: Полный список активных прокси (для исключения занятых).
            proxy_service: Сервис прокси (опциональный).
            memory_monitor: Монитор RAM.

        Returns:
            Список запущенных asyncio.Task.
        """
        all_tasks: list[asyncio.Task] = []

        # Разбиваем на пачки
        total_batches = (
            (len(worker_configs) + _WORKER_BATCH_SIZE - 1) // _WORKER_BATCH_SIZE
        )

        for batch_idx in range(total_batches):
            batch_start = batch_idx * _WORKER_BATCH_SIZE
            batch_end = min(batch_start + _WORKER_BATCH_SIZE, len(worker_configs))
            batch = worker_configs[batch_start:batch_end]

            # Пауза между пачками (кроме первой)
            if batch_idx > 0:
                logger.info(
                    "пауза_между_пачками_воркеров",
                    step=f"ожидание={_BATCH_DELAY_SECONDS}с "
                         f"перед пачкой {batch_idx + 1}/{total_batches}",
                )
                await asyncio.sleep(_BATCH_DELAY_SECONDS)

            logger.info(
                "запуск_пачки_воркеров",
                step=f"пачка={batch_idx + 1}/{total_batches}, "
                     f"воркеров={len(batch)}, "
                     f"задержка_внутри={_WORKER_START_DELAY_SECONDS}с",
            )

            # Запускаем воркеры пачки как Task'и с задержкой между стартом
            for i, (worker_idx, chunk, proxy) in enumerate(batch):
                # Задержка перед запуском каждого воркера (кроме первого в пачке)
                if i > 0:
                    await asyncio.sleep(_WORKER_START_DELAY_SECONDS)

                task = asyncio.create_task(
                    EnrichStrategies._worker(
                        settings, chunk, proxy, worker_idx,
                        active_proxies, proxy_service, memory_monitor,
                    ),
                    name=f"worker-{worker_idx}",
                )
                all_tasks.append(task)

        logger.info(
            "все_воркеры_запущены",
            step=f"всего_задач={len(all_tasks)}, пачек={total_batches}",
        )

        return all_tasks

    @staticmethod
    async def enrich_listings_parallel(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Воркеры запускаются ПОЭТАПНО (staggered start) — пачками по
        _WORKER_BATCH_SIZE с паузой _BATCH_DELAY_SECONDS между пачками.
        Внутри пачки каждый воркер стартует с задержкой
        _WORKER_START_DELAY_SECONDS. После создания всех Task'ов — все
        воркеры работают параллельно до завершения.

        При падении воркера с исключением его карточки считаются
        необработанными. После основного раунда необработанные карточки
        перераспределяются между оставшимися рабочими прокси и запускаются
        повторно (до _MAX_PARALLEL_RETRY_ROUNDS раундов).

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

        # Расчёт количества пачек для логирования
        total_batches = (len(active_proxies) + _WORKER_BATCH_SIZE - 1) // _WORKER_BATCH_SIZE

        logger.info(
            "параллельная_обработка",
            total=len(listings),
            step=f"прокси={len(active_proxies)}"
                 f"{f' (ограничено_по_ram из {requested_workers})' if safe_workers < requested_workers else ''}"
                 f", вкладок_на_прокси={settings.max_tabs}"
                 f", пачек={total_batches}"
                 f", размер_пачки={_WORKER_BATCH_SIZE}"
                 f", пауза_между_пачками={_BATCH_DELAY_SECONDS}с"
                 f", задержка_внутри_пачки={_WORKER_START_DELAY_SECONDS}с",
        )

        # ── Запуск фонового мониторинга RAM ──
        await memory_monitor.start_monitoring()

        parallel_start = time.perf_counter()

        # Отслеживаем прокси, на которых воркеры упали (исключаем из retry)
        failed_proxies: set[str] = set()

        try:
            # ── Основной раунд: поэтапный запуск всех воркеров ──
            worker_configs: list[tuple[int, list[RawListing], ProxyConfig]] = [
                (idx, chunk, proxy)
                for idx, (chunk, proxy) in enumerate(
                    zip(chunks, active_proxies), start=1
                )
            ]

            all_tasks = await EnrichStrategies._launch_workers_staggered(
                worker_configs=worker_configs,
                settings=settings,
                active_proxies=active_proxies,
                proxy_service=proxy_service,
                memory_monitor=memory_monitor,
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
                # Собираем необработанные карточки из всех чанков
                unenriched = [
                    l for l in listings
                    if not EnrichStrategies._is_enriched(l)
                ]

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

                retry_tasks = await EnrichStrategies._launch_workers_staggered(
                    worker_configs=retry_configs,
                    settings=settings,
                    active_proxies=retry_proxies,
                    proxy_service=proxy_service,
                    memory_monitor=memory_monitor,
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
            # Гарантированная остановка мониторинга — даже при исключениях
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

        logger.info(
            "параллельная_обработка_завершена",
            total=len(all_enriched),
        )

        return all_enriched

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
        """Останавливает все браузеры из списка.

        Args:
            browsers_to_stop: Список (browser_service, worker_idx).
        """
        if not browsers_to_stop:
            return

        logger.info("остановка_прокси_браузеров", total=len(browsers_to_stop))
        for browser_svc, w_idx in browsers_to_stop:
            await safe_stop_browser(browser_svc, w_idx)
        logger.info("все_прокси_браузеры_остановлены")

    @staticmethod
    async def _worker(
        settings: "any",  # type: ignore[name-defined]
        listings: list[RawListing],
        proxy: ProxyConfig,
        worker_idx: int,
        all_proxies: list[ProxyConfig],
        proxy_service: "ProxyService | None" = None,
        memory_monitor: MemoryMonitor | None = None,
    ) -> tuple[list[RawListing], float, BrowserService]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер.

        При срабатывании монитора соединения выполняет перезапуск браузера
        с проверкой текущей прокси. Если текущая мертва — ищет замену
        из пула proxy_service (уже проверенных на старте прокси).

        При срабатывании монитора памяти (should_reduce_workers) воркер
        досрочно завершает работу, возвращая уже обработанные карточки.
        Необработанные остаются в списке — их подхватит retry-цикл
        в __main__.py на следующем раунде.

        Гарантирует остановку старого браузера при перезапуске — даже
        если stop() бросит исключение, ссылка на старый экземпляр
        не сохраняется и ресурсы будут освобождены.

        Args:
            settings: Настройки приложения.
            listings: Порция карточек для этого воркера.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).
            all_proxies: Полный список прокси всех воркеров (для исключения занятых).
            proxy_service: Сервис прокси с заполненным пулом (опциональный).
            memory_monitor: Монитор RAM (опциональный). Общий для всех воркеров.

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
                # ── Проверка монитора памяти перед каждой итерацией ──
                # Если RAM превышает порог — воркер досрочно завершается,
                # освобождая память для оставшихся воркеров.
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
                await listing_service.enrich_listings_tabbed(remaining)

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

                    # Гарантированная остановка старого браузера —
                    # даже при исключении в stop() переходим к созданию нового
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

                    # Создаём НОВЫЙ browser_service — старый уже остановлен
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
