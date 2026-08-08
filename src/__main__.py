"""Точка входа приложения — сборка зависимостей и запуск pipeline."""

import asyncio
import json
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.booking_event import AnyEvent
from src.models.proxy import ProxyConfig
from src.repositories.base import BaseListingRepository
from src.repositories.db_factory import RepositoryPair, create_repositories
from src.repositories.snapshot_repository import BaseSnapshotRepository
from src.services.browser_service import BrowserService
from src.services.comparison_export_service import ComparisonExportService
from src.services.comparison_service import ComparisonService
from src.services.data_cleaner_service import DataCleanerService
from src.services.export_service import ExportService
from src.services.listing.batch_enrichment_service import BatchEnrichmentService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService


# ── Константы цикла ──────────────────────────────────────────

# Пауза между прогонами (секунды)
_CYCLE_PAUSE_SECONDS: int = 120

# Путь к JSON-файлу статуса прогона (относительно data/)
_RUN_STATUS_FILENAME: str = "last_run_status.json"

# Часовой пояс Москвы (UTC+3) — для расчёта окна паузы
_MSK_OFFSET: timedelta = timedelta(hours=3)
_MSK_TZ: timezone = timezone(_MSK_OFFSET)

# Флаг остановки цикла (устанавливается через SIGTERM/SIGINT)
_shutdown_requested: bool = False


def _request_shutdown(signum: int, frame: "any") -> None:  # type: ignore[name-defined]
    """Обработчик сигналов SIGTERM и SIGINT — запрашивает остановку цикла.

    Устанавливает глобальный флаг, который проверяется в паузе между прогонами
    и перед стартом нового прогона. Текущий прогон не прерывается — он
    завершается штатно, после чего цикл останавливается.

    Args:
        signum: Номер сигнала.
        frame: Текущий стек-фрейм (не используется).
    """
    global _shutdown_requested  # noqa: PLW0603
    _shutdown_requested = True


def _is_in_pause_window(
    pause_start: tuple[int, int],
    pause_end: tuple[int, int],
) -> bool:
    """Проверяет, попадает ли текущее время МСК в окно паузы.

    Поддерживает окна, пересекающие полночь (например, 22:50–00:10).

    Args:
        pause_start: Начало паузы (часы, минуты) по МСК.
        pause_end: Конец паузы (часы, минуты) по МСК.

    Returns:
        True если текущее время находится внутри окна паузы.
    """
    now_msk = datetime.now(_MSK_TZ)
    current_minutes = now_msk.hour * 60 + now_msk.minute

    start_minutes = pause_start[0] * 60 + pause_start[1]
    end_minutes = pause_end[0] * 60 + pause_end[1]

    if start_minutes <= end_minutes:
        # Обычное окно (например, 02:00–06:00)
        return start_minutes <= current_minutes < end_minutes
    else:
        # Окно пересекает полночь (например, 22:50–00:10)
        return current_minutes >= start_minutes or current_minutes < end_minutes


def _seconds_until_pause_end(pause_end: tuple[int, int]) -> int:
    """Вычисляет количество секунд до конца паузы.

    Args:
        pause_end: Конец паузы (часы, минуты) по МСК.

    Returns:
        Количество секунд до момента pause_end. Если pause_end уже
        «в прошлом» сегодня (окно через полночь) — считает до завтра.
    """
    now_msk = datetime.now(_MSK_TZ)

    # Целевое время сегодня
    target_today = now_msk.replace(
        hour=pause_end[0],
        minute=pause_end[1],
        second=0,
        microsecond=0,
    )

    if target_today > now_msk:
        delta = target_today - now_msk
    else:
        # Конец паузы уже прошёл сегодня — значит это «завтра»
        target_tomorrow = target_today + timedelta(days=1)
        delta = target_tomorrow - now_msk

    return int(delta.total_seconds())


async def _wait_for_pause_end(
    pause_start: tuple[int, int],
    pause_end: tuple[int, int],
    logger: "any",  # type: ignore[name-defined]
) -> None:
    """Ожидает окончания паузы, если текущее время попадает в окно.

    Проверяет SIGTERM каждую секунду — при запросе остановки
    выходит немедленно, не дожидаясь конца паузы.

    Args:
        pause_start: Начало паузы (часы, минуты) по МСК.
        pause_end: Конец паузы (часы, минуты) по МСК.
        logger: Логгер для записи состояния.
    """
    global _shutdown_requested

    if not _is_in_pause_window(pause_start, pause_end):
        return

    wait_seconds = _seconds_until_pause_end(pause_end)
    end_time_str = f"{pause_end[0]:02d}:{pause_end[1]:02d}"

    logger.info(
        "ночная_пауза_начата",
        step=f"окно={pause_start[0]:02d}:{pause_start[1]:02d}–{end_time_str} МСК, "
             f"ожидание≈{wait_seconds // 60}мин",
    )

    # Ожидаем с проверкой SIGTERM каждую секунду
    elapsed = 0
    while elapsed < wait_seconds:
        if _shutdown_requested:
            logger.info(
                "остановка_запрошена_во_время_ночной_паузы",
                step=f"ожидание_прервано_на={elapsed}с",
            )
            return

        # Перепроверяем: вдруг время уже вышло из окна паузы
        if not _is_in_pause_window(pause_start, pause_end):
            break

        await asyncio.sleep(1.0)
        elapsed += 1

    logger.info(
        "ночная_пауза_завершена",
        step=f"ожидание={elapsed}с",
    )


def _write_run_status(
    data_dir: str,
    status: str,
    started_at: str,
    finished_at: str,
    listings_count: int = 0,
    events_count: int = 0,
    error: str | None = None,
    run_number: int = 0,
) -> None:
    """Записывает результат прогона в JSON-файл для чтения админкой.

    Файл перезаписывается при каждом прогоне. Админка мониторит
    изменение файла (по полю run_number или finished_at) и создаёт
    запись в истории запусков.

    Args:
        data_dir: Путь к папке data/ парсера.
        status: Статус прогона ("success", "failed", "cancelled").
        started_at: Время начала прогона (ISO 8601, UTC).
        finished_at: Время завершения прогона (ISO 8601, UTC).
        listings_count: Количество объявлений в БД после прогона.
        events_count: Количество событий (брони + отмены).
        error: Текст ошибки (только для status="failed").
        run_number: Порядковый номер прогона в текущей сессии.
    """
    status_path = Path(data_dir) / _RUN_STATUS_FILENAME

    data = {
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "listings_count": listings_count,
        "events_count": events_count,
        "error": error,
        "run_number": run_number,
    }

    try:
        status_path.parent.mkdir(parents=True, exist_ok=True)
        status_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError:
        # Не критично — админка просто не увидит этот прогон
        pass


def _count_enriched(listings: list) -> int:
    """Подсчитывает количество обогащённых карточек.

    Args:
        listings: Список карточек.

    Returns:
        Количество карточек с данными календаря или цен.
    """
    count = 0
    for listing in listings:
        if listing.enrichment_skip_reason is not None:
            continue
        if listing.calendar_60_days and any(c == 1 for c in listing.calendar_60_days):
            count += 1
        elif listing.prices_60_days and any(p > 0 for p in listing.prices_60_days):
            count += 1
    return count


def _count_unenriched(listings: list) -> int:
    """Подсчитывает количество необогащённых карточек (без фатальных).

    Args:
        listings: Список карточек.

    Returns:
        Количество карточек без данных и без фатальной причины.
    """
    count = 0
    for listing in listings:
        if listing.enrichment_skip_reason is not None:
            continue
        has_calendar = listing.calendar_60_days and any(
            c == 1 for c in listing.calendar_60_days
        )
        has_prices = listing.prices_60_days and any(
            p > 0 for p in listing.prices_60_days
        )
        if not has_calendar and not has_prices:
            count += 1
    return count


def _get_unenriched_listings(listings: list) -> list:
    """Возвращает список необогащённых карточек без фатальных причин.

    Args:
        listings: Полный список карточек.

    Returns:
        Карточки, у которых нет данных календаря/цен и нет enrichment_skip_reason.
    """
    return [
        l for l in listings
        if l.enrichment_skip_reason is None
        and not (l.calendar_60_days and any(c == 1 for c in l.calendar_60_days))
        and not (l.prices_60_days and any(p > 0 for p in l.prices_60_days))
    ]


async def _batch_retry_without_proxy(
    settings: Settings,
    batch_enrichment_service: BatchEnrichmentService,
    unenriched_listings: list,
    logger: "any",  # type: ignore[name-defined]
) -> None:
    """Однократный batch-retry необработанных карточек без прокси.

    Запускает один браузер без прокси, загружает страницу поиска,
    перехватывает токен API и выполняет batch-обогащение (bulk +
    скользящее окно) только для необработанных карточек.

    Выполняется один раз. Если после этого остались необработанные
    карточки — они пропускаются, парсинг продолжается к снимкам и экспорту.

    Args:
        settings: Настройки приложения.
        batch_enrichment_service: Сервис batch-обогащения.
        unenriched_listings: Список необогащённых карточек.
        logger: Логгер.
    """
    browser_service = BrowserService(settings=settings)

    try:
        await browser_service.start()

        page = browser_service.page
        search_url = settings.search_urls[0]

        logger.info(
            "batch_retry_загрузка_страницы_поиска",
            step=f"url={search_url[:80]}...",
        )

        # ── Перехват токена со страницы поиска ──
        captured_token: list[str] = []

        async def _intercept(route, request):
            url = request.url
            if "sutochno.ru/api/json" in url and not captured_token:
                token = (
                    request.headers.get("token")
                    or request.headers.get("Token")
                )
                if token:
                    captured_token.append(token)
            try:
                await route.continue_()
            except Exception as e:
                if "Route is already handled" not in str(e):
                    raise

        await page.route("**/api/json/**", _intercept)

        await page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=settings.navigation_timeout,
        )

        # Ожидание перехвата токена (до 20 секунд)
        elapsed = 0.0
        while elapsed < 20.0:
            if captured_token:
                break
            await asyncio.sleep(0.5)
            elapsed += 0.5

        # Снимаем route handler — критически важно для последующих fetch()
        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass

        # Стабилизация сессии
        await asyncio.sleep(3.0)

        if not captured_token:
            logger.warning(
                "batch_retry_токен_не_перехвачен",
                step=f"ожидание={elapsed:.1f}с",
            )
            return

        token = captured_token[0]

        logger.info(
            "batch_retry_токен_получен",
            step=f"ожидание={elapsed:.1f}с, карточек={len(unenriched_listings)}",
        )

        # ── Batch-обогащение необработанных карточек ──
        await batch_enrichment_service.enrich_batch(
            page=page,
            token=token,
            listings=unenriched_listings,
            search_url=search_url,
        )

        retry_enriched = _count_enriched(unenriched_listings)
        retry_still_empty = _count_unenriched(unenriched_listings)
        retry_fatal = sum(
            1 for l in unenriched_listings
            if l.enrichment_skip_reason is not None
        )

        logger.info(
            "batch_retry_завершён",
            step=f"обогащено={retry_enriched}, "
                 f"фатальных={retry_fatal}, "
                 f"осталось_пустых={retry_still_empty}",
        )

    except Exception as e:
        logger.warning(
            "ошибка_batch_retry",
            error=str(e)[:300],
            error_type=type(e).__name__,
            step="batch_retry",
        )
    finally:
        try:
            await browser_service.stop()
        except Exception:
            pass


async def run() -> None:
    """Один прогон pipeline — сбор каталога, обогащение, снимки, экспорт.

    Возвращает управление после завершения. Не содержит цикла —
    цикл реализован в run_loop(). Это позволяет корректно обрабатывать
    критические ошибки на уровне цикла.

    Raises:
        RuntimeError: При ошибке загрузки конфигурации.
        Exception: Любая критическая ошибка pipeline.
    """
    # --- Шаг 1: Загрузка конфигурации ---
    settings = Settings.load()

    # --- Шаг 2: Конфигурация логирования ---
    configure_logging(
        log_level=settings.log_level,
        log_file_path=settings.log_file_path,
    )
    logger = get_logger("main")
    logger.info(
        "прогон_начат",
        step="init",
        search_urls_count=len(settings.search_urls),
        max_pages=settings.max_pages,
        db_type=settings.db_type,
    )

    # --- Шаг 3: Инициализация репозиториев через фабрику ---
    repos = create_repositories(settings)
    repository = repos.listing
    snapshot_repository = repos.snapshot

    # --- Шаг 4: Загрузка и проверка прокси ---
    working_proxies: list[ProxyConfig] = []
    proxy_service: ProxyService | None = None

    if settings.use_proxy:
        logger.info("загрузка_прокси", step="proxy")
        proxy_service = ProxyService(settings=settings)

        try:
            proxies = proxy_service.load_proxies()
            working_proxies = await proxy_service.check_proxies(proxies)
        except RuntimeError as e:
            logger.warning(
                "ошибка_загрузки_прокси",
                error=str(e),
                step="proxy",
            )

        if working_proxies:
            logger.info(
                "прокси_готовы",
                total=len(working_proxies),
                step="proxy",
            )
        else:
            logger.warning(
                "рабочих_прокси_нет_работа_без_прокси",
                step="proxy",
            )

    # --- Шаг 5: Создание сервисов ---
    browser_service = BrowserService(settings=settings)
    scraper_service = ScraperService(
        settings=settings,
        browser_service=browser_service,
        proxies=working_proxies,
    )
    export_service = ExportService(settings=settings)

    snapshot_service = SnapshotService(repository=snapshot_repository)
    comparison_service = ComparisonService()

    export_dir = str(Path(settings.export_path).parent)
    comparison_export_service = ComparisonExportService(export_dir=export_dir)

    batch_enrichment_service = BatchEnrichmentService()
    data_cleaner_service = DataCleanerService(
        price_deviation_up=settings.price_deviation_up,
        price_deviation_down=settings.price_deviation_down,
    )

    try:
        # --- Шаг 6: Парсинг каталога через API (Этап 1) ---
        logger.info(
            "начало_парсинга_каталога",
            step="scraping",
            urls_count=len(settings.search_urls),
            proxies_available=len(working_proxies),
        )
        listings, catalog_token = await scraper_service.scrape_catalog()

        if not listings:
            logger.warning("объявления_не_найдены")
            await browser_service.stop()
            return

        logger.info(
            "каталог_собран",
            total=len(listings),
            token_available=catalog_token is not None,
            step="scraping",
        )

        # --- Шаг 7: Batch-обогащение через API (Этап 2a) ---
        batch_enriched_count = 0

        if settings.use_proxy and working_proxies:
            # ── Параллельный batch: N прокси-браузеров ──
            await browser_service.stop()

            first_search_url = settings.search_urls[0]

            logger.info(
                "начало_batch_обогащения_parallel",
                total=len(listings),
                step=f"воркеров={min(len(working_proxies), settings.max_proxy_workers)}",
            )

            try:
                await batch_enrichment_service.enrich_batch_parallel(
                    settings=settings,
                    listings=listings,
                    proxies=working_proxies,
                    search_url=first_search_url,
                    proxy_service=proxy_service,
                )

                batch_enriched_count = _count_enriched(listings)
                batch_fatal_count = sum(
                    1 for l in listings
                    if l.enrichment_skip_reason is not None
                )
                batch_unenriched = _count_unenriched(listings)

                logger.info(
                    "batch_обогащение_parallel_завершено",
                    step=f"обогащено={batch_enriched_count}, "
                         f"фатальных={batch_fatal_count}, "
                         f"для_retry={batch_unenriched}",
                )

            except Exception as e:
                logger.warning(
                    "ошибка_batch_обогащения_parallel",
                    error=str(e)[:300],
                    error_type=type(e).__name__,
                    step="batch_enrichment",
                )

        elif catalog_token is not None:
            # ── Однопоточный batch: токен от каталога ──
            try:
                catalog_page_alive = await browser_service.is_alive()

                if catalog_page_alive:
                    catalog_page = browser_service.page

                    logger.info(
                        "начало_batch_обогащения",
                        total=len(listings),
                        step="batch_enrichment",
                    )

                    await batch_enrichment_service.enrich_batch(
                        page=catalog_page,
                        token=catalog_token,
                        listings=listings,
                        search_url=settings.search_urls[0],
                    )

                    batch_enriched_count = _count_enriched(listings)
                    batch_fatal_count = sum(
                        1 for l in listings
                        if l.enrichment_skip_reason is not None
                    )
                    batch_unenriched = _count_unenriched(listings)

                    logger.info(
                        "batch_обогащение_завершено",
                        step=f"обогащено={batch_enriched_count}, "
                             f"фатальных={batch_fatal_count}, "
                             f"для_retry={batch_unenriched}",
                    )
                else:
                    logger.warning(
                        "браузер_каталога_не_жив_пропуск_batch",
                        step="batch_enrichment",
                    )

            except Exception as e:
                logger.warning(
                    "ошибка_batch_обогащения",
                    error=str(e)[:300],
                    error_type=type(e).__name__,
                    step="batch_enrichment",
                )

            # Закрываем браузер каталога
            await browser_service.stop()
        else:
            logger.info(
                "токен_каталога_отсутствует_пропуск_batch",
                step="batch_enrichment",
            )
            await browser_service.stop()

        # --- Шаг 8: Batch-retry необработанных (один раз, без прокси) ---
        unenriched_listings = _get_unenriched_listings(listings)

        if unenriched_listings:
            logger.info(
                "начало_batch_retry",
                step=f"необогащённых={len(unenriched_listings)}, "
                     f"уже_обогащено={batch_enriched_count}",
            )

            await _batch_retry_without_proxy(
                settings=settings,
                batch_enrichment_service=batch_enrichment_service,
                unenriched_listings=unenriched_listings,
                logger=logger,
            )

            # Финальная статистика после retry
            final_enriched = _count_enriched(listings)
            final_unenriched = _count_unenriched(listings)
            final_fatal = sum(
                1 for l in listings if l.enrichment_skip_reason is not None
            )

            if final_unenriched > 0:
                logger.info(
                    "batch_retry_остались_необработанные_пропускаем",
                    step=f"обогащено={final_enriched}, "
                         f"фатальных={final_fatal}, "
                         f"пропущено={final_unenriched}",
                )
            else:
                logger.info(
                    "все_карточки_обработаны_после_retry",
                    step=f"обогащено={final_enriched}, "
                         f"фатальных={final_fatal}",
                )
        else:
            logger.info(
                "все_карточки_обогащены_batch_retry_не_нужен",
                step=f"обогащено={batch_enriched_count}",
            )

        logger.info(
            "карточки_обработаны",
            total=len(listings),
            step="enrichment",
        )

        # --- Шаг 8.5: Расчёт price_per_sqm ---
        data_cleaner_service.clean_listings(listings)

        # --- Шаг 9: Сохранение в базу данных ---
        logger.info("сохранение_в_бд", step="storage")
        saved_count = repository.upsert_many(listings)
        logger.info(
            "данные_сохранены",
            total=saved_count,
            step="storage",
        )

        # --- Шаг 9.5: Удаление объявлений, отсутствующих на сайте ---
        active_ids = {l.external_id for l in listings}
        db_count_before = repository.count()

        min_threshold = max(100, db_count_before // 2)

        if len(active_ids) >= min_threshold:
            deleted_count = repository.delete_not_in_ids(active_ids)
            if deleted_count > 0:
                logger.info(
                    "удалены_объявления_отсутствующие_на_сайте",
                    deleted=deleted_count,
                    active_in_catalog=len(active_ids),
                    was_in_db=db_count_before,
                    step="cleanup",
                )
        else:
            logger.warning(
                "очистка_пропущена_мало_объявлений_в_каталоге",
                step=f"каталог={len(active_ids)}, БД={db_count_before}, "
                     f"порог={min_threshold}",
            )

        # --- Шаг 10: Сохранение снимков ---
        logger.info("сохранение_снимков", step="snapshots")
        all_listings = repository.get_all()
        snapshot_service.save_snapshots(all_listings)

        # --- Шаг 11: Сравнение снимков ---
        all_events = _run_comparison(
            listings=all_listings,
            snapshot_repository=snapshot_repository,
            comparison_service=comparison_service,
            logger=logger,
        )

        if all_events:
            logger.info(
                "экспорт_отчёта_сравнения",
                total=len(all_events),
                step="comparison_export",
            )
            comparison_path = comparison_export_service.export(all_events)
            logger.info(
                "отчёт_сравнения_сохранён",
                path=comparison_path,
                step="comparison_export",
            )
        else:
            logger.info(
                "событий_не_обнаружено_отчёт_не_создан",
                step="comparison_export",
            )

        # --- Шаг 12: Экспорт отчётов ---
        logger.info("экспорт_в_excel", step="export")

        export_path = export_service.export(all_listings)
        logger.info(
            "основной_отчёт_сохранён",
            path=export_path,
            total=len(all_listings),
            step="export",
        )

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base_name = Path(settings.export_path).stem
        snapshot_filename = f"{base_name}_{run_timestamp}.xlsx"
        snapshot_path = str(Path(export_dir) / snapshot_filename)

        export_service.export(all_listings, output_path=snapshot_path)
        logger.info(
            "датированный_отчёт_сохранён",
            path=snapshot_path,
            total=len(all_listings),
            step="export",
        )

    finally:
        try:
            if await browser_service.is_alive():
                await browser_service.stop()
        except Exception:
            pass

        repository.close()
        snapshot_repository.close()


async def run_loop() -> None:
    """Бесконечный цикл прогонов парсера.

    Выполняет run() в цикле с паузой _CYCLE_PAUSE_SECONDS между прогонами.
    При критической ошибке — записывает статус "failed" и завершает процесс.
    При SIGTERM/SIGINT — корректно завершает текущий прогон и выходит.

    Перед каждым прогоном проверяется ночная пауза (PAUSE_START–PAUSE_END).
    Если текущее время МСК попадает в окно паузы — парсер ожидает до конца
    паузы с проверкой SIGTERM каждую секунду.

    Каждый прогон записывает результат в JSON-файл (data/last_run_status.json),
    который админка мониторит для отображения истории запусков.
    """
    global _shutdown_requested  # noqa: PLW0603

    # Регистрируем обработчики сигналов для корректной остановки
    signal.signal(signal.SIGTERM, _request_shutdown)
    signal.signal(signal.SIGINT, _request_shutdown)

    # Загружаем settings один раз для определения data_dir и паузы
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    configure_logging(
        log_level=settings.log_level,
        log_file_path=settings.log_file_path,
    )
    logger = get_logger("main")

    data_dir = str(Path(settings.export_path).parent)
    run_number = 0

    # Логируем настройки паузы
    if settings.pause_start and settings.pause_end:
        logger.info(
            "цикл_парсера_запущен",
            step=f"пауза_между_прогонами={_CYCLE_PAUSE_SECONDS}с, "
                 f"ночная_пауза={settings.pause_start[0]:02d}:{settings.pause_start[1]:02d}"
                 f"–{settings.pause_end[0]:02d}:{settings.pause_end[1]:02d} МСК",
        )
    else:
        logger.info(
            "цикл_парсера_запущен",
            step=f"пауза_между_прогонами={_CYCLE_PAUSE_SECONDS}с, "
                 f"ночная_пауза=отключена",
        )

    while not _shutdown_requested:
        # ── Проверка ночной паузы перед стартом прогона ──
        if settings.pause_start and settings.pause_end:
            await _wait_for_pause_end(
                pause_start=settings.pause_start,
                pause_end=settings.pause_end,
                logger=logger,
            )

            # После ожидания паузы проверяем, не запросили ли остановку
            if _shutdown_requested:
                logger.info(
                    "остановка_запрошена_после_ночной_паузы",
                    step="shutdown",
                )
                break

        run_number += 1
        started_at = datetime.now(timezone.utc)
        started_at_iso = started_at.isoformat()

        logger.info("═" * 60)
        logger.info(
            "начало_прогона",
            step=f"прогон={run_number}",
        )

        run_start_time = time.perf_counter()

        try:
            await run()

            run_elapsed = time.perf_counter() - run_start_time
            finished_at_iso = datetime.now(timezone.utc).isoformat()

            # Читаем статистику из БД для JSON-статуса
            listings_count = 0
            events_count = 0
            try:
                stats_repos = create_repositories(settings)
                listings_count = stats_repos.listing.count()
                stats_repos.listing.close()
                stats_repos.snapshot.close()
            except Exception:
                pass

            # Считаем события из последнего отчёта сравнения
            try:
                comparison_dir = Path(data_dir)
                comparison_files = sorted(
                    comparison_dir.glob("comparison_report_*.xlsx"),
                    reverse=True,
                )
                if comparison_files:
                    from openpyxl import load_workbook

                    wb = load_workbook(
                        str(comparison_files[0]), read_only=True
                    )
                    ws = wb.active
                    events_count = max(0, ws.max_row - 1) if ws.max_row else 0
                    wb.close()
            except Exception:
                pass

            _write_run_status(
                data_dir=data_dir,
                status="success",
                started_at=started_at_iso,
                finished_at=finished_at_iso,
                listings_count=listings_count,
                events_count=events_count,
                run_number=run_number,
            )

            logger.info(
                "прогон_завершён_успешно",
                step=f"прогон={run_number}, "
                     f"время={_format_duration(run_elapsed)}, "
                     f"объявлений={listings_count}, "
                     f"событий={events_count}",
            )

        except KeyboardInterrupt:
            finished_at_iso = datetime.now(timezone.utc).isoformat()
            _write_run_status(
                data_dir=data_dir,
                status="cancelled",
                started_at=started_at_iso,
                finished_at=finished_at_iso,
                run_number=run_number,
            )
            logger.warning(
                "прогон_прерван_пользователем",
                step=f"прогон={run_number}",
            )
            break

        except Exception as e:
            run_elapsed = time.perf_counter() - run_start_time
            finished_at_iso = datetime.now(timezone.utc).isoformat()

            error_msg = f"{type(e).__name__}: {e}"

            _write_run_status(
                data_dir=data_dir,
                status="failed",
                started_at=started_at_iso,
                finished_at=finished_at_iso,
                error=error_msg[:500],
                run_number=run_number,
            )

            logger.exception(
                "критическая_ошибка_прогона",
                error=str(e),
                error_type=type(e).__name__,
                step=f"прогон={run_number}, "
                     f"время={_format_duration(run_elapsed)}",
            )
            sys.exit(1)

        # ── Проверка флага остановки перед паузой ──
        if _shutdown_requested:
            logger.info(
                "остановка_запрошена_после_прогона",
                step=f"прогон={run_number}",
            )
            break

        # ── Пауза между прогонами с проверкой SIGTERM каждую секунду ──
        logger.info(
            "пауза_между_прогонами",
            step=f"пауза={_CYCLE_PAUSE_SECONDS}с, "
                 f"следующий_прогон={run_number + 1}",
        )

        for _ in range(_CYCLE_PAUSE_SECONDS):
            if _shutdown_requested:
                logger.info(
                    "остановка_запрошена_во_время_паузы",
                    step=f"прогон={run_number}",
                )
                break
            await asyncio.sleep(1.0)

    logger.info(
        "цикл_парсера_завершён",
        step=f"прогонов_выполнено={run_number}",
    )


def _format_duration(seconds: float) -> str:
    """Форматирует длительность в человекочитаемый вид.

    Args:
        seconds: Длительность в секундах.

    Returns:
        Строка вида «Xм Yс» или «Yс».
    """
    total = int(seconds)
    minutes = total // 60
    secs = total % 60
    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


def _run_comparison(
    listings: list,
    snapshot_repository: BaseSnapshotRepository,
    comparison_service: ComparisonService,
    logger: "any",  # type: ignore[name-defined]
) -> list[AnyEvent]:
    """Сравнивает последние два снимка для каждого объявления."""
    start_time = time.perf_counter()

    id_title_map: dict[str, str] = {}
    for listing in listings:
        external_id: str = getattr(listing, "external_id", "")
        title: str = getattr(listing, "title", "")
        if external_id:
            id_title_map[external_id] = title

    if not id_title_map:
        logger.info("сравнение_пропущено_нет_id", step="comparison")
        return []

    logger.info(
        "загрузка_снимков_для_сравнения",
        total=len(id_title_map),
        step="comparison",
    )

    snapshots_map = snapshot_repository.get_last_two_batch(
        list(id_title_map.keys())
    )

    load_elapsed = time.perf_counter() - start_time
    logger.info(
        "снимки_загружены",
        total_ids=len(id_title_map),
        total_with_snapshots=len(snapshots_map),
        elapsed=f"{load_elapsed:.2f}с",
        step="comparison",
    )

    all_events: list[AnyEvent] = []
    compared = 0
    skipped = 0

    for external_id, title in id_title_map.items():
        snapshots = snapshots_map.get(external_id)

        if not snapshots or len(snapshots) < 2:
            skipped += 1
            continue

        old_snapshot, new_snapshot = snapshots[0], snapshots[1]
        events = comparison_service.compare(
            old_snapshot=old_snapshot,
            new_snapshot=new_snapshot,
            listing_title=title,
        )

        all_events.extend(events)
        compared += 1

    total_elapsed = time.perf_counter() - start_time

    logger.info(
        "сравнение_завершено",
        compared=compared,
        skipped=skipped,
        total_events=len(all_events),
        elapsed=f"{total_elapsed:.2f}с",
        step="comparison",
    )

    return sorted(all_events, key=lambda e: e.checkin_date)


def main() -> None:
    """Синхронная точка входа — запускает бесконечный цикл прогонов."""
    asyncio.run(run_loop())


if __name__ == "__main__":
    main()
