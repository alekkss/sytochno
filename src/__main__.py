"""Точка входа приложения — сборка зависимостей и запуск pipeline."""

import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.booking_event import AnyEvent
from src.models.proxy import ProxyConfig
from src.repositories.snapshot_repository import SQLiteSnapshotRepository
from src.repositories.sqlite_repository import SQLiteListingRepository
from src.services.browser_service import BrowserService
from src.services.comparison_export_service import ComparisonExportService
from src.services.comparison_service import ComparisonService
from src.services.export_service import ExportService
from src.services.listing.batch_enrichment_service import BatchEnrichmentService
from src.services.listing.concurrency_controller import ConcurrencyController
from src.services.listing_service import ListingService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService


# Максимальное количество раундов повторного обогащения
_MAX_RETRY_ROUNDS: int = 3

# Пауза между раундами повторного обогащения (секунды)
_RETRY_ROUND_PAUSE_SECONDS: float = 30.0


def _create_concurrency_controller(
    settings: Settings,
    working_proxies: list[ProxyConfig],
) -> ConcurrencyController | None:
    """Создаёт контроллер адаптивного параллелизма.

    Контроллер создаётся только если есть прокси (параллельный режим).
    В последовательном режиме или режиме вкладок без прокси —
    контроллер не нужен (нечего адаптировать).

    Args:
        settings: Настройки приложения.
        working_proxies: Список рабочих прокси.

    Returns:
        Экземпляр ConcurrencyController или None если параллелизм не используется.
    """
    if not settings.use_proxy or not working_proxies:
        return None

    max_workers = min(len(working_proxies), settings.max_proxy_workers)

    if settings.concurrency_max > 0:
        ceiling = settings.concurrency_max
    else:
        ceiling = max_workers * settings.max_tabs

    floor = settings.concurrency_min

    if floor > ceiling:
        floor = ceiling

    start = settings.concurrency_start if settings.concurrency_start > 0 else None

    return ConcurrencyController(
        floor=floor,
        ceiling=ceiling,
        start=start,
    )


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


async def run() -> None:
    """Основной асинхронный pipeline приложения.

    Последовательно выполняет:
    1. Загрузку конфигурации.
    2. Инициализацию базы данных.
    3. Загрузку и проверку прокси (если USE_PROXY=true).
    4. Создание контроллера адаптивного параллелизма.
    5. Парсинг каталога через API.
    6. Batch-обогащение через API:
       - С прокси: параллельно через N прокси-браузеров.
       - Без прокси: один браузер с токеном от каталога.
    7. Fallback: поштучное обогащение необработанных карточек.
    8. Сохранение, снимки, сравнение, экспорт.
    """
    # --- Шаг 1: Загрузка конфигурации ---
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # --- Шаг 2: Конфигурация логирования ---
    configure_logging(
        log_level=settings.log_level,
        log_file_path=settings.log_file_path,
    )
    logger = get_logger("main")
    logger.info(
        "приложение_запущено",
        step="init",
        search_urls_count=len(settings.search_urls),
        max_pages=settings.max_pages,
    )

    # --- Шаг 3: Инициализация репозиториев ---
    repository = SQLiteListingRepository(db_path=settings.db_path)
    repository.initialize()

    snapshot_repository = SQLiteSnapshotRepository(db_path=settings.db_path)
    snapshot_repository.initialize()

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

    # --- Шаг 5: Создание контроллера адаптивного параллелизма ---
    concurrency_controller = _create_concurrency_controller(settings, working_proxies)

    if concurrency_controller is not None:
        logger.info(
            "контроллер_параллелизма_готов",
            step=f"floor={concurrency_controller.floor}, "
                 f"ceiling={concurrency_controller.ceiling}, "
                 f"start={concurrency_controller.current_limit}",
        )

    # --- Шаг 6: Создание сервисов ---
    browser_service = BrowserService(settings=settings)
    scraper_service = ScraperService(
        settings=settings,
        browser_service=browser_service,
        proxies=working_proxies,
    )
    listing_service = ListingService(
        settings=settings,
        browser_service=browser_service,
        proxy_service=proxy_service,
        concurrency_controller=concurrency_controller,
    )
    export_service = ExportService(settings=settings)

    snapshot_service = SnapshotService(repository=snapshot_repository)
    comparison_service = ComparisonService()

    export_dir = str(Path(settings.export_path).parent)
    comparison_export_service = ComparisonExportService(export_dir=export_dir)

    batch_enrichment_service = BatchEnrichmentService()

    enrichment_browser_started = False

    try:
        # --- Шаг 7: Парсинг каталога через API (Этап 1) ---
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

        # --- Шаг 8: Batch-обогащение через API (Этап 2a) ---
        batch_enriched_count = 0

        if settings.use_proxy and working_proxies:
            # ── Параллельный batch: N прокси-браузеров ──
            # Каждый воркер сам загружает страницу поиска и получает токен.
            # Браузер каталога больше не нужен — закрываем.
            await browser_service.stop()

            # Первый URL поиска — для загрузки страницы в воркерах
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
                         f"для_fallback={batch_unenriched}",
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
                             f"для_fallback={batch_unenriched}",
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

        # --- Шаг 9: Fallback — поштучное обогащение необогащённых ---
        unenriched_count = _count_unenriched(listings)

        if unenriched_count > 0:
            logger.info(
                "начало_fallback_обогащения",
                total=len(listings),
                step=f"необогащённых={unenriched_count}, "
                     f"уже_обогащено_batch={batch_enriched_count}",
            )

            unenriched_listings = [
                l for l in listings
                if l.enrichment_skip_reason is None
                and not (l.calendar_60_days and any(c == 1 for c in l.calendar_60_days))
                and not (l.prices_60_days and any(p > 0 for p in l.prices_60_days))
            ]

            if unenriched_listings:
                await browser_service.start()
                enrichment_browser_started = True

                unenriched_listings = await _enrich_with_proxy_or_sequential(
                    settings=settings,
                    listings=unenriched_listings,
                    listing_service=listing_service,
                    working_proxies=working_proxies,
                    proxy_service=proxy_service,
                    concurrency_controller=concurrency_controller,
                    logger=logger,
                )

                logger.info(
                    "fallback_обогащение_завершено",
                    total=len(unenriched_listings),
                    step="enrichment",
                )
        else:
            logger.info(
                "все_карточки_обогащены_batch_fallback_не_нужен",
                step=f"обогащено={batch_enriched_count}",
            )

        logger.info(
            "карточки_обработаны",
            total=len(listings),
            step="enrichment",
        )

        # --- Шаг 10: Сохранение в базу данных ---
        logger.info("сохранение_в_бд", step="storage")
        saved_count = repository.upsert_many(listings)
        logger.info(
            "данные_сохранены",
            total=saved_count,
            step="storage",
        )

        # --- Шаг 11: Остановка основного браузера ---
        if enrichment_browser_started:
            await browser_service.stop()
            enrichment_browser_started = False

        # --- Шаг 12: Повторное обогащение через прокси ---
        if working_proxies:
            await _retry_empty_listings(
                settings=settings,
                repository=repository,
                working_proxies=working_proxies,
                proxy_service=proxy_service,
                snapshot_service=snapshot_service,
                concurrency_controller=concurrency_controller,
                logger=logger,
            )
        else:
            empty_count = len(repository.get_empty_listings())
            if empty_count > 0:
                logger.warning(
                    "повторное_обогащение_пропущено_нет_прокси",
                    step=f"пустых_карточек={empty_count}",
                )

        # --- Шаг 13: Сохранение снимков ---
        logger.info("сохранение_снимков", step="snapshots")
        all_listings = repository.get_all()
        snapshot_service.save_snapshots(all_listings)

        # --- Шаг 14: Сравнение снимков ---
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

        # --- Шаг 15: Экспорт отчётов ---
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

    except KeyboardInterrupt:
        logger.warning("прервано_пользователем")
    except Exception as e:
        logger.exception(
            "критическая_ошибка",
            error=str(e),
            error_type=type(e).__name__,
        )
        sys.exit(1)
    finally:
        if enrichment_browser_started:
            await browser_service.stop()

        try:
            if await browser_service.is_alive():
                await browser_service.stop()
        except Exception:
            pass

        if concurrency_controller is not None:
            logger.info("─" * 50)
            logger.info("финальная_статистика_контроллера_параллелизма")
            concurrency_controller.log_stats()
            logger.info("─" * 50)

        repository.close()
        snapshot_repository.close()
        logger.info("приложение_завершено", step="shutdown")


async def _retry_empty_listings(
    settings: Settings,
    repository: SQLiteListingRepository,
    working_proxies: list[ProxyConfig],
    proxy_service: ProxyService | None,
    snapshot_service: SnapshotService,
    concurrency_controller: ConcurrencyController | None,
    logger: "any",  # type: ignore[name-defined]
) -> None:
    """Цикл повторного обогащения карточек с пустыми данными через прокси."""
    threshold = settings.blacklist_threshold
    min_cards_threshold = settings.retry_min_cards_threshold

    fail_counts: dict[str, int] = {}
    blacklisted_ids: set[str] = set()
    skip_reasons_stats: dict[str, int] = {}

    logger.info(
        "параметры_повторного_обогащения",
        step=f"порог_чёрного_списка={threshold}, "
             f"порог_досрочного_завершения={min_cards_threshold}, "
             f"макс_раундов={_MAX_RETRY_ROUNDS}, "
             f"пауза={_RETRY_ROUND_PAUSE_SECONDS}с",
    )

    for round_num in range(1, _MAX_RETRY_ROUNDS + 1):
        empty_listings = repository.get_empty_listings()

        if not empty_listings:
            logger.info(
                "повторное_обогащение_не_требуется",
                step="все_карточки_заполнены",
            )
            return

        instantly_excluded = 0
        for listing in empty_listings:
            if (
                listing.enrichment_skip_reason is not None
                and listing.external_id not in blacklisted_ids
            ):
                blacklisted_ids.add(listing.external_id)
                instantly_excluded += 1

                reason = listing.enrichment_skip_reason
                skip_reasons_stats[reason] = skip_reasons_stats.get(reason, 0) + 1

        if instantly_excluded > 0:
            logger.info(
                "мгновенное_исключение_необогащаемых",
                step=f"раунд={round_num}, исключено={instantly_excluded}",
                total=f"причины={skip_reasons_stats}",
            )

        candidates = [
            listing for listing in empty_listings
            if listing.external_id not in blacklisted_ids
        ]

        excluded_count = len(empty_listings) - len(candidates)

        if not candidates:
            logger.info(
                "повторное_обогащение_завершено_все_в_чёрном_списке",
                step=f"пустых={len(empty_listings)}, "
                     f"в_чёрном_списке={len(blacklisted_ids)}, "
                     f"причины={skip_reasons_stats}",
            )
            return

        if 0 < min_cards_threshold and len(candidates) < min_cards_threshold:
            logger.info(
                "повторное_обогащение_завершено_досрочно_по_порогу",
                step=f"раунд={round_num}/{_MAX_RETRY_ROUNDS}, "
                     f"кандидатов={len(candidates)}, "
                     f"порог={min_cards_threshold}",
                total=f"пустых_всего={len(empty_listings)}, "
                      f"в_чёрном_списке={len(blacklisted_ids)}, "
                      f"причины={skip_reasons_stats}",
            )
            return

        logger.info("═" * 60)
        logger.info(
            "начало_раунда_повторного_обогащения",
            step=f"раунд={round_num}/{_MAX_RETRY_ROUNDS}",
            total=f"пустых={len(empty_listings)}, к_обработке={len(candidates)}, "
                  f"исключено={excluded_count}",
        )

        max_workers = settings.max_proxy_workers
        proxies_to_use = working_proxies[:max_workers]

        candidate_ids = {listing.external_id for listing in candidates}

        enriched_listings = await ListingService.enrich_listings_parallel(
            settings=settings,
            listings=candidates,
            proxies=proxies_to_use,
            proxy_service=proxy_service,
            concurrency_controller=concurrency_controller,
        )

        newly_skipped = 0
        for listing in enriched_listings:
            if (
                listing.enrichment_skip_reason is not None
                and listing.external_id not in blacklisted_ids
            ):
                blacklisted_ids.add(listing.external_id)
                newly_skipped += 1

                reason = listing.enrichment_skip_reason
                skip_reasons_stats[reason] = skip_reasons_stats.get(reason, 0) + 1

        if newly_skipped > 0:
            logger.info(
                "мгновенное_исключение_после_раунда",
                step=f"раунд={round_num}, исключено={newly_skipped}",
                total=f"причины={skip_reasons_stats}",
            )

        newly_enriched = [
            listing for listing in enriched_listings
            if _is_listing_enriched(listing)
        ]

        enriched_ids = {listing.external_id for listing in newly_enriched}
        failed_ids = candidate_ids - enriched_ids - blacklisted_ids

        for ext_id in enriched_ids:
            fail_counts.pop(ext_id, None)

        for ext_id in failed_ids:
            fail_counts[ext_id] = fail_counts.get(ext_id, 0) + 1

            if fail_counts[ext_id] >= threshold:
                blacklisted_ids.add(ext_id)

        newly_blacklisted_by_threshold = len([
            ext_id for ext_id in failed_ids
            if fail_counts.get(ext_id, 0) >= threshold
        ])

        logger.info(
            "раунд_повторного_обогащения_завершён",
            step=f"раунд={round_num}/{_MAX_RETRY_ROUNDS}",
            total=f"обогащено={len(newly_enriched)} из {len(candidates)}, "
                  f"необогащаемых={newly_skipped}, "
                  f"новых_по_порогу={newly_blacklisted_by_threshold}, "
                  f"всего_в_чёрном_списке={len(blacklisted_ids)}",
        )

        if newly_enriched:
            repository.upsert_many(newly_enriched)
            logger.info(
                "результаты_раунда_сохранены",
                total=len(newly_enriched),
                step=f"раунд={round_num}",
            )

        if not newly_enriched and newly_skipped == 0:
            logger.warning(
                "повторное_обогащение_остановлено_нет_прогресса",
                step=f"раунд={round_num}, "
                     f"оставшихся={len(failed_ids)}, "
                     f"в_чёрном_списке={len(blacklisted_ids)}",
            )
            return

        remaining_candidates = len(failed_ids) - newly_blacklisted_by_threshold
        if remaining_candidates <= 0:
            logger.info(
                "повторное_обогащение_завершено",
                step=f"раундов={round_num}, "
                     f"в_чёрном_списке={len(blacklisted_ids)}, "
                     f"причины={skip_reasons_stats}",
            )
            return

        logger.info(
            "пауза_между_раундами",
            step=f"пауза={_RETRY_ROUND_PAUSE_SECONDS}с, "
                 f"осталось_кандидатов={remaining_candidates}",
        )
        await asyncio.sleep(_RETRY_ROUND_PAUSE_SECONDS)

    final_empty = repository.get_empty_listings()
    logger.warning(
        "лимит_раундов_повторного_обогащения_исчерпан",
        step=f"раундов={_MAX_RETRY_ROUNDS}",
        total=f"осталось_пустых={len(final_empty)}, "
              f"в_чёрном_списке={len(blacklisted_ids)}, "
              f"причины={skip_reasons_stats}",
    )


def _is_listing_enriched(listing: "RawListing") -> bool:
    """Проверяет, содержит ли карточка данные календаря и/или цен."""
    if listing.calendar_60_days and any(c == 1 for c in listing.calendar_60_days):
        return True
    if listing.prices_60_days and any(p > 0 for p in listing.prices_60_days):
        return True
    return False


def _run_comparison(
    listings: list,
    snapshot_repository: SQLiteSnapshotRepository,
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


async def _enrich_with_proxy_or_sequential(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    working_proxies: list[ProxyConfig],
    proxy_service: ProxyService | None,
    concurrency_controller: ConcurrencyController | None,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки: параллельно через прокси, через вкладки или последовательно."""
    if settings.use_proxy and working_proxies:
        max_workers = settings.max_proxy_workers
        proxies_to_use = working_proxies[:max_workers]

        if len(working_proxies) > max_workers:
            logger.info(
                "ограничение_воркеров",
                total=len(working_proxies),
                step=f"лимит={max_workers}",
            )

        logger.info(
            "начало_параллельного_парсинга",
            total=len(listings),
            step=f"прокси={len(proxies_to_use)}, вкладок_на_прокси={settings.max_tabs}",
        )

        return await ListingService.enrich_listings_parallel(
            settings=settings,
            listings=listings,
            proxies=proxies_to_use,
            proxy_service=proxy_service,
            concurrency_controller=concurrency_controller,
        )

    return await _enrich_without_proxy(settings, listings, listing_service, logger)


async def _enrich_without_proxy(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки без прокси: через вкладки или последовательно."""
    if settings.max_tabs > 1:
        logger.info(
            "начало_парсинга_карточек_вкладки",
            total=len(listings),
            step=f"вкладок={settings.max_tabs}, tab_delay={settings.tab_delay_ms}мс",
        )
        return await listing_service.enrich_listings_tabbed(listings)

    logger.info(
        "начало_парсинга_карточек_последовательно",
        total=len(listings),
        step="enrichment",
    )
    return await listing_service.enrich_listings(listings)


def main() -> None:
    """Синхронная точка входа — запускает asyncio event loop."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
