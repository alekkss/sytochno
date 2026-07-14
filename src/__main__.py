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
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService


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
    """Основной асинхронный pipeline приложения.

    Последовательно выполняет:
    1. Загрузку конфигурации.
    2. Инициализацию базы данных.
    3. Загрузку и проверку прокси (если USE_PROXY=true).
    4. Парсинг каталога через API.
    5. Batch-обогащение через API:
       - С прокси: параллельно через N прокси-браузеров.
       - Без прокси: один браузер с токеном от каталога.
    6. Batch-retry: один воркер без прокси для необработанных (однократно).
    7. Сохранение, снимки, сравнение, экспорт.
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

        # --- Шаг 9: Сохранение в базу данных ---
        logger.info("сохранение_в_бд", step="storage")
        saved_count = repository.upsert_many(listings)
        logger.info(
            "данные_сохранены",
            total=saved_count,
            step="storage",
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
        try:
            if await browser_service.is_alive():
                await browser_service.stop()
        except Exception:
            pass

        repository.close()
        snapshot_repository.close()
        logger.info("приложение_завершено", step="shutdown")


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


def main() -> None:
    """Синхронная точка входа — запускает asyncio event loop."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
