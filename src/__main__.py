"""Точка входа приложения — сборка зависимостей и запуск pipeline."""

import asyncio
import sys
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
from src.services.listing_service import ListingService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService


async def run() -> None:
    """Основной асинхронный pipeline приложения.

    Последовательно выполняет:
    1. Загрузку конфигурации.
    2. Инициализацию базы данных.
    3. Загрузку и проверку прокси (если USE_PROXY=true).
    4. Парсинг каталога (двунаправленный обход для каждой ссылки).
    5. Запуск браузера для обогащения карточек.
    6. Обогащение объявлений данными календаря.
    7. Сохранение результатов в SQLite.
    8. Сохранение снимков текущего прогона.
    9. Сравнение с предыдущими снимками и экспорт отчёта изменений.
    10. Экспорт основного отчёта в Excel (перезаписывается).
    11. Экспорт датированного снимка Excel (накапливается).
    12. Корректное завершение работы.
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

    # --- Шаг 4: Загрузка и проверка прокси (до создания сервисов) ---
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

    # --- Шаг 5: Создание сервисов (Dependency Injection) ---
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
    )
    export_service = ExportService(settings=settings)

    snapshot_service = SnapshotService(repository=snapshot_repository)
    comparison_service = ComparisonService()

    # Папка для отчётов сравнения — рядом с основным Excel-файлом
    export_dir = str(Path(settings.export_path).parent)
    comparison_export_service = ComparisonExportService(export_dir=export_dir)

    # Флаг: был ли запущен браузер для обогащения
    enrichment_browser_started = False

    try:
        # --- Шаг 6: Парсинг каталога (Этап 1) ---
        logger.info(
            "начало_парсинга_каталога",
            step="scraping",
            urls_count=len(settings.search_urls),
            proxies_available=len(working_proxies),
        )
        listings = await scraper_service.scrape_catalog()

        if not listings:
            logger.warning("объявления_не_найдены")
            return

        logger.info(
            "каталог_собран",
            total=len(listings),
            step="scraping",
        )

        # --- Шаг 7: Запуск браузера для обогащения (Этап 2) ---
        await browser_service.start()
        enrichment_browser_started = True

        # --- Шаг 8: Обогащение — парсинг карточек (календарь + цены) ---
        listings = await _enrich_with_proxy_or_sequential(
            settings=settings,
            listings=listings,
            listing_service=listing_service,
            working_proxies=working_proxies,
            proxy_service=proxy_service,
            logger=logger,
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

        # --- Шаг 10: Сохранение снимков текущего прогона ---
        logger.info("сохранение_снимков", step="snapshots")
        snapshot_service.save_snapshots(listings)

        # --- Шаг 11: Сравнение снимков и экспорт отчёта изменений ---
        all_events = _run_comparison(
            listings=listings,
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

        # --- Шаг 12: Экспорт отчётов в Excel ---
        logger.info("экспорт_в_excel", step="export")
        all_listings = repository.get_all()

        # Основной отчёт — перезаписывается при каждом запуске
        export_path = export_service.export(all_listings)
        logger.info(
            "основной_отчёт_сохранён",
            path=export_path,
            total=len(all_listings),
            step="export",
        )

        # Датированный снимок — накапливается, имя содержит дату и время парсинга.
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
        # --- Шаг 13: Корректное завершение ---
        if enrichment_browser_started:
            await browser_service.stop()
        repository.close()
        snapshot_repository.close()
        logger.info("приложение_завершено", step="shutdown")


def _run_comparison(
    listings: list,
    snapshot_repository: SQLiteSnapshotRepository,
    comparison_service: ComparisonService,
    logger: "any",  # type: ignore[name-defined]
) -> list[AnyEvent]:
    """Сравнивает последние два снимка для каждого объявления.

    Для каждого объявления из текущего прогона:
    1. Загружает два последних снимка из БД.
    2. Если снимков два — запускает сравнение.
    3. Собирает все события в общий список.

    Args:
        listings: Список объявлений текущего прогона.
        snapshot_repository: Репозиторий снимков.
        comparison_service: Сервис сравнения.
        logger: Логгер.

    Returns:
        Объединённый список всех событий по всем объявлениям,
        отсортированный по дате заезда.
    """
    all_events: list[AnyEvent] = []
    compared = 0
    skipped = 0

    for listing in listings:
        external_id: str = getattr(listing, "external_id", "")
        title: str = getattr(listing, "title", "")

        if not external_id:
            skipped += 1
            continue

        snapshots = snapshot_repository.get_last_two(external_id)

        if len(snapshots) < 2:
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

    logger.info(
        "сравнение_завершено",
        compared=compared,
        skipped=skipped,
        total_events=len(all_events),
        step="comparison",
    )

    return sorted(all_events, key=lambda e: e.checkin_date)


async def _enrich_with_proxy_or_sequential(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    working_proxies: list[ProxyConfig],
    proxy_service: ProxyService | None,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки: параллельно через прокси, через вкладки или последовательно.

    Прокси уже проверены на этапе инициализации — повторная проверка не нужна.

    Логика выбора режима:
    1. Если USE_PROXY=true и есть рабочие прокси — параллельно через прокси.
       Каждый прокси-браузер использует MAX_TABS вкладок.
    2. Если прокси выключены или нерабочие, MAX_TABS > 1 — параллельные вкладки.
    3. Если MAX_TABS = 1 — последовательная обработка.

    Args:
        settings: Настройки приложения.
        listings: Список карточек для обогащения.
        listing_service: Сервис парсинга карточек.
        working_proxies: Список проверенных рабочих прокси.
        proxy_service: Сервис прокси с заполненным пулом (для передачи в воркеры).
        logger: Логгер.

    Returns:
        Список обогащённых карточек.
    """
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
        )

    return await _enrich_without_proxy(settings, listings, listing_service, logger)


async def _enrich_without_proxy(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки без прокси: через вкладки или последовательно.

    Если MAX_TABS > 1 — параллельные вкладки в одном браузере.
    Если MAX_TABS = 1 — последовательная обработка.

    Args:
        settings: Настройки приложения.
        listings: Список карточек для обогащения.
        listing_service: Сервис парсинга карточек.
        logger: Логгер.

    Returns:
        Список обогащённых карточек.
    """
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
