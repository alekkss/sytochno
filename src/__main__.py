"""Точка входа приложения — сборка зависимостей и запуск pipeline."""

import asyncio
import sys

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.repositories.sqlite_repository import SQLiteListingRepository
from src.services.browser_service import BrowserService
from src.services.export_service import ExportService
from src.services.listing_service import ListingService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService


async def run() -> None:
    """Основной асинхронный pipeline приложения.

    Последовательно выполняет:
    1. Загрузку конфигурации.
    2. Инициализацию базы данных.
    3. Запуск браузера.
    4. Парсинг каталога.
    5. Обогащение объявлений данными календаря (последовательно, вкладки или прокси+вкладки).
    6. Сохранение результатов в SQLite.
    7. Экспорт в Excel.
    8. Корректное завершение работы.
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
    logger.info("приложение_запущено", step="init")

    # --- Шаг 3: Инициализация репозитория ---
    repository = SQLiteListingRepository(db_path=settings.db_path)
    repository.initialize()

    # --- Шаг 4: Создание сервисов (Dependency Injection) ---
    browser_service = BrowserService(settings=settings)
    scraper_service = ScraperService(settings=settings, browser_service=browser_service)
    listing_service = ListingService(settings=settings, browser_service=browser_service)
    export_service = ExportService(settings=settings)

    try:
        # --- Шаг 5: Запуск браузера ---
        await browser_service.start()

        # --- Шаг 6: Парсинг каталога ---
        logger.info("начало_парсинга_каталога", step="scraping")
        listings = await scraper_service.scrape_catalog()

        if not listings:
            logger.warning("объявления_не_найдены")
            return

        logger.info(
            "каталог_собран",
            total=len(listings),
            step="scraping",
        )

        # --- Шаг 7: Обогащение — парсинг карточек (календарь + цены) ---
        listings = await _enrich_with_proxy_or_sequential(
            settings=settings,
            listings=listings,
            listing_service=listing_service,
            logger=logger,
        )

        logger.info(
            "карточки_обработаны",
            total=len(listings),
            step="enrichment",
        )

        # --- Шаг 8: Сохранение в базу данных ---
        logger.info("сохранение_в_бд", step="storage")
        saved_count = repository.upsert_many(listings)
        logger.info(
            "данные_сохранены",
            total=saved_count,
            step="storage",
        )

        # --- Шаг 9: Экспорт в Excel ---
        logger.info("экспорт_в_excel", step="export")
        all_listings = repository.get_all()
        export_path = export_service.export(all_listings)
        logger.info(
            "экспорт_завершён",
            path=export_path,
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
        # --- Шаг 10: Корректное завершение ---
        await browser_service.stop()
        repository.close()
        logger.info("приложение_завершено", step="shutdown")


async def _enrich_with_proxy_or_sequential(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки: параллельно через прокси, через вкладки или последовательно.

    Логика выбора режима:
    1. Если USE_PROXY=true — загружает и проверяет прокси.
       Каждый прокси-браузер использует MAX_TABS вкладок.
    2. Если прокси выключены и MAX_TABS > 1 — параллельные вкладки в одном браузере.
    3. Если прокси выключены и MAX_TABS = 1 — последовательная обработка.

    Args:
        settings: Настройки приложения.
        listings: Список карточек для обогащения.
        listing_service: Сервис парсинга карточек.
        logger: Логгер.

    Returns:
        Список обогащённых карточек.
    """
    if settings.use_proxy:
        logger.info("режим_прокси_включён", step="enrichment")

        proxy_service = ProxyService(settings=settings)

        # Загружаем прокси из файла
        try:
            proxies = proxy_service.load_proxies()
        except RuntimeError as e:
            logger.warning(
                "ошибка_загрузки_прокси",
                error=str(e),
                step="enrichment",
            )
            logger.info("переход_в_режим_без_прокси", step="enrichment")
            return await _enrich_without_proxy(settings, listings, listing_service, logger)

        # Проверяем прокси на работоспособность
        working_proxies = await proxy_service.check_proxies(proxies)

        if not working_proxies:
            logger.warning(
                "нет_рабочих_прокси",
                step="enrichment",
            )
            logger.info("переход_в_режим_без_прокси", step="enrichment")
            return await _enrich_without_proxy(settings, listings, listing_service, logger)

        # Ограничиваем количество воркеров
        max_workers = settings.max_proxy_workers
        if len(working_proxies) > max_workers:
            logger.info(
                "ограничение_воркеров",
                total=len(working_proxies),
                step=f"лимит={max_workers}",
            )
            working_proxies = working_proxies[:max_workers]

        # Параллельная обработка через рабочие прокси (каждая с MAX_TABS вкладками)
        logger.info(
            "начало_параллельного_парсинга",
            total=len(listings),
            step=f"прокси={len(working_proxies)}, вкладок_на_прокси={settings.max_tabs}",
        )

        enriched = await ListingService.enrich_listings_parallel(
            settings=settings,
            listings=listings,
            proxies=working_proxies,
        )

        return enriched

    # Режим без прокси
    return await _enrich_without_proxy(settings, listings, listing_service, logger)


async def _enrich_without_proxy(
    settings: Settings,
    listings: list,
    listing_service: ListingService,
    logger: "any",  # type: ignore[name-defined]
) -> list:
    """Обогащает карточки без прокси: через вкладки или последовательно.

    Если MAX_TABS > 1 — параллельные вкладки в одном браузере.
    Если MAX_TABS = 1 — последовательная обработка (как раньше).

    Args:
        settings: Настройки приложения.
        listings: Список карточек для обогащения.
        listing_service: Сервис парсинга карточек.
        logger: Логгер.

    Returns:
        Список обогащённых карточек.
    """
    if settings.max_tabs > 1:
        # Параллельные вкладки в одном браузере
        logger.info(
            "начало_парсинга_карточек_вкладки",
            total=len(listings),
            step=f"вкладок={settings.max_tabs}, tab_delay={settings.tab_delay_ms}мс",
        )
        return await listing_service.enrich_listings_tabbed(listings)

    # Последовательная обработка — одна вкладка
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
