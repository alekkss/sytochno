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
from src.services.scraper_service import ScraperService


async def run() -> None:
    """Основной асинхронный pipeline приложения.

    Последовательно выполняет:
    1. Загрузку конфигурации.
    2. Инициализацию базы данных.
    3. Запуск браузера.
    4. Парсинг каталога.
    5. Обогащение объявлений данными календаря.
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

        # --- Шаг 7: Обогащение — парсинг карточек (календарь занятости) ---
        logger.info(
            "начало_парсинга_карточек",
            total=len(listings),
            step="enrichment",
        )
        listings = await listing_service.enrich_listings(listings)
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


def main() -> None:
    """Синхронная точка входа — запускает asyncio event loop."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
