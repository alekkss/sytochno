"""Тестовый скрипт: два прогона с паузой 10 минут для проверки сравнения снимков.

Запуск:
    python -m scripts.test_comparison

Что делает:
1. Собирает каталог только с первой страницы (≤50 объявлений), берёт первые 20.
2. Обогащает карточки (календарь + цены).
3. Сохраняет снимки первого прогона.
4. Ждёт 10 минут с обратным отсчётом в консоли.
5. Повторяет шаги 1–3 для второго прогона.
6. Запускает сравнение снимков и выводит события в консоль.
7. Если события есть — сохраняет отчёт comparison_report_*.xlsx.

Логи пишутся в logs/test_comparison.log (отдельный файл).
"""

import asyncio
import sys
from dataclasses import replace
from pathlib import Path

# Добавляем корень проекта в sys.path, чтобы работал импорт src.*
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.booking_event import AnyEvent
from src.repositories.snapshot_repository import SQLiteSnapshotRepository
from src.repositories.sqlite_repository import SQLiteListingRepository
from src.services.browser_service import BrowserService
from src.services.comparison_export_service import ComparisonExportService
from src.services.comparison_service import ComparisonService
from src.services.listing_service import ListingService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService

# Количество объявлений для теста
TEST_LISTINGS_LIMIT = 20

# Пауза между прогонами в секундах (10 минут)
PAUSE_SECONDS = 10 * 60


async def run_single_pass(
    pass_number: int,
    settings: Settings,
    browser_service: BrowserService,
    scraper_service: ScraperService,
    listing_service: ListingService,
    repository: SQLiteListingRepository,
    snapshot_service: SnapshotService,
    logger: object,
) -> list:
    """Выполняет один прогон: каталог → карточки → сохранение → снимок.

    Args:
        pass_number: Номер прогона (1 или 2) — для логов.
        settings: Настройки приложения.
        browser_service: Сервис браузера.
        scraper_service: Сервис парсинга каталога.
        listing_service: Сервис обогащения карточек.
        repository: Репозиторий объявлений.
        snapshot_service: Сервис снимков.
        logger: Логгер.

    Returns:
        Список обогащённых объявлений прогона.
    """
    logger.info(
        "прогон_начало",
        step=f"прогон={pass_number}",
    )

    # --- Парсинг каталога (только первая страница) ---
    logger.info("парсинг_каталога", step=f"прогон={pass_number}")
    listings = await scraper_service.scrape_catalog()

    if not listings:
        logger.warning("объявления_не_найдены", step=f"прогон={pass_number}")
        return []

    # Ограничиваем до TEST_LISTINGS_LIMIT объявлений
    if len(listings) > TEST_LISTINGS_LIMIT:
        listings = listings[:TEST_LISTINGS_LIMIT]
        logger.info(
            "список_обрезан",
            total=TEST_LISTINGS_LIMIT,
            step=f"прогон={pass_number}",
        )

    logger.info(
        "каталог_собран",
        total=len(listings),
        step=f"прогон={pass_number}",
    )

    # --- Обогащение карточек ---
    logger.info("обогащение_карточек", step=f"прогон={pass_number}")

    if settings.max_tabs > 1:
        listings = await listing_service.enrich_listings_tabbed(listings)
    else:
        listings = await listing_service.enrich_listings(listings)

    logger.info(
        "карточки_обработаны",
        total=len(listings),
        step=f"прогон={pass_number}",
    )

    # --- Сохранение в БД ---
    saved_count = repository.upsert_many(listings)
    logger.info(
        "данные_сохранены",
        total=saved_count,
        step=f"прогон={pass_number}",
    )

    # --- Сохранение снимков ---
    snapshot_service.save_snapshots(listings)
    logger.info("снимки_сохранены", step=f"прогон={pass_number}")

    logger.info(
        "прогон_завершён",
        step=f"прогон={pass_number}",
        total=len(listings),
    )

    return listings


async def wait_with_countdown(seconds: int, logger: object) -> None:
    """Ожидает заданное количество секунд с обратным отсчётом в консоли.

    Каждую минуту выводит сообщение об оставшемся времени.

    Args:
        seconds: Количество секунд ожидания.
        logger: Логгер.
    """
    logger.info(
        "ожидание_начало",
        total=f"{seconds // 60} минут",
    )

    print(f"\n⏳ Ожидание {seconds // 60} минут перед вторым прогоном...\n")  # noqa: T201

    elapsed = 0
    interval = 60  # отсчёт каждую минуту

    while elapsed < seconds:
        remaining = seconds - elapsed
        minutes_left = remaining // 60
        seconds_left = remaining % 60

        print(  # noqa: T201
            f"   ⏱  Осталось: {minutes_left} мин {seconds_left:02d} сек",
            flush=True,
        )

        sleep_time = min(interval, remaining)
        await asyncio.sleep(sleep_time)
        elapsed += sleep_time

    print("\n✅ Ожидание завершено. Запускаем второй прогон...\n")  # noqa: T201
    logger.info("ожидание_завершено")


def run_comparison(
    listings: list,
    snapshot_repository: SQLiteSnapshotRepository,
    comparison_service: ComparisonService,
    logger: object,
) -> list[AnyEvent]:
    """Сравнивает два последних снимка для каждого объявления.

    Args:
        listings: Список объявлений второго прогона.
        snapshot_repository: Репозиторий снимков.
        comparison_service: Сервис сравнения.
        logger: Логгер.

    Returns:
        Список всех найденных событий (брони и отмены).
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
    )

    return sorted(all_events, key=lambda e: e.checkin_date)


def print_events_summary(events: list[AnyEvent]) -> None:
    """Выводит сводку найденных событий в консоль.

    Args:
        events: Список событий для вывода.
    """
    if not events:
        print("\n📭 Изменений не обнаружено.")  # noqa: T201
        return

    bookings = [e for e in events if e.event_type.value == "бронь"]
    cancellations = [e for e in events if e.event_type.value == "отмена"]

    print(f"\n📊 Итог сравнения: {len(events)} событий")  # noqa: T201
    print(f"   ✅ Броней:  {len(bookings)}")  # noqa: T201
    print(f"   ❌ Отмен:   {len(cancellations)}")  # noqa: T201
    print()  # noqa: T201

    header = (
        f"{'Тип':<8} {'ID':>10}  {'Название':<40}  "
        f"{'Заезд':<12} {'Выезд':<12} {'Ночей':>6} {'Цена/ночь':>12}"
    )
    print(header)  # noqa: T201
    print("-" * len(header))  # noqa: T201

    for event in events:
        print(  # noqa: T201
            f"{event.event_type.value:<8} "
            f"{event.listing_external_id:>10}  "
            f"{event.listing_title[:40]:<40}  "
            f"{event.checkin_date.strftime('%d.%m.%Y'):<12} "
            f"{event.checkout_date.strftime('%d.%m.%Y'):<12} "
            f"{event.nights:>6} "
            f"{event.price_per_night:>11.0f}₽"
        )

    print()  # noqa: T201


async def run() -> None:
    """Главная функция тестового скрипта: два прогона + сравнение."""

    # --- Загрузка конфигурации ---
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # --- Логи пишутся в отдельный файл test_comparison.log ---
    test_log_path = str(Path(settings.log_file_path).parent / "test_comparison.log")
    configure_logging(
        log_level=settings.log_level,
        log_file_path=test_log_path,
    )
    logger = get_logger("test.comparison")

    print(f"\n🚀 Тест сравнения снимков — {TEST_LISTINGS_LIMIT} объявлений\n")  # noqa: T201
    print(f"   Логи: {test_log_path}\n")  # noqa: T201
    logger.info(
        "тест_запущен",
        limit=TEST_LISTINGS_LIMIT,
        log_file=test_log_path,
    )

    # --- Настройки для теста: только первая страница каталога ---
    # dataclasses.replace() создаёт копию frozen-датакласса с нужными полями.
    # Оригинальный settings не меняется — используется для всего остального.
    test_settings = replace(settings, max_pages=1)

    # --- Инициализация репозиториев ---
    repository = SQLiteListingRepository(db_path=settings.db_path)
    repository.initialize()

    snapshot_repository = SQLiteSnapshotRepository(db_path=settings.db_path)
    snapshot_repository.initialize()

    # --- Инициализация сервисов ---
    browser_service = BrowserService(settings=settings)

    # scraper_service получает test_settings (max_pages=1) — парсит только первую страницу.
    # listing_service и остальные сервисы используют оригинальный settings.
    scraper_service = ScraperService(settings=test_settings, browser_service=browser_service)
    listing_service = ListingService(settings=settings, browser_service=browser_service)
    snapshot_service = SnapshotService(repository=snapshot_repository)
    comparison_service = ComparisonService()

    export_dir = str(Path(settings.export_path).parent)
    comparison_export_service = ComparisonExportService(export_dir=export_dir)

    listings_pass2: list = []

    try:
        await browser_service.start()

        # === ПРОГОН 1 ===
        print("=" * 60)  # noqa: T201
        print("  ПРОГОН 1 / 2")  # noqa: T201
        print("=" * 60)  # noqa: T201

        await run_single_pass(
            pass_number=1,
            settings=settings,
            browser_service=browser_service,
            scraper_service=scraper_service,
            listing_service=listing_service,
            repository=repository,
            snapshot_service=snapshot_service,
            logger=logger,
        )

        # === ПАУЗА 10 МИНУТ ===
        await wait_with_countdown(seconds=PAUSE_SECONDS, logger=logger)

        # === ПРОГОН 2 ===
        print("=" * 60)  # noqa: T201
        print("  ПРОГОН 2 / 2")  # noqa: T201
        print("=" * 60)  # noqa: T201

        listings_pass2 = await run_single_pass(
            pass_number=2,
            settings=settings,
            browser_service=browser_service,
            scraper_service=scraper_service,
            listing_service=listing_service,
            repository=repository,
            snapshot_service=snapshot_service,
            logger=logger,
        )

        # === СРАВНЕНИЕ ===
        print("\n🔍 Запуск сравнения снимков...\n")  # noqa: T201

        all_events = run_comparison(
            listings=listings_pass2,
            snapshot_repository=snapshot_repository,
            comparison_service=comparison_service,
            logger=logger,
        )

        print_events_summary(all_events)

        # === ЭКСПОРТ ОТЧЁТА ===
        if all_events:
            comparison_path = comparison_export_service.export(all_events)
            logger.info("отчёт_сравнения_сохранён", path=comparison_path)
            print(f"📁 Отчёт сохранён: {comparison_path}\n")  # noqa: T201
        else:
            logger.info("событий_не_найдено_отчёт_не_создан")

    except KeyboardInterrupt:
        logger.warning("прервано_пользователем")
        print("\n⛔ Прервано пользователем.")  # noqa: T201
    except Exception as e:
        logger.exception(
            "критическая_ошибка",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"\n[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)
    finally:
        await browser_service.stop()
        repository.close()
        snapshot_repository.close()
        logger.info("тест_завершён")
        print("✔ Готово.\n")  # noqa: T201


def main() -> None:
    """Точка входа — запускает asyncio event loop."""
    asyncio.run(run())


if __name__ == "__main__":
    main()