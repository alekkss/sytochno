"""Тестовый скрипт парсинга каталога — сбор карточек без обогащения.

Запуск:
    python -m scripts.test_catalog

Скрипт использует те же компоненты, что и основная программа:
- Загружает настройки из .env
- Запускает браузер со stealth-настройками
- Обходит все страницы каталога с пагинацией
- Выводит результаты в консоль

Карточки НЕ обогащаются (нет захода в отдельные объявления,
нет сбора календаря и цен через API).
"""

import asyncio
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для корректного импорта
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from src.config.logger import configure as configure_logging, get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.services.browser_service import BrowserService
from src.services.scraper_service import ScraperService

# Логгер для тестового скрипта (файл логов отдельный)
_LOG_FILE = "logs/test_catalog.log"


def _print_summary(listings: list[RawListing]) -> None:
    """Выводит сводку по собранным объявлениям в консоль.

    Args:
        listings: Список собранных объявлений.
    """
    print("\n" + "=" * 80)
    print(f"  РЕЗУЛЬТАТ: собрано {len(listings)} объявлений")
    print("=" * 80)

    if not listings:
        print("  Объявления не найдены.")
        return

    # Заголовок таблицы
    print(
        f"  {'№':<4} {'ID':<10} {'Цена':<8} {'Рейтинг':<9} "
        f"{'Отзывы':<8} {'Площадь':<9} {'Гости':<7} {'Бронь':<6} {'Название'}"
    )
    print("  " + "-" * 78)

    for i, listing in enumerate(listings, start=1):
        price_str = f"{listing.price_per_night}" if listing.price_per_night else "—"
        rating_str = f"{listing.rating}" if listing.rating is not None else "—"
        reviews_str = f"{listing.review_count}" if listing.review_count is not None else "—"
        area_str = f"{listing.area_m2} м²" if listing.area_m2 is not None else "—"
        guests_str = f"{listing.guests}" if listing.guests is not None else "—"
        booking_str = "Да" if listing.has_instant_booking else "Нет"
        title_short = listing.title[:35] + "..." if len(listing.title) > 35 else listing.title

        print(
            f"  {i:<4} {listing.external_id:<10} {price_str:<8} {rating_str:<9} "
            f"{reviews_str:<8} {area_str:<9} {guests_str:<7} {booking_str:<6} {title_short}"
        )

    # Статистика
    print("\n  " + "-" * 78)

    prices = [l.price_per_night for l in listings if l.price_per_night]
    if prices:
        print(f"  Цены: мин={min(prices)} руб., макс={max(prices)} руб., средняя={sum(prices)//len(prices)} руб.")

    ratings = [l.rating for l in listings if l.rating is not None]
    if ratings:
        avg_rating = sum(ratings) / len(ratings)
        print(f"  Рейтинг: мин={min(ratings)}, макс={max(ratings)}, средний={avg_rating:.1f}")

    with_metro = sum(1 for l in listings if l.metro_station)
    with_address = sum(1 for l in listings if l.address)
    with_booking = sum(1 for l in listings if l.has_instant_booking)

    print(f"  С адресом: {with_address}/{len(listings)}")
    print(f"  С метро: {with_metro}/{len(listings)}")
    print(f"  Быстрое бронирование: {with_booking}/{len(listings)}")
    print("=" * 80 + "\n")


def _print_details(listings: list[RawListing], max_items: int = 5) -> None:
    """Выводит детали первых N объявлений.

    Args:
        listings: Список объявлений.
        max_items: Максимальное количество для детального вывода.
    """
    if not listings:
        return

    count = min(len(listings), max_items)
    print(f"\n  Детали первых {count} объявлений:")
    print("  " + "-" * 78)

    for i, listing in enumerate(listings[:count], start=1):
        print(f"\n  [{i}] {listing.title}")
        print(f"      ID:       {listing.external_id}")
        print(f"      Цена:     {listing.price_per_night or '—'} руб./сут.")
        print(f"      Рейтинг:  {listing.rating or '—'} ({listing.review_count or 0} отзывов)")
        print(f"      Площадь:  {listing.area_m2 or '—'} м²")
        print(f"      Гостей:   {listing.guests or '—'}")
        print(f"      Адрес:    {listing.address or '—'}")
        print(f"      Метро:    {listing.metro_station or '—'}")
        print(f"      Бронь:    {'Да' if listing.has_instant_booking else 'Нет'}")
        print(f"      Ссылка:   {listing.url}")


async def _run() -> None:
    """Основная асинхронная функция тестового скрипта."""
    # Загружаем настройки
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"\n  ОШИБКА КОНФИГУРАЦИИ: {e}\n")
        sys.exit(1)

    # Настраиваем логирование (отдельный файл для тестового скрипта)
    configure_logging(
        log_level=settings.log_level,
        log_file_path=_LOG_FILE,
    )

    logger = get_logger("test_catalog")

    logger.info(
        "запуск_тестового_парсинга_каталога",
        urls_count=len(settings.search_urls),
        max_pages=settings.max_pages,
        headless=settings.headless_mode,
    )

    print("\n" + "=" * 80)
    print("  ТЕСТОВЫЙ ПАРСИНГ КАТАЛОГА SUTOCHNO.RU")
    print("  (без захода в карточки, только список объявлений)")
    print("=" * 80)
    print(f"\n  Ссылок поиска: {len(settings.search_urls)}")
    print(f"  Лимит страниц: {settings.max_pages or 'все'}")
    print(f"  Headless: {settings.headless_mode}")
    print(f"  Задержки: {settings.min_delay_ms}–{settings.max_delay_ms} мс")
    print(f"  Логи: {_LOG_FILE}")
    print()

    # Создаём сервисы
    browser_service = BrowserService(settings)
    scraper_service = ScraperService(settings, browser_service)

    try:
        # Запускаем браузер
        print("  [1/3] Запуск браузера...")
        await browser_service.start()
        print("  [1/3] Браузер запущен ✓")

        # Парсим каталог
        print("  [2/3] Парсинг каталога...")
        listings = await scraper_service.scrape_catalog()
        print(f"  [2/3] Парсинг завершён ✓ (найдено: {len(listings)} объявлений)")

        # Выводим результаты
        print("  [3/3] Формирование отчёта...")
        _print_summary(listings)
        _print_details(listings)
        print("  [3/3] Готово ✓")

        logger.info(
            "тестовый_парсинг_завершён",
            total_listings=len(listings),
        )

    except KeyboardInterrupt:
        print("\n\n  Прервано пользователем (Ctrl+C).")
        logger.warning("прервано_пользователем")

    except Exception as e:
        print(f"\n  ОШИБКА: {type(e).__name__}: {e}")
        logger.exception("ошибка_тестового_парсинга", error=str(e))

    finally:
        # Останавливаем браузер
        print("\n  Остановка браузера...")
        await browser_service.stop()
        print("  Браузер остановлен.")


def main() -> None:
    """Точка входа тестового скрипта."""
    asyncio.run(_run())


if __name__ == "__main__":
    main()
