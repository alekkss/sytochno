"""Тестовый скрипт первого этапа — парсинг каталога с максимальным логированием.

Выполняет только Этап 1 (обход каталога), пропуская обогащение карточек.
Использует прокси если USE_PROXY=true и есть рабочие прокси.

Логирование установлено на уровень DEBUG — в консоль и файл выводится
каждый шаг: загрузка страниц, прокрутка, парсинг карточек, очистка DOM,
пагинация, восстановление после крахов, ротация прокси.

Запуск:
    python -m scripts.test_catalog

Логи:
    logs/test_catalog.log (отдельный файл, не смешивается с основным)
"""

import asyncio
import sys
import time
from pathlib import Path

# Добавляем корень проекта в sys.path для импорта src.*
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService


# Путь к файлу логов тестового скрипта (отдельный от основного)
_TEST_LOG_FILE = "logs/test_catalog.log"


async def run() -> None:
    """Основной pipeline тестового скрипта.

    Последовательно выполняет:
    1. Загрузку конфигурации из .env.
    2. Настройку логирования на уровне DEBUG.
    3. Загрузку и проверку прокси (если USE_PROXY=true).
    4. Парсинг каталога (Этап 1) — двунаправленный обход всех ссылок.
    5. Вывод итоговой статистики.
    """
    start_time = time.monotonic()

    # --- Шаг 1: Загрузка конфигурации ---
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # --- Шаг 2: Конфигурация логирования (DEBUG для максимальной детализации) ---
    configure_logging(
        log_level="DEBUG",
        log_file_path=_TEST_LOG_FILE,
    )
    logger = get_logger("test_catalog")

    logger.info("=" * 70)
    logger.info(
        "тест_каталога_запущен",
        step="init",
        search_urls_count=len(settings.search_urls),
        max_pages=settings.max_pages,
        headless=settings.headless_mode,
        min_delay_ms=settings.min_delay_ms,
        max_delay_ms=settings.max_delay_ms,
        max_tabs=settings.max_tabs,
        tab_delay_ms=settings.tab_delay_ms,
        use_proxy=settings.use_proxy,
        max_proxy_workers=settings.max_proxy_workers,
        navigation_timeout=settings.navigation_timeout,
    )

    # Выводим все URL поиска
    for i, url in enumerate(settings.search_urls, start=1):
        logger.info(
            "url_поиска",
            step=f"url_{i}",
            url=url,
        )

    # --- Шаг 3: Загрузка и проверка прокси ---
    working_proxies: list[ProxyConfig] = []

    if settings.use_proxy:
        logger.info("=" * 50)
        logger.info("начало_загрузки_прокси", step="proxy")

        proxy_service = ProxyService(settings=settings)

        try:
            proxies = proxy_service.load_proxies()
            logger.info(
                "прокси_загружены_из_файла",
                total=len(proxies),
                path=settings.proxies_path,
                step="proxy",
            )

            # Выводим каждую загруженную прокси
            for i, proxy in enumerate(proxies, start=1):
                logger.debug(
                    "прокси_загружена",
                    step=f"прокси_{i}/{len(proxies)}",
                    proxy=str(proxy),
                )

            logger.info(
                "начало_проверки_прокси",
                total=len(proxies),
                step="proxy",
            )
            check_start = time.monotonic()
            working_proxies = await proxy_service.check_proxies(proxies)
            check_elapsed = time.monotonic() - check_start

            logger.info(
                "проверка_прокси_завершена",
                total_checked=len(proxies),
                working=len(working_proxies),
                failed=len(proxies) - len(working_proxies),
                elapsed_seconds=round(check_elapsed, 1),
                step="proxy",
            )

            # Выводим рабочие прокси
            for i, proxy in enumerate(working_proxies, start=1):
                logger.info(
                    "рабочая_прокси",
                    step=f"прокси_{i}/{len(working_proxies)}",
                    proxy=str(proxy),
                )

        except RuntimeError as e:
            logger.error(
                "ошибка_загрузки_прокси",
                error=str(e),
                step="proxy",
            )
    else:
        logger.info(
            "прокси_отключены",
            step="USE_PROXY=false",
        )

    # --- Шаг 4: Создание сервисов ---
    logger.info("=" * 50)
    logger.info(
        "создание_сервисов",
        step="init",
        proxies_available=len(working_proxies),
    )

    browser_service = BrowserService(settings=settings)
    scraper_service = ScraperService(
        settings=settings,
        browser_service=browser_service,
        proxies=working_proxies,
    )

    # --- Шаг 5: Парсинг каталога (Этап 1) ---
    logger.info("=" * 50)
    logger.info(
        "начало_парсинга_каталога",
        step="scraping",
        urls_count=len(settings.search_urls),
        max_pages=settings.max_pages,
        proxies=len(working_proxies),
    )

    scrape_start = time.monotonic()

    try:
        listings = await scraper_service.scrape_catalog()
    except KeyboardInterrupt:
        logger.warning("прервано_пользователем", step="scraping")
        return
    except Exception as e:
        logger.exception(
            "критическая_ошибка_парсинга",
            error=str(e),
            error_type=type(e).__name__,
            step="scraping",
        )
        return

    scrape_elapsed = time.monotonic() - scrape_start
    total_elapsed = time.monotonic() - start_time

    # --- Шаг 6: Итоговая статистика ---
    logger.info("=" * 70)
    logger.info(
        "парсинг_завершён",
        total_listings=len(listings),
        scrape_seconds=round(scrape_elapsed, 1),
        total_seconds=round(total_elapsed, 1),
        step="result",
    )

    if not listings:
        logger.warning(
            "объявления_не_найдены",
            step="result",
        )
    else:
        # Статистика по собранным данным
        prices = [l.price_per_night for l in listings if l.price_per_night]
        ratings = [l.rating for l in listings if l.rating]
        with_metro = sum(1 for l in listings if l.metro_station)
        with_instant = sum(1 for l in listings if l.has_instant_booking)

        logger.info(
            "статистика_объявлений",
            total=len(listings),
            with_price=len(prices),
            min_price=min(prices) if prices else 0,
            max_price=max(prices) if prices else 0,
            avg_price=round(sum(prices) / len(prices)) if prices else 0,
            with_rating=len(ratings),
            avg_rating=round(sum(ratings) / len(ratings), 1) if ratings else 0,
            with_metro=with_metro,
            with_instant_booking=with_instant,
            step="result",
        )

        # Выводим первые 5 объявлений для визуальной проверки
        logger.info("=" * 50)
        logger.info("примеры_объявлений", step="result")
        for i, listing in enumerate(listings[:5], start=1):
            logger.info(
                f"объявление_{i}",
                id=listing.external_id,
                title=listing.title[:60] if listing.title else "—",
                price=listing.price_per_night,
                rating=listing.rating,
                reviews=listing.review_count,
                area=listing.area_m2,
                guests=listing.guests,
                address=(listing.address[:50] if listing.address else "—"),
                metro=listing.metro_station or "—",
                instant=listing.has_instant_booking,
                url=listing.url[:80] if listing.url else "—",
            )

        # Выводим последние 5 объявлений (чтобы увидеть данные с последних страниц)
        if len(listings) > 10:
            logger.info("=" * 50)
            logger.info("последние_объявления", step="result")
            for i, listing in enumerate(listings[-5:], start=len(listings) - 4):
                logger.info(
                    f"объявление_{i}",
                    id=listing.external_id,
                    title=listing.title[:60] if listing.title else "—",
                    price=listing.price_per_night,
                    rating=listing.rating,
                    reviews=listing.review_count,
                    area=listing.area_m2,
                    guests=listing.guests,
                    address=(listing.address[:50] if listing.address else "—"),
                    metro=listing.metro_station or "—",
                    instant=listing.has_instant_booking,
                    url=listing.url[:80] if listing.url else "—",
                )

    # Итоговая сводка в консоль (удобно видеть даже без логов)
    logger.info("=" * 70)
    logger.info(
        "итого",
        объявлений=len(listings),
        время_парсинга=f"{round(scrape_elapsed, 1)} сек",
        общее_время=f"{round(total_elapsed, 1)} сек",
        прокси_использовано=len(working_proxies),
        step="done",
    )


def main() -> None:
    """Синхронная точка входа."""
    asyncio.run(run())


if __name__ == "__main__":
    main()
