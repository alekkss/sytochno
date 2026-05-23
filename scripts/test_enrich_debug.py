"""Отладочный скрипт: проверяет сбор данных по конкретным ID объявлений.

Запуск:
    python -m scripts.test_enrich_debug

Что делает:
1. Строит минимальный RawListing для каждого тестового ID.
2. Запускает enrich_listing для каждого объявления последовательно.
3. Логирует детальный результат: токен, цены, занятость, время.
4. Выводит сводную таблицу в консоль.

Логи пишутся в logs/test_enrich_debug.log.
"""

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.services.browser_service import BrowserService
from src.services.listing_service import ListingService

# ─── Список проблемных ID для проверки ───────────────────────────────────────
TEST_IDS: list[str] = [
    "1242499", "1767433", "1368129", "2028517", "1733855", "2199692",
    "875371",  "1336523", "1904591", "957229",  "2043172", "1660813",
    "410571",  "1084933", "1462937", "976157",
]

# Базовый URL карточки — такой же формат, как в основном парсере
_BASE_DETAIL_URL = "https://sutochno.ru/front/searchapp/detail/{id}"


def build_test_listing(external_id: str) -> RawListing:
    """Строит минимальный RawListing для тестового прогона.

    Все необязательные поля оставляем пустыми — нас интересует
    только результат enrich_listing (calendar_60_days, prices_60_days).

    Args:
        external_id: ID объявления на sutochno.ru.

    Returns:
        Минимальный объект RawListing.
    """
    return RawListing(
        external_id=external_id,
        title=f"Тест ID={external_id}",
        url=_BASE_DETAIL_URL.format(id=external_id),
    )


def format_result_row(
    external_id: str,
    listing: RawListing,
    elapsed: float,
    error: str | None,
) -> str:
    """Форматирует одну строку сводной таблицы.

    Args:
        external_id: ID объявления.
        listing: Обогащённый (или нет) объект.
        elapsed: Время обработки в секундах.
        error: Текст ошибки или None.

    Returns:
        Отформатированная строка для вывода в консоль.
    """
    has_calendar = len(listing.calendar_60_days) == 60
    prices_count = sum(1 for p in listing.prices_60_days if p > 0)
    busy_count = sum(listing.calendar_60_days)
    free_count = len(listing.calendar_60_days) - busy_count

    if error:
        status = "❌ ОШИБКА"
    elif not has_calendar:
        status = "⚠️  НЕТ ДАННЫХ"
    elif prices_count == 0:
        status = "⚠️  НЕТ ЦЕН"
    else:
        status = "✅ OK"

    return (
        f"{external_id:>10}  {status:<16}  "
        f"календарь={len(listing.calendar_60_days):>2}/60  "
        f"цен={prices_count:>3}  "
        f"свободных={free_count:>3}  "
        f"занятых={busy_count:>3}  "
        f"время={elapsed:>5.1f}с"
        + (f"  ошибка={error}" if error else "")
    )


async def run() -> None:
    """Главная функция: обогащает каждый тестовый ID и выводит итог."""

    # --- Загрузка конфигурации ---
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # --- Логи в отдельный файл ---
    debug_log_path = str(Path(settings.log_file_path).parent / "test_enrich_debug.log")

    # Для отладки принудительно включаем DEBUG-уровень,
    # чтобы видеть токены, bulk-запросы и скользящее окно.
    configure_logging(
        log_level="DEBUG",
        log_file_path=debug_log_path,
    )
    logger = get_logger("test.enrich_debug")

    print(f"\n🔍 Отладка сбора данных — {len(TEST_IDS)} объявлений\n")  # noqa: T201
    print(f"   Логи (DEBUG): {debug_log_path}\n")  # noqa: T201

    logger.info(
        "тест_запущен",
        total=len(TEST_IDS),
        log_file=debug_log_path,
    )

    # --- Инициализация сервисов ---
    browser_service = BrowserService(settings=settings)
    listing_service = ListingService(
        settings=settings,
        browser_service=browser_service,
    )

    results: list[tuple[str, RawListing, float, str | None]] = []

    try:
        await browser_service.start()

        for idx, external_id in enumerate(TEST_IDS, start=1):
            listing = build_test_listing(external_id)
            error: str | None = None

            print(  # noqa: T201
                f"[{idx:>2}/{len(TEST_IDS)}] Обрабатываем ID={external_id}...",
                flush=True,
            )
            logger.info(
                "обработка_тестового_id",
                current=idx,
                total=len(TEST_IDS),
                step=f"id={external_id}",
            )

            start = time.perf_counter()
            try:
                listing = await listing_service.enrich_listing(listing)
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                logger.error(
                    "ошибка_обогащения",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"id={external_id}",
                )
            elapsed = time.perf_counter() - start

            # Детальный лог результата для каждого ID
            logger.info(
                "результат_тестового_id",
                step=f"id={external_id}",
                total=(
                    f"calendar={len(listing.calendar_60_days)}/60, "
                    f"цен={sum(1 for p in listing.prices_60_days if p > 0)}, "
                    f"занятых={sum(listing.calendar_60_days)}, "
                    f"время={elapsed:.1f}с"
                ),
                error=error or "",
            )

            results.append((external_id, listing, elapsed, error))

            # Пауза между карточками (защита от антибота)
            await browser_service.random_delay()

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
    finally:
        await browser_service.stop()
        logger.info("тест_завершён", total=len(results))

    # --- Сводная таблица ---
    print("\n" + "=" * 90)  # noqa: T201
    print("  ИТОГИ ОТЛАДКИ")  # noqa: T201
    print("=" * 90)  # noqa: T201

    ok_count = 0
    no_data_count = 0
    no_prices_count = 0
    error_count = 0

    for external_id, listing, elapsed, error in results:
        row = format_result_row(external_id, listing, elapsed, error)
        print(row)  # noqa: T201

        if error:
            error_count += 1
        elif len(listing.calendar_60_days) != 60:
            no_data_count += 1
        elif sum(1 for p in listing.prices_60_days if p > 0) == 0:
            no_prices_count += 1
        else:
            ok_count += 1

    print("=" * 90)  # noqa: T201
    print(  # noqa: T201
        f"\n  ✅ Успешно:        {ok_count}"
        f"\n  ⚠️  Нет данных:     {no_data_count}"
        f"\n  ⚠️  Нет цен:        {no_prices_count}"
        f"\n  ❌ Ошибки:         {error_count}"
        f"\n  Всего:            {len(results)}"
    )
    print(f"\n  Подробности: {debug_log_path}\n")  # noqa: T201


def main() -> None:
    """Точка входа — запускает asyncio event loop."""
    asyncio.run(run())


if __name__ == "__main__":
    main()