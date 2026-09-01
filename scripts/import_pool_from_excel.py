"""Одноразовый импорт ID объявлений из Excel-отчёта в пул.

Запуск (из корня проекта, при активированном виртуальном окружении):

    python -m scripts.import_pool_from_excel --excel /root/sutochno/data/sutochno_report_20260831_231803.xlsx

Что делает:
1. Читает Excel-файл, находит столбец «ID объявления».
2. Добавляет все уникальные ID в таблицу listing_pool
   с источником excel_import.
3. Печатает итог и размер пула после импорта.

Скрипт идемпотентен: существующие в пуле ID пропускаются,
повторный запуск дублей не создаёт. После первичного импорта
скрипт и Excel-файл больше не нужны.

Рекомендуется запускать при остановленном парсере
(во избежание одновременной записи в лог-файл).
"""

import argparse
import sys

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.pool import POOL_SOURCE_EXCEL
from src.repositories.db_factory import create_repositories
from src.services.excel_pool_importer import ExcelPoolImporter
from src.services.pool_service import PoolService


def _parse_args() -> argparse.Namespace:
    """Разбирает аргументы командной строки.

    Returns:
        Распарсенные аргументы.
    """
    parser = argparse.ArgumentParser(
        prog="import_pool_from_excel",
        description=(
            "Одноразовый импорт ID объявлений из Excel-отчёта "
            "в пул (таблица listing_pool)."
        ),
    )
    parser.add_argument(
        "--excel",
        required=True,
        help=(
            "Путь к Excel-файлу отчёта со столбцом «ID объявления» "
            "(например, sutochno_report_20260831_231803.xlsx)."
        ),
    )
    return parser.parse_args()


def main() -> int:
    """Выполняет импорт и возвращает код выхода.

    Returns:
        0 при успехе, 1 при ошибке (файл не найден, формат неверен,
        ошибка БД).
    """
    args = _parse_args()

    # ── Настройки и логирование (как в основном приложении) ──
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        return 1

    configure_logging(
        log_level=settings.log_level,
        log_file_path=settings.log_file_path,
    )
    logger = get_logger("import_pool")

    logger.info("импорт_пула_начат", excel=args.excel)

    # ── Извлечение ID из Excel ──
    importer = ExcelPoolImporter()

    try:
        external_ids = importer.extract_ids(args.excel)
    except (FileNotFoundError, RuntimeError) as e:
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        logger.error("импорт_пула_провален", error=str(e)[:300])
        return 1

    if not external_ids:
        message = (
            "В столбце «ID объявления» не найдено ни одного корректного "
            f"ID. Проверьте файл: {args.excel}"
        )
        print(f"[ОШИБКА] {message}", file=sys.stderr)  # noqa: T201
        logger.error("импорт_пула_провален", error=message)
        return 1

    # ── Запись в пул ──
    repos = create_repositories(settings)

    try:
        pool_service = PoolService(pool_repository=repos.pool)
        result = pool_service.add_ids(
            external_ids=external_ids,
            source=POOL_SOURCE_EXCEL,
        )
        pool_total = repos.pool.count()

    except Exception as e:
        print(  # noqa: T201
            f"[ОШИБКА] Не удалось записать ID в базу данных: "
            f"{type(e).__name__}: {e}",
            file=sys.stderr,
        )
        logger.error(
            "импорт_пула_провален",
            error=str(e)[:300],
            error_type=type(e).__name__,
        )
        return 1
    finally:
        repos.pool.close()
        repos.listing.close()
        repos.snapshot.close()

    # ── Итог ──
    print(  # noqa: T201
        f"[OK] Импорт завершён: из Excel извлечено {result.requested} ID, "
        f"добавлено новых {result.added}, "
        f"уже было в пуле {result.skipped_existing}, "
        f"отброшено некорректных {result.skipped_invalid}. "
        f"Текущий размер пула: {pool_total}."
    )
    logger.info(
        "импорт_пула_завершён",
        requested=result.requested,
        added=result.added,
        skipped_existing=result.skipped_existing,
        skipped_invalid=result.skipped_invalid,
        pool_total=pool_total,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
