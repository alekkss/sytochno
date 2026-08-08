"""Тестовый скрипт для проверки PostgreSQL-репозиториев на реальном pipeline.

Изолированно создаёт БД `rentpulse_test`, выполняет полный pipeline парсера
на первых 100 объявлениях из SUTOCHNO_SEARCH_URL_1, делает два снимка
с искусственной модификацией календарей двух объявлений между ними
(симуляция брони и отмены), запускает сравнение и выводит структурированный
отчёт по всем этапам.

Прод-БД `rentpulse` не трогается. Прод-отчёт `data/sutochno_report.xlsx`
не перезаписывается — тестовые файлы имеют суффикс `_test`.

Запуск:
    python -m scripts.test_full_pipeline

После завершения тестовая БД остаётся для ручной проверки:
    psql -U rentpulse -d rentpulse_test
"""

from __future__ import annotations

import asyncio
import sys
import time
import traceback
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Импорты проекта
from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.booking_event import EventType
from src.models.listing import RawListing
from src.repositories.db_factory import create_repositories
from src.services.browser_service import BrowserService
from src.services.comparison_export_service import ComparisonExportService
from src.services.comparison_service import ComparisonService
from src.services.data_cleaner_service import DataCleanerService
from src.services.export_service import ExportService
from src.services.listing.batch_enrichment_service import BatchEnrichmentService
from src.services.proxy_service import ProxyService
from src.services.scraper_service import ScraperService
from src.services.snapshot_service import SnapshotService
from src.__main__ import (  # переиспользуем существующие вспомогательные функции
    _batch_retry_without_proxy,
    _count_enriched,
    _count_unenriched,
    _get_unenriched_listings,
)


# ── Константы теста ─────────────────────────────────────────

_TEST_DB_NAME: str = "rentpulse_test"
_TEST_EXPORT_PATH: str = "data/sutochno_report_test.xlsx"
_TEST_LOG_PATH: str = "logs/test_pipeline.log"
_LISTING_LIMIT: int = 100

# Модификация календаря
_BOOKING_NIGHTS: int = 3   # сколько подряд 0→1 для симуляции брони
_CANCEL_NIGHTS: int = 2    # сколько подряд 1→0 для симуляции отмены


# ── Регистр результатов этапов ──────────────────────────────


class _StageReport:
    """Собирает результаты каждого этапа для финального вывода."""

    def __init__(self) -> None:
        self.stages: list[tuple[str, str, list[str]]] = []
        # (название, статус ✅/❌/⚠, список строк с деталями)

    def add(self, name: str, status: str, details: list[str]) -> None:
        self.stages.append((name, status, details))

    def render(self, db_name: str, run_started: datetime) -> str:
        lines: list[str] = []
        border = "═" * 60
        sep = "─" * 60

        lines.append(border)
        lines.append("  ТЕСТ POSTGRESQL PIPELINE — РЕЗУЛЬТАТЫ")
        lines.append(border)
        lines.append(f"БД:              {db_name}")
        lines.append(f"Прогон:          {run_started.isoformat(timespec='seconds')}")
        lines.append(sep)

        for i, (name, status, details) in enumerate(self.stages, start=1):
            lines.append(f"ЭТАП {i} — {name}  {status}")
            for line in details:
                lines.append(f"  {line}")
            lines.append("")

        lines.append(sep)
        # Общий вердикт
        any_failed = any(status == "❌" for _, status, _ in self.stages)
        if any_failed:
            lines.append("❌ Тест провален — см. этапы с отметкой ❌")
        else:
            lines.append("✅ PostgreSQL работает корректно")
            lines.append(f"   Данные сохранены в БД {db_name}")
            lines.append(f"   Для проверки: psql -U rentpulse -d {db_name}")
        lines.append(border)

        return "\n".join(lines)


# ── Создание тестовой БД ────────────────────────────────────


def _create_test_database(settings: Settings) -> tuple[bool, str]:
    """Пытается создать чистую БД rentpulse_test через DROP+CREATE.

    Args:
        settings: Прод-настройки (используются PG_HOST/PORT/USER/PASSWORD).

    Returns:
        Кортеж (успех, сообщение). При успехе message содержит SQL,
        который был выполнен. При неудаче — инструкцию для ручного создания.
    """
    try:
        import psycopg  # type: ignore
    except ImportError:
        return False, (
            "Модуль psycopg не установлен. Выполните:\n"
            '   pip install "psycopg[binary]" psycopg_pool'
        )

    # Подключаемся к системной БД postgres (существует всегда)
    admin_dsn = (
        f"host={settings.pg_host} "
        f"port={settings.pg_port} "
        f"user={settings.pg_user} "
        f"password={settings.pg_password} "
        f"dbname=postgres"
    )

    try:
        with psycopg.connect(admin_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                # Отключаем всех клиентов от тестовой БД (если она существует)
                cur.execute(
                    """
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                    """,
                    (_TEST_DB_NAME,),
                )
                cur.execute(f'DROP DATABASE IF EXISTS "{_TEST_DB_NAME}"')
                cur.execute(
                    f'CREATE DATABASE "{_TEST_DB_NAME}" OWNER "{settings.pg_user}"'
                )
        return True, f"БД {_TEST_DB_NAME} создана заново"

    except Exception as e:
        message = (
            f"Не удалось создать БД автоматически: {type(e).__name__}: {e}\n"
            f"Выполните вручную под суперпользователем PostgreSQL:\n"
            f"   psql -U postgres -c \"DROP DATABASE IF EXISTS {_TEST_DB_NAME};\"\n"
            f"   psql -U postgres -c \"CREATE DATABASE {_TEST_DB_NAME} "
            f"OWNER {settings.pg_user};\""
        )
        return False, message


# ── Модификация календарей ──────────────────────────────────


def _find_consecutive(
    calendar: list[int],
    value: int,
    length: int,
) -> int | None:
    """Ищет индекс начала первой подпоследовательности из `length` подряд идущих `value`.

    Args:
        calendar: Календарь на 60 дней.
        value: Искомое значение (0 или 1).
        length: Требуемая длина подряд.

    Returns:
        Индекс начала или None если не найдено.
    """
    if len(calendar) < length:
        return None

    for i in range(len(calendar) - length + 1):
        if all(calendar[i + j] == value for j in range(length)):
            return i
    return None


def _modify_calendars(
    listings: list[RawListing],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Модифицирует календари двух объявлений в памяти.

    - Первое найденное с ≥_BOOKING_NIGHTS подряд '0' → превращаем в '1'
      (симуляция брони).
    - Другое (не то же самое) с ≥_CANCEL_NIGHTS подряд '1' → превращаем в '0'
      (симуляция отмены).

    Args:
        listings: Список объявлений (модифицируется in-place).

    Returns:
        Кортеж (описание_брони, описание_отмены). Каждый элемент —
        словарь с полями {listing, start_idx, nights, calendar_before,
        calendar_after} или None если подходящее не найдено.
    """
    booking_info: dict[str, Any] | None = None
    cancellation_info: dict[str, Any] | None = None
    booking_listing_id: str | None = None

    # ── Симуляция брони: 0→1 ──
    for listing in listings:
        if not listing.calendar_60_days or len(listing.calendar_60_days) != 60:
            continue

        idx = _find_consecutive(listing.calendar_60_days, 0, _BOOKING_NIGHTS)
        if idx is None:
            continue

        calendar_before = "".join(str(c) for c in listing.calendar_60_days)

        # Модифицируем: заменяем _BOOKING_NIGHTS подряд идущих 0 на 1
        for j in range(_BOOKING_NIGHTS):
            listing.calendar_60_days[idx + j] = 1

        calendar_after = "".join(str(c) for c in listing.calendar_60_days)

        booking_info = {
            "listing": listing,
            "start_idx": idx,
            "nights": _BOOKING_NIGHTS,
            "calendar_before": calendar_before,
            "calendar_after": calendar_after,
        }
        booking_listing_id = listing.external_id
        break

    # ── Симуляция отмены: 1→0 (в другом объявлении) ──
    for listing in listings:
        if listing.external_id == booking_listing_id:
            continue
        if not listing.calendar_60_days or len(listing.calendar_60_days) != 60:
            continue

        idx = _find_consecutive(listing.calendar_60_days, 1, _CANCEL_NIGHTS)
        if idx is None:
            continue

        calendar_before = "".join(str(c) for c in listing.calendar_60_days)

        for j in range(_CANCEL_NIGHTS):
            listing.calendar_60_days[idx + j] = 0

        calendar_after = "".join(str(c) for c in listing.calendar_60_days)

        cancellation_info = {
            "listing": listing,
            "start_idx": idx,
            "nights": _CANCEL_NIGHTS,
            "calendar_before": calendar_before,
            "calendar_after": calendar_after,
        }
        break

    return booking_info, cancellation_info


# ── Основной pipeline ───────────────────────────────────────


async def _run_test_pipeline(report: _StageReport) -> int:
    """Выполняет полный тестовый pipeline.

    Args:
        report: Регистр этапов для сбора результатов.

    Returns:
        Код возврата процесса (0 — успех, 1 — ошибка).
    """
    # ── ЭТАП 0: Загрузка настроек ──
    try:
        prod_settings = Settings.load()
    except RuntimeError as e:
        report.add("Загрузка настроек", "❌", [f"Ошибка: {e}"])
        return 1

    if prod_settings.db_type != "postgresql":
        report.add(
            "Загрузка настроек", "❌",
            [
                f"DB_TYPE={prod_settings.db_type}, ожидается 'postgresql'.",
                "Установите DB_TYPE=postgresql в .env для этого теста.",
            ],
        )
        return 1

    if not prod_settings.search_urls:
        report.add(
            "Загрузка настроек", "❌",
            ["В .env нет ни одной SUTOCHNO_SEARCH_URL_*"],
        )
        return 1

    # ── ЭТАП 1: Создание тестовой БД ──
    ok, msg = _create_test_database(prod_settings)
    if not ok:
        report.add("Создание БД rentpulse_test", "❌", msg.split("\n"))
        return 1
    report.add(
        "Создание БД rentpulse_test", "✅",
        [msg, f"Хост: {prod_settings.pg_host}:{prod_settings.pg_port}"],
    )

    # ── Переопределяем настройки для теста ──
    # Settings — frozen dataclass, используем dataclasses.replace
    test_settings = replace(
        prod_settings,
        pg_name=_TEST_DB_NAME,
        search_urls=[prod_settings.search_urls[0]],
        export_path=_TEST_EXPORT_PATH,
        log_file_path=_TEST_LOG_PATH,
    )

    configure_logging(
        log_level=test_settings.log_level,
        log_file_path=test_settings.log_file_path,
    )
    logger = get_logger("test_pipeline")
    logger.info("тест_pipeline_старт", step=f"db={_TEST_DB_NAME}")

    # ── ЭТАП 2: Инициализация репозиториев ──
    try:
        repos = create_repositories(test_settings)
        repository = repos.listing
        snapshot_repository = repos.snapshot
    except Exception as e:
        report.add(
            "Инициализация репозиториев", "❌",
            [f"{type(e).__name__}: {e}"],
        )
        return 1

    report.add(
        "Инициализация репозиториев", "✅",
        [
            "PostgreSQLListingRepository инициализирован",
            "PostgreSQLSnapshotRepository инициализирован",
            "Таблицы созданы автоматически",
        ],
    )

    # ── ЭТАП 3: Прокси (опционально) ──
    working_proxies: list = []
    proxy_service: ProxyService | None = None

    if test_settings.use_proxy:
        proxy_service = ProxyService(settings=test_settings)
        try:
            proxies = proxy_service.load_proxies()
            working_proxies = await proxy_service.check_proxies(proxies)
            report.add(
                "Проверка прокси", "✅" if working_proxies else "⚠",
                [
                    f"Загружено: {len(proxies)}",
                    f"Рабочих: {len(working_proxies)}",
                ],
            )
        except Exception as e:
            report.add(
                "Проверка прокси", "⚠",
                [f"Не удалось: {type(e).__name__}: {e}", "Продолжаем без прокси"],
            )
    else:
        report.add("Проверка прокси", "⏭", ["USE_PROXY=false — пропущено"])

    # ── ЭТАП 4: Парсинг каталога ──
    browser_service = BrowserService(settings=test_settings)
    scraper_service = ScraperService(
        settings=test_settings,
        browser_service=browser_service,
        proxies=working_proxies,
    )

    try:
        stage_start = time.perf_counter()
        listings, catalog_token = await scraper_service.scrape_catalog()
        stage_elapsed = time.perf_counter() - stage_start
    except Exception as e:
        report.add(
            "Парсинг каталога", "❌",
            [f"{type(e).__name__}: {e}"],
        )
        await browser_service.stop()
        return 1

    catalog_total = len(listings)
    listings = listings[:_LISTING_LIMIT]

    report.add(
        "Парсинг каталога", "✅" if listings else "❌",
        [
            f"URL: {test_settings.search_urls[0][:80]}...",
            f"Собрано всего: {catalog_total}",
            f"Ограничено до: {len(listings)} (лимит {_LISTING_LIMIT})",
            f"Токен получен: {'да' if catalog_token else 'нет'}",
            f"Время: {stage_elapsed:.1f}с",
        ],
    )

    if not listings:
        await browser_service.stop()
        return 1

    # ── ЭТАП 5: Batch-обогащение ──
    batch_enrichment_service = BatchEnrichmentService()

    try:
        stage_start = time.perf_counter()

        if test_settings.use_proxy and working_proxies:
            await browser_service.stop()
            await batch_enrichment_service.enrich_batch_parallel(
                settings=test_settings,
                listings=listings,
                proxies=working_proxies,
                search_url=test_settings.search_urls[0],
                proxy_service=proxy_service,
            )
        elif catalog_token is not None:
            if await browser_service.is_alive():
                await batch_enrichment_service.enrich_batch(
                    page=browser_service.page,
                    token=catalog_token,
                    listings=listings,
                    search_url=test_settings.search_urls[0],
                )
            await browser_service.stop()
        else:
            await browser_service.stop()

        stage_elapsed = time.perf_counter() - stage_start
    except Exception as e:
        report.add(
            "Batch-обогащение", "❌",
            [f"{type(e).__name__}: {e}"],
        )
        await browser_service.stop()
        return 1

    enriched_after_batch = _count_enriched(listings)
    unenriched_after_batch = _count_unenriched(listings)
    fatal_after_batch = sum(
        1 for l in listings if l.enrichment_skip_reason is not None
    )

    report.add(
        "Batch-обогащение", "✅",
        [
            f"Обогащено: {enriched_after_batch}",
            f"Фатальных (пропущено сайтом): {fatal_after_batch}",
            f"Требует retry: {unenriched_after_batch}",
            f"Время: {stage_elapsed:.1f}с",
        ],
    )

    # ── ЭТАП 6: Batch-retry необработанных ──
    unenriched_listings = _get_unenriched_listings(listings)
    retry_details: list[str]

    if unenriched_listings:
        try:
            stage_start = time.perf_counter()
            await _batch_retry_without_proxy(
                settings=test_settings,
                batch_enrichment_service=batch_enrichment_service,
                unenriched_listings=unenriched_listings,
                logger=logger,
            )
            stage_elapsed = time.perf_counter() - stage_start

            final_enriched = _count_enriched(listings)
            final_unenriched = _count_unenriched(listings)

            retry_details = [
                f"Retry для: {len(unenriched_listings)}",
                f"Обогащено после retry: {final_enriched - enriched_after_batch}",
                f"Осталось пустых: {final_unenriched}",
                f"Время: {stage_elapsed:.1f}с",
            ]
            report.add("Batch-retry без прокси", "✅", retry_details)
        except Exception as e:
            report.add(
                "Batch-retry без прокси", "⚠",
                [f"Ошибка: {type(e).__name__}: {e}", "Продолжаем с тем, что есть"],
            )
    else:
        report.add(
            "Batch-retry без прокси", "⏭",
            ["Все карточки обогащены — retry не нужен"],
        )

    # ── ЭТАП 7: Расчёт price_per_sqm ──
    try:
        data_cleaner = DataCleanerService(
            price_deviation_up=test_settings.price_deviation_up,
            price_deviation_down=test_settings.price_deviation_down,
        )
        data_cleaner.clean_listings(listings)

        with_price_per_sqm = sum(
            1 for l in listings if l.price_per_sqm is not None
        )
        report.add(
            "Расчёт price_per_sqm", "✅",
            [
                f"Обработано: {len(listings)}",
                f"С расчётом: {with_price_per_sqm}",
                f"Без площади/цены: {len(listings) - with_price_per_sqm}",
            ],
        )
    except Exception as e:
        report.add(
            "Расчёт price_per_sqm", "❌",
            [f"{type(e).__name__}: {e}"],
        )

    # ── ЭТАП 8: Сохранение в PostgreSQL ──
    try:
        stage_start = time.perf_counter()
        saved = repository.upsert_many(listings)
        stage_elapsed = time.perf_counter() - stage_start

        count_in_db = repository.count()
        report.add(
            "Сохранение в PostgreSQL", "✅" if count_in_db == len(listings) else "⚠",
            [
                f"upsert_many вернул: {saved}",
                f"COUNT(*) в БД: {count_in_db}",
                f"Ожидалось: {len(listings)}",
                f"Время: {stage_elapsed:.2f}с",
            ],
        )
    except Exception as e:
        report.add(
            "Сохранение в PostgreSQL", "❌",
            [f"{type(e).__name__}: {e}"],
        )
        return _finalize(report, repository, snapshot_repository, prod_settings, 1)

    # ── ЭТАП 9: Загрузка из БД (проверка round-trip) ──
    try:
        all_listings = repository.get_all()
        report.add(
            "Загрузка из PostgreSQL (get_all)", "✅",
            [
                f"Загружено: {len(all_listings)}",
                f"С календарём: {sum(1 for l in all_listings if l.calendar_60_days)}",
                f"С ценами: {sum(1 for l in all_listings if l.prices_60_days)}",
            ],
        )
    except Exception as e:
        report.add(
            "Загрузка из PostgreSQL", "❌",
            [f"{type(e).__name__}: {e}"],
        )
        return _finalize(report, repository, snapshot_repository, prod_settings, 1)

    # ── ЭТАП 10: Снимок №1 ──
    snapshot_service = SnapshotService(repository=snapshot_repository)

    try:
        stage_start = time.perf_counter()
        snap1_list = snapshot_service.save_snapshots(all_listings)
        stage_elapsed = time.perf_counter() - stage_start

        report.add(
            "Снимок №1 (SnapshotService)", "✅" if snap1_list else "⚠",
            [
                f"Создано снимков: {len(snap1_list)}",
                f"Пропущено (пустой календарь): {len(all_listings) - len(snap1_list)}",
                f"Время: {stage_elapsed:.2f}с",
            ],
        )
    except Exception as e:
        report.add(
            "Снимок №1", "❌", [f"{type(e).__name__}: {e}"],
        )
        return _finalize(report, repository, snapshot_repository, prod_settings, 1)

    # ── ЭТАП 11: Модификация календарей ──
    booking_info, cancellation_info = _modify_calendars(all_listings)

    mod_details: list[str] = []
    if booking_info is not None:
        listing_b: RawListing = booking_info["listing"]
        idx = booking_info["start_idx"]
        n = booking_info["nights"]
        mod_details.append(
            f"БРОНЬ: ID={listing_b.external_id} \"{listing_b.title[:40]}\""
        )
        mod_details.append(
            f"  Дни {idx}–{idx + n - 1}: 0→1 ({n} ночей)"
        )
        mod_details.append(
            f"  До:    ...{booking_info['calendar_before'][max(0, idx - 3):idx + n + 3]}..."
        )
        mod_details.append(
            f"  После: ...{booking_info['calendar_after'][max(0, idx - 3):idx + n + 3]}..."
        )
    else:
        mod_details.append("БРОНЬ: не найдено подходящих объявлений (нет ≥3 подряд 0)")

    if cancellation_info is not None:
        listing_c: RawListing = cancellation_info["listing"]
        idx = cancellation_info["start_idx"]
        n = cancellation_info["nights"]
        mod_details.append(
            f"ОТМЕНА: ID={listing_c.external_id} \"{listing_c.title[:40]}\""
        )
        mod_details.append(
            f"  Дни {idx}–{idx + n - 1}: 1→0 ({n} ночей)"
        )
        mod_details.append(
            f"  До:    ...{cancellation_info['calendar_before'][max(0, idx - 3):idx + n + 3]}..."
        )
        mod_details.append(
            f"  После: ...{cancellation_info['calendar_after'][max(0, idx - 3):idx + n + 3]}..."
        )
    else:
        mod_details.append("ОТМЕНА: не найдено подходящих объявлений (нет ≥2 подряд 1)")

    report.add(
        "Модификация календарей",
        "✅" if (booking_info or cancellation_info) else "⚠",
        mod_details,
    )

    # ── ЭТАП 12: Снимок №2 ──
    # Пауза, чтобы snapshot_dt отличался хотя бы на секунду
    await asyncio.sleep(1.5)

    try:
        stage_start = time.perf_counter()
        snap2_list = snapshot_service.save_snapshots(all_listings)
        stage_elapsed = time.perf_counter() - stage_start

        report.add(
            "Снимок №2 (с модификациями)", "✅" if snap2_list else "⚠",
            [
                f"Создано снимков: {len(snap2_list)}",
                f"Время: {stage_elapsed:.2f}с",
            ],
        )
    except Exception as e:
        report.add(
            "Снимок №2", "❌", [f"{type(e).__name__}: {e}"],
        )
        return _finalize(report, repository, snapshot_repository, prod_settings, 1)

    # ── ЭТАП 13: Сравнение (детекция брони и отмены) ──
    comparison_service = ComparisonService()
    events_all: list = []
    compare_details: list[str] = []

    target_ids: list[str] = []
    if booking_info is not None:
        target_ids.append(booking_info["listing"].external_id)
    if cancellation_info is not None:
        target_ids.append(cancellation_info["listing"].external_id)

    if not target_ids:
        compare_details.append("Нет модифицированных listing-ов — сравнение пропущено")
        report.add("Сравнение снимков", "⚠", compare_details)
    else:
        try:
            snapshots_map = snapshot_repository.get_last_two_batch(target_ids)

            # ── Проверка брони ──
            if booking_info is not None:
                listing_b = booking_info["listing"]
                snaps = snapshots_map.get(listing_b.external_id, [])
                if len(snaps) < 2:
                    compare_details.append(
                        f"БРОНЬ {listing_b.external_id}: снимков в БД {len(snaps)}<2"
                    )
                else:
                    events = comparison_service.compare(
                        old_snapshot=snaps[0],
                        new_snapshot=snaps[1],
                        listing_title=listing_b.title,
                    )
                    events_all.extend(events)
                    booking_events = [
                        e for e in events if e.event_type == EventType.BOOKING
                    ]
                    if booking_events:
                        e = booking_events[0]
                        compare_details.append(
                            f"БРОНЬ ✅ {e.checkin_date}→{e.checkout_date}, "
                            f"{e.nights}н, глубина {e.depth_days}д, "
                            f"цена {e.price_per_night}₽/ночь, итого {e.total_price}₽"
                        )
                    else:
                        compare_details.append(
                            f"БРОНЬ ❌ событий BOOKING не обнаружено "
                            f"(всего событий: {len(events)})"
                        )

            # ── Проверка отмены ──
            if cancellation_info is not None:
                listing_c = cancellation_info["listing"]
                snaps = snapshots_map.get(listing_c.external_id, [])
                if len(snaps) < 2:
                    compare_details.append(
                        f"ОТМЕНА {listing_c.external_id}: снимков в БД {len(snaps)}<2"
                    )
                else:
                    events = comparison_service.compare(
                        old_snapshot=snaps[0],
                        new_snapshot=snaps[1],
                        listing_title=listing_c.title,
                    )
                    events_all.extend(events)
                    cancel_events = [
                        e for e in events if e.event_type == EventType.CANCELLATION
                    ]
                    if cancel_events:
                        e = cancel_events[0]
                        compare_details.append(
                            f"ОТМЕНА ✅ {e.checkin_date}→{e.checkout_date}, "
                            f"{e.nights}н, глубина {e.depth_days}д, "
                            f"цена {e.price_per_night}₽/ночь, итого {e.total_price}₽"
                        )
                    else:
                        compare_details.append(
                            f"ОТМЕНА ❌ событий CANCELLATION не обнаружено "
                            f"(всего событий: {len(events)})"
                        )

            expected = (1 if booking_info else 0) + (1 if cancellation_info else 0)
            compare_details.append(
                f"Всего событий обнаружено: {len(events_all)} (ожидалось: {expected})"
            )

            status = "✅" if len(events_all) >= expected else "⚠"
            report.add("Сравнение снимков", status, compare_details)

        except Exception as e:
            compare_details.append(f"Ошибка: {type(e).__name__}: {e}")
            report.add("Сравнение снимков", "❌", compare_details)

    # ── ЭТАП 14: Экспорт основного отчёта ──
    try:
        export_service = ExportService(settings=test_settings)
        export_path = export_service.export(all_listings)
        report.add(
            "Экспорт основного отчёта", "✅",
            [
                f"Файл: {export_path}",
                f"Строк: {len(all_listings)}",
            ],
        )
    except Exception as e:
        report.add(
            "Экспорт основного отчёта", "❌",
            [f"{type(e).__name__}: {e}"],
        )

    # ── ЭТАП 15: Экспорт отчёта сравнения ──
    if events_all:
        try:
            export_dir = str(Path(test_settings.export_path).parent)
            comparison_export = ComparisonExportService(export_dir=export_dir)
            comp_path = comparison_export.export(events_all)
            report.add(
                "Экспорт отчёта сравнения", "✅",
                [
                    f"Файл: {comp_path}",
                    f"Событий: {len(events_all)}",
                ],
            )
        except Exception as e:
            report.add(
                "Экспорт отчёта сравнения", "❌",
                [f"{type(e).__name__}: {e}"],
            )
    else:
        report.add(
            "Экспорт отчёта сравнения", "⏭",
            ["Событий нет — файл не создан"],
        )

    return _finalize(report, repository, snapshot_repository, prod_settings, 0)


def _finalize(
    report: _StageReport,
    repository: Any,
    snapshot_repository: Any,
    prod_settings: Settings,
    exit_code: int,
) -> int:
    """Закрывает репозитории и возвращает код возврата."""
    try:
        repository.close()
    except Exception:
        pass
    try:
        snapshot_repository.close()
    except Exception:
        pass
    return exit_code


# ── Точка входа ─────────────────────────────────────────────


def main() -> None:
    """Точка входа теста."""
    run_started = datetime.now(timezone.utc)
    report = _StageReport()

    try:
        exit_code = asyncio.run(_run_test_pipeline(report))
    except KeyboardInterrupt:
        report.add("Прерывание", "❌", ["Тест прерван пользователем (Ctrl+C)"])
        exit_code = 130
    except Exception as e:
        report.add(
            "Необработанное исключение", "❌",
            [f"{type(e).__name__}: {e}", *traceback.format_exc().splitlines()[-5:]],
        )
        exit_code = 1

    # Финальный вывод в консоль
    print()  # noqa: T201
    print(report.render(_TEST_DB_NAME, run_started))  # noqa: T201
    print()  # noqa: T201

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
