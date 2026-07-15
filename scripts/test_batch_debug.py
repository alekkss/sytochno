"""Диагностический скрипт: batch-обогащение 100 объявлений с подробными логами.

Цель: выявить точные тексты ошибок API, которые приводят к росту
ошибочных_дней в скользящем окне. Запускается без прокси, один браузер.

Запуск:
    python -m scripts.test_batch_debug

Логи: logs/test_batch_debug.log + вывод в консоль.
"""

import asyncio
import re
import sys
import time
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

# ── Добавляем корень проекта в sys.path ──
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings
from src.services.browser_service import BrowserService
from src.services.listing.batch_enrichment_service import (
    BATCH_SIZE,
    BatchEnrichmentService,
)
from src.services.listing.constants import (
    API_PRICES_URL,
    DAYS_COUNT,
    DEFAULT_GUESTS,
    FALLBACK_GUESTS,
    MIN_NIGHTS_ERROR_KEYWORDS,
    MIN_NIGHTS_VARIANTS,
)
from src.services.listing.price_parser import PriceParser

# ── Тестовые ID ──────────────────────────────────────────────

TEST_IDS: list[int] = [
    2298659, 2305672, 20353, 728489, 2056536, 1915947, 1686733, 1862243,
    2047403, 2271993, 1735249, 2325412, 956337, 2069377, 2205242, 1534367,
    2288071, 2223254, 966819, 1856774, 2179010, 1491671, 2257776, 1351541,
    573675, 1534230, 1642380, 2025468, 762965, 2223620, 1060959, 1337691,
    2083706, 1073321, 2175550, 1931754, 2078662, 1883791, 2000444, 1279831,
    471475, 1419503, 1185431, 1873281, 2220427, 2303295, 982831, 2010577,
    2056435, 1741035, 1595421, 1513877, 1410491, 653397, 1344789, 1899116,
    1978873, 1229857, 1395421, 1286229, 843453, 2065466, 1223705, 2015659,
    2242484, 2042902, 2089219, 854247, 706483, 2029771, 1914067, 1671358,
    2276195, 2263360, 1529294, 1957161, 26885, 2300621, 2295779, 1066651,
    1961988, 2216752, 2274177, 1069721, 2076215, 1093725, 1497144, 287676,
    2120791, 886753, 2319996, 954561, 1911872, 1437111, 2308892, 1543587,
    1405925, 1723130, 2294684, 1732863,
]

# Таймаут одного fetch-запроса внутри браузера (секунды)
_FETCH_TIMEOUT_SECONDS: int = 60

# Таймаут page.evaluate (секунды)
_EVALUATE_TIMEOUT: float = 120.0

# Пауза между batch-запросами (секунды)
_BATCH_PAUSE: float = 0.5

# Количество ночей для скользящего окна по умолчанию
_DEFAULT_SLIDING_NIGHTS: int = 2

# Таймаут перехвата токена (секунды)
_TOKEN_INTERCEPT_TIMEOUT: float = 20.0

# Интервал поллинга токена (секунды)
_TOKEN_POLL_INTERVAL: float = 0.5


async def _fetch_batch_raw(
    page,
    token: str,
    object_ids: list[int],
    date_begin: str,
    date_end: str,
    guests: int = DEFAULT_GUESTS,
) -> dict:
    """Выполняет batch-запрос и возвращает ПОЛНЫЙ сырой ответ API.

    В отличие от BatchEnrichmentService._fetch_batch(), не извлекает
    results[] — возвращает весь JSON для анализа.

    Args:
        page: Страница Playwright.
        token: Токен API.
        object_ids: Список ID объявлений.
        date_begin: Дата начала периода.
        date_end: Дата конца периода.
        guests: Количество гостей.

    Returns:
        Полный словарь ответа API (или словарь с ошибкой).
    """
    try:
        raw_result = await asyncio.wait_for(
            page.evaluate(
                """
                async ({apiUrl, objectIds, dateBegin, dateEnd, token,
                        guests, fetchTimeout}) => {
                    try {
                        const controller = new AbortController();
                        const tid = setTimeout(
                            () => controller.abort(), fetchTimeout * 1000
                        );

                        const resp = await fetch(apiUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'token': token,
                                'platform': 'js',
                                'api-version': '1.13'
                            },
                            body: JSON.stringify({
                                objects: objectIds,
                                rooms_cnt: {},
                                guests: guests,
                                date_begin: dateBegin,
                                date_end: dateEnd,
                                currency_id: 1,
                                is_pets: 0,
                                documents: 0,
                                target: 0,
                                ages: [],
                                no_time: 1
                            }),
                            credentials: 'include',
                            signal: controller.signal
                        });

                        clearTimeout(tid);

                        const text = await resp.text();

                        try {
                            const data = JSON.parse(text);
                            return {
                                success: true,
                                http_status: resp.status,
                                data: data,
                                raw_length: text.length
                            };
                        } catch (e) {
                            return {
                                success: false,
                                error: 'json_parse_error',
                                http_status: resp.status,
                                raw_preview: text.substring(0, 1000)
                            };
                        }

                    } catch (e) {
                        if (e.name === 'AbortError') {
                            return {success: false, error: 'fetch_timeout'};
                        }
                        return {success: false, error: e.message};
                    }
                }
                """,
                {
                    "apiUrl": API_PRICES_URL,
                    "objectIds": object_ids,
                    "dateBegin": date_begin,
                    "dateEnd": date_end,
                    "token": token,
                    "guests": guests,
                    "fetchTimeout": _FETCH_TIMEOUT_SECONDS,
                },
            ),
            timeout=_EVALUATE_TIMEOUT,
        )
    except asyncio.TimeoutError:
        return {"success": False, "error": "evaluate_timeout"}
    except Exception as e:
        return {"success": False, "error": str(e)[:300]}

    return raw_result


def _is_min_nights_error(error_text: str) -> bool:
    """Проверяет, является ли ошибка связанной с min_nights."""
    text = error_text.lower()
    return any(kw in text for kw in MIN_NIGHTS_ERROR_KEYWORDS)


def _extract_min_nights(error_text: str) -> int | None:
    """Извлекает значение min_nights из текста ошибки."""
    if not error_text:
        return None
    is_mn = any(kw in error_text.lower() for kw in MIN_NIGHTS_ERROR_KEYWORDS)
    if not is_mn:
        return None
    numbers = re.findall(r"(\d+)", error_text)
    for num_str in numbers:
        num = int(num_str)
        if 2 <= num <= 999:
            return num
    return None


async def run_diagnostic() -> None:
    """Основная диагностическая процедура."""
    # ── Настройка логирования ──
    configure_logging(
        log_level="DEBUG",
        log_file_path="logs/test_batch_debug.log",
    )
    logger = get_logger("batch_debug")

    settings = Settings.load()

    print("=" * 70)  # noqa: T201
    print("  ДИАГНОСТИКА BATCH-ОБОГАЩЕНИЯ — 100 объявлений")  # noqa: T201
    print("=" * 70)  # noqa: T201
    print(f"  Объектов: {len(TEST_IDS)}")  # noqa: T201
    print(f"  Дней: {DAYS_COUNT}")  # noqa: T201
    print(f"  Гостей: {DEFAULT_GUESTS}")  # noqa: T201
    print("=" * 70)  # noqa: T201

    browser_service = BrowserService(settings=settings)
    today = date.today()

    try:
        # ── Шаг 1: Запуск браузера и получение токена ──
        print("\n[1/4] Запуск браузера и получение токена...")  # noqa: T201
        await browser_service.start()
        page = browser_service.page

        search_url = settings.search_urls[0]
        print(f"  Загрузка: {search_url[:80]}...")  # noqa: T201

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

        elapsed = 0.0
        while elapsed < _TOKEN_INTERCEPT_TIMEOUT:
            if captured_token:
                break
            await asyncio.sleep(_TOKEN_POLL_INTERVAL)
            elapsed += _TOKEN_POLL_INTERVAL

        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass

        await asyncio.sleep(3.0)

        if not captured_token:
            print("  ОШИБКА: Токен не перехвачен!")  # noqa: T201
            return

        token = captured_token[0]
        print(f"  Токен получен (длина={len(token)})")  # noqa: T201

        # ══════════════════════════════════════════════════════════
        #  Шаг 2: ФАЗА 1 — Batch bulk (60 ночей)
        # ══════════════════════════════════════════════════════════
        print(f"\n[2/4] Фаза 1: Batch bulk ({DAYS_COUNT} ночей)...")  # noqa: T201

        date_begin = f"{today.isoformat()} 14:00:00"
        date_end_bulk = f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"

        # Статистика bulk
        bulk_unbusy: list[int] = []
        bulk_busy: list[int] = []
        bulk_errors: dict[int, str] = {}  # object_id → полный текст ошибки
        bulk_fatal: dict[int, str] = {}  # object_id → причина пропуска
        bulk_guests_retry: list[int] = []

        total_batches = (len(TEST_IDS) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(0, len(TEST_IDS), BATCH_SIZE):
            batch = TEST_IDS[batch_idx: batch_idx + BATCH_SIZE]
            batch_num = batch_idx // BATCH_SIZE + 1

            print(f"  Bulk пачка {batch_num}/{total_batches} "  # noqa: T201
                  f"({len(batch)} ID)...", end="")

            raw = await _fetch_batch_raw(
                page, token, batch, date_begin, date_end_bulk,
                guests=DEFAULT_GUESTS,
            )

            if not raw.get("success"):
                error = raw.get("error", "unknown")
                print(f" ОШИБКА FETCH: {error[:100]}")  # noqa: T201
                logger.error(
                    "bulk_fetch_ошибка",
                    batch=batch_num,
                    error=error,
                    raw_preview=raw.get("raw_preview", "")[:500],
                )
                for oid in batch:
                    bulk_errors[oid] = f"fetch_error: {error}"
                await asyncio.sleep(_BATCH_PAUSE)
                continue

            api_data = raw.get("data", {})

            # Проверяем верхнеуровневую ошибку API
            if not api_data.get("success"):
                api_errors = api_data.get("errors", [])
                print(f" ОШИБКА API: {api_errors}")  # noqa: T201
                logger.error(
                    "bulk_api_ошибка",
                    batch=batch_num,
                    errors=api_errors,
                )
                for oid in batch:
                    bulk_errors[oid] = f"api_error: {api_errors}"
                await asyncio.sleep(_BATCH_PAUSE)
                continue

            objects = (api_data.get("data") or {}).get("objects", [])

            batch_ok = 0
            batch_err = 0

            for obj in objects:
                obj_id = obj.get("id", 0)

                if obj.get("success"):
                    busy = (obj.get("data") or {}).get("busy", "unknown")
                    if busy == "unbusy":
                        bulk_unbusy.append(obj_id)
                    elif busy == "busy":
                        bulk_busy.append(obj_id)
                    else:
                        bulk_errors[obj_id] = f"unknown_busy_status: {busy}"
                    batch_ok += 1
                else:
                    errors_raw = obj.get("errors", [])
                    error_text = str(errors_raw)
                    bulk_errors[obj_id] = error_text

                    # Классификация ошибки
                    error_lower = error_text.lower()

                    if "no_objects" in error_lower:
                        bulk_fatal[obj_id] = "object_not_found"
                    elif _is_min_nights_error(error_lower):
                        mn_value = _extract_min_nights(error_lower)
                        if mn_value and mn_value >= 60:
                            bulk_fatal[obj_id] = f"min_nights_exceeded ({mn_value})"
                        else:
                            bulk_errors[obj_id] = f"min_nights={mn_value}: {error_text}"
                    elif "вмещает" in error_lower or "гост" in error_lower:
                        bulk_guests_retry.append(obj_id)
                    # Иначе — неклассифицированная ошибка (останется в bulk_errors)

                    batch_err += 1

                    logger.info(
                        "bulk_объект_ошибка",
                        object_id=obj_id,
                        error_text=error_text[:300],
                    )

            print(f" ok={batch_ok}, err={batch_err}")  # noqa: T201
            await asyncio.sleep(_BATCH_PAUSE)

        # ── Retry с guests=1 ──
        if bulk_guests_retry:
            print(f"\n  Retry {len(bulk_guests_retry)} объектов "  # noqa: T201
                  f"с guests={FALLBACK_GUESTS}...")

            for batch_idx in range(0, len(bulk_guests_retry), BATCH_SIZE):
                batch = bulk_guests_retry[batch_idx: batch_idx + BATCH_SIZE]

                raw = await _fetch_batch_raw(
                    page, token, batch, date_begin, date_end_bulk,
                    guests=FALLBACK_GUESTS,
                )

                if raw.get("success"):
                    api_data = raw.get("data", {})
                    objects = (api_data.get("data") or {}).get("objects", [])

                    for obj in objects:
                        obj_id = obj.get("id", 0)
                        if obj.get("success"):
                            busy = (obj.get("data") or {}).get("busy", "unknown")
                            if busy == "unbusy":
                                bulk_unbusy.append(obj_id)
                            elif busy == "busy":
                                bulk_busy.append(obj_id)
                            # Убираем из errors
                            bulk_errors.pop(obj_id, None)
                        else:
                            error_text = str(obj.get("errors", []))
                            bulk_errors[obj_id] = f"guests_retry_fail: {error_text}"
                            logger.info(
                                "guests_retry_ошибка",
                                object_id=obj_id,
                                error_text=error_text[:300],
                            )

                await asyncio.sleep(_BATCH_PAUSE)

        # ── Сводка Фазы 1 ──
        print(f"\n  ─── Сводка Фазы 1 (Bulk) ───")  # noqa: T201
        print(f"  unbusy (свободны 60 дней): {len(bulk_unbusy)}")  # noqa: T201
        print(f"  busy (нужно скользящее окно): {len(bulk_busy)}")  # noqa: T201
        print(f"  фатальных (пропуск): {len(bulk_fatal)}")  # noqa: T201
        print(f"  ошибок (неклассифицированных): "  # noqa: T201
              f"{len(bulk_errors) - len(bulk_fatal)}")

        if bulk_fatal:
            print(f"\n  Фатальные ошибки:")  # noqa: T201
            for oid, reason in sorted(bulk_fatal.items()):
                print(f"    ID {oid}: {reason}")  # noqa: T201

        non_fatal_errors = {
            oid: txt for oid, txt in bulk_errors.items()
            if oid not in bulk_fatal
        }
        if non_fatal_errors:
            print(f"\n  Неклассифицированные ошибки bulk:")  # noqa: T201
            for oid, txt in sorted(non_fatal_errors.items()):
                print(f"    ID {oid}: {txt[:200]}")  # noqa: T201

        if not bulk_busy:
            print("\n  Нет busy-объектов — скользящее окно не требуется.")  # noqa: T201
            return

        # ══════════════════════════════════════════════════════════
        #  Шаг 3: ФАЗА 2 — Скользящее окно (день за днём)
        # ══════════════════════════════════════════════════════════
        print(f"\n[3/4] Фаза 2: Скользящее окно "  # noqa: T201
              f"({len(bulk_busy)} busy-объектов, {DAYS_COUNT} дней)...")

        # Календари: object_id → [status_day_0, ..., status_day_59]
        # -1 = не определён, 0 = свободен, 1 = занят
        calendars: dict[int, list[int]] = {
            oid: [-1] * DAYS_COUNT for oid in bulk_busy
        }

        # Статистика ошибок скользящего окна
        # Ключ — нормализованный текст ошибки, значение — количество
        sliding_error_counter: Counter = Counter()
        # Подробные ошибки: (object_id, day_offset) → полный текст
        sliding_error_details: dict[tuple[int, int], str] = {}

        nights = _DEFAULT_SLIDING_NIGHTS

        for day_offset in range(DAYS_COUNT):
            day = today + timedelta(days=day_offset)
            end_day = day + timedelta(days=nights)
            d_begin = f"{day.isoformat()} 14:00:00"
            d_end = f"{end_day.isoformat()} 11:00:00"

            # Только ещё не определённые объекты
            pending_ids = [
                oid for oid in bulk_busy
                if calendars[oid][day_offset] == -1
            ]
            if not pending_ids:
                continue

            day_ok = 0
            day_errors = 0

            for batch_start in range(0, len(pending_ids), BATCH_SIZE):
                batch = pending_ids[batch_start: batch_start + BATCH_SIZE]

                raw = await _fetch_batch_raw(
                    page, token, batch, d_begin, d_end,
                    guests=DEFAULT_GUESTS,
                )

                if not raw.get("success"):
                    error = raw.get("error", "unknown")
                    for oid in batch:
                        sliding_error_counter[f"fetch: {error}"] += 1
                        sliding_error_details[(oid, day_offset)] = f"fetch: {error}"
                    day_errors += len(batch)
                    await asyncio.sleep(_BATCH_PAUSE)
                    continue

                api_data = raw.get("data", {})

                if not api_data.get("success"):
                    api_errors = str(api_data.get("errors", []))
                    for oid in batch:
                        sliding_error_counter[f"api: {api_errors[:100]}"] += 1
                        sliding_error_details[(oid, day_offset)] = (
                            f"api: {api_errors}"
                        )
                    day_errors += len(batch)
                    await asyncio.sleep(_BATCH_PAUSE)
                    continue

                objects = (api_data.get("data") or {}).get("objects", [])

                for obj in objects:
                    obj_id = obj.get("id", 0)
                    if obj_id not in calendars:
                        continue

                    if obj.get("success"):
                        busy = (obj.get("data") or {}).get("busy", "unknown")
                        if busy == "busy":
                            calendars[obj_id][day_offset] = 1
                            day_ok += 1
                        elif busy == "unbusy":
                            calendars[obj_id][day_offset] = 0
                            day_ok += 1
                        else:
                            error_key = f"unknown_busy: {busy}"
                            sliding_error_counter[error_key] += 1
                            sliding_error_details[(obj_id, day_offset)] = error_key
                            day_errors += 1
                    else:
                        errors_raw = obj.get("errors", [])
                        error_text = str(errors_raw)

                        # Проверяем min_nights
                        if _is_min_nights_error(error_text.lower()):
                            # Как в основном коде — помечаем как свободный
                            calendars[obj_id][day_offset] = 0
                            day_ok += 1

                            logger.debug(
                                "sliding_min_nights_как_свободный",
                                object_id=obj_id,
                                day=day_offset,
                                error=error_text[:100],
                            )
                        else:
                            # НЕКЛАССИФИЦИРОВАННАЯ ОШИБКА —
                            # именно это приводит к -1 → error_count
                            error_normalized = _normalize_error(error_text)
                            sliding_error_counter[error_normalized] += 1
                            sliding_error_details[(obj_id, day_offset)] = error_text
                            day_errors += 1

                            logger.info(
                                "sliding_неклассифицированная_ошибка",
                                object_id=obj_id,
                                day=day_offset,
                                error_text=error_text[:300],
                            )

                if batch_start + BATCH_SIZE < len(pending_ids):
                    await asyncio.sleep(_BATCH_PAUSE)

            # Прогресс
            if (day_offset + 1) % 10 == 0 or day_offset == DAYS_COUNT - 1:
                remaining_errors = sum(
                    1 for oid in bulk_busy
                    for d in range(DAYS_COUNT)
                    if calendars[oid][d] == -1
                )
                print(f"  День {day_offset + 1}/{DAYS_COUNT}: "  # noqa: T201
                      f"ok={day_ok}, ошибок={day_errors}, "
                      f"всего_нерешённых={remaining_errors}")

            await asyncio.sleep(_BATCH_PAUSE)

        # ══════════════════════════════════════════════════════════
        #  Шаг 3b: Адаптация min_nights
        # ══════════════════════════════════════════════════════════
        failed_ids = [
            oid for oid, cal in calendars.items()
            if all(c == -1 for c in cal)
        ]

        if failed_ids:
            print(f"\n  Адаптация min_nights для "  # noqa: T201
                  f"{len(failed_ids)} полностью провалившихся объектов...")

            for nights_var in MIN_NIGHTS_VARIANTS:
                if nights_var <= _DEFAULT_SLIDING_NIGHTS:
                    continue

                still_failed = [
                    oid for oid in failed_ids
                    if all(c == -1 for c in calendars[oid])
                ]
                if not still_failed:
                    break

                print(f"  Пробуем nights={nights_var} "  # noqa: T201
                      f"для {len(still_failed)} объектов...")

                for day_offset in range(DAYS_COUNT):
                    day = today + timedelta(days=day_offset)
                    end_day = day + timedelta(days=nights_var)
                    d_begin = f"{day.isoformat()} 14:00:00"
                    d_end = f"{end_day.isoformat()} 11:00:00"

                    pending = [
                        oid for oid in still_failed
                        if calendars[oid][day_offset] == -1
                    ]
                    if not pending:
                        continue

                    for batch_start in range(0, len(pending), BATCH_SIZE):
                        batch = pending[batch_start: batch_start + BATCH_SIZE]

                        raw = await _fetch_batch_raw(
                            page, token, batch, d_begin, d_end,
                            guests=DEFAULT_GUESTS,
                        )

                        if raw.get("success"):
                            api_data = raw.get("data", {})
                            if api_data.get("success"):
                                objects = (
                                    (api_data.get("data") or {}).get("objects", [])
                                )
                                for obj in objects:
                                    obj_id = obj.get("id", 0)
                                    if obj_id not in calendars:
                                        continue
                                    if obj.get("success"):
                                        busy = (
                                            (obj.get("data") or {}).get("busy", "unknown")
                                        )
                                        if busy == "busy":
                                            calendars[obj_id][day_offset] = 1
                                        elif busy == "unbusy":
                                            calendars[obj_id][day_offset] = 0
                                    else:
                                        err = str(obj.get("errors", []))
                                        if _is_min_nights_error(err.lower()):
                                            calendars[obj_id][day_offset] = 0

                        await asyncio.sleep(_BATCH_PAUSE)

                    await asyncio.sleep(_BATCH_PAUSE)

        # ══════════════════════════════════════════════════════════
        #  Шаг 4: ИТОГОВАЯ СТАТИСТИКА
        # ══════════════════════════════════════════════════════════
        print(f"\n{'=' * 70}")  # noqa: T201
        print(f"  ИТОГОВАЯ СТАТИСТИКА")  # noqa: T201
        print(f"{'=' * 70}")  # noqa: T201

        # Нормализация и подсчёт
        total_cells = len(bulk_busy) * DAYS_COUNT
        error_cells = sum(
            1 for oid in bulk_busy
            for d in range(DAYS_COUNT)
            if calendars[oid][d] == -1
        )
        busy_cells = sum(
            1 for oid in bulk_busy
            for d in range(DAYS_COUNT)
            if calendars[oid][d] == 1
        )
        free_cells = sum(
            1 for oid in bulk_busy
            for d in range(DAYS_COUNT)
            if calendars[oid][d] == 0
        )

        print(f"\n  Busy-объектов: {len(bulk_busy)}")  # noqa: T201
        print(f"  Всего ячеек (объекты × дни): {total_cells}")  # noqa: T201
        print(f"  Занято (1): {busy_cells} ({busy_cells * 100 / total_cells:.1f}%)")  # noqa: T201
        print(f"  Свободно (0): {free_cells} ({free_cells * 100 / total_cells:.1f}%)")  # noqa: T201
        print(f"  Ошибочных (-1): {error_cells} ({error_cells * 100 / total_cells:.1f}%)")  # noqa: T201

        print(f"\n  ─── Ошибки скользящего окна по типам ───")  # noqa: T201
        if sliding_error_counter:
            for error_type, count in sliding_error_counter.most_common(20):
                print(f"  [{count:5d}] {error_type[:120]}")  # noqa: T201
        else:
            print(f"  Ошибок нет!")  # noqa: T201

        # Примеры ошибок с ID и днями
        if sliding_error_details:
            print(f"\n  ─── Первые 30 ошибочных ячеек (ID, день, текст) ───")  # noqa: T201
            shown = 0
            for (oid, day_off), error_text in sorted(
                sliding_error_details.items()
            ):
                if shown >= 30:
                    break
                real_date = today + timedelta(days=day_off)
                print(f"    ID={oid}, день={day_off} ({real_date}): "  # noqa: T201
                      f"{error_text[:150]}")
                shown += 1

        # Объекты с наибольшим количеством ошибок
        obj_error_counts: Counter = Counter()
        for (oid, _), _ in sliding_error_details.items():
            obj_error_counts[oid] += 1

        if obj_error_counts:
            print(f"\n  ─── Топ-10 объектов по количеству ошибочных дней ───")  # noqa: T201
            for oid, err_count in obj_error_counts.most_common(10):
                cal = calendars[oid]
                busy_d = sum(1 for c in cal if c == 1)
                free_d = sum(1 for c in cal if c == 0)
                err_d = sum(1 for c in cal if c == -1)
                print(f"    ID={oid}: ошибок={err_d}, "  # noqa: T201
                      f"занят={busy_d}, свободен={free_d}")

        # ── Запись полного дампа в лог-файл ──
        logger.info(
            "диагностика_завершена",
            total_ids=len(TEST_IDS),
            unbusy=len(bulk_unbusy),
            busy=len(bulk_busy),
            fatal=len(bulk_fatal),
            total_cells=total_cells,
            error_cells=error_cells,
            error_types=dict(sliding_error_counter.most_common(20)),
        )

        print(f"\n  Подробные логи: logs/test_batch_debug.log")  # noqa: T201
        print(f"{'=' * 70}")  # noqa: T201

    except Exception as e:
        print(f"\n  КРИТИЧЕСКАЯ ОШИБКА: {type(e).__name__}: {e}")  # noqa: T201
        logger.exception(
            "диагностика_критическая_ошибка",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise

    finally:
        try:
            await browser_service.stop()
        except Exception:
            pass


def _normalize_error(error_text: str) -> str:
    """Нормализует текст ошибки для группировки в статистике.

    Убирает ID объектов, конкретные даты и числа — оставляет
    шаблон ошибки для группировки одинаковых.

    Args:
        error_text: Полный текст ошибки.

    Returns:
        Нормализованный шаблон ошибки.
    """
    text = error_text[:200]
    # Убираем конкретные ID объектов
    text = re.sub(r"\b\d{5,8}\b", "<ID>", text)
    # Убираем конкретные даты
    text = re.sub(r"\d{4}-\d{2}-\d{2}", "<DATE>", text)
    # Убираем числа в контексте «N суток/ночей»
    text = re.sub(r"\b\d+\s*(суток|ночей|гост)", r"<N> \1", text)
    return text.strip()


def main() -> None:
    """Точка входа."""
    asyncio.run(run_diagnostic())


if __name__ == "__main__":
    main()
