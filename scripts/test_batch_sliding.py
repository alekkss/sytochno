"""Тестовый скрипт: batch скользящее окно и лимиты API.

Проверяет три гипотезы:
1. Можно ли определить занятость конкретного дня для пачки объявлений
   одним запросом (batch скользящее окно: 1 день × N ID).
2. Каков максимальный размер batch (50, 100, 200 ID в objects[]).
3. Каков rate-limit API — сколько запросов подряд можно отправить.

Если batch скользящее окно работает — этап 2 полностью заменяется:
  60 дней × ceil(total_ids / batch_size) запросов вместо
  total_ids × (1 загрузка страницы + 1 bulk + до 60 скользящих).

Пример: 1000 объявлений, batch=50 →
  60 × 20 = 1200 запросов ≈ 10–20 минут вместо 16 часов.

Запуск:
    python -m scripts.test_batch_sliding
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# ── Корень проекта в sys.path ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(PROJECT_ROOT / ".env")

# ── Константы ──

API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
API_PAGE_SIZE: int = 50
DAYS_COUNT: int = 60
DEFAULT_GUESTS: int = 2
API_INTERCEPT_TIMEOUT: float = 30.0
POLL_INTERVAL: float = 0.5
POST_INTERCEPT_WAIT: float = 5.0

SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}

# Размеры batch для теста лимитов
BATCH_LIMIT_SIZES: list[int] = [50, 100, 200]

# Количество запросов для теста rate-limit
RATE_LIMIT_REQUESTS: int = 30

# Пауза между запросами в тесте rate-limit (секунды)
RATE_LIMIT_PAUSE: float = 0.3


def _get_search_url() -> str:
    """Получает первый URL поиска из .env."""
    for i in range(1, 9):
        url = os.getenv(f"SUTOCHNO_SEARCH_URL_{i}", "").strip()
        if url:
            return url
    raise RuntimeError(
        "Не найден ни один SUTOCHNO_SEARCH_URL_* в .env. "
        "Заполните хотя бы SUTOCHNO_SEARCH_URL_1."
    )


def _replace_offset(url: str, new_offset: int) -> str:
    """Заменяет параметр offset в URL."""
    result = re.sub(r"offset=\d+", f"offset={new_offset}", url)
    if "offset=" not in result:
        separator = "&" if "?" in result else "?"
        result = f"{result}{separator}offset={new_offset}"
    return result


def _print_separator(title: str) -> None:
    """Печатает разделитель с заголовком."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


async def _fetch_post(page, url: str, body: dict, token: str) -> dict:
    """Выполняет POST-запрос через fetch() в контексте браузера."""
    try:
        result = await page.evaluate(
            """
            async ({url, body, token}) => {
                try {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), 60000);

                    const resp = await fetch(url, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'token': token,
                            'platform': 'js',
                            'api-version': '1.13'
                        },
                        body: JSON.stringify(body),
                        credentials: 'include',
                        signal: controller.signal
                    });

                    clearTimeout(timeoutId);

                    const text = await resp.text();
                    try {
                        return {
                            success: true,
                            status: resp.status,
                            data: JSON.parse(text)
                        };
                    } catch (e) {
                        return {
                            success: false,
                            error: 'JSON parse error',
                            raw: text.substring(0, 1000)
                        };
                    }
                } catch (e) {
                    if (e.name === 'AbortError') {
                        return {success: false, error: 'timeout'};
                    }
                    return {success: false, error: e.message};
                }
            }
            """,
            {"url": url, "body": body, "token": token},
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


async def _fetch_get(page, url: str, headers: dict[str, str]) -> dict:
    """Выполняет GET-запрос через fetch() в контексте браузера."""
    try:
        result = await page.evaluate(
            """
            async ({url, headers}) => {
                try {
                    const resp = await fetch(url, {
                        method: 'GET',
                        headers: headers,
                        credentials: 'include'
                    });
                    const text = await resp.text();
                    try {
                        return {
                            success: true,
                            status: resp.status,
                            data: JSON.parse(text)
                        };
                    } catch (e) {
                        return {
                            success: false,
                            error: 'JSON parse error',
                            raw: text.substring(0, 500)
                        };
                    }
                } catch (e) {
                    return {success: false, error: e.message};
                }
            }
            """,
            {"url": url, "headers": headers},
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def _build_price_body(
    object_ids: list[int],
    date_begin: str,
    date_end: str,
    guests: int = DEFAULT_GUESTS,
) -> dict:
    """Формирует тело запроса getPricesAndAvailabilities."""
    return {
        "objects": object_ids,
        "rooms_cnt": {},
        "guests": guests,
        "date_begin": date_begin,
        "date_end": date_end,
        "currency_id": 1,
        "is_pets": 0,
        "documents": 0,
        "target": 0,
        "ages": [],
        "no_time": 1,
    }


def _parse_api_response(result: dict) -> tuple[bool, list[dict], str]:
    """Разбирает ответ API.

    Returns:
        Кортеж (success, objects_list, error_message).
    """
    if not result.get("success"):
        return False, [], result.get("error", "fetch error")

    api_data = result.get("data", {})
    if not api_data.get("success"):
        errors = api_data.get("errors", [])
        return False, [], f"API errors: {errors}"

    objects_resp = api_data.get("data", {}).get("objects", [])
    return True, objects_resp, ""


def _extract_objects_from_response(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа searchObjectsOnMap.

    Пробует разные структуры ответа API:
    - data (если сам массив)
    - data.data (массив)
    - data.data.objects
    - data.objects
    и другие варианты.

    Args:
        data: Parsed JSON ответа.

    Returns:
        Список объектов или None.
    """
    if isinstance(data, list) and data:
        return data

    if not isinstance(data, dict):
        return None

    # Прямые ключи
    for key in ("data", "objects", "items", "results", "list"):
        val = data.get(key)
        if isinstance(val, list) and val:
            return val

    # Вложенные: data.data, data.objects и т.д.
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("objects", "items", "results", "list", "data"):
            val = inner.get(key)
            if isinstance(val, list) and val:
                return val

    return None


async def _collect_ids(
    page, map_url: str, api_headers: dict[str, str], max_ids: int = 200,
) -> list[int]:
    """Собирает ID через searchObjectsOnMap с пагинацией.

    Args:
        page: Страница Playwright.
        map_url: Перехваченный URL searchObjectsOnMap.
        api_headers: Заголовки API.
        max_ids: Максимальное количество ID для сбора.

    Returns:
        Список уникальных ID.
    """
    collected: list[int] = []
    seen: set[int] = set()
    offset = 0

    while len(collected) < max_ids:
        paginated_url = _replace_offset(map_url, offset)
        print(f"  Запрос offset={offset}...")

        result = await _fetch_get(page, paginated_url, api_headers)

        if not result.get("success"):
            print(f"    Ошибка fetch: {result.get('error')}")
            print(f"    Raw: {result.get('raw', '')[:500]}")
            break

        data = result.get("data", {})

        # Универсальное извлечение объектов
        objects = _extract_objects_from_response(data)

        if not objects:
            # Диагностика: показываем структуру ответа
            print(f"    Не удалось извлечь объекты.")
            print(f"    Тип data: {type(data).__name__}")
            if isinstance(data, dict):
                print(f"    Ключи data: {list(data.keys())}")
                inner = data.get("data")
                if isinstance(inner, dict):
                    print(f"    Ключи data.data: {list(inner.keys())}")
                elif isinstance(inner, list):
                    print(f"    data.data — массив из {len(inner)} элементов")
                    if inner:
                        sample = json.dumps(inner[0], ensure_ascii=False)[:300]
                        print(f"    Первый элемент: {sample}")
                else:
                    print(f"    data.data: тип={type(inner).__name__}, значение={str(inner)[:200]}")
            # Сырой ответ
            print(f"    Сырой ответ (первые 800 символов):")
            print(f"    {json.dumps(data, ensure_ascii=False, indent=2)[:800]}")
            break

        page_new = 0
        for obj in objects:
            obj_id = obj.get("id")
            if obj_id and obj_id not in seen:
                seen.add(obj_id)
                collected.append(obj_id)
                page_new += 1

        print(
            f"    Получено {len(objects)}, новых {page_new}, "
            f"всего {len(collected)}"
        )

        if len(objects) < API_PAGE_SIZE:
            print(f"    Последняя страница (получено {len(objects)} < {API_PAGE_SIZE})")
            break

        offset += API_PAGE_SIZE
        await asyncio.sleep(0.5)

    return collected[:max_ids]


async def main() -> None:
    """Основная логика тестового скрипта."""
    search_url = _get_search_url()
    print(f"URL поиска: {search_url[:100]}...")

    today = date.today()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="ru-RU",
        )
        page = await context.new_page()

        # ── Этап 1: Загрузка и перехват ──
        _print_separator("ЭТАП 1: Загрузка страницы поиска")

        captured: dict = {"url": None, "headers": None, "token": None}

        async def _intercept(route, request):
            url = request.url
            if "searchObjectsOnMap" in url and captured["url"] is None:
                captured["url"] = url
                captured["headers"] = dict(request.headers)
            if "sutochno.ru/api/json" in url and captured["token"] is None:
                token = request.headers.get("token") or request.headers.get("Token")
                if token:
                    captured["token"] = token
            await route.continue_()

        await page.route("**/api/json/**", _intercept)

        print("Загружаем страницу поиска...")
        await page.goto(search_url, wait_until="domcontentloaded")

        elapsed = 0.0
        while elapsed < API_INTERCEPT_TIMEOUT:
            if captured["url"] and captured["token"]:
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        if captured["url"]:
            print(f"API URL перехвачен за {elapsed:.1f}с. Ждём загрузки...")
            await asyncio.sleep(POST_INTERCEPT_WAIT)

        await page.unroute("**/api/json/**")

        if not captured["token"]:
            print("ОШИБКА: Токен не перехвачен!")
            await browser.close()
            return

        token = captured["token"]
        map_url = captured["url"]
        api_headers = {
            k: v for k, v in (captured["headers"] or {}).items()
            if k.lower() not in SKIP_HEADERS
        } if captured["headers"] else {}

        print(f"Токен: {token[:20]}...")

        if not map_url:
            print("ОШИБКА: URL searchObjectsOnMap не перехвачен!")
            await browser.close()
            return

        # ── Этап 2: Сбор ID (до 200 для тестов) ──
        _print_separator("ЭТАП 2: Сбор ID (до 200)")

        collected_ids = await _collect_ids(page, map_url, api_headers, max_ids=200)
        print(f"\nВсего собрано ID: {len(collected_ids)}")

        if collected_ids:
            print(f"Первые 10: {collected_ids[:10]}")

        if len(collected_ids) < 10:
            print("ОШИБКА: Недостаточно ID для тестирования.")
            await browser.close()
            return

        # ── Тест A: Batch скользящее окно ──
        _print_separator("ТЕСТ A: Batch скользящее окно (1 день × N ID)")
        print(
            "Проверяем: возвращает ли API разный busy-статус\n"
            "для разных объектов в одном batch-запросе на 1 день.\n"
        )

        # Берём 10 ID и проверяем 5 разных дней
        test_ids = collected_ids[:10]
        test_days = [0, 7, 14, 30, 55]  # смещения от сегодня

        print(f"ID для теста: {test_ids}")
        print(f"Дни для теста: сегодня + {test_days}\n")

        # Сначала одиночные запросы — эталон
        print("--- Одиночные запросы (эталон) ---")
        single_day_results: dict[int, dict[int, str]] = {}

        for obj_id in test_ids[:5]:
            single_day_results[obj_id] = {}
            for day_offset in test_days:
                day = today + timedelta(days=day_offset)
                end_day = day + timedelta(days=2)
                body = _build_price_body(
                    [obj_id],
                    f"{day.isoformat()} 14:00:00",
                    f"{end_day.isoformat()} 11:00:00",
                )
                result = await _fetch_post(page, API_PRICES_URL, body, token)
                success, objects, error = _parse_api_response(result)

                if success and objects:
                    obj_data = objects[0]
                    if obj_data.get("success"):
                        busy = obj_data.get("data", {}).get("busy", "???")
                        single_day_results[obj_id][day_offset] = busy
                    else:
                        errors = obj_data.get("errors", [])
                        single_day_results[obj_id][day_offset] = f"err:{str(errors)[:20]}"
                else:
                    single_day_results[obj_id][day_offset] = f"fail:{error[:30]}"

                await asyncio.sleep(0.3)

        # Печатаем эталон
        print(f"\n  {'ID':<12}", end="")
        for d in test_days:
            print(f"  день+{d:<3}", end="")
        print()

        for obj_id in test_ids[:5]:
            print(f"  {obj_id:<12}", end="")
            for d in test_days:
                val = single_day_results.get(obj_id, {}).get(d, "?")
                if val == "busy":
                    display = "busy"
                elif val == "unbusy":
                    display = "FREE"
                else:
                    display = val[:8]
                print(f"  {display:<8}", end="")
            print()

        # Теперь batch-запросы — те же дни, 10 ID за раз
        print("\n--- Batch-запросы (10 ID × 1 день) ---")
        batch_day_results: dict[int, dict[int, str]] = {}

        for day_offset in test_days:
            day = today + timedelta(days=day_offset)
            end_day = day + timedelta(days=2)

            body = _build_price_body(
                test_ids,
                f"{day.isoformat()} 14:00:00",
                f"{end_day.isoformat()} 11:00:00",
            )

            t0 = time.monotonic()
            result = await _fetch_post(page, API_PRICES_URL, body, token)
            elapsed_ms = (time.monotonic() - t0) * 1000

            success, objects, error = _parse_api_response(result)

            if not success:
                print(f"  день+{day_offset}: ОШИБКА — {error}")
                continue

            print(f"  день+{day_offset}: получено {len(objects)} объектов за {elapsed_ms:.0f}мс")

            busy_count = 0
            free_count = 0
            error_count = 0

            for obj in objects:
                obj_id = obj.get("id")
                if obj_id not in batch_day_results:
                    batch_day_results[obj_id] = {}

                if obj.get("success"):
                    busy = obj.get("data", {}).get("busy", "???")
                    batch_day_results[obj_id][day_offset] = busy
                    if busy == "busy":
                        busy_count += 1
                    elif busy == "unbusy":
                        free_count += 1
                else:
                    batch_day_results[obj_id][day_offset] = "err"
                    error_count += 1

            print(
                f"    busy={busy_count}, free={free_count}, "
                f"errors={error_count}"
            )

            await asyncio.sleep(0.5)

        # Сравниваем batch vs одиночные
        print("\n--- Сравнение batch vs одиночные ---")
        print(f"  {'ID':<12}", end="")
        for d in test_days:
            print(f"  день+{d:<3}", end="")
        print("  Совпадение")

        total_checks = 0
        total_matches = 0

        for obj_id in test_ids[:5]:
            print(f"  {obj_id:<12}", end="")
            row_matches = True
            for d in test_days:
                single_val = single_day_results.get(obj_id, {}).get(d, "?")
                batch_val = batch_day_results.get(obj_id, {}).get(d, "?")

                match = single_val == batch_val
                total_checks += 1
                if match:
                    total_matches += 1
                else:
                    row_matches = False

                indicator = "ok" if match else "DIFF"
                print(f"  {indicator:<8}", end="")
            status = "DA" if row_matches else "NET"
            print(f"  {status}")

        if total_checks > 0:
            pct = total_matches / total_checks * 100
            print(f"\n  Итого: {total_matches}/{total_checks} совпадений ({pct:.0f}%)")

        # ── Тест B: Предельный размер batch ──
        _print_separator("ТЕСТ B: Предельный размер batch")
        print("Проверяем максимальное количество ID в одном запросе.\n")

        date_begin_short = f"{today.isoformat()} 14:00:00"
        date_end_short = f"{(today + timedelta(days=2)).isoformat()} 11:00:00"

        for batch_size in BATCH_LIMIT_SIZES:
            if batch_size > len(collected_ids):
                print(
                    f"  Batch {batch_size}: ПРОПУЩЕН "
                    f"(собрано только {len(collected_ids)} ID)"
                )
                continue

            batch_ids = collected_ids[:batch_size]
            body = _build_price_body(batch_ids, date_begin_short, date_end_short)

            t0 = time.monotonic()
            result = await _fetch_post(page, API_PRICES_URL, body, token)
            elapsed_s = time.monotonic() - t0

            success, objects, error = _parse_api_response(result)

            if success:
                ok_count = sum(1 for o in objects if o.get("success"))
                err_count = sum(1 for o in objects if not o.get("success"))
                print(
                    f"  Batch {batch_size}: УСПЕХ за {elapsed_s:.1f}с | "
                    f"получено={len(objects)}, ok={ok_count}, errors={err_count}"
                )
            else:
                print(
                    f"  Batch {batch_size}: ОШИБКА за {elapsed_s:.1f}с | "
                    f"{error[:200]}"
                )
                raw = result.get("raw", "") or result.get("data", "")
                if raw:
                    print(f"    Raw: {str(raw)[:500]}")

            await asyncio.sleep(1.0)

        # ── Тест C: Rate-limit ──
        _print_separator("ТЕСТ C: Rate-limit (серия быстрых запросов)")
        print(
            f"Отправляем {RATE_LIMIT_REQUESTS} запросов подряд с паузой "
            f"{RATE_LIMIT_PAUSE}с.\n"
            f"Каждый запрос: 10 ID x 2 ночи.\n"
        )

        rate_test_ids = collected_ids[:10]
        successes = 0
        failures = 0
        first_failure_at: int | None = None
        timings: list[float] = []

        for i in range(1, RATE_LIMIT_REQUESTS + 1):
            day_offset = i % DAYS_COUNT
            day = today + timedelta(days=day_offset)
            end_day = day + timedelta(days=2)

            body = _build_price_body(
                rate_test_ids,
                f"{day.isoformat()} 14:00:00",
                f"{end_day.isoformat()} 11:00:00",
            )

            t0 = time.monotonic()
            result = await _fetch_post(page, API_PRICES_URL, body, token)
            elapsed_ms = (time.monotonic() - t0) * 1000
            timings.append(elapsed_ms)

            success, objects, error = _parse_api_response(result)

            if success:
                successes += 1
            else:
                failures += 1
                if first_failure_at is None:
                    first_failure_at = i
                    print(f"  #{i}: ПЕРВАЯ ОШИБКА — {error[:200]}")
                    raw = result.get("raw", "")
                    if raw:
                        print(f"    Raw: {raw[:500]}")

            # Прогресс каждые 5 запросов
            if i % 5 == 0 or i == RATE_LIMIT_REQUESTS:
                recent = timings[-5:]
                avg_ms = sum(recent) / len(recent)
                print(
                    f"  #{i}/{RATE_LIMIT_REQUESTS}: "
                    f"успехов={successes}, ошибок={failures}, "
                    f"среднее={avg_ms:.0f}мс"
                )

            # Если пошли массовые ошибки — останавливаемся
            if failures >= 5:
                print(f"\n  Остановка: {failures} ошибок.")
                break

            await asyncio.sleep(RATE_LIMIT_PAUSE)

        # Статистика rate-limit
        print(f"\n  --- Статистика rate-limit ---")
        print(f"  Всего запросов: {successes + failures}")
        print(f"  Успешных: {successes}")
        print(f"  Ошибок: {failures}")
        if first_failure_at:
            print(f"  Первая ошибка на запросе: #{first_failure_at}")
        else:
            print(f"  Блокировки не обнаружено при паузе {RATE_LIMIT_PAUSE}с")
        if timings:
            print(f"  Среднее время: {sum(timings) / len(timings):.0f}мс")
            print(f"  Мин: {min(timings):.0f}мс, Макс: {max(timings):.0f}мс")

        # ── Тест D: Batch 60 ночей × 50 ID ──
        _print_separator("ТЕСТ D: Финальный тест — batch 60 ночей x 50 ID")

        if len(collected_ids) >= 50:
            final_ids = collected_ids[:50]
            date_begin_60 = f"{today.isoformat()} 14:00:00"
            date_end_60 = (
                f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"
            )

            body = _build_price_body(final_ids, date_begin_60, date_end_60)

            print("Запрос: 50 ID x 60 ночей...")
            t0 = time.monotonic()
            result = await _fetch_post(page, API_PRICES_URL, body, token)
            elapsed_s = time.monotonic() - t0

            success, objects, error = _parse_api_response(result)

            if success:
                busy_count = 0
                free_count = 0
                error_count = 0
                has_detail = 0

                for obj in objects:
                    if obj.get("success"):
                        data = obj.get("data", {})
                        busy = data.get("busy")
                        if busy == "busy":
                            busy_count += 1
                        elif busy == "unbusy":
                            free_count += 1
                        if data.get("detail"):
                            has_detail += 1
                    else:
                        error_count += 1

                print(f"  УСПЕХ за {elapsed_s:.1f}с")
                print(f"  Объектов в ответе: {len(objects)}")
                print(
                    f"  busy={busy_count}, free={free_count}, "
                    f"errors={error_count}"
                )
                print(f"  С ценами (detail[]): {has_detail}")
                print(f"  Отправлено: {len(final_ids)}, получено: {len(objects)}")
            else:
                print(f"  ОШИБКА за {elapsed_s:.1f}с: {error[:300]}")
        else:
            print(
                f"  ПРОПУЩЕН "
                f"(собрано только {len(collected_ids)} ID, нужно 50)"
            )

        # ── Итоги ──
        _print_separator("ИТОГИ И ВЫВОДЫ")

        print("Тест A (batch скользящее окно):")
        if total_checks > 0 and total_matches == total_checks:
            print("  Batch скользящее окно РАБОТАЕТ — данные совпадают.")
            print("    Можно определять занятость для пачки ID одним запросом.")
        elif total_checks > 0:
            pct = total_matches / total_checks * 100
            print(f"  Совпадение {pct:.0f}% — возможны расхождения.")
        else:
            print("  Не удалось проверить.")

        print(f"\nТест B (размер batch):")
        print(f"  Проверены размеры: {BATCH_LIMIT_SIZES}")

        print(f"\nТест C (rate-limit):")
        print(f"  {successes} успешных из {successes + failures} запросов")
        if first_failure_at:
            print(f"  Блокировка начинается с запроса #{first_failure_at}")
        else:
            print(f"  Блокировки не обнаружено при паузе {RATE_LIMIT_PAUSE}с")

        print(f"\nТест D (50 ID x 60 ночей):")
        print("  Финальная проверка — см. результаты выше.")

        print(
            "\n----------------------------------------------"
            "\n  ПЛАН ОПТИМИЗАЦИИ (если все работает):"
            "\n----------------------------------------------"
            "\n"
            "\n  Текущий этап 2: для каждой из N карточек"
            "\n    -> загрузка страницы (5-15с)"
            "\n    -> перехват токена"
            "\n    -> bulk 60 ночей (1-3с)"
            "\n    -> скользящее окно для busy (30-120с)"
            "\n    Итого: N x 30-120с"
            "\n"
            "\n  Новый подход: без загрузки карточек"
            "\n    -> 1 токен со страницы поиска"
            "\n    -> batch bulk 60 ночей: ceil(N/50) запросов -> цены + busy"
            "\n    -> batch скользящее окно для busy:"
            "\n      60 дней x ceil(N_busy/50) запросов"
            "\n    Итого: ~100-2000 запросов вместо N x 60"
            "\n"
            "\n  Пример: 1000 объявлений, 700 busy:"
            "\n    Сейчас:  1000 x ~60с = ~16 часов"
            "\n    Новый:   20 bulk + 60x14 sliding = 860 запросов"
            "\n             = 860 x 0.5с = ~7 минут"
        )

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
