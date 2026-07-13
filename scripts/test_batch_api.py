"""Тестовый скрипт: проверка batch-запросов getPricesAndAvailabilities.

Гипотеза: API принимает массив objects[] с несколькими ID и возвращает
данные по каждому за один запрос. Если да — можно получить занятость
и цены для 50 объявлений одним запросом со страницы поиска, без захода
в каждую карточку (ускорение в десятки раз).

Что проверяет:
1. Токен со страницы поиска работает для getPricesAndAvailabilities.
2. API принимает массив из нескольких ID (5, 10, 20).
3. Ответ содержит данные по каждому ID (busy, detail[], цены).
4. Данные batch-запроса совпадают с одиночными запросами.

Запуск:
    python -m scripts.test_batch_api
"""

import asyncio
import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Корень проекта в sys.path ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

# Загружаем .env для получения URL поиска
load_dotenv(PROJECT_ROOT / ".env")

# ── Константы ──

API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
API_PAGE_SIZE: int = 50
DAYS_COUNT: int = 60
DEFAULT_GUESTS: int = 2
API_INTERCEPT_TIMEOUT: float = 30.0
POLL_INTERVAL: float = 0.5

# Дополнительное ожидание после перехвата URL — даём фронтенду
# время получить ответ от API и отрендерить результаты.
POST_INTERCEPT_WAIT: float = 5.0

# Заголовки, которые не нужно передавать в fetch()
SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}

# Размеры пачек для тестирования
BATCH_SIZES: list[int] = [1, 5, 10, 20]


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


def _print_object_result(obj_data: dict, obj_id: int) -> None:
    """Печатает результат для одного объекта из ответа API."""
    if not obj_data.get("success"):
        errors = obj_data.get("errors", [])
        print(f"    ID {obj_id}: ОШИБКА — {errors}")
        return

    data = obj_data.get("data", {})
    busy = data.get("busy", "???")
    detail = data.get("detail", [])
    rooms = data.get("rooms_available")
    price_default = data.get("price_default")
    price = data.get("price")

    # Извлекаем цены из detail[]
    season_prices: list[int] = []
    base_price: int = 0
    for d in detail:
        d_type = d.get("type")
        cost = d.get("cost")
        if d_type == "season_price" and cost:
            season_prices.append(int(cost))
        elif d_type == 1 and cost:
            base_price = int(cost)

    print(f"    ID {obj_id}:")
    print(f"      busy          = {busy}")
    print(f"      rooms_avail   = {rooms}")
    print(f"      price         = {price}")
    print(f"      price_default = {price_default}")
    print(f"      detail[] записей = {len(detail)}")
    if season_prices:
        print(f"      season_prices = {season_prices[:5]}{'...' if len(season_prices) > 5 else ''}")
    if base_price:
        print(f"      base_price    = {base_price}")


async def _fetch_post(page, url: str, body: dict, token: str) -> dict:
    """Выполняет POST-запрос через fetch() в контексте браузера."""
    try:
        result = await page.evaluate(
            """
            async ({url, body, token}) => {
                try {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), 30000);

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


async def main() -> None:
    """Основная логика тестового скрипта."""
    search_url = _get_search_url()
    print(f"URL поиска: {search_url[:100]}...")

    today = date.today()
    date_begin = f"{today.isoformat()} 14:00:00"
    date_end = f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"

    # Короткий период для одиночных тестов (2 ночи)
    date_begin_short = f"{today.isoformat()} 14:00:00"
    date_end_short = f"{(today + timedelta(days=2)).isoformat()} 11:00:00"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="ru-RU",
        )
        page = await context.new_page()

        # ── Этап 1: Загрузка страницы поиска и перехват токена + URL ──
        _print_separator("ЭТАП 1: Загрузка страницы поиска и перехват токена")

        captured: dict = {"url": None, "headers": None, "token": None}

        async def _intercept(route, request):
            url = request.url
            if "searchObjectsOnMap" in url and captured["url"] is None:
                captured["url"] = url
                captured["headers"] = dict(request.headers)
            # Перехватываем токен из любого API-запроса
            if "sutochno.ru/api/json" in url and captured["token"] is None:
                token = request.headers.get("token") or request.headers.get("Token")
                if token:
                    captured["token"] = token
            await route.continue_()

        await page.route("**/api/json/**", _intercept)

        print("Загружаем страницу поиска...")
        await page.goto(search_url, wait_until="domcontentloaded")

        # Ждём перехвата URL и токена
        elapsed = 0.0
        while elapsed < API_INTERCEPT_TIMEOUT:
            if captured["url"] and captured["token"]:
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        # Даём фронтенду время завершить загрузку данных —
        # после перехвата URL ответ ещё не пришёл
        if captured["url"]:
            print(
                f"API URL перехвачен за {elapsed:.1f}с. "
                f"Ждём {POST_INTERCEPT_WAIT}с для завершения загрузки..."
            )
            await asyncio.sleep(POST_INTERCEPT_WAIT)

        await page.unroute("**/api/json/**")

        if not captured["token"]:
            print("ОШИБКА: Токен не перехвачен! Возможно, страница не загрузилась.")
            await browser.close()
            return

        token = captured["token"]
        map_url = captured["url"]
        api_headers = {
            k: v for k, v in (captured["headers"] or {}).items()
            if k.lower() not in SKIP_HEADERS
        } if captured["headers"] else {}

        print(f"Токен перехвачен: {token[:20]}...")
        print(f"API URL перехвачен: {'Да' if map_url else 'Нет'}")

        # ── Этап 2: Сбор ID через fetch (повторяем перехваченный запрос) ──
        _print_separator("ЭТАП 2: Сбор ID через searchObjectsOnMap")

        collected_ids: list[int] = []

        if map_url:
            # Первый запрос — offset=0
            paginated_url = _replace_offset(map_url, 0)
            print(f"Запрашиваем первую страницу: offset=0")

            result = await _fetch_get(page, paginated_url, api_headers)

            if not result.get("success"):
                print(f"ОШИБКА fetch: {result.get('error')}")
                print(f"Raw: {result.get('raw', '')[:500]}")
            else:
                data = result.get("data", {})

                # API может возвращать данные в разных структурах
                objects = None
                if isinstance(data, dict):
                    # Пробуем data.data (массив)
                    inner = data.get("data")
                    if isinstance(inner, list):
                        objects = inner
                    elif isinstance(inner, dict):
                        # Может быть data.data.objects
                        objects = inner.get("objects", inner.get("items"))
                    # Или data.objects напрямую
                    if not objects:
                        objects = data.get("objects")

                if objects and isinstance(objects, list):
                    for obj in objects:
                        obj_id = obj.get("id")
                        if obj_id:
                            collected_ids.append(obj_id)
                    print(f"Получено объектов из первой страницы: {len(objects)}")
                    print(f"Извлечено ID: {len(collected_ids)}")
                else:
                    print("Не удалось извлечь объекты из ответа.")
                    print(f"Структура data: {type(data).__name__}")
                    if isinstance(data, dict):
                        print(f"Ключи data: {list(data.keys())}")
                        inner = data.get("data")
                        if isinstance(inner, dict):
                            print(f"Ключи data.data: {list(inner.keys())}")
                        elif isinstance(inner, list):
                            print(f"data.data — массив из {len(inner)} элементов")
                            if inner:
                                print(f"Первый элемент: {json.dumps(inner[0], ensure_ascii=False)[:300]}")
                    # Показываем сырой ответ для отладки
                    print(f"Сырой ответ (первые 1000 символов):")
                    print(json.dumps(data, ensure_ascii=False, indent=2)[:1000])

            # Если первая страница дала результаты — пробуем ещё одну
            if len(collected_ids) >= API_PAGE_SIZE:
                paginated_url = _replace_offset(map_url, API_PAGE_SIZE)
                print(f"\nЗапрашиваем вторую страницу: offset={API_PAGE_SIZE}")
                await asyncio.sleep(0.5)

                result = await _fetch_get(page, paginated_url, api_headers)
                if result.get("success"):
                    data = result.get("data", {})
                    inner = data.get("data") if isinstance(data, dict) else None
                    objects = inner if isinstance(inner, list) else None
                    if objects:
                        for obj in objects:
                            obj_id = obj.get("id")
                            if obj_id and obj_id not in collected_ids:
                                collected_ids.append(obj_id)
                        print(f"Получено объектов из второй страницы: {len(objects)}")

        print(f"\nВсего собрано уникальных ID: {len(collected_ids)}")
        if collected_ids:
            print(f"Первые 10: {collected_ids[:10]}")

        if not collected_ids:
            print("ОШИБКА: Не удалось собрать ID.")
            print("Проверьте URL поиска и попробуйте увеличить POST_INTERCEPT_WAIT.")
            await browser.close()
            return

        # ── Этап 3: Тест — одиночный запрос (2 ночи) ──
        _print_separator("ЭТАП 3: Одиночный запрос (1 ID, 2 ночи)")

        test_id = collected_ids[0]
        body_single = {
            "objects": [test_id],
            "rooms_cnt": {},
            "guests": DEFAULT_GUESTS,
            "date_begin": date_begin_short,
            "date_end": date_end_short,
            "currency_id": 1,
            "is_pets": 0,
            "documents": 0,
            "target": 0,
            "ages": [],
            "no_time": 1,
        }

        print(f"Запрашиваем ID={test_id}, период: 2 ночи...")
        result = await _fetch_post(page, API_PRICES_URL, body_single, token)

        if not result.get("success"):
            print(f"ОШИБКА fetch: {result.get('error')}")
            print(f"Raw: {result.get('raw', '')[:500]}")
            print(
                "\nТокен со страницы поиска НЕ работает "
                "для getPricesAndAvailabilities."
            )
            await browser.close()
            return

        api_data = result.get("data", {})
        if not api_data.get("success"):
            print("ОШИБКА API: success=false")
            print(
                f"Ответ: "
                f"{json.dumps(api_data, ensure_ascii=False, indent=2)[:1000]}"
            )
            await browser.close()
            return

        objects_resp = api_data.get("data", {}).get("objects", [])
        if not objects_resp:
            print("ОШИБКА: API вернул пустой objects[]")
            await browser.close()
            return

        print("УСПЕХ! Токен со страницы поиска работает.")
        print(f"Объектов в ответе: {len(objects_resp)}")
        _print_object_result(objects_resp[0], test_id)

        # Сохраняем результат одиночного запроса для сравнения
        single_results: dict[int, dict] = {}
        single_results[test_id] = objects_resp[0]

        # ── Этап 4: Одиночные запросы для первых 5 ID (для сравнения) ──
        _print_separator("ЭТАП 4: Одиночные запросы для сравнения (первые 5 ID)")

        compare_ids = collected_ids[:5]
        print(f"Запрашиваем по одному: {compare_ids}")

        for cid in compare_ids:
            if cid in single_results:
                continue
            body = {
                "objects": [cid],
                "rooms_cnt": {},
                "guests": DEFAULT_GUESTS,
                "date_begin": date_begin_short,
                "date_end": date_end_short,
                "currency_id": 1,
                "is_pets": 0,
                "documents": 0,
                "target": 0,
                "ages": [],
                "no_time": 1,
            }
            result = await _fetch_post(page, API_PRICES_URL, body, token)
            if result.get("success"):
                api_data = result.get("data", {})
                objects_resp = api_data.get("data", {}).get("objects", [])
                if objects_resp:
                    single_results[cid] = objects_resp[0]
                    _print_object_result(objects_resp[0], cid)
                else:
                    print(f"    ID {cid}: пустой objects[] в ответе")
            else:
                print(f"    ID {cid}: ОШИБКА fetch — {result.get('error')}")
            await asyncio.sleep(0.5)

        print(f"\nСобрано одиночных результатов: {len(single_results)}")

        # ── Этап 5: Batch-запросы (несколько ID в массиве objects[]) ──
        _print_separator("ЭТАП 5: Batch-запросы (несколько ID в objects[])")

        for batch_size in BATCH_SIZES:
            if batch_size > len(collected_ids):
                print(f"\n--- Batch {batch_size}: ПРОПУЩЕН (недостаточно ID) ---")
                continue

            batch_ids = collected_ids[:batch_size]

            print(f"\n--- Batch {batch_size} ID ---")
            print(f"  ID: {batch_ids[:10]}{'...' if batch_size > 10 else ''}")

            body_batch = {
                "objects": batch_ids,
                "rooms_cnt": {},
                "guests": DEFAULT_GUESTS,
                "date_begin": date_begin_short,
                "date_end": date_end_short,
                "currency_id": 1,
                "is_pets": 0,
                "documents": 0,
                "target": 0,
                "ages": [],
                "no_time": 1,
            }

            result = await _fetch_post(page, API_PRICES_URL, body_batch, token)

            if not result.get("success"):
                print(f"  ОШИБКА fetch: {result.get('error')}")
                print(f"  Raw: {result.get('raw', '')[:500]}")
                continue

            api_data = result.get("data", {})

            if not api_data.get("success"):
                errors = api_data.get("errors", [])
                print(f"  ОШИБКА API: success=false, errors={errors}")
                print(
                    f"  Ответ: "
                    f"{json.dumps(api_data, ensure_ascii=False, indent=2)[:1000]}"
                )
                continue

            objects_resp = api_data.get("data", {}).get("objects", [])
            print(f"  Отправлено ID: {batch_size}")
            print(f"  Получено объектов: {len(objects_resp)}")

            # Показываем результаты
            returned_ids: list[int] = []
            for obj in objects_resp:
                obj_id = obj.get("id")
                returned_ids.append(obj_id)
                _print_object_result(obj, obj_id)

            # Проверяем: все ли запрошенные ID вернулись
            missing = set(batch_ids) - set(returned_ids)
            extra = set(returned_ids) - set(batch_ids)

            if missing:
                print(f"  ВНИМАНИЕ: Не вернулись ID: {missing}")
            if extra:
                print(f"  ВНИМАНИЕ: Лишние ID в ответе: {extra}")

            # Сравниваем с одиночными запросами
            if batch_size <= 5:
                print(f"\n  --- Сравнение с одиночными запросами ---")
                mismatches = 0
                for obj in objects_resp:
                    obj_id = obj.get("id")
                    if obj_id in single_results:
                        single = single_results[obj_id]
                        # Сравниваем busy статус
                        s_busy = single.get("data", {}).get("busy")
                        b_busy = obj.get("data", {}).get("busy")
                        # Сравниваем цену
                        s_price = single.get("data", {}).get("price")
                        b_price = obj.get("data", {}).get("price")

                        match = s_busy == b_busy and s_price == b_price
                        status = "СОВПАДАЕТ" if match else "РАЗЛИЧАЕТСЯ"
                        if not match:
                            mismatches += 1
                        print(
                            f"    ID {obj_id}: {status}"
                            f" | busy: {s_busy} vs {b_busy}"
                            f" | price: {s_price} vs {b_price}"
                        )
                    else:
                        print(f"    ID {obj_id}: нет одиночного результата для сравнения")

                if mismatches == 0:
                    print("  Все данные совпадают с одиночными запросами!")
                else:
                    print(f"  ВНИМАНИЕ: {mismatches} различий!")

            await asyncio.sleep(1.0)

        # ── Этап 6: Bulk-запрос на 60 ночей с несколькими ID ──
        _print_separator("ЭТАП 6: Bulk 60 ночей с несколькими ID")

        for batch_size in [1, 5, 10]:
            if batch_size > len(collected_ids):
                continue

            batch_ids = collected_ids[:batch_size]
            print(f"\n--- Bulk 60 ночей × {batch_size} ID ---")

            body_bulk = {
                "objects": batch_ids,
                "rooms_cnt": {},
                "guests": DEFAULT_GUESTS,
                "date_begin": date_begin,
                "date_end": date_end,
                "currency_id": 1,
                "is_pets": 0,
                "documents": 0,
                "target": 0,
                "ages": [],
                "no_time": 1,
            }

            result = await _fetch_post(page, API_PRICES_URL, body_bulk, token)

            if not result.get("success"):
                print(f"  ОШИБКА fetch: {result.get('error')}")
                continue

            api_data = result.get("data", {})

            if not api_data.get("success"):
                errors = api_data.get("errors", [])
                print(f"  ОШИБКА API: errors={errors}")
                print(
                    f"  Ответ: "
                    f"{json.dumps(api_data, ensure_ascii=False, indent=2)[:1000]}"
                )
                continue

            objects_resp = api_data.get("data", {}).get("objects", [])
            print(f"  Получено объектов: {len(objects_resp)}")

            for obj in objects_resp:
                obj_id = obj.get("id")
                data = obj.get("data", {})
                busy = data.get("busy", "???")
                detail = data.get("detail", [])
                price = data.get("price")

                season_count = sum(
                    1 for d in detail if d.get("type") == "season_price"
                )
                base_count = sum(1 for d in detail if d.get("type") == 1)

                print(
                    f"    ID {obj_id}: busy={busy}, "
                    f"price={price}, "
                    f"detail[]={len(detail)} "
                    f"(season={season_count}, base={base_count})"
                )

            await asyncio.sleep(1.0)

        # ── Итоги ──
        _print_separator("ИТОГИ")
        print(
            "Если batch-запросы вернули данные по всем ID — можно отказаться\n"
            "от захода в каждую карточку. Вместо этого:\n"
            "  1. Перехватываем токен со страницы поиска (один раз).\n"
            "  2. Вызываем getPricesAndAvailabilities с пачкой ID (50 штук).\n"
            "  3. Получаем busy + detail[] для всех объявлений за один запрос.\n"
            "  4. Для busy='busy' — отдельный запрос скользящим окном\n"
            "     (или тоже batch, если API это позволяет).\n"
            "\n"
            "Потенциальное ускорение: вместо 60 сек × 1000 карточек = 16 часов\n"
            "→ 1 сек × 20 batch-запросов = 20 секунд (только для цен).\n"
            "Скользящее окно для busy-карточек потребует отдельных запросов."
        )

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
