"""Тестовый скрипт — проверка searchObjectsByLocation.

Стратегия:
1. Загружает страницу, перехватывает токен и заголовки.
2. Через searchObjectsOnMap собирает ID первых 50 объявлений.
3. Вызывает searchObjectsByLocation с этими ID.
4. Показывает полную структуру объекта — все поля.

Цель: понять, отдаёт ли searchObjectsByLocation полные данные
(название, рейтинг, отзывы, площадь, гости, адрес, метро,
быстрое бронирование) — всё, что сейчас парсится из DOM.

Запуск:
    python -m scripts.test_catalog_api_v3
"""

import asyncio
import json
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from playwright.async_api import async_playwright


# ── Настройки ──────────────────────────────────────────────

_OUTPUT_DIR = Path("data/api_debug")

_INIT_URL = (
    "https://sutochno.ru/front/searchapp/search"
    "?type=city&id=397367&term=Санкт-Петербург"
    "&price_per=1&guests_adults=2&price_min=0&price_max=2000"
)

_NAV_TIMEOUT_MS = 60000

_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
    "--blink-settings=imagesEnabled=false",
]

_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {
        get: () => ['ru-RU', 'ru', 'en-US', 'en']
    });
    window.chrome = {runtime: {}};
"""

_CONTEXT_OPTIONS = {
    "viewport": {"width": 1920, "height": 1080},
    "locale": "ru-RU",
    "timezone_id": "Europe/Moscow",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def _print_separator(title: str) -> None:
    """Печатает разделитель с заголовком."""
    print(f"\n{'═' * 70}")  # noqa: T201
    print(f"  {title}")  # noqa: T201
    print(f"{'═' * 70}")  # noqa: T201


def _modify_offset(url: str, new_offset: int) -> str:
    """Заменяет параметр offset в URL.

    Args:
        url: Исходный URL с параметрами.
        new_offset: Новое значение offset.

    Returns:
        URL с изменённым offset.
    """
    parsed = urlparse(url)
    params = parse_qs(parsed.query, keep_blank_values=True)
    params["offset"] = [str(new_offset)]
    new_query = urlencode(params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def _analyze_object_full(obj: dict, index: int) -> None:
    """Детальный анализ одного объекта со всеми вложенными полями.

    Args:
        obj: Словарь объекта из API.
        index: Порядковый номер.
    """
    print(f"\n{'─' * 70}")  # noqa: T201
    print(f"  ОБЪЕКТ #{index}")  # noqa: T201
    print(f"{'─' * 70}")  # noqa: T201

    print(f"\n  Ключи верхнего уровня ({len(obj)}): {sorted(obj.keys())}")  # noqa: T201

    # Поля, которые нам нужны для RawListing — расширенный поиск
    needed = {
        "ID": [
            "id", "object_id", "objectId", "external_id",
        ],
        "Название": [
            "title", "name", "object_name", "heading",
            "description", "short_title",
        ],
        "Цена": [
            "price", "price_per_night", "cost", "price_default",
            "min_price", "base_price", "salePrice",
        ],
        "Рейтинг": [
            "rating", "rate", "avg_rating", "review_rating",
            "score", "mark",
        ],
        "Отзывы": [
            "reviews_count", "review_count", "comments_count",
            "cnt_reviews", "cnt_comments", "reviews",
        ],
        "Площадь": [
            "area", "square", "size", "area_m2", "total_area",
        ],
        "Гости": [
            "max_guests", "guests", "cnt_guests", "capacity",
        ],
        "Адрес": [
            "address", "addr", "location_address", "full_address",
            "street", "location",
        ],
        "Метро": [
            "metro", "metro_station", "nearest_metro",
            "metro_stations", "subway",
        ],
        "Быстрое бронирование": [
            "is_booking_now", "instant_booking", "booking_now",
            "fast_booking", "instant",
        ],
        "Координаты": [
            "lat", "lng", "longitude", "latitude",
            "coordinates", "geo",
        ],
        "Тип объекта": [
            "type", "object_type", "housing_type", "property_type",
            "category",
        ],
        "Фото": [
            "photos", "images", "photo", "image", "main_photo",
            "thumbnail",
        ],
        "URL / Slug": [
            "url", "link", "href", "slug", "uri", "path",
        ],
        "Комнаты": [
            "rooms", "rooms_count", "cnt_rooms", "bedrooms",
        ],
        "Этаж": [
            "floor", "floors", "storey",
        ],
        "Удобства": [
            "amenities", "facilities", "features", "options",
            "comforts",
        ],
        "Хозяин": [
            "owner", "host", "user", "landlord", "owner_id",
            "user_id",
        ],
    }

    print("\n  МАППИНГ НА НАШИ ПОЛЯ:")  # noqa: T201

    for field_name, possible_keys in needed.items():
        found = False
        for key in possible_keys:
            if key in obj:
                value = obj[key]
                value_str = str(value)
                if len(value_str) > 200:
                    value_str = value_str[:200] + "..."
                vtype = type(value).__name__
                print(  # noqa: T201
                    f"    + {field_name}: "
                    f"{key} ({vtype}) = {value_str}"
                )
                found = True
                break
        if not found:
            # Ищем в подобъектах
            found_nested = False
            for obj_key, obj_val in obj.items():
                if isinstance(obj_val, dict):
                    for key in possible_keys:
                        if key in obj_val:
                            value = obj_val[key]
                            value_str = str(value)
                            if len(value_str) > 200:
                                value_str = value_str[:200] + "..."
                            vtype = type(value).__name__
                            print(  # noqa: T201
                                f"    + {field_name}: "
                                f"{obj_key}.{key} ({vtype}) = {value_str}"
                            )
                            found_nested = True
                            break
                if found_nested:
                    break
            if not found_nested:
                print(f"    - {field_name}: НЕ НАЙДЕНО")  # noqa: T201

    # Все поля рекурсивно (до 2 уровней вложенности)
    print("\n  ВСЕ ПОЛЯ (до 2 уровней):")  # noqa: T201
    for key in sorted(obj.keys()):
        value = obj[key]
        vtype = type(value).__name__

        if isinstance(value, dict):
            print(f"    {key} (dict, {len(value)} ключей):")  # noqa: T201
            for subkey in sorted(value.keys()):
                subval = value[subkey]
                subval_str = str(subval)
                if len(subval_str) > 150:
                    subval_str = subval_str[:150] + "..."
                subtype = type(subval).__name__
                print(  # noqa: T201
                    f"      .{subkey} ({subtype}): {subval_str}"
                )
        elif isinstance(value, list):
            print(f"    {key} (list, {len(value)} элементов):")  # noqa: T201
            # Показываем первые 3 элемента
            for i, item in enumerate(value[:3]):
                item_str = str(item)
                if len(item_str) > 150:
                    item_str = item_str[:150] + "..."
                print(f"      [{i}]: {item_str}")  # noqa: T201
            if len(value) > 3:
                print(f"      ... ещё {len(value) - 3}")  # noqa: T201
        else:
            value_str = str(value)
            if len(value_str) > 200:
                value_str = value_str[:200] + "..."
            print(f"    {key} ({vtype}): {value_str}")  # noqa: T201


def _print_field_summary(all_objects: list[dict]) -> None:
    """Сводка по полям всех объектов.

    Args:
        all_objects: Список объектов.
    """
    if not all_objects:
        return

    # Собираем все ключи (включая вложенные)
    flat_stats: dict[str, int] = {}
    for obj in all_objects:
        for key, val in obj.items():
            flat_stats[key] = flat_stats.get(key, 0) + 1
            if isinstance(val, dict):
                for subkey in val:
                    compound = f"{key}.{subkey}"
                    flat_stats[compound] = flat_stats.get(compound, 0) + 1

    print("\n  Все поля (включая вложенные) и их наличие:")  # noqa: T201
    for key, count in sorted(flat_stats.items(), key=lambda x: -x[1]):
        pct = count / len(all_objects) * 100
        marker = "  " if "." not in key else "    "
        print(  # noqa: T201
            f"  {marker}{key}: {count}/{len(all_objects)} ({pct:.0f}%)"
        )


async def _fetch_get(page, url: str, headers: dict) -> dict:
    """GET-запрос через fetch() в контексте браузера.

    Args:
        page: Страница Playwright.
        url: URL запроса.
        headers: Заголовки.

    Returns:
        Словарь с success, status, data/error.
    """
    return await page.evaluate("""
        async ({url, headers}) => {
            try {
                const resp = await fetch(url, {
                    method: 'GET',
                    headers: headers,
                    credentials: 'include'
                });
                const text = await resp.text();
                try {
                    return {success: true, status: resp.status, data: JSON.parse(text)};
                } catch (e) {
                    return {success: false, error: 'JSON parse', raw: text.substring(0, 1000)};
                }
            } catch (e) {
                return {success: false, error: e.message};
            }
        }
    """, {"url": url, "headers": headers})


def _extract_objects(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа API.

    Args:
        data: Parsed JSON.

    Returns:
        Список объектов или None.
    """
    if isinstance(data, list) and data:
        return data
    if not isinstance(data, dict):
        return None

    for key in ["objects", "items", "results", "list", "data"]:
        val = data.get(key)
        if isinstance(val, list) and val:
            return val
        if isinstance(val, dict):
            for subkey in ["objects", "items", "results", "list"]:
                subval = val.get(subkey)
                if isinstance(subval, list) and subval:
                    return subval
    return None


async def run() -> None:
    """Основная логика.

    1. Загружает страницу, перехватывает токен + URL searchObjectsOnMap
       + URL searchObjectsByLocation (вместе с заголовками).
    2. Через searchObjectsOnMap получает ID первых 50 объектов.
    3. Формирует запрос searchObjectsByLocation с этими ID.
    4. Показывает полную структуру объектов.
    """
    _print_separator("ТЕСТ: searchObjectsByLocation — полные данные?")
    print(f"  Результаты: {_OUTPUT_DIR}/")  # noqa: T201

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    captured = {
        "token": None,
        "map_url": None,
        "map_headers": None,
        "location_url": None,
        "location_headers": None,
    }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=_BROWSER_ARGS,
            ignore_default_args=["--enable-automation"],
        )

        context = await browser.new_context(**_CONTEXT_OPTIONS)
        await context.add_init_script(_STEALTH_SCRIPT)

        page = await context.new_page()
        page.set_default_navigation_timeout(_NAV_TIMEOUT_MS)

        # ── Перехватчик ──
        async def _intercept(route, request):
            """Перехватывает токен, searchObjectsOnMap и searchObjectsByLocation."""
            url = request.url
            headers = request.headers

            token = headers.get("token")
            if token and not captured["token"]:
                captured["token"] = token
                print(f"\n  Токен: {token[:40]}...")  # noqa: T201

            if "searchObjectsOnMap" in url and not captured["map_url"]:
                captured["map_url"] = url
                captured["map_headers"] = dict(headers)
                print(f"  searchObjectsOnMap перехвачен")  # noqa: T201

            if "searchObjectsByLocation" in url and not captured["location_url"]:
                captured["location_url"] = url
                captured["location_headers"] = dict(headers)
                print(f"  searchObjectsByLocation перехвачен")  # noqa: T201
                print(f"    URL: {url[:150]}...")  # noqa: T201

            await route.continue_()

        await page.route("**/api/**", _intercept)

        # ── Шаг 1: Загрузка страницы ──
        _print_separator("Шаг 1: Загрузка страницы + перехват")

        await page.goto(_INIT_URL, wait_until="networkidle")

        try:
            await page.wait_for_selector(
                ".card[data-observe-id]", timeout=30000
            )
            print("  Карточки загружены")  # noqa: T201
        except Exception:
            print("  Карточки не найдены")  # noqa: T201

        await asyncio.sleep(10)

        # Проверяем перехват
        if not captured["token"]:
            print("\n  ОШИБКА: Токен не перехвачен!")  # noqa: T201
            await browser.close()
            return

        if not captured["map_url"]:
            print("\n  ОШИБКА: searchObjectsOnMap не перехвачен!")  # noqa: T201
            await browser.close()
            return

        token = captured["token"]

        # Подготовка заголовков
        skip_headers = {
            "host", "connection", "content-length",
            "accept-encoding", "sec-fetch-dest",
            "sec-fetch-mode", "sec-fetch-site",
            "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
        }

        # Заголовки для searchObjectsOnMap
        map_headers = {
            k: v for k, v in captured["map_headers"].items()
            if k.lower() not in skip_headers
        }

        # ── Шаг 2: Собираем ID через searchObjectsOnMap ──
        _print_separator("Шаг 2: Сбор ID через searchObjectsOnMap")

        map_url_0 = _modify_offset(captured["map_url"], 0)
        result = await _fetch_get(page, map_url_0, map_headers)

        if not result.get("success"):
            print(f"  Ошибка: {result.get('error')}")  # noqa: T201
            await browser.close()
            return

        map_data = result["data"]
        map_objects = _extract_objects(map_data)

        if not map_objects:
            print("  Объекты не найдены")  # noqa: T201
            await browser.close()
            return

        # Извлекаем ID
        object_ids = [obj["id"] for obj in map_objects if "id" in obj]
        print(f"  Получено ID: {len(object_ids)}")  # noqa: T201
        print(f"  Первые 10: {object_ids[:10]}")  # noqa: T201

        # ── Шаг 3: searchObjectsByLocation ──
        _print_separator("Шаг 3: searchObjectsByLocation")

        # Если перехвачен реальный URL — берём его как базу для заголовков
        if captured["location_url"]:
            location_headers = {
                k: v for k, v in captured["location_headers"].items()
                if k.lower() not in skip_headers
            }
            print("  Используем перехваченные заголовки")  # noqa: T201
        else:
            location_headers = dict(map_headers)
            print("  searchObjectsByLocation не был перехвачен — используем заголовки от map")  # noqa: T201

        # Формируем URL с ID — как это делает фронтенд
        # Из перехвата: /api/json/search/searchObjectsByLocation?ids[]=1010733&ids[]=1075861&...
        base_location_url = "https://sutochno.ru/api/json/search/searchObjectsByLocation"

        # Тест 1: Первые 4 ID (как делает фронтенд)
        test_ids_small = object_ids[:4]
        ids_params = "&".join(f"ids[]={oid}" for oid in test_ids_small)
        extra_params = "max_guests=2&relevance=pairs&currencyId=1"
        location_url_small = f"{base_location_url}?{ids_params}&{extra_params}"

        print(f"\n  Тест A: 4 ID")  # noqa: T201
        print(f"  URL: {location_url_small[:150]}...")  # noqa: T201

        result_small = await _fetch_get(page, location_url_small, location_headers)

        if not result_small.get("success"):
            print(f"  Ошибка: {result_small.get('error')}")  # noqa: T201
            if "raw" in result_small:
                print(f"  Raw: {result_small['raw'][:500]}")  # noqa: T201
        else:
            data_small = result_small["data"]
            print(f"  HTTP: {result_small.get('status')}")  # noqa: T201

            if isinstance(data_small, dict):
                print(f"  success: {data_small.get('success')}")  # noqa: T201
                if data_small.get("errors"):
                    print(f"  errors: {data_small['errors']}")  # noqa: T201

            objects_small = _extract_objects(data_small)

            if objects_small:
                print(f"  Объектов: {len(objects_small)}")  # noqa: T201

                # Детальный анализ первого объекта
                _analyze_object_full(objects_small[0], index=1)

                if len(objects_small) > 1:
                    _analyze_object_full(objects_small[1], index=2)

                # Сохраняем
                path_small = _OUTPUT_DIR / "searchObjectsByLocation_4ids.json"
                with open(path_small, "w", encoding="utf-8") as f:
                    json.dump(objects_small, f, ensure_ascii=False, indent=2)
                print(f"\n  Сохранено: {path_small}")  # noqa: T201
            else:
                print("  Объекты не найдены в ответе")  # noqa: T201
                if isinstance(data_small, dict):
                    print(f"  Ключи: {list(data_small.keys())}")  # noqa: T201
                # Сохраняем raw для анализа
                raw_loc = _OUTPUT_DIR / "searchObjectsByLocation_raw.json"
                with open(raw_loc, "w", encoding="utf-8") as f:
                    json.dump(data_small, f, ensure_ascii=False, indent=2)
                print(f"  Raw сохранён: {raw_loc}")  # noqa: T201

        await asyncio.sleep(2)

        # Тест 2: Все 50 ID (проверяем лимит)
        _print_separator("Тест B: Все 50 ID за один запрос")

        ids_params_all = "&".join(f"ids[]={oid}" for oid in object_ids)
        location_url_all = f"{base_location_url}?{ids_params_all}&{extra_params}"

        print(f"  ID: {len(object_ids)}")  # noqa: T201

        result_all = await _fetch_get(page, location_url_all, location_headers)

        if not result_all.get("success"):
            print(f"  Ошибка: {result_all.get('error')}")  # noqa: T201
        else:
            data_all = result_all["data"]
            print(f"  HTTP: {result_all.get('status')}")  # noqa: T201

            if isinstance(data_all, dict):
                print(f"  success: {data_all.get('success')}")  # noqa: T201
                if data_all.get("errors"):
                    print(f"  errors: {data_all['errors']}")  # noqa: T201

            objects_all = _extract_objects(data_all)

            if objects_all:
                print(f"  Объектов получено: {len(objects_all)}")  # noqa: T201

                # Сводка по полям
                _print_field_summary(objects_all)

                # Сохраняем
                path_all = _OUTPUT_DIR / "searchObjectsByLocation_50ids.json"
                with open(path_all, "w", encoding="utf-8") as f:
                    json.dump(objects_all, f, ensure_ascii=False, indent=2)
                print(f"\n  Сохранено: {path_all}")  # noqa: T201

                # Первый объект отдельно
                first_path = _OUTPUT_DIR / "searchObjectsByLocation_first.json"
                with open(first_path, "w", encoding="utf-8") as f:
                    json.dump(objects_all[0], f, ensure_ascii=False, indent=2)
                print(f"  Первый объект: {first_path}")  # noqa: T201
            else:
                print("  Объекты не найдены")  # noqa: T201

        await browser.close()

    # ── Итог ──
    _print_separator("ГОТОВО")
    print(  # noqa: T201
        f"\n  Файлы в: {_OUTPUT_DIR}/\n"
        f"\n"
        f"  Если searchObjectsByLocation отдаёт полные данные:\n"
        f"    1. searchObjectsOnMap (offset 0,50,100...) → собрать все ID\n"
        f"    2. searchObjectsByLocation (пачками по 50 ID) → полные данные\n"
        f"    3. Весь первый этап = цикл API-запросов без браузера/DOM\n"
        f"\n"
        f"  Если данных мало — DOM-парсинг останется, но ID можно\n"
        f"  собирать через API (быстрее чем кликать по страницам).\n"
    )


if __name__ == "__main__":
    asyncio.run(run())
