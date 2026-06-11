"""Тестовый скрипт — прямой вызов searchObjectsOnMap через браузер.

Перехватывает РЕАЛЬНЫЙ запрос searchObjectsOnMap, который отправляет
фронтенд, и повторяет его с другим offset для пагинации.
Показывает полную структуру объектов.

Запуск:
    python -m scripts.test_catalog_api_v2
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

_PAGE_SIZE = 50
_TEST_PAGES = 3
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

    # Заменяем offset (parse_qs возвращает списки значений)
    params["offset"] = [str(new_offset)]

    # Собираем обратно — используем doseq=True для списков
    new_query = urlencode(params, doseq=True)
    new_parsed = parsed._replace(query=new_query)

    return urlunparse(new_parsed)


def _analyze_object(obj: dict, index: int) -> None:
    """Анализирует один объект и выводит все его поля.

    Args:
        obj: Словарь объекта из API.
        index: Порядковый номер объекта.
    """
    print(f"\n{'─' * 70}")  # noqa: T201
    print(f"  ОБЪЕКТ #{index}")  # noqa: T201
    print(f"{'─' * 70}")  # noqa: T201

    print(f"\n  Все ключи ({len(obj)}): {sorted(obj.keys())}")  # noqa: T201

    # Поля, которые нам нужны для RawListing
    needed_fields = {
        "ID": ["id", "object_id", "objectId"],
        "Название": ["title", "name", "object_name"],
        "Цена": ["price", "price_per_night", "cost", "price_default"],
        "Рейтинг": ["rating", "rate", "avg_rating"],
        "Отзывы": [
            "reviews_count", "review_count",
            "comments_count", "cnt_reviews",
        ],
        "Площадь": ["area", "square", "size", "area_m2"],
        "Гости": ["max_guests", "guests", "cnt_guests"],
        "Адрес": ["address", "addr", "location_address"],
        "Метро": ["metro", "metro_station", "nearest_metro"],
        "Быстрое бронирование": [
            "is_booking_now", "instant_booking",
            "booking_now", "fast_booking",
        ],
        "Координаты": ["lat", "lng", "longitude", "latitude"],
        "Тип объекта": ["type", "object_type", "housing_type"],
        "Фото": ["photos", "images", "photo", "image"],
        "URL": ["url", "link", "href", "slug"],
    }

    print("\n  МАППИНГ НА НАШИ ПОЛЯ:")  # noqa: T201

    for field_name, possible_keys in needed_fields.items():
        found = False
        for key in possible_keys:
            if key in obj:
                value = obj[key]
                value_str = str(value)
                if len(value_str) > 150:
                    value_str = value_str[:150] + "..."
                print(f"    + {field_name}: {key} = {value_str}")  # noqa: T201
                found = True
                break
        if not found:
            print(f"    - {field_name}: НЕ НАЙДЕНО")  # noqa: T201

    # Все поля с их значениями
    print("\n  ВСЕ ПОЛЯ:")  # noqa: T201
    for key in sorted(obj.keys()):
        value = obj[key]
        value_str = str(value)
        if len(value_str) > 200:
            value_str = value_str[:200] + "..."
        type_name = type(value).__name__
        print(f"    {key} ({type_name}): {value_str}")  # noqa: T201


def _print_summary(all_objects: list[dict]) -> None:
    """Выводит сводку по всем собранным объектам.

    Args:
        all_objects: Список всех объектов из API.
    """
    _print_separator(f"СВОДКА: {len(all_objects)} объектов")

    if not all_objects:
        print("  Объектов не найдено")  # noqa: T201
        return

    field_stats: dict[str, int] = {}
    for obj in all_objects:
        for key in obj.keys():
            field_stats[key] = field_stats.get(key, 0) + 1

    print("\n  Поля и их наличие:")  # noqa: T201
    for key, count in sorted(field_stats.items(), key=lambda x: -x[1]):
        pct = count / len(all_objects) * 100
        print(f"    {key}: {count}/{len(all_objects)} ({pct:.0f}%)")  # noqa: T201

    ids = set()
    for obj in all_objects:
        obj_id = obj.get("id") or obj.get("object_id")
        if obj_id:
            ids.add(obj_id)

    print(f"\n  Уникальных ID: {len(ids)}")  # noqa: T201
    print(f"  Дубликатов: {len(all_objects) - len(ids)}")  # noqa: T201


def _extract_objects(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа API.

    Args:
        data: Ответ API (parsed JSON).

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


async def _fetch_with_headers(page, url: str, token: str, headers: dict) -> dict:
    """Выполняет fetch() с произвольными заголовками.

    Args:
        page: Страница Playwright.
        url: URL для запроса.
        token: Сессионный токен.
        headers: Дополнительные заголовки (из перехвата).

    Returns:
        Словарь с success, status, data/error.
    """
    # Формируем итоговые заголовки: перехваченные + токен
    final_headers = {**headers, "token": token}

    result = await page.evaluate("""
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
                        raw: text.substring(0, 1000)
                    };
                }
            } catch (e) {
                return {success: false, error: e.message};
            }
        }
    """, {"url": url, "headers": final_headers})

    return result


async def run() -> None:
    """Основная логика.

    Стратегия: не формировать URL вручную, а перехватить РЕАЛЬНЫЙ
    запрос searchObjectsOnMap из фронтенда вместе с его заголовками,
    затем повторить его с другим offset.
    """
    _print_separator("ТЕСТ: Перехват и повтор searchObjectsOnMap")
    print(f"  Результаты: {_OUTPUT_DIR}/")  # noqa: T201

    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Хранилище для перехваченных данных
    captured = {
        "token": None,
        "search_url": None,
        "search_headers": None,
        "search_response": None,
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

        # ── Перехватчик: ловим токен и реальный URL searchObjectsOnMap ──
        async def _intercept(route, request):
            """Перехватывает токен и URL searchObjectsOnMap."""
            url = request.url
            headers = request.headers

            # Перехватываем токен из любого запроса к API
            token = headers.get("token")
            if token and not captured["token"]:
                captured["token"] = token
                print(f"\n  Токен: {token[:40]}...")  # noqa: T201

            # Перехватываем URL searchObjectsOnMap
            if "searchObjectsOnMap" in url and not captured["search_url"]:
                captured["search_url"] = url
                captured["search_headers"] = dict(headers)
                print(f"  searchObjectsOnMap URL перехвачен!")  # noqa: T201
                print(f"  URL: {url[:120]}...")  # noqa: T201
                print(f"  Заголовки: {list(headers.keys())}")  # noqa: T201

            await route.continue_()

        # Перехватываем ВСЕ запросы к sutochno.ru/api/
        await page.route("**/api/json/**", _intercept)
        await page.route("**/api/rest/**", _intercept)

        # ── Шаг 1: Загружаем страницу, ждём пока фронтенд сам вызовет API ──
        _print_separator("Шаг 1: Загрузка страницы + перехват")

        await page.goto(_INIT_URL, wait_until="networkidle")

        try:
            await page.wait_for_selector(
                ".card[data-observe-id]", timeout=30000
            )
            cards = await page.query_selector_all(".card[data-observe-id]")
            print(f"  Карточек на странице: {len(cards)}")  # noqa: T201
        except Exception:
            print("  Карточки не найдены")  # noqa: T201

        # Ждём — фронтенд может отправить searchObjectsOnMap с задержкой
        await asyncio.sleep(8)

        # Проверяем, что перехватили
        if not captured["token"]:
            print("\n  ОШИБКА: Токен не перехвачен!")  # noqa: T201
            await browser.close()
            return

        if not captured["search_url"]:
            print(  # noqa: T201
                "\n  ОШИБКА: URL searchObjectsOnMap не перехвачен!\n"
                "  Фронтенд не вызвал этот эндпоинт.\n"
                "  Возможно, данные рендерятся на сервере (SSR).\n"
            )
            await browser.close()
            return

        token = captured["token"]
        base_url = captured["search_url"]
        original_headers = captured["search_headers"]

        _print_separator("Перехваченные данные")
        print(f"  Токен: {token}")  # noqa: T201
        print(f"  URL: {base_url}")  # noqa: T201
        print(f"  Заголовки: {json.dumps(original_headers, indent=2)}")  # noqa: T201

        # Сохраняем перехваченные данные
        intercept_path = _OUTPUT_DIR / "intercepted_request.json"
        with open(intercept_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "token": token,
                    "url": base_url,
                    "headers": original_headers,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"  Сохранено в: {intercept_path}")  # noqa: T201

        # ── Шаг 2: Повторяем перехваченный запрос напрямую ──
        _print_separator("Шаг 2: Повтор перехваченного запроса (offset=0)")

        # Убираем из заголовков те, что браузер добавит сам
        api_headers = {}
        for key, value in original_headers.items():
            lower = key.lower()
            if lower not in (
                "host", "connection", "content-length",
                "accept-encoding", "sec-fetch-dest",
                "sec-fetch-mode", "sec-fetch-site",
                "sec-ch-ua", "sec-ch-ua-mobile", "sec-ch-ua-platform",
            ):
                api_headers[key] = value

        # Убеждаемся что offset=0
        url_offset0 = _modify_offset(base_url, 0)
        print(f"  URL: {url_offset0[:120]}...")  # noqa: T201

        result = await _fetch_with_headers(page, url_offset0, token, api_headers)

        if not result.get("success"):
            print(f"  Ошибка: {result.get('error')}")  # noqa: T201
            if "raw" in result:
                print(f"  Raw: {result['raw'][:500]}")  # noqa: T201
            await browser.close()
            return

        data = result["data"]
        print(f"  HTTP статус: {result.get('status')}")  # noqa: T201

        # Сохраняем полный ответ
        raw_path = _OUTPUT_DIR / "raw_response_page1.json"
        with open(raw_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  Полный ответ: {raw_path}")  # noqa: T201

        # Проверяем success
        if isinstance(data, dict):
            print(f"  success: {data.get('success')}")  # noqa: T201
            if data.get("errors"):
                print(f"  errors: {data['errors']}")  # noqa: T201
            print(f"  Ключи: {list(data.keys())}")  # noqa: T201

        objects = _extract_objects(data)

        if objects is None:
            print("  Объекты не найдены в ответе!")  # noqa: T201
            # Пробуем просто вывести структуру для анализа
            if isinstance(data, dict):
                for key, val in data.items():
                    val_str = str(val)[:200]
                    print(f"    {key}: {val_str}")  # noqa: T201
            await browser.close()
            return

        print(f"  Получено объектов: {len(objects)}")  # noqa: T201

        # ── Шаг 3: Анализ объектов ──
        _print_separator("Шаг 3: Анализ объектов")

        all_objects = list(objects)

        if objects:
            _analyze_object(objects[0], index=1)
            if len(objects) > 1:
                _analyze_object(objects[1], index=2)

        # ── Шаг 4: Пагинация — offset=50 и offset=100 ──
        for page_num in range(1, _TEST_PAGES):
            offset = page_num * _PAGE_SIZE
            _print_separator(f"Пагинация: offset={offset}")

            url_next = _modify_offset(base_url, offset)
            result = await _fetch_with_headers(
                page, url_next, token, api_headers
            )

            if not result.get("success"):
                print(f"  Ошибка: {result.get('error')}")  # noqa: T201
                break

            next_data = result["data"]
            if isinstance(next_data, dict):
                print(f"  success: {next_data.get('success')}")  # noqa: T201
                if next_data.get("errors"):
                    print(f"  errors: {next_data['errors']}")  # noqa: T201

            next_objects = _extract_objects(next_data)

            if next_objects is None:
                print("  Нет объектов — конец данных")  # noqa: T201
                break

            print(f"  Получено: {len(next_objects)}")  # noqa: T201
            all_objects.extend(next_objects)
            print(f"  Итого: {len(all_objects)}")  # noqa: T201

            await asyncio.sleep(1)

        # ── Шаг 5: Сводка ──
        _print_summary(all_objects)

        # ── Шаг 6: Сохранение ──
        _print_separator("Сохранение")

        all_path = _OUTPUT_DIR / "searchObjectsOnMap_all.json"
        with open(all_path, "w", encoding="utf-8") as f:
            json.dump(all_objects, f, ensure_ascii=False, indent=2)
        print(f"  Все объекты ({len(all_objects)}): {all_path}")  # noqa: T201

        if all_objects:
            first_path = _OUTPUT_DIR / "searchObjectsOnMap_first_object.json"
            with open(first_path, "w", encoding="utf-8") as f:
                json.dump(all_objects[0], f, ensure_ascii=False, indent=2)
            print(f"  Первый объект: {first_path}")  # noqa: T201

        await browser.close()

    _print_separator("ГОТОВО")
    print(  # noqa: T201
        f"\n  Собрано: {len(all_objects)} объектов\n"
        f"  Файлы: {_OUTPUT_DIR}/\n"
    )


if __name__ == "__main__":
    asyncio.run(run())
