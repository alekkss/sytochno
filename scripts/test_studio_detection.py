"""Тестовый скрипт: анализ данных API для определения студий.

Загружает страницу поиска sutochno.ru, перехватывает API-запрос,
собирает первые 50 объявлений и выводит ВСЕ поля каждого объекта
для анализа отличий студии от однокомнатной квартиры.

Запуск:
    cd /root/sutochno
    source venv/bin/activate
    python -m scripts.test_studio_detection
"""

import asyncio
import json
import sys
from pathlib import Path

from playwright.async_api import async_playwright

# ── Настройки ──────────────────────────────────────────────

# URL поиска — используем фильтр rooms=studio для получения студий
# и rooms=1 для однокомнатных
SEARCH_URL_STUDIOS = (
    "https://sutochno.ru/front/searchapp/search"
    "?type=city&id=397367&term=Санкт-Петербург"
    "&price_per=1&guests_adults=2&rooms=studio"
)

SEARCH_URL_ONE_ROOM = (
    "https://sutochno.ru/front/searchapp/search"
    "?type=city&id=397367&term=Санкт-Петербург"
    "&price_per=1&guests_adults=2&rooms=1"
)

# Таймаут ожидания перехвата API-запроса (секунды)
API_INTERCEPT_TIMEOUT: float = 25.0

# URL для получения полных данных
API_SEARCH_BY_LOCATION: str = (
    "https://sutochno.ru/api/json/search/searchObjectsByLocation"
)

# Заголовки, которые не передаём в fetch
SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}

# Аргументы запуска Chromium
BROWSER_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
]

# Stealth-скрипт
STEALTH_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
    window.chrome = {runtime: {}};
"""


def extract_ids_from_response(data: dict) -> list[int]:
    """Извлекает список ID из ответа searchObjectsOnMap.

    Пробует разные структуры данных, т.к. формат может отличаться.

    Args:
        data: Полный JSON-ответ от API.

    Returns:
        Список числовых ID объявлений.
    """
    ids: list[int] = []

    # Ищем массив объектов в разных местах структуры
    candidates = []

    # Прямой массив в data
    if isinstance(data, list):
        candidates.append(data)
    elif isinstance(data, dict):
        # data.data (наиболее вероятно для sutochno)
        inner = data.get("data")
        if isinstance(inner, list):
            candidates.append(inner)
        elif isinstance(inner, dict):
            for key in ("objects", "items", "results", "data", "list"):
                val = inner.get(key)
                if isinstance(val, list):
                    candidates.append(val)

        # Прямые ключи в корне
        for key in ("objects", "items", "results", "list"):
            val = data.get(key)
            if isinstance(val, list):
                candidates.append(val)

    # Пробуем извлечь ID из каждого кандидата
    for candidate in candidates:
        for item in candidate:
            if isinstance(item, dict):
                obj_id = item.get("id")
                if obj_id is not None and isinstance(obj_id, int):
                    ids.append(obj_id)
            elif isinstance(item, (int, float)):
                # Массив может содержать просто числовые ID
                ids.append(int(item))

    return ids


async def load_page_and_intercept(
    search_url: str,
    label: str,
) -> tuple[dict, dict[str, str]] | None:
    """Загружает страницу поиска и получает полные данные объявлений через API.

    Args:
        search_url: URL страницы поиска.
        label: Метка для логов (например, «студии» или «однокомнатные»).

    Returns:
        Кортеж (данные searchObjectsByLocation, заголовки API) или None при ошибке.
    """
    print(f"\n{'='*70}")
    print(f"  Загрузка: {label}")
    print(f"  URL: {search_url[:100]}...")
    print(f"{'='*70}")

    captured_url: str | None = None
    captured_headers: dict[str, str] | None = None

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=BROWSER_ARGS,
            ignore_default_args=["--enable-automation"],
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        await context.add_init_script(STEALTH_SCRIPT)
        page = await context.new_page()
        page.set_default_navigation_timeout(60000)

        # Перехватчик API-запроса searchObjectsOnMap
        async def intercept_route(route, request):
            nonlocal captured_url, captured_headers
            url = request.url
            if "searchObjectsOnMap" in url and captured_url is None:
                captured_url = url
                captured_headers = dict(request.headers)
                print(f"  ✓ Перехвачен API URL: {url[:120]}...")
            await route.continue_()

        await page.route("**/api/json/**", intercept_route)

        # Загрузка страницы
        try:
            await page.goto(search_url, wait_until="domcontentloaded")
        except Exception as e:
            print(f"  ✗ Ошибка загрузки страницы: {e}")
            await browser.close()
            return None

        # Ожидание перехвата
        elapsed = 0.0
        while elapsed < API_INTERCEPT_TIMEOUT:
            if captured_url is not None:
                break
            await asyncio.sleep(0.5)
            elapsed += 0.5

        await page.unroute("**/api/json/**")

        if captured_url is None:
            print(f"  ✗ API searchObjectsOnMap не перехвачен за {elapsed:.1f}с")
            await browser.close()
            return None

        # Фильтруем заголовки
        api_headers = {
            k: v for k, v in captured_headers.items()
            if k.lower() not in SKIP_HEADERS
        }

        # ── Получаем ID из searchObjectsOnMap ──
        print(f"  → Запрос searchObjectsOnMap (offset=0)...")

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
                        return {success: true, data: JSON.parse(text)};
                    } catch (e) {
                        return {success: false, error: 'JSON parse error', raw: text.substring(0, 1000)};
                    }
                } catch (e) {
                    return {success: false, error: e.message};
                }
            }
        """, {"url": captured_url, "headers": api_headers})

        if not result.get("success"):
            print(f"  ✗ Ошибка API: {result.get('error')}")
            print(f"  Raw: {result.get('raw', '')[:500]}")
            await browser.close()
            return None

        # Отладка: показываем структуру ответа
        api_data = result.get("data", {})
        print(f"\n  ── Отладка структуры searchObjectsOnMap ──")
        print(f"  Тип data: {type(api_data).__name__}")

        if isinstance(api_data, dict):
            print(f"  Ключи верхнего уровня: {list(api_data.keys())}")
            inner = api_data.get("data")
            if inner is not None:
                print(f"  Тип data.data: {type(inner).__name__}")
                if isinstance(inner, list) and inner:
                    print(f"  Длина data.data: {len(inner)}")
                    first_item = inner[0]
                    print(f"  Тип первого элемента: {type(first_item).__name__}")
                    if isinstance(first_item, dict):
                        print(f"  Ключи первого элемента: {list(first_item.keys())}")
                        print(f"  Первый элемент: {json.dumps(first_item, ensure_ascii=False)[:300]}")
                    else:
                        print(f"  Значение первого элемента: {str(first_item)[:200]}")
                elif isinstance(inner, dict):
                    print(f"  Ключи data.data: {list(inner.keys())}")

        # Извлекаем ID
        ids = extract_ids_from_response(api_data)

        if not ids:
            print(f"\n  ✗ Не удалось извлечь ID из ответа")
            print(f"  Полный ответ (первые 1000 символов):")
            print(f"  {json.dumps(api_data, ensure_ascii=False)[:1000]}")
            await browser.close()
            return None

        print(f"  ✓ Извлечено {len(ids)} ID")

        # ── Получаем полные данные через searchObjectsByLocation ──
        batch_ids = ids[:50]
        ids_params = "&".join(f"ids[]={oid}" for oid in batch_ids)
        full_url = (
            f"{API_SEARCH_BY_LOCATION}"
            f"?{ids_params}"
            f"&max_guests=2&relevance=pairs&currencyId=1"
        )

        print(f"  → Запрос searchObjectsByLocation ({len(batch_ids)} ID)...")

        full_result = await page.evaluate("""
            async ({url, headers}) => {
                try {
                    const resp = await fetch(url, {
                        method: 'GET',
                        headers: headers,
                        credentials: 'include'
                    });
                    const text = await resp.text();
                    try {
                        return {success: true, data: JSON.parse(text)};
                    } catch (e) {
                        return {success: false, error: 'JSON parse error', raw: text.substring(0, 1000)};
                    }
                } catch (e) {
                    return {success: false, error: e.message};
                }
            }
        """, {"url": full_url, "headers": api_headers})

        await browser.close()

        if not full_result.get("success"):
            print(f"  ✗ Ошибка searchObjectsByLocation: {full_result.get('error')}")
            print(f"  Raw: {full_result.get('raw', '')[:500]}")
            return None

        print(f"  ✓ Данные searchObjectsByLocation получены")
        return full_result.get("data"), api_headers


def extract_objects_from_response(data: dict) -> list[dict]:
    """Извлекает массив объектов из ответа searchObjectsByLocation.

    Args:
        data: JSON-ответ от API.

    Returns:
        Список словарей-объектов.
    """
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if not isinstance(data, dict):
        return []

    # Пробуем разные структуры
    for key in ("data", "objects", "items", "results"):
        val = data.get(key)
        if isinstance(val, list):
            result = [item for item in val if isinstance(item, dict)]
            if result:
                return result
        elif isinstance(val, dict):
            for subkey in ("data", "objects", "items", "results"):
                subval = val.get(subkey)
                if isinstance(subval, list):
                    result = [item for item in subval if isinstance(item, dict)]
                    if result:
                        return result

    return []


def analyze_objects(data: dict, label: str) -> None:
    """Анализирует и выводит поля объектов из ответа API.

    Args:
        data: JSON-ответ от searchObjectsByLocation.
        label: Метка категории (для заголовков).
    """
    objects = extract_objects_from_response(data)

    if not objects:
        print(f"\n  ✗ Не удалось извлечь объекты из ответа ({label})")
        print(f"  Тип data: {type(data).__name__}")
        if isinstance(data, dict):
            print(f"  Ключи: {list(data.keys())}")
        print(f"  Превью: {json.dumps(data, ensure_ascii=False)[:500]}")
        return

    print(f"\n{'='*70}")
    print(f"  АНАЛИЗ ОБЪЕКТОВ: {label} (всего: {len(objects)})")
    print(f"{'='*70}")

    # Собираем статистику по ключевым полям
    types_seen: dict[str, int] = {}
    rooms_seen: dict[str, int] = {}
    all_property_keys: set[str] = set()
    studio_markers: list[dict] = []
    all_top_keys: set[str] = set()

    for i, obj in enumerate(objects):
        obj_id = obj.get("id", "?")
        obj_type = obj.get("type", "НЕТ ПОЛЯ")
        title = obj.get("title", "без названия")
        props = obj.get("properties", {})
        rooms = props.get("rooms", "НЕТ ПОЛЯ") if isinstance(props, dict) else "НЕТ props"

        # Собираем все ключи
        all_top_keys.update(obj.keys())
        if isinstance(props, dict):
            all_property_keys.update(props.keys())

        # Статистика
        type_key = str(obj_type)
        types_seen[type_key] = types_seen.get(type_key, 0) + 1
        rooms_key = str(rooms)
        rooms_seen[rooms_key] = rooms_seen.get(rooms_key, 0) + 1

        # Ищем любые поля со словом "studio" / "студия" / "студ"
        studio_fields = {}

        # В верхнем уровне объекта
        for key, val in obj.items():
            key_lower = key.lower()
            if "studio" in key_lower or "студ" in key_lower:
                studio_fields[key] = val

        # В properties
        if isinstance(props, dict):
            for key, val in props.items():
                key_lower = key.lower()
                if "studio" in key_lower or "студ" in key_lower:
                    studio_fields[f"properties.{key}"] = val

        # Во всех вложенных dict первого уровня
        for key, val in obj.items():
            if isinstance(val, dict):
                for subkey, subval in val.items():
                    if "studio" in subkey.lower() or "студ" in subkey.lower():
                        studio_fields[f"{key}.{subkey}"] = subval

        if studio_fields:
            studio_markers.append({"id": obj_id, "title": title, "fields": studio_fields})

        # Детальный вывод первых 10 объектов
        if i < 10:
            print(f"\n  ── Объект #{i+1} (ID: {obj_id}) ──")
            print(f"  Название:     {title}")
            print(f"  type:         {obj_type}")
            print(f"  rooms:        {rooms}")

            if studio_fields:
                print(f"  🔍 STUDIO-ПОЛЯ: {json.dumps(studio_fields, ensure_ascii=False)}")

            # Все properties
            if isinstance(props, dict):
                props_str = json.dumps(props, ensure_ascii=False)
                print(f"  properties:   {props_str[:300]}")

    # ── Итоговая статистика ──
    print(f"\n{'─'*70}")
    print(f"  СТАТИСТИКА ({label}, всего {len(objects)} объектов):")
    print(f"{'─'*70}")

    print(f"\n  Все ключи верхнего уровня объекта:")
    print(f"    {sorted(all_top_keys)}")

    print(f"\n  Все ключи properties:")
    print(f"    {sorted(all_property_keys)}")

    print(f"\n  Распределение type:")
    for t, count in sorted(types_seen.items(), key=lambda x: -x[1]):
        print(f"    {t}: {count}")

    print(f"\n  Распределение rooms:")
    for r, count in sorted(rooms_seen.items(), key=lambda x: -x[1]):
        print(f"    {r}: {count}")

    if studio_markers:
        print(f"\n  🔍 НАЙДЕНЫ ПОЛЯ СО СЛОВОМ 'studio'/'студ' ({len(studio_markers)} объектов):")
        for marker in studio_markers[:10]:
            print(f"    ID={marker['id']}: {marker['fields']}")
            print(f"      Название: {marker['title']}")
    else:
        print(f"\n  ⚠ Поля со словом 'studio'/'студ' НЕ НАЙДЕНЫ ни в одном объекте")

    # Выводим один полный объект как JSON
    if objects:
        print(f"\n{'─'*70}")
        print(f"  ПОЛНЫЙ JSON ПЕРВОГО ОБЪЕКТА ({label}):")
        print(f"{'─'*70}")
        print(json.dumps(objects[0], ensure_ascii=False, indent=2))


async def main() -> None:
    """Основная функция скрипта."""
    print("\n" + "=" * 70)
    print("  ТЕСТ: Как API sutochno.ru различает студии и однокомнатные квартиры")
    print("=" * 70)
    print("\nСтратегия:")
    print("  1. Загружаем страницу с фильтром rooms=studio")
    print("  2. Загружаем страницу с фильтром rooms=1")
    print("  3. Сравниваем поля type, properties.rooms и ищем маркеры студий")

    # ── Запрос 1: Студии ──
    result_studios = await load_page_and_intercept(
        SEARCH_URL_STUDIOS, "СТУДИИ (rooms=studio)",
    )

    if result_studios is not None:
        data_studios, _ = result_studios
        analyze_objects(data_studios, "СТУДИИ")
    else:
        print("\n  ✗ Не удалось получить данные по студиям")

    # Пауза между запросами
    print("\n  ⏳ Пауза 5 секунд перед следующим запросом...")
    await asyncio.sleep(5)

    # ── Запрос 2: Однокомнатные ──
    result_one_room = await load_page_and_intercept(
        SEARCH_URL_ONE_ROOM, "ОДНОКОМНАТНЫЕ (rooms=1)",
    )

    if result_one_room is not None:
        data_one_room, _ = result_one_room
        analyze_objects(data_one_room, "ОДНОКОМНАТНЫЕ")
    else:
        print("\n  ✗ Не удалось получить данные по однокомнатным")

    # ── Итоговый вывод ──
    print("\n" + "=" * 70)
    print("  ВЫВОД")
    print("=" * 70)
    print("""
  Сравните:
  - Значение поля 'type' у студий vs однокомнатных
  - Значение поля 'properties.rooms' у студий vs однокомнатных
  - Наличие специальных полей (is_studio, isStudio, studio и т.п.)

  Это покажет, как API различает студии — по type, по rooms=0,
  по отдельному флагу, или никак.
    """)


if __name__ == "__main__":
    asyncio.run(main())
