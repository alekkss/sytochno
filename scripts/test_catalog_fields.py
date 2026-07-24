"""Диагностический скрипт: все данные доступные на этапе 1 (каталог).

Показывает полную структуру ответов двух API-эндпоинтов каталога:
1. searchObjectsOnMap — минимальные данные (маркеры карты).
2. searchObjectsByLocation — полные данные объявлений по ID.

Цель: увидеть ВСЕ поля, которые можно получить без входа в карточку
(без этапа обогащения), чтобы оценить полноту данных каталога.

Запуск:
    python -m scripts.test_catalog_fields

Результат выводится в консоль + сохраняется в data/catalog_fields_dump.json.
"""

import asyncio
import json
import os
import re
import sys
from pathlib import Path

# ── Корень проекта в sys.path ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv
from playwright.async_api import async_playwright

load_dotenv(PROJECT_ROOT / ".env")

# ── Константы ──────────────────────────────────────────────

API_SEARCH_BY_LOCATION: str = (
    "https://sutochno.ru/api/json/search/searchObjectsByLocation"
)

# Таймаут ожидания перехвата API-запроса (секунды)
API_INTERCEPT_TIMEOUT: float = 30.0

# Интервал поллинга (секунды)
POLL_INTERVAL: float = 0.5

# Ожидание после перехвата URL перед повторным fetch (секунды)
POST_INTERCEPT_WAIT: float = 5.0

# Максимальное количество объектов для детального вывода
MAX_DETAIL_OBJECTS: int = 3

# Максимальное количество ID для запроса searchObjectsByLocation
MAX_IDS_FOR_LOCATION: int = 50

# Путь к файлу с полным дампом
DUMP_FILE_PATH: str = "data/catalog_fields_dump.json"

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

# Stealth-скрипт для обхода детекции автоматизации
STEALTH_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
    window.chrome = {runtime: {}};
"""


def _get_search_url() -> str:
    """Получает первый URL поиска из .env.

    Returns:
        URL страницы поиска.

    Raises:
        RuntimeError: Если ни один URL не задан.
    """
    for i in range(1, 9):
        url = os.getenv(f"SUTOCHNO_SEARCH_URL_{i}", "").strip()
        if url:
            return url
    raise RuntimeError(
        "Не найден ни один SUTOCHNO_SEARCH_URL_* в .env. "
        "Заполните хотя бы SUTOCHNO_SEARCH_URL_1."
    )


def _replace_offset(url: str, new_offset: int) -> str:
    """Заменяет параметр offset в URL.

    Args:
        url: Исходный URL.
        new_offset: Новое значение offset.

    Returns:
        URL с обновлённым offset.
    """
    result = re.sub(r"offset=\d+", f"offset={new_offset}", url)
    if "offset=" not in result:
        separator = "&" if "?" in result else "?"
        result = f"{result}{separator}offset={new_offset}"
    return result


def _print_separator(title: str) -> None:
    """Печатает разделитель с заголовком.

    Args:
        title: Текст заголовка.
    """
    print(f"\n{'═' * 70}")  # noqa: T201
    print(f"  {title}")  # noqa: T201
    print(f"{'═' * 70}")  # noqa: T201


def _print_subseparator(title: str) -> None:
    """Печатает подразделитель.

    Args:
        title: Текст подзаголовка.
    """
    print(f"\n{'─' * 70}")  # noqa: T201
    print(f"  {title}")  # noqa: T201
    print(f"{'─' * 70}")  # noqa: T201


def _analyze_field_types(objects: list[dict], label: str) -> dict[str, dict]:
    """Анализирует типы и значения всех полей в списке объектов.

    Для каждого поля определяет: тип данных, примеры значений,
    процент заполненности, вложенные ключи (для dict/list).

    Args:
        objects: Список словарей-объектов из API.
        label: Метка для идентификации (для дампа).

    Returns:
        Словарь анализа полей.
    """
    if not objects:
        return {}

    field_analysis: dict[str, dict] = {}
    total = len(objects)

    # Собираем все ключи из всех объектов
    all_keys: set[str] = set()
    for obj in objects:
        if isinstance(obj, dict):
            all_keys.update(obj.keys())

    for key in sorted(all_keys):
        values = [obj.get(key) for obj in objects if isinstance(obj, dict)]
        non_none = [v for v in values if v is not None]

        # Определяем типы
        types_seen: set[str] = set()
        for v in non_none:
            types_seen.add(type(v).__name__)

        # Примеры значений (первые 3 уникальных)
        examples: list = []
        seen_examples: set[str] = set()
        for v in non_none:
            v_str = json.dumps(v, ensure_ascii=False)[:200]
            if v_str not in seen_examples and len(examples) < 3:
                seen_examples.add(v_str)
                examples.append(v)

        # Вложенные ключи для словарей
        nested_keys: set[str] = set()
        if "dict" in types_seen:
            for v in non_none:
                if isinstance(v, dict):
                    nested_keys.update(v.keys())

        # Для списков — анализируем элементы
        list_element_keys: set[str] = set()
        if "list" in types_seen:
            for v in non_none:
                if isinstance(v, list):
                    for item in v[:5]:
                        if isinstance(item, dict):
                            list_element_keys.update(item.keys())

        field_analysis[key] = {
            "types": sorted(types_seen),
            "filled": len(non_none),
            "filled_pct": round(len(non_none) / total * 100, 1),
            "examples": examples,
            "nested_keys": sorted(nested_keys) if nested_keys else None,
            "list_element_keys": sorted(list_element_keys) if list_element_keys else None,
        }

    return field_analysis


def _print_field_analysis(analysis: dict[str, dict], indent: str = "  ") -> None:
    """Выводит анализ полей в консоль в читаемом формате.

    Args:
        analysis: Словарь анализа полей из _analyze_field_types.
        indent: Отступ для форматирования.
    """
    for key, info in analysis.items():
        types_str = ", ".join(info["types"])
        filled_str = f"{info['filled_pct']}%"

        print(  # noqa: T201
            f"{indent}├─ {key:<25} "
            f"тип={types_str:<12} "
            f"заполнено={filled_str:<6}"
        )

        # Примеры значений
        for example in info["examples"]:
            example_str = json.dumps(example, ensure_ascii=False)
            if len(example_str) > 120:
                example_str = example_str[:117] + "..."
            print(f"{indent}│    пример: {example_str}")  # noqa: T201

        # Вложенные ключи
        if info.get("nested_keys"):
            print(  # noqa: T201
                f"{indent}│    вложенные ключи: {info['nested_keys']}"
            )

        if info.get("list_element_keys"):
            print(  # noqa: T201
                f"{indent}│    элементы списка: {info['list_element_keys']}"
            )


def _deep_analyze_nested(
    objects: list[dict],
    parent_key: str,
) -> dict[str, dict]:
    """Рекурсивный анализ вложенного словаря (второй уровень).

    Args:
        objects: Список объектов верхнего уровня.
        parent_key: Ключ вложенного словаря.

    Returns:
        Словарь анализа вложенных полей.
    """
    nested_objects: list[dict] = []
    for obj in objects:
        if isinstance(obj, dict):
            val = obj.get(parent_key)
            if isinstance(val, dict):
                nested_objects.append(val)

    if not nested_objects:
        return {}

    return _analyze_field_types(nested_objects, f"{parent_key}_nested")


async def _fetch_get(page, url: str, headers: dict[str, str]) -> dict:
    """Выполняет GET-запрос через fetch() в контексте браузера.

    Args:
        page: Страница Playwright.
        url: URL для запроса.
        headers: Заголовки запроса.

    Returns:
        Словарь с результатом (success, data/error).
    """
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
                            raw: text.substring(0, 2000)
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


def _extract_objects_from_response(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа API.

    Пробует разные структуры ответа (data, data.data, data.objects и т.д.).

    Args:
        data: Parsed JSON ответа.

    Returns:
        Список объектов или None.
    """
    if isinstance(data, list) and data:
        return [item for item in data if isinstance(item, dict)]

    if not isinstance(data, dict):
        return None

    # Прямые ключи верхнего уровня
    for key in ("data", "objects", "items", "results", "list"):
        val = data.get(key)
        if isinstance(val, list) and val:
            items = [item for item in val if isinstance(item, dict)]
            if items:
                return items

    # Вложенные: data.data, data.objects и т.д.
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("objects", "items", "results", "list", "data"):
            val = inner.get(key)
            if isinstance(val, list) and val:
                items = [item for item in val if isinstance(item, dict)]
                if items:
                    return items

    # data.data как массив
    if isinstance(inner, list) and inner:
        items = [item for item in inner if isinstance(item, dict)]
        if items:
            return items

    return None


async def main() -> None:
    """Основная логика диагностического скрипта."""
    search_url = _get_search_url()

    print("\n" + "═" * 70)  # noqa: T201
    print("  ДИАГНОСТИКА: ВСЕ ДАННЫЕ ЭТАПА 1 (КАТАЛОГ) БЕЗ ВХОДА В КАРТОЧКУ")  # noqa: T201
    print("═" * 70)  # noqa: T201
    print(f"\n  URL поиска: {search_url[:100]}...")  # noqa: T201

    # Данные для итогового дампа
    dump_data: dict = {
        "search_url": search_url,
        "searchObjectsOnMap": {},
        "searchObjectsByLocation": {},
    }

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

        # ── Перехват API-запроса ──────────────────────────────────

        _print_separator("ЭТАП 1: Загрузка страницы и перехват API")

        captured: dict = {"url": None, "headers": None, "token": None}

        async def _intercept(route, request):
            url = request.url
            if "searchObjectsOnMap" in url and captured["url"] is None:
                captured["url"] = url
                captured["headers"] = dict(request.headers)
            if "sutochno.ru/api/json" in url and captured["token"] is None:
                token = (
                    request.headers.get("token")
                    or request.headers.get("Token")
                )
                if token:
                    captured["token"] = token
            await route.continue_()

        await page.route("**/api/json/**", _intercept)

        print("  Загружаем страницу поиска...")  # noqa: T201
        try:
            await page.goto(search_url, wait_until="domcontentloaded")
        except Exception as e:
            print(f"  ОШИБКА загрузки страницы: {e}")  # noqa: T201
            await browser.close()
            return

        # Ждём перехвата
        elapsed = 0.0
        while elapsed < API_INTERCEPT_TIMEOUT:
            if captured["url"] and captured["token"]:
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        if captured["url"]:
            print(  # noqa: T201
                f"  API перехвачен за {elapsed:.1f}с. "
                f"Ждём {POST_INTERCEPT_WAIT}с..."
            )
            await asyncio.sleep(POST_INTERCEPT_WAIT)

        await page.unroute("**/api/json/**")

        if not captured["token"]:
            print("  ОШИБКА: Токен не перехвачен!")  # noqa: T201
            await browser.close()
            return

        if not captured["url"]:
            print("  ОШИБКА: URL searchObjectsOnMap не перехвачен!")  # noqa: T201
            await browser.close()
            return

        token = captured["token"]
        map_url = captured["url"]
        api_headers = {
            k: v for k, v in (captured["headers"] or {}).items()
            if k.lower() not in SKIP_HEADERS
        } if captured["headers"] else {}

        print(f"  Токен: {token[:20]}...")  # noqa: T201
        print(f"  URL searchObjectsOnMap: {map_url[:120]}...")  # noqa: T201

        # ══════════════════════════════════════════════════════════
        #  API 1: searchObjectsOnMap
        # ══════════════════════════════════════════════════════════

        _print_separator("API 1: searchObjectsOnMap (маркеры карты)")

        paginated_url = _replace_offset(map_url, 0)
        print(f"  GET {paginated_url[:120]}...")  # noqa: T201

        result = await _fetch_get(page, paginated_url, api_headers)

        if not result.get("success"):
            print(f"  ОШИБКА: {result.get('error')}")  # noqa: T201
            print(f"  Raw: {result.get('raw', '')[:500]}")  # noqa: T201
            await browser.close()
            return

        map_data = result.get("data", {})
        map_objects = _extract_objects_from_response(map_data)

        if not map_objects:
            print("  Не удалось извлечь объекты из ответа.")  # noqa: T201
            print(f"  Структура: {json.dumps(map_data, ensure_ascii=False)[:1000]}")  # noqa: T201
            await browser.close()
            return

        print(f"  Получено объектов: {len(map_objects)}")  # noqa: T201

        # ── Анализ полей searchObjectsOnMap ──
        _print_subseparator("Анализ полей searchObjectsOnMap")

        map_analysis = _analyze_field_types(map_objects, "searchObjectsOnMap")
        _print_field_analysis(map_analysis)

        # ── Полный JSON первых N объектов ──
        _print_subseparator(
            f"Полный JSON первых {MAX_DETAIL_OBJECTS} объектов "
            f"(searchObjectsOnMap)"
        )

        for i, obj in enumerate(map_objects[:MAX_DETAIL_OBJECTS]):
            print(f"\n  ── Объект #{i + 1} (ID: {obj.get('id', '?')}) ──")  # noqa: T201
            print(  # noqa: T201
                json.dumps(obj, ensure_ascii=False, indent=4)
            )

        # Сохраняем в дамп
        dump_data["searchObjectsOnMap"] = {
            "total_objects": len(map_objects),
            "field_analysis": map_analysis,
            "sample_objects": map_objects[:MAX_DETAIL_OBJECTS],
            "all_top_level_keys": sorted(
                set(k for obj in map_objects for k in obj.keys())
            ),
        }

        # ══════════════════════════════════════════════════════════
        #  API 2: searchObjectsByLocation
        # ══════════════════════════════════════════════════════════

        _print_separator("API 2: searchObjectsByLocation (полные данные)")

        # Собираем ID из searchObjectsOnMap
        collected_ids: list[int] = []
        for obj in map_objects:
            obj_id = obj.get("id")
            if obj_id is not None and isinstance(obj_id, int):
                collected_ids.append(obj_id)

        if not collected_ids:
            print("  ОШИБКА: Не удалось извлечь ID из searchObjectsOnMap")  # noqa: T201
            await browser.close()
            return

        batch_ids = collected_ids[:MAX_IDS_FOR_LOCATION]
        ids_params = "&".join(f"ids[]={oid}" for oid in batch_ids)
        location_url = (
            f"{API_SEARCH_BY_LOCATION}"
            f"?{ids_params}"
            f"&max_guests=2&relevance=pairs&currencyId=1"
        )

        print(f"  Запрашиваем {len(batch_ids)} ID...")  # noqa: T201

        result = await _fetch_get(page, location_url, api_headers)

        if not result.get("success"):
            print(f"  ОШИБКА: {result.get('error')}")  # noqa: T201
            print(f"  Raw: {result.get('raw', '')[:500]}")  # noqa: T201
            await browser.close()
            return

        location_data = result.get("data", {})
        location_objects = _extract_objects_from_response(location_data)

        if not location_objects:
            print("  Не удалось извлечь объекты из ответа.")  # noqa: T201
            print(  # noqa: T201
                f"  Структура: "
                f"{json.dumps(location_data, ensure_ascii=False)[:1000]}"
            )
            await browser.close()
            return

        print(f"  Получено объектов: {len(location_objects)}")  # noqa: T201

        # ── Анализ полей верхнего уровня ──
        _print_subseparator("Анализ полей searchObjectsByLocation (верхний уровень)")

        location_analysis = _analyze_field_types(
            location_objects, "searchObjectsByLocation"
        )
        _print_field_analysis(location_analysis)

        # ── Глубокий анализ вложенных словарей ──
        nested_dict_keys = [
            key for key, info in location_analysis.items()
            if "dict" in info["types"]
        ]

        for nested_key in nested_dict_keys:
            _print_subseparator(
                f"Вложенные поля: {nested_key}"
            )
            nested_analysis = _deep_analyze_nested(location_objects, nested_key)
            if nested_analysis:
                _print_field_analysis(nested_analysis, indent="    ")

                # Ещё один уровень вглубь для вложенных словарей
                deeper_dict_keys = [
                    k for k, info in nested_analysis.items()
                    if "dict" in info["types"]
                ]
                for deeper_key in deeper_dict_keys:
                    # Собираем объекты третьего уровня
                    third_level_objects: list[dict] = []
                    for obj in location_objects:
                        if isinstance(obj, dict):
                            parent = obj.get(nested_key)
                            if isinstance(parent, dict):
                                child = parent.get(deeper_key)
                                if isinstance(child, dict):
                                    third_level_objects.append(child)

                    if third_level_objects:
                        print(  # noqa: T201
                            f"\n      ── Вложенные поля: "
                            f"{nested_key}.{deeper_key} ──"
                        )
                        deeper_analysis = _analyze_field_types(
                            third_level_objects,
                            f"{nested_key}.{deeper_key}",
                        )
                        _print_field_analysis(deeper_analysis, indent="      ")

        # ── Полный JSON первых N объектов ──
        _print_subseparator(
            f"Полный JSON первых {MAX_DETAIL_OBJECTS} объектов "
            f"(searchObjectsByLocation)"
        )

        for i, obj in enumerate(location_objects[:MAX_DETAIL_OBJECTS]):
            print(f"\n  ── Объект #{i + 1} (ID: {obj.get('id', '?')}) ──")  # noqa: T201
            print(  # noqa: T201
                json.dumps(obj, ensure_ascii=False, indent=4)
            )

        # Сохраняем в дамп
        dump_data["searchObjectsByLocation"] = {
            "total_objects": len(location_objects),
            "field_analysis": location_analysis,
            "sample_objects": location_objects[:MAX_DETAIL_OBJECTS],
            "all_top_level_keys": sorted(
                set(k for obj in location_objects for k in obj.keys())
            ),
        }

        # ══════════════════════════════════════════════════════════
        #  СРАВНЕНИЕ: Какие данные есть в каждом API
        # ══════════════════════════════════════════════════════════

        _print_separator("СРАВНЕНИЕ: searchObjectsOnMap vs searchObjectsByLocation")

        map_keys = set(map_analysis.keys())
        location_keys = set(location_analysis.keys())

        only_in_map = map_keys - location_keys
        only_in_location = location_keys - map_keys
        in_both = map_keys & location_keys

        print(f"\n  Полей в searchObjectsOnMap:       {len(map_keys)}")  # noqa: T201
        print(f"  Полей в searchObjectsByLocation:   {len(location_keys)}")  # noqa: T201
        print(f"  Общих полей:                      {len(in_both)}")  # noqa: T201

        if in_both:
            print(f"\n  Общие поля: {sorted(in_both)}")  # noqa: T201

        if only_in_map:
            print(f"\n  Только в searchObjectsOnMap: {sorted(only_in_map)}")  # noqa: T201

        if only_in_location:
            print(  # noqa: T201
                f"\n  Только в searchObjectsByLocation: "
                f"{sorted(only_in_location)}"
            )

        # ══════════════════════════════════════════════════════════
        #  ИТОГОВАЯ СВОДКА
        # ══════════════════════════════════════════════════════════

        _print_separator("ИТОГОВАЯ СВОДКА: ЧТО ДОСТУПНО НА ЭТАПЕ 1")

        print(  # noqa: T201
            "\n  searchObjectsOnMap (быстрый, для маркеров карты):"
        )
        print(f"    Ключи: {sorted(map_keys)}")  # noqa: T201

        print(  # noqa: T201
            "\n  searchObjectsByLocation (полный, по ID):"
        )
        print(f"    Ключи верхнего уровня: {sorted(location_keys)}")  # noqa: T201

        # Список всех вложенных ключей
        all_nested: dict[str, list[str]] = {}
        for nested_key in nested_dict_keys:
            nested_analysis = _deep_analyze_nested(location_objects, nested_key)
            if nested_analysis:
                all_nested[nested_key] = sorted(nested_analysis.keys())

        if all_nested:
            print("\n    Вложенные структуры:")  # noqa: T201
            for parent, children in sorted(all_nested.items()):
                print(f"      {parent}: {children}")  # noqa: T201

        dump_data["summary"] = {
            "searchObjectsOnMap_keys": sorted(map_keys),
            "searchObjectsByLocation_keys": sorted(location_keys),
            "only_in_map": sorted(only_in_map),
            "only_in_location": sorted(only_in_location),
            "common_keys": sorted(in_both),
            "nested_structures": all_nested,
        }

        # ── Сохранение полного дампа в файл ──
        dump_path = Path(DUMP_FILE_PATH)
        dump_path.parent.mkdir(parents=True, exist_ok=True)

        # Очищаем примеры от несериализуемых типов
        with open(dump_path, "w", encoding="utf-8") as f:
            json.dump(dump_data, f, ensure_ascii=False, indent=2, default=str)

        print(f"\n  Полный дамп сохранён: {dump_path}")  # noqa: T201
        print(f"  (JSON с анализом полей и примерами объектов)")  # noqa: T201

        print(f"\n{'═' * 70}\n")  # noqa: T201

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
