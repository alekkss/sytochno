"""Тестовый скрипт — полный первый этап через API.

Стратегия для каждой ссылки:
1. Загрузить страницу в браузере → фронтенд сам вызовет searchObjectsOnMap.
2. Перехватить реальный URL этого запроса (с правильными параметрами и price).
3. Повторить этот же URL через fetch() с offset=0, 50, 100... — пагинация.
4. По собранным ID — searchObjectsByLocation пачками по 50.

Никакой прокрутки, никаких кликов «Далее», никакого DOM-парсинга.

Запуск:
    python -m scripts.test_catalog_api_full
"""

import asyncio
import json
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
from playwright.async_api import Page, Response, async_playwright


# ── Настройки ──────────────────────────────────────────────

_OUTPUT_DIR = Path("data/api_debug/full_test")
_PAGE_SIZE = 50
_PAUSE_BETWEEN_API = 0.5  # между API-запросами fetch()
_PAUSE_AFTER_PAGE_LOAD = 10.0  # после загрузки каждой ссылки
_PAUSE_BETWEEN_URLS = 3.0  # между ссылками
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

_API_SEARCH_BY_LOCATION = (
    "https://sutochno.ru/api/json/search/searchObjectsByLocation"
)


# ── Модели ──────────────────────────────────────────────────


@dataclass
class CatalogListing:
    """Объявление, собранное через API."""

    external_id: str
    title: str
    price_per_night: int | None
    rating: float | None
    review_count: int | None
    area_m2: int | None
    guests: int | None
    address: str | None
    metro_station: str | None
    has_instant_booking: bool
    url: str
    listing_type: str | None = None


# ── Утилиты ──────────────────────────────────────────────────


def _print_separator(title: str) -> None:
    """Печатает разделитель."""
    print(f"\n{'═' * 70}")  # noqa: T201
    print(f"  {title}")  # noqa: T201
    print(f"{'═' * 70}")  # noqa: T201


def _extract_price_from_url(url: str) -> str:
    """Извлекает price из URL для логов."""
    params = parse_qs(urlparse(url).query, keep_blank_values=True)
    values = params.get("price", ["?"])
    return values[0] if values else "?"


def _replace_offset_in_url(url: str, new_offset: int) -> str:
    """Заменяет ТОЛЬКО offset в URL простой строковой заменой.

    Не трогает остальные параметры, не перекодирует скобки,
    не ломает запятые в price. Просто regex замена offset=N.

    Args:
        url: Исходный URL.
        new_offset: Новое значение offset.

    Returns:
        URL с заменённым offset.
    """
    # Ищем offset=число и заменяем
    result = re.sub(r'offset=\d+', f'offset={new_offset}', url)

    # Если offset не было в URL — добавляем
    if 'offset=' not in result:
        separator = '&' if '?' in result else '?'
        result = f"{result}{separator}offset={new_offset}"

    return result


def _parse_listing(obj: dict) -> CatalogListing:
    """Преобразует объект из searchObjectsByLocation в CatalogListing."""
    external_id = str(obj.get("id", ""))
    title = obj.get("title", "")

    prices = obj.get("prices", {})
    per_day = prices.get("perDay", {})
    price_per_night = (
        per_day.get("value") if isinstance(per_day, dict) else None
    )

    rating_data = obj.get("rating", {})
    rating = None
    review_count = None
    if isinstance(rating_data, dict):
        raw = rating_data.get("value")
        if raw is not None:
            rating = round(float(raw), 1)
        review_count = rating_data.get("count")

    props = obj.get("properties", {})
    area_m2 = props.get("area") if isinstance(props, dict) else None
    guests = props.get("maxGuests") if isinstance(props, dict) else None
    has_instant_booking = bool(
        props.get("bookingNow", False) if isinstance(props, dict) else False
    )

    location = obj.get("location", {})
    address = None
    if isinstance(location, dict):
        addr_data = location.get("address", {})
        if isinstance(addr_data, dict):
            address = addr_data.get("title")

    metro_station = None
    if isinstance(location, dict):
        relations = location.get("relations", {})
        if isinstance(relations, dict):
            metro_data = relations.get("metro", {})
            if isinstance(metro_data, dict):
                metro_title = metro_data.get("title", "")
                metro_dist = metro_data.get("distance", "")
                if metro_title:
                    metro_station = (
                        f"{metro_title}, {metro_dist} м"
                        if metro_dist
                        else metro_title
                    )

    return CatalogListing(
        external_id=external_id,
        title=title,
        price_per_night=price_per_night,
        rating=rating,
        review_count=review_count,
        area_m2=area_m2,
        guests=guests,
        address=address,
        metro_station=metro_station,
        has_instant_booking=has_instant_booking,
        url=f"https://sutochno.ru/{external_id}",
        listing_type=obj.get("type"),
    )


def _extract_objects(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа API."""
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


async def _fetch_get(page: Page, url: str, headers: dict) -> dict:
    """GET-запрос через fetch() в контексте браузера."""
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
                    return {success: false, error: 'JSON parse', raw: text.substring(0, 500)};
                }
            } catch (e) {
                return {success: false, error: e.message};
            }
        }
    """, {"url": url, "headers": headers})


def _load_search_urls() -> list[str]:
    """Загружает URL из .env."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)
    urls = []
    for i in range(1, 9):
        value = os.getenv(f"SUTOCHNO_SEARCH_URL_{i}", "").strip()
        if value:
            urls.append(value)
    return urls


# ── Основная логика ──────────────────────────────────────────


async def run() -> None:
    """Полный первый этап.

    Для каждой ссылки:
    1. page.goto(url) → фронтенд вызывает searchObjectsOnMap
    2. Перехватываем URL этого запроса
    3. fetch(url, offset=0,50,100...) → собираем все ID
    4. По всем ID → searchObjectsByLocation → полные данные
    """
    start_time = time.time()

    search_urls = _load_search_urls()
    if not search_urls:
        print("ОШИБКА: Нет ссылок в .env")  # noqa: T201
        return

    _print_separator(f"ПОЛНЫЙ ПЕРВЫЙ ЭТАП ({len(search_urls)} ссылок)")
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for i, url in enumerate(search_urls, 1):
        print(f"  {i}. price={_extract_price_from_url(url)}")  # noqa: T201

    seen_ids: set[int] = set()
    all_unique_ids: list[int] = []
    url_stats: list[dict] = []
    api_headers: dict | None = None

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

        # ── Обход ссылок ──
        for url_index, search_url in enumerate(search_urls, 1):
            price = _extract_price_from_url(search_url)
            _print_separator(
                f"Ссылка {url_index}/{len(search_urls)}: price={price}"
            )

            url_start = time.time()

            # --- Перехватчик для этой ссылки ---
            captured_map_url: dict = {"value": None}
            captured_headers: dict = {"value": None}

            async def _intercept(route, request):
                """Перехватывает searchObjectsOnMap URL и заголовки."""
                url = request.url
                headers = request.headers

                if "searchObjectsOnMap" in url and captured_map_url["value"] is None:
                    captured_map_url["value"] = url
                    captured_headers["value"] = dict(headers)
                    print(f"  Перехвачен searchObjectsOnMap")  # noqa: T201

                await route.continue_()

            await page.route("**/api/json/**", _intercept)

            # Загрузка страницы
            print("  Загрузка страницы...")  # noqa: T201
            await page.goto(search_url, wait_until="networkidle")

            try:
                await page.wait_for_selector(
                    ".card[data-observe-id]", timeout=30000
                )
            except Exception:
                pass

            print(f"  Ожидание {_PAUSE_AFTER_PAGE_LOAD} сек...")  # noqa: T201
            await asyncio.sleep(_PAUSE_AFTER_PAGE_LOAD)

            # Снимаем перехватчик (чтобы не мешал fetch)
            await page.unroute("**/api/json/**")

            # Проверяем перехват
            if not captured_map_url["value"]:
                print("  searchObjectsOnMap не перехвачен — пропускаем")  # noqa: T201
                url_stats.append({
                    "url_index": url_index,
                    "price": price,
                    "new_unique": 0,
                    "total_from_url": 0,
                    "duplicates": 0,
                    "cumulative": len(seen_ids),
                    "duration_sec": round(time.time() - url_start, 1),
                    "error": "searchObjectsOnMap не перехвачен",
                })
                continue

            map_url = captured_map_url["value"]
            print(f"  URL: {map_url[:120]}...")  # noqa: T201

            # Заголовки — берём из перехвата, фильтруем browser-only
            if captured_headers["value"]:
                skip = {
                    "host", "connection", "content-length",
                    "accept-encoding", "sec-fetch-dest",
                    "sec-fetch-mode", "sec-fetch-site",
                    "sec-ch-ua", "sec-ch-ua-mobile",
                    "sec-ch-ua-platform",
                }
                api_headers = {
                    k: v for k, v in captured_headers["value"].items()
                    if k.lower() not in skip
                }

            # --- Пагинация через fetch с подменой offset ---
            print("  Сбор ID через пагинацию...")  # noqa: T201

            new_ids_from_url: list[int] = []
            duplicates_from_url = 0
            offset = 0
            consecutive_empty = 0

            while True:
                paginated_url = _replace_offset_in_url(map_url, offset)

                result = await _fetch_get(page, paginated_url, api_headers)

                if not result.get("success"):
                    print(  # noqa: T201
                        f"    offset={offset}: "
                        f"ошибка — {result.get('error')}"
                    )
                    break

                data = result["data"]

                if isinstance(data, dict) and not data.get("success", True):
                    errors = data.get("errors", [])
                    print(  # noqa: T201
                        f"    offset={offset}: API ошибка — {errors}"
                    )
                    break

                objects = _extract_objects(data)

                if not objects:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        print(  # noqa: T201
                            f"    offset={offset}: пустой ответ x2 — конец"
                        )
                        break
                    offset += _PAGE_SIZE
                    await asyncio.sleep(_PAUSE_BETWEEN_API)
                    continue

                consecutive_empty = 0
                page_new = 0
                page_dups = 0

                for obj in objects:
                    obj_id = obj.get("id")
                    if obj_id is None:
                        continue
                    if obj_id in seen_ids:
                        page_dups += 1
                        duplicates_from_url += 1
                        continue
                    seen_ids.add(obj_id)
                    all_unique_ids.append(obj_id)
                    new_ids_from_url.append(obj_id)
                    page_new += 1

                print(  # noqa: T201
                    f"    offset={offset}: "
                    f"получено={len(objects)}, "
                    f"+{page_new} новых, "
                    f"{page_dups} дублей"
                )

                if len(objects) < _PAGE_SIZE:
                    print(f"    Последняя страница (< {_PAGE_SIZE})")  # noqa: T201
                    break

                offset += _PAGE_SIZE
                await asyncio.sleep(_PAUSE_BETWEEN_API)

            url_duration = time.time() - url_start

            stats = {
                "url_index": url_index,
                "price": price,
                "new_unique": len(new_ids_from_url),
                "total_from_url": len(new_ids_from_url) + duplicates_from_url,
                "duplicates": duplicates_from_url,
                "cumulative": len(seen_ids),
                "duration_sec": round(url_duration, 1),
            }
            url_stats.append(stats)

            print(  # noqa: T201
                f"\n  ИТОГ: +{len(new_ids_from_url)} уникальных, "
                f"{duplicates_from_url} дублей, "
                f"накоплено={len(seen_ids)}, "
                f"{url_duration:.1f} сек"
            )

            if url_index < len(search_urls):
                print(f"  Пауза {_PAUSE_BETWEEN_URLS} сек...")  # noqa: T201
                await asyncio.sleep(_PAUSE_BETWEEN_URLS)

        # ── Сбор ID завершён ──
        _print_separator(f"ID собраны: {len(all_unique_ids)} уникальных")

        if not all_unique_ids:
            print("  Ни одного ID!")  # noqa: T201
            await browser.close()
            return

        if not api_headers:
            print("  Нет заголовков API!")  # noqa: T201
            await browser.close()
            return

        # ── searchObjectsByLocation ──
        _print_separator("Получение полных данных")

        listings: list[CatalogListing] = []
        total_batches = (len(all_unique_ids) + _PAGE_SIZE - 1) // _PAGE_SIZE

        for i in range(0, len(all_unique_ids), _PAGE_SIZE):
            batch = all_unique_ids[i : i + _PAGE_SIZE]
            batch_num = i // _PAGE_SIZE + 1

            ids_params = "&".join(f"ids[]={oid}" for oid in batch)
            url = (
                f"{_API_SEARCH_BY_LOCATION}"
                f"?{ids_params}"
                f"&max_guests=2&relevance=pairs&currencyId=1"
            )

            result = await _fetch_get(page, url, api_headers)

            if not result.get("success"):
                print(  # noqa: T201
                    f"  Пачка {batch_num}/{total_batches}: "
                    f"ошибка — {result.get('error')}"
                )
                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            data = result["data"]
            if isinstance(data, dict) and not data.get("success", True):
                print(  # noqa: T201
                    f"  Пачка {batch_num}/{total_batches}: "
                    f"API ошибка — {data.get('errors')}"
                )
                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            objects = _extract_objects(data)
            if not objects:
                print(  # noqa: T201
                    f"  Пачка {batch_num}/{total_batches}: нет объектов"
                )
                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            for obj in objects:
                listings.append(_parse_listing(obj))

            print(  # noqa: T201
                f"  Пачка {batch_num}/{total_batches}: "
                f"{len(objects)} → итого {len(listings)}"
            )

            await asyncio.sleep(_PAUSE_BETWEEN_API)

        await browser.close()

    # ── Сводка ──
    total_time = time.time() - start_time

    _print_separator("РЕЗУЛЬТАТЫ")

    print(f"\n  Ссылок: {len(search_urls)}")  # noqa: T201
    print(f"  Уникальных ID: {len(all_unique_ids)}")  # noqa: T201
    print(f"  Объявлений с данными: {len(listings)}")  # noqa: T201
    print(f"  Время: {total_time:.1f} сек ({total_time / 60:.1f} мин)")  # noqa: T201

    print("\n  По ссылкам:")  # noqa: T201
    for s in url_stats:
        err = f" [{s['error']}]" if s.get("error") else ""
        print(  # noqa: T201
            f"    #{s['url_index']} price={s['price']}: "
            f"+{s['new_unique']} уник., "
            f"{s['duplicates']} дублей, "
            f"{s['duration_sec']} сек{err}"
        )

    if listings:
        filled = {
            "title": sum(1 for l in listings if l.title),
            "price": sum(1 for l in listings if l.price_per_night),
            "rating": sum(1 for l in listings if l.rating is not None),
            "reviews": sum(1 for l in listings if l.review_count is not None),
            "area": sum(1 for l in listings if l.area_m2 is not None),
            "guests": sum(1 for l in listings if l.guests is not None),
            "address": sum(1 for l in listings if l.address),
            "metro": sum(1 for l in listings if l.metro_station),
            "booking": sum(1 for l in listings if l.has_instant_booking),
        }

        print("\n  Заполненность:")  # noqa: T201
        total = len(listings)
        for name, count in filled.items():
            print(f"    {name}: {count}/{total} ({count / total * 100:.0f}%)")  # noqa: T201

        print("\n  Первые 5:")  # noqa: T201
        for i, l in enumerate(listings[:5], 1):
            print(  # noqa: T201
                f"    {i}. [{l.external_id}] {l.title}\n"
                f"       {l.price_per_night} руб. | "
                f"{l.rating} ({l.review_count} отз.) | "
                f"{l.area_m2} м² | {l.guests} гостей\n"
                f"       {l.address} | {l.metro_station}\n"
                f"       Бронирование: "
                f"{'Да' if l.has_instant_booking else 'Нет'}"
            )

    # Сохранение
    _print_separator("Сохранение")

    listings_data = [
        {
            "external_id": l.external_id,
            "title": l.title,
            "price_per_night": l.price_per_night,
            "rating": l.rating,
            "review_count": l.review_count,
            "area_m2": l.area_m2,
            "guests": l.guests,
            "address": l.address,
            "metro_station": l.metro_station,
            "has_instant_booking": l.has_instant_booking,
            "url": l.url,
            "type": l.listing_type,
        }
        for l in listings
    ]

    all_path = _OUTPUT_DIR / "all_listings.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(listings_data, f, ensure_ascii=False, indent=2)
    print(f"  Объявления: {all_path}")  # noqa: T201

    stats_path = _OUTPUT_DIR / "stats.json"
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump({
            "total_urls": len(search_urls),
            "total_ids": len(all_unique_ids),
            "total_listings": len(listings),
            "total_time_sec": round(total_time, 1),
            "url_stats": url_stats,
        }, f, ensure_ascii=False, indent=2)
    print(f"  Статистика: {stats_path}")  # noqa: T201

    _print_separator("ГОТОВО")


if __name__ == "__main__":
    asyncio.run(run())
