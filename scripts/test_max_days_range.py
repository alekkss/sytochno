"""Диагностический скрипт: проверка максимальной глубины календаря API.

Цель: выяснить, отдаёт ли getPricesAndAvailabilities данные дальше 60 дней
вперёд (нужно для задачи с диапазоном 0-90 дней), или это жёсткий лимит.

Логика:
1. Загружаем страницу поиска, перехватываем токен API (как в test_batch_api.py).
2. Собираем несколько реальных ID объявлений.
3. Для каждого ID по очереди отправляем запрос getPricesAndAvailabilities
   с разной глубиной календаря: 60, 75, 90, 97, 120 дней от сегодня.
4. Смотрим на длину detail[] в ответе и наличие ошибок:
   - Если detail[] растёт пропорционально запрошенным дням — лимита нет
     (или он выше 120).
   - Если detail[] перестаёт расти после 60 (или другого числа) —
     это и есть реальный лимит API.
   - Если API возвращает ошибку при увеличении диапазона — тоже указывает
     на лимит, текст ошибки будет напечатан.

Запуск (из корня существующего проекта sutochno_parser):
    python -m scripts.test_max_days_range
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

load_dotenv(PROJECT_ROOT / ".env")

# ── Константы ──

API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
DEFAULT_GUESTS: int = 2
API_INTERCEPT_TIMEOUT: float = 30.0
POLL_INTERVAL: float = 0.5
POST_INTERCEPT_WAIT: float = 5.0

# Глубины календаря, которые проверяем (в днях от сегодня)
DAY_RANGES_TO_TEST: list[int] = [60, 75, 90, 97, 120]

# Сколько разных объявлений проверить (на случай, если лимит зависит
# от конкретного объекта, а не является общим для API)
OBJECTS_TO_TEST: int = 3

SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}


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


def _replace_offset(url: str, new_offset: int) -> str:
    """Заменяет параметр offset в URL."""
    result = re.sub(r"offset=\d+", f"offset={new_offset}", url)
    if "offset=" not in result:
        separator = "&" if "?" in result else "?"
        result = f"{result}{separator}offset={new_offset}"
    return result


async def _test_one_range(
    page, token: str, object_id: int, days: int
) -> dict:
    """Отправляет запрос на заданную глубину календаря и возвращает сводку.

    Args:
        page: Страница Playwright с активной сессией.
        token: Перехваченный API-токен.
        object_id: ID тестируемого объявления.
        days: Глубина календаря в днях от сегодня.

    Returns:
        Словарь с результатом: detail_count, busy, errors, raw_error.
    """
    today = date.today()
    date_begin = f"{today.isoformat()} 14:00:00"
    date_end = f"{(today + timedelta(days=days)).isoformat()} 11:00:00"

    body = {
        "objects": [object_id],
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

    result = await _fetch_post(page, API_PRICES_URL, body, token)

    if not result.get("success"):
        return {
            "days": days,
            "ok": False,
            "reason": f"fetch_error: {result.get('error')}",
            "detail_count": None,
            "busy": None,
        }

    api_data = result.get("data", {})
    if not api_data.get("success"):
        return {
            "days": days,
            "ok": False,
            "reason": f"api_error: {api_data.get('errors')}",
            "detail_count": None,
            "busy": None,
        }

    objects_resp = api_data.get("data", {}).get("objects", [])
    if not objects_resp:
        return {
            "days": days,
            "ok": False,
            "reason": "пустой objects[] в ответе",
            "detail_count": None,
            "busy": None,
        }

    obj = objects_resp[0]
    if not obj.get("success"):
        return {
            "days": days,
            "ok": False,
            "reason": f"объект_ошибка: {obj.get('errors')}",
            "detail_count": None,
            "busy": None,
        }

    data = obj.get("data", {})
    detail = data.get("detail", [])
    busy = data.get("busy")

    return {
        "days": days,
        "ok": True,
        "reason": None,
        "detail_count": len(detail),
        "busy": busy,
    }


async def main() -> None:
    """Основная логика: перехват токена, сбор ID, проверка диапазонов дней."""
    search_url = _get_search_url()
    print(f"URL поиска: {search_url[:100]}...")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="ru-RU",
        )
        page = await context.new_page()

        # ── Перехват токена ──
        _print_separator("Загрузка страницы поиска и перехват токена")

        captured: dict = {"url": None, "headers": None, "token": None}
        seen_api_calls: list[str] = []

        async def _intercept(route, request):
            url = request.url
            if "/api/json/" in url:
                # Короткое имя эндпоинта после /api/json/ — для диагностики
                short_name = url.split("/api/json/", 1)[-1].split("?", 1)[0]
                seen_api_calls.append(short_name)
            if "searchObjectsOnMap" in url and captured["url"] is None:
                captured["url"] = url
                captured["headers"] = dict(request.headers)
            if "sutochno.ru/api/json" in url and captured["token"] is None:
                token = request.headers.get("token") or request.headers.get("Token")
                if token:
                    captured["token"] = token
            await route.continue_()

        await page.route("**/api/json/**", _intercept)
        await page.goto(search_url, wait_until="domcontentloaded")

        elapsed = 0.0
        while elapsed < API_INTERCEPT_TIMEOUT:
            if captured["url"] and captured["token"]:
                break
            await asyncio.sleep(POLL_INTERVAL)
            elapsed += POLL_INTERVAL

        if captured["url"]:
            await asyncio.sleep(POST_INTERCEPT_WAIT)

        await page.unroute("**/api/json/**")

        if not captured["token"]:
            print("ОШИБКА: Токен не перехвачен! Страница не загрузилась как ожидалось.")
            await browser.close()
            return

        token = captured["token"]
        map_url = captured["url"]
        api_headers = {
            k: v for k, v in (captured["headers"] or {}).items()
            if k.lower() not in SKIP_HEADERS
        } if captured["headers"] else {}

        print(f"Токен перехвачен: {token[:20]}...")
        print(f"Всего перехвачено api/json-запросов: {len(seen_api_calls)}")
        if seen_api_calls:
            print("Список перехваченных эндпоинтов:")
            for name in seen_api_calls:
                print(f"  - {name}")
        else:
            print("ВНИМАНИЕ: не перехвачено ни одного /api/json/ запроса.")

        # ── Диагностика страницы, если searchObjectsOnMap не перехвачен ──
        if not map_url:
            print(
                "\nВНИМАНИЕ: запрос searchObjectsOnMap не перехвачен. "
                "Сохраняю скриншот и HTML страницы для диагностики..."
            )
            diag_dir = PROJECT_ROOT / "logs"
            diag_dir.mkdir(exist_ok=True)
            screenshot_path = diag_dir / "diagnostic_screenshot.png"
            html_path = diag_dir / "diagnostic_page.html"

            try:
                await page.screenshot(path=str(screenshot_path), full_page=True)
                print(f"Скриншот сохранён: {screenshot_path}")
            except Exception as e:
                print(f"Не удалось сохранить скриншот: {e}")

            try:
                html_content = await page.content()
                html_path.write_text(html_content, encoding="utf-8")
                print(f"HTML сохранён: {html_path}")
            except Exception as e:
                print(f"Не удалось сохранить HTML: {e}")
                html_content = ""

            # Простая проверка на типичные признаки антибот-блокировки
            antibot_markers = [
                "captcha", "recaptcha", "подтвердите, что вы не робот",
                "just a moment", "checking your browser", "access denied",
                "доступ ограничен", "доступ запрещён",
            ]
            lowered = html_content.lower()
            found_markers = [m for m in antibot_markers if m in lowered]
            if found_markers:
                print(
                    f"\nОБНАРУЖЕНЫ ПРИЗНАКИ АНТИБОТ-ЗАЩИТЫ: {found_markers}\n"
                    "Похоже, сайт распознал автоматизацию и не отдал обычную "
                    "страницу поиска (например, вместо неё капча или заглушка). "
                    "Это частая причина при смене IP-адреса сервера. "
                    "Проверьте скриншот выше."
                )
            else:
                print(
                    "\nЯвных признаков капчи/блокировки в HTML не найдено. "
                    "Возможно, сайт просто изменил внутреннюю логику API "
                    "(например, эндпоинт переименован) — сверьте скриншот "
                    "и список перехваченных эндпоинтов выше вручную."
                )

            print(
                "\nПродолжить проверку глубины календаря без searchObjectsOnMap "
                "нельзя — нет реальных ID для теста. Завершаю работу."
            )
            await browser.close()
            return

        # ── Сбор нескольких реальных ID ──
        _print_separator("Сбор ID через searchObjectsOnMap")

        collected_ids: list[int] = []
        paginated_url = _replace_offset(map_url, 0)
        print(f"Повторный GET-запрос: {paginated_url[:120]}...")

        result = await _fetch_get(page, paginated_url, api_headers)

        if not result.get("success"):
            print(f"ОШИБКА fetch: {result.get('error')}")
            print(f"Raw (первые 500 симв.): {result.get('raw', '')[:500]}")
        else:
            data = result.get("data", {})
            print(f"Статус ответа: {result.get('status')}")

            objects = None
            if isinstance(data, dict):
                inner = data.get("data")
                if isinstance(inner, list):
                    objects = inner
                elif isinstance(inner, dict):
                    objects = inner.get("objects", inner.get("items"))
                if not objects:
                    objects = data.get("objects")

            if objects and isinstance(objects, list):
                for obj in objects:
                    obj_id = obj.get("id")
                    if obj_id:
                        collected_ids.append(obj_id)
                print(f"Извлечено ID: {len(collected_ids)}")
            else:
                print("Не удалось извлечь объекты из ответа — дамп структуры:")
                print(f"  Тип data: {type(data).__name__}")
                if isinstance(data, dict):
                    print(f"  Ключи верхнего уровня: {list(data.keys())}")
                    inner = data.get("data")
                    print(f"  Тип data.data: {type(inner).__name__}")
                    if isinstance(inner, dict):
                        print(f"  Ключи data.data: {list(inner.keys())}")
                print("  Сырой ответ (первые 1500 симв.):")
                print(f"  {json.dumps(data, ensure_ascii=False, indent=2)[:1500]}")

        if not collected_ids:
            print(
                "\nОШИБКА: Не удалось собрать ID даже после расширенного разбора "
                "ответа. Смотрите дамп структуры выше — вероятно, sutochno.ru "
                "изменил формат ответа searchObjectsOnMap, и нужно обновить "
                "логику извлечения objects[] под новую структуру."
            )
            await browser.close()
            return

        print(f"Собрано ID: {len(collected_ids)}. Берём первые {OBJECTS_TO_TEST}.")
        test_ids = collected_ids[:OBJECTS_TO_TEST]

        # ── Проверка глубины календаря для каждого объекта ──
        _print_separator("Проверка глубины календаря по дням")

        all_results: dict[int, list[dict]] = {}

        for object_id in test_ids:
            print(f"\n--- Объект ID={object_id} ---")
            per_object: list[dict] = []
            for days in DAY_RANGES_TO_TEST:
                res = await _test_one_range(page, token, object_id, days)
                per_object.append(res)
                if res["ok"]:
                    print(
                        f"  {days:>4} дней → OK | "
                        f"busy={res['busy']} | detail[]={res['detail_count']}"
                    )
                else:
                    print(f"  {days:>4} дней → ОШИБКА | {res['reason']}")
                await asyncio.sleep(1.0)
            all_results[object_id] = per_object

        # ── Итоговый анализ ──
        _print_separator("ИТОГИ")

        for object_id, results in all_results.items():
            print(f"\nОбъект {object_id}:")
            prev_count = None
            for res in results:
                if not res["ok"]:
                    print(f"  {res['days']} дней: ОШИБКА ({res['reason']})")
                    continue
                count = res["detail_count"]
                growth = ""
                if prev_count is not None:
                    growth = " (без роста!)" if count <= prev_count else " (растёт)"
                print(f"  {res['days']} дней: detail[]={count}{growth}")
                prev_count = count

        print(
            "\nЕсли detail[] перестаёт расти после определённого числа дней\n"
            "(или все объекты стабильно возвращают одно и то же количество\n"
            "записей независимо от запрошенного диапазона) — это и есть\n"
            "реальный лимит глубины календаря API. Если растёт пропорционально\n"
            "запрошенным дням вплоть до 120 — лимита в районе 90 дней нет,\n"
            "и DAYS_COUNT можно смело увеличивать."
        )

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
