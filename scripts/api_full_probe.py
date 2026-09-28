"""Глубокий пробник API sutochno.ru — поиск признака «свободен, но не бронируется».

Задача: найти в API пометку, отличающую дни 08-10 октября объекта 923051
(не заняты, но забронировать нельзя из-за ограничения минимального срока)
от реально проданных дней (11-15, 20-25 октября).

Четыре блока:
  A. Полные дампы ответов getPricesAndAvailabilities — БЕЗ выборки полей.
     Сравниваем ответы для разных категорий дней: возможно, полное тело
     содержит поля, которых не было в нашей выжимке (rooms_available,
     расширенный detail[], служебные флаги).
  B. checkBookingAbility — 6 вариантов формата тела запроса
     (предыдущая попытка вернула 400 «Bad request» — тело было неверным).
  C. calculateBookingPrice — те же варианты тела.
  D. Перебор гипотетических эндпоинтов календаря/занятости
     (GET и POST) — вдруг существует неисследованный метод.

Все ответы сохраняются целиком в data/api_full_probe_results.json.

Запуск из корня проекта парсера:

    python -m scripts.api_full_probe
"""

import asyncio
import json
import os
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

# ── Конфигурация ──────────────────────────────────────────────

OBJECT_ID: int = 923051

API_PRICES_URL: str = (
    "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
)
CHECK_ABILITY_URL: str = (
    "https://sutochno.ru/api/json/objects/checkBookingAbility"
)
CALC_PRICE_URL: str = (
    "https://sutochno.ru/api/json/objects/calculateBookingPrice"
)

GUESTS: int = 2
REQUEST_PAUSE: float = 1.3
FETCH_TIMEOUT: int = 45
TOKEN_TIMEOUT: float = 30.0

OUT_JSON: Path = Path("data/api_full_probe_results.json")

# Блок A: (дата, ночей, пояснение для вывода)
PART_A_CASES: list[tuple[date, int, str]] = [
    (date(2026, 10, 8), 1, "спорная: min_nights ошибка"),
    (date(2026, 10, 8), 4, "спорная: BUSY из-за проданной 11-й в окне"),
    (date(2026, 10, 9), 4, "спорная: BUSY"),
    (date(2026, 10, 10), 4, "спорная: BUSY"),
    (date(2026, 10, 11), 1, "продана: min_nights ошибка"),
    (date(2026, 10, 11), 5, "продана: BUSY (ночи реально заняты)"),
    (date(2026, 10, 16), 4, "свободна: UNBUSY"),
    (date(2026, 10, 17), 4, "свободна с огранич.: BUSY из-за 21-й"),
    (date(2026, 10, 21), 5, "продана: BUSY"),
    (date(2026, 10, 26), 5, "свободна: UNBUSY"),
]

# Блок B/C: варианты тела запроса для checkBookingAbility / calculateBookingPrice
D1: str = "2026-10-08 14:00:00"      # спорная дата, 1 ночь
D1_END: str = "2026-10-09 11:00:00"
D4: str = "2026-10-08 14:00:00"      # спорная дата, 4 ночи
D4_END: str = "2026-10-12 11:00:00"

BODY_VARIANTS: list[tuple[str, dict, str]] = [
    ("readme-camel", {
        "objectId": OBJECT_ID, "dateBegin": D1, "dateEnd": D1_END,
        "guests": GUESTS, "currencyId": 1, "childAges": [], "conditions": [],
    }, "формат из README (уже давал 400 — контрольная точка)"),
    ("readme-camel-4n", {
        "objectId": OBJECT_ID, "dateBegin": D4, "dateEnd": D4_END,
        "guests": GUESTS, "currencyId": 1, "childAges": [], "conditions": [],
    }, "то же, но 4 ночи (внутри проданная 11-я)"),
    ("snake-case", {
        "object_id": OBJECT_ID, "date_begin": D1, "date_end": D1_END,
        "guests": GUESTS, "currency_id": 1, "child_ages": [], "conditions": [],
    }, "змеиный_регистр как у bulk-API"),
    ("bulk-style", {
        "objects": [OBJECT_ID], "rooms_cnt": {}, "guests": GUESTS,
        "date_begin": D1, "date_end": D1_END, "currency_id": 1,
        "is_pets": 0, "documents": 0, "target": 0, "ages": [], "no_time": 1,
    }, "тело в стиле getPricesAndAvailabilities"),
    ("date-only", {
        "objectId": OBJECT_ID, "dateBegin": "2026-10-08",
        "dateEnd": "2026-10-09", "guests": GUESTS,
        "currencyId": 1, "childAges": [], "conditions": [],
    }, "даты без времени"),
    ("camel-plus", {
        "objectId": OBJECT_ID, "dateBegin": D1, "dateEnd": D1_END,
        "guests": GUESTS, "currencyId": 1, "childAges": [],
        "conditions": [], "precostFull": 0, "precostVirtual": 0,
        "is_pets": 0, "documents": 0, "target": 0,
    }, "с полями precost как у getCancellationRules"),
]

# Блок D: гипотетические эндпоинты (имя, метод)
PART_D_ENDPOINTS: list[tuple[str, str]] = [
    ("objects/getCalendar", "POST"),
    ("objects/getObjectCalendar", "POST"),
    ("objects/getAvailability", "POST"),
    ("objects/getBusyDates", "POST"),
    ("objects/getFreeDates", "POST"),
    ("objects/getOrders", "POST"),
    ("objects/getObjectOrders", "POST"),
    ("objects/getDates", "POST"),
    ("objects/getMinNights", "POST"),
    ("objects/getRestrictions", "POST"),
    ("objects/calendar", "GET"),
    ("objects/getOccupancy", "POST"),
]

# ── JS: универсальный запрос с полным телом ответа ────────────

_FETCH_RAW_JS = """
async ({apiUrl, method, headers, bodyJson, fetchTimeout}) => {
    try {
        const controller = new AbortController();
        const tid = setTimeout(() => controller.abort(), fetchTimeout * 1000);

        const opts = {
            method: method,
            headers: headers,
            credentials: 'include',
            signal: controller.signal
        };
        if (bodyJson !== null) {
            opts.body = JSON.stringify(bodyJson);
        }

        const resp = await fetch(apiUrl, opts);
        clearTimeout(tid);

        const text = await resp.text();
        let json = null;
        try { json = JSON.parse(text); } catch (e) { json = null; }

        return {
            status: resp.status,
            contentType: resp.headers.get('content-type') || '',
            text: text.substring(0, 20000),
            json: json
        };
    } catch (e) {
        return {status: 0, contentType: '', text: String(e.message), json: null};
    }
}
"""

# ── Вспомогательные функции ───────────────────────────────────


async def _get_token_and_page(context) -> tuple[str | None, Page | None]:
    """Загружает страницу поиска, перехватывает токен API.

    Args:
        context: Браузерный контекст Playwright.

    Returns:
        Кортеж (token, page) или (None, None) при неудаче.
    """
    search_url = ""
    for idx in range(1, 9):
        url = os.getenv(f"SUTOCHNO_SEARCH_URL_{idx}", "").strip()
        if url:
            search_url = url
            break

    if not search_url:
        raise SystemExit(
            "Ошибка: не задана ни одна SUTOCHNO_SEARCH_URL_* в .env."
        )

    page = await context.new_page()
    captured: list[str] = []

    async def _intercept(route, request) -> None:
        if "sutochno.ru/api/json" in request.url and not captured:
            token = (
                request.headers.get("token")
                or request.headers.get("Token")
            )
            if token:
                captured.append(token)
        try:
            await route.continue_()
        except Exception:
            pass

    await page.route("**/api/json/**", _intercept)
    try:
        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
        elapsed = 0.0
        while elapsed < TOKEN_TIMEOUT and not captured:
            await asyncio.sleep(0.5)
            elapsed += 0.5
    except Exception as e:
        print(f"Предупреждение: страница поиска не загрузилась ({str(e)[:120]})")
    finally:
        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass

    return (captured[0] if captured else None), page


def _api_headers(token: str | None) -> dict:
    """Собирает заголовки API-запроса (как фронтенд сайта)."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "platform": "js",
        "api-version": "1.13",
    }
    if token:
        headers["token"] = token
    return headers


def _part_a_body(d: date, nights: int) -> dict:
    """Тело bulk-запроса getPricesAndAvailabilities для одного объекта."""
    return {
        "objects": [OBJECT_ID],
        "rooms_cnt": {},
        "guests": GUESTS,
        "date_begin": f"{d.isoformat()} 14:00:00",
        "date_end": f"{(d + timedelta(days=nights)).isoformat()} 11:00:00",
        "currency_id": 1,
        "is_pets": 0,
        "documents": 0,
        "target": 0,
        "ages": [],
        "no_time": 1,
    }


def _obj_payload(json_resp: dict | None) -> dict | None:
    """Извлекает объект объявления из ответа (без фильтрации полей!)."""
    if not isinstance(json_resp, dict):
        return None
    data = json_resp.get("data")
    if not isinstance(data, dict):
        return None
    objects = data.get("objects")
    if isinstance(objects, list) and objects:
        return objects[0]
    return None


# ── Точка входа ───────────────────────────────────────────────


async def main() -> None:
    """Основной сценарий: токен → блоки A-D → сводка → JSON."""
    load_dotenv()

    print("=" * 72)
    print("Глубокий пробник API: поиск признака «свободен, но не бронируется»")
    print(f"Объект: {OBJECT_ID}")
    print("=" * 72)

    storage: dict = {"object_id": OBJECT_ID, "parts": {}}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=os.getenv("HEADLESS_MODE", "false").lower() == "true",
        )
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="ru-RU",
        )

        token, page = await _get_token_and_page(context)
        if token is None or page is None:
            print("ОШИБКА: токен не перехвачен. Перезапустите скрипт.")
            await browser.close()
            return

        print(f"Токен получен (длина {len(token)}).\n")
        headers = _api_headers(token)

        async def _call(url: str, method: str, body: dict | None) -> dict:
            """Выполняет запрос с полной передачей тела ответа."""
            return await page.evaluate(
                _FETCH_RAW_JS,
                {
                    "apiUrl": url,
                    "method": method,
                    "headers": headers,
                    "bodyJson": body,
                    "fetchTimeout": FETCH_TIMEOUT,
                },
            )

        # ── Блок A: полные дампы getPricesAndAvailabilities ──
        print("[A] Полные ответы getPricesAndAvailabilities "
              f"({len(PART_A_CASES)} запросов):")
        part_a: list[dict] = []

        for d, nights, note in PART_A_CASES:
            resp = await _call(API_PRICES_URL, "POST", _part_a_body(d, nights))
            obj = _obj_payload(resp.get("json"))
            entry = {
                "date": d.isoformat(),
                "nights": nights,
                "note": note,
                "http_status": resp["status"],
                "object_raw": obj,
                "top_level": (
                    {k: v for k, v in resp["json"].items() if k != "data"}
                    if isinstance(resp.get("json"), dict) else None
                ),
            }
            part_a.append(entry)

            if obj is not None and obj.get("success"):
                data = obj.get("data") or {}
                busy = data.get("busy")
                keys = sorted(data.keys())
                print(f"  {d.strftime('%d.%m')} n={nights}: busy={busy} | "
                      f"поля data: {keys}")
            else:
                errors = obj.get("errors") if obj else resp["text"][:120]
                print(f"  {d.strftime('%d.%m')} n={nights}: ОШИБКА {errors}")

            await asyncio.sleep(REQUEST_PAUSE)

        storage["parts"]["A_full_dumps"] = part_a

        # ── Блок B: checkBookingAbility ──
        print(f"\n[B] checkBookingAbility ({len(BODY_VARIANTS)} вариантов тела):")
        part_b: list[dict] = []

        for name, body, note in BODY_VARIANTS:
            resp = await _call(CHECK_ABILITY_URL, "POST", body)
            entry = {
                "variant": name, "note": note, "body": body,
                "http_status": resp["status"],
                "response": resp["json"] if resp["json"] else resp["text"][:500],
            }
            part_b.append(entry)
            marker = "OK!" if resp["status"] == 200 else "    "
            print(f"  [{marker}] {name}: HTTP {resp['status']} → "
                  f"{resp['text'][:160]}")
            await asyncio.sleep(REQUEST_PAUSE)

        storage["parts"]["B_check_ability"] = part_b

        # ── Блок C: calculateBookingPrice ──
        print(f"\n[C] calculateBookingPrice ({len(BODY_VARIANTS)} вариантов тела):")
        part_c: list[dict] = []

        for name, body, note in BODY_VARIANTS:
            resp = await _call(CALC_PRICE_URL, "POST", body)
            entry = {
                "variant": name, "note": note, "body": body,
                "http_status": resp["status"],
                "response": resp["json"] if resp["json"] else resp["text"][:500],
            }
            part_c.append(entry)
            marker = "OK!" if resp["status"] == 200 else "    "
            print(f"  [{marker}] {name}: HTTP {resp['status']} → "
                  f"{resp['text'][:160]}")
            await asyncio.sleep(REQUEST_PAUSE)

        storage["parts"]["C_calc_price"] = part_c

        # ── Блок D: гипотетические эндпоинты ──
        print(f"\n[D] Перебор гипотетических эндпоинтов "
              f"({len(PART_D_ENDPOINTS)} шт.):")
        part_d: list[dict] = []

        post_body_probe = {
            "objects": [OBJECT_ID], "object_id": OBJECT_ID,
            "objectId": OBJECT_ID, "guests": GUESTS,
            "date_begin": D1, "date_end": D1_END,
            "currency_id": 1,
        }
        get_query = (
            f"https://sutochno.ru/api/json/objects/x?"
            f"object_id={OBJECT_ID}"
        ).replace("/objects/x?", "/{path}?")

        for path, method in PART_D_ENDPOINTS:
            url = f"https://sutochno.ru/api/json/{path}"
            if method == "GET":
                url = f"{url}?object_id={OBJECT_ID}&id={OBJECT_ID}"
                body = None
            else:
                body = post_body_probe

            resp = await _call(url, method, body)
            interesting = resp["status"] not in (404, 405)
            entry = {
                "path": path, "method": method,
                "http_status": resp["status"],
                "response": resp["json"] if resp["json"] else resp["text"][:500],
            }
            part_d.append(entry)
            marker = "???" if interesting else "   "
            print(f"  [{marker}] {method} {path}: HTTP {resp['status']} → "
                  f"{resp['text'][:120]}")
            await asyncio.sleep(REQUEST_PAUSE)

        storage["parts"]["D_endpoint_scan"] = part_d

        await browser.close()

    # ── Сводка ──
    print("\n" + "=" * 72)
    print("НА ЧТО СМОТРЕТЬ ПРИ АНАЛИЗЕ:")
    print("  A. Сравнить поля data[] между категориями дней (8/11/16/21/26):")
    print("     любое поле, различающееся между «продан» и «свободен с")
    print("     ограничением» — это и есть искомый признак.")
    print("     Особо: rooms_available, is_booking_now, bonus, service_fee,")
    print("     состав detail[] (есть ли цены на подсегменты окна).")
    print("  B/C. Вариант тела с HTTP 200 — правильный формат; смотреть")
    print("     restrictions/warnings в ответе для дат 8 и 21.")
    print("  D. Статус отличный от 404/405 — возможно, живой эндпоинт.")
    print("=" * 72)

    try:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(
            json.dumps(storage, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nПолные ответы сохранены: {OUT_JSON}")
        print("Перешлите файл и вывод консоли для анализа.")
    except Exception as e:
        print(f"\nПредупреждение: не удалось сохранить JSON ({e})")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
