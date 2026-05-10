"""Тестовый скрипт v2: проверка API с учётом min_nights=2.

Проблема: объявление 1629188 требует минимум 2 ночи.
При запросе на 1 ночь API возвращает ошибку.

Решение: запрашиваем интервал в 2 ночи, но извлекаем цену
за конкретный день из detail[].cost (разбивка по сезонам).

Скрипт тестирует три стратегии:
1. Запрос на 2 ночи (date_end = день + 2)
2. Запрос на 3 ночи (для объектов с min_nights=3)
3. Один запрос на весь диапазон 60 дней (date_end = день + 60)

Запуск:
    python scripts/test_api_prices_v2.py
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path

from playwright.async_api import Request, async_playwright


# === Конфигурация ===
LISTING_ID: int = 1629188
LISTING_URL: str = (
    "https://sutochno.ru/front/searchapp/detail/1629188"
    "?guests_adults=2"
    "&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3"
    "&id=397367&type=city"
    "&SW.lat=59.74409827797147&SW.lng=30.028533683105454"
    "&NE.lat=60.090924462880835&NE.lng=30.58128331689453"
    "&price_per=1&is_studio=1&is_apartment=1"
)
API_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
GUESTS: int = 2
DAYS_TO_CHECK: int = 14

DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "test_api_prices_v2_report.json"


async def fetch_api(
    page,
    listing_id: int,
    date_begin: str,
    date_end: str,
    token: str,
    guests: int,
) -> dict:
    """Выполняет один запрос к API через контекст браузера."""
    return await page.evaluate(
        """
        async ({apiUrl, listingId, dateBegin, dateEnd, token, guests}) => {
            try {
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
                        objects: [listingId],
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
                    })
                });
                const data = await resp.json();
                return {status: resp.status, body: data, error: null};
            } catch (e) {
                return {status: 0, body: null, error: e.message};
            }
        }
        """,
        {
            "apiUrl": API_URL,
            "listingId": listing_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "token": token,
            "guests": guests,
        },
    )


async def main() -> None:
    """Основная логика тестового скрипта v2."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    captured_token: list[str] = []

    print(f"\n{'═' * 70}")
    print(f"  ТЕСТ API v2: стратегия с min_nights=2")
    print(f"  Объявление: {LISTING_ID}")
    print(f"  Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 70}\n")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
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
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()

        # ─── Перехват токена ───
        def on_request(request: Request) -> None:
            if "/api/json/" in request.url:
                token_value = request.headers.get("token", "")
                if token_value and token_value not in captured_token:
                    captured_token.append(token_value)
                    print(f"  ✓ Токен перехвачен: {token_value[:30]}...")

        page.on("request", on_request)

        print("[1/5] Загружаем страницу карточки...")
        await page.goto(LISTING_URL, wait_until="domcontentloaded")

        print("[2/5] Ждём перехвата токена (15 сек)...")
        await asyncio.sleep(15)

        if not captured_token:
            print("  Токен не перехвачен, провоцируем запросы...")
            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(3)
            checkin_block = await page.query_selector(".sc-detail-dates__item_in")
            if checkin_block:
                await checkin_block.click()
                await asyncio.sleep(5)

        if not captured_token:
            token_from_storage = await page.evaluate("""
                () => {
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        if (key && key.toLowerCase().includes('token'))
                            return localStorage.getItem(key);
                    }
                    return null;
                }
            """)
            if token_from_storage:
                captured_token.append(token_from_storage)

        if not captured_token:
            print("\n  ✗ Токен не найден! Завершение.")
            await browser.close()
            return

        token = captured_token[0]
        today = datetime.now().date()
        all_results: dict = {}

        # ═══════════════════════════════════════════════════════════════
        # СТРАТЕГИЯ 1: Запрос на 2 ночи (день + 2 дня)
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  СТРАТЕГИЯ 1: Запрос на 2 ночи (min_nights=2)")
        print(f"{'─' * 70}\n")

        strategy1_results: list[dict] = []

        for day_offset in range(DAYS_TO_CHECK):
            check_date = today + timedelta(days=day_offset + 1)
            end_date = check_date + timedelta(days=2)  # +2 ночи вместо +1

            date_begin = f"{check_date.strftime('%Y-%m-%d')} 14:00:00"
            date_end = f"{end_date.strftime('%Y-%m-%d')} 11:00:00"

            response = await fetch_api(page, LISTING_ID, date_begin, date_end, token, GUESTS)

            body = response.get("body", {})
            result_entry: dict = {
                "date": str(check_date),
                "date_begin": date_begin,
                "date_end": date_end,
                "nights_requested": 2,
            }

            if response.get("error"):
                result_entry["error"] = response["error"]
                print(f"  {check_date}: ОШИБКА {response['error']}")
            elif body.get("success") and body.get("data", {}).get("objects"):
                obj = body["data"]["objects"][0]
                if obj.get("success"):
                    d = obj["data"]
                    result_entry.update({
                        "busy": d.get("busy"),
                        "price": d.get("price"),
                        "rooms_available": d.get("rooms_available"),
                        "is_booking_now": d.get("is_booking_now"),
                        "detail": d.get("detail", []),
                    })

                    # Извлекаем цены из detail
                    detail = d.get("detail", [])
                    costs_info = []
                    for det in detail:
                        costs_info.append(
                            f"cost={det.get('cost')} "
                            f"({det.get('date_begin','?')[:10]}→"
                            f"{det.get('date_end','?')[:10]}, "
                            f"{det.get('nights')}н, {det.get('type')})"
                        )

                    print(f"  {check_date}: busy={d.get('busy'):<7} "
                          f"price={d.get('price'):<6} "
                          f"rooms={d.get('rooms_available')} "
                          f"detail: {'; '.join(costs_info)}")
                else:
                    errors = obj.get("errors", [])
                    result_entry["errors"] = errors
                    print(f"  {check_date}: ОШИБКА — {errors}")
            else:
                result_entry["api_error"] = body
                print(f"  {check_date}: API ОШИБКА")

            strategy1_results.append(result_entry)
            await asyncio.sleep(0.8)

        all_results["strategy_1_two_nights"] = strategy1_results

        # ═══════════════════════════════════════════════════════════════
        # СТРАТЕГИЯ 2: Один большой запрос на 60 дней
        # Проверяем, вернёт ли API разбивку detail[] по всем дням
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  СТРАТЕГИЯ 2: Один запрос на 60 ночей")
        print(f"{'─' * 70}\n")

        start_date = today + timedelta(days=1)
        end_date_60 = start_date + timedelta(days=60)

        date_begin_60 = f"{start_date.strftime('%Y-%m-%d')} 14:00:00"
        date_end_60 = f"{end_date_60.strftime('%Y-%m-%d')} 11:00:00"

        print(f"  Запрос: {date_begin_60} → {date_end_60} (60 ночей)")

        response_60 = await fetch_api(
            page, LISTING_ID, date_begin_60, date_end_60, token, GUESTS
        )

        body_60 = response_60.get("body", {})
        strategy2_result: dict = {
            "date_begin": date_begin_60,
            "date_end": date_end_60,
            "nights_requested": 60,
        }

        if response_60.get("error"):
            strategy2_result["error"] = response_60["error"]
            print(f"  ОШИБКА: {response_60['error']}")
        elif body_60.get("success") and body_60.get("data", {}).get("objects"):
            obj_60 = body_60["data"]["objects"][0]
            if obj_60.get("success"):
                d60 = obj_60["data"]
                detail_60 = d60.get("detail", [])

                strategy2_result.update({
                    "busy": d60.get("busy"),
                    "price_total": d60.get("price"),
                    "rooms_available": d60.get("rooms_available"),
                    "detail_count": len(detail_60),
                    "detail": detail_60,
                })

                print(f"  busy={d60.get('busy')} | total_price={d60.get('price')} | "
                      f"rooms={d60.get('rooms_available')}")
                print(f"  detail содержит {len(detail_60)} записей:")
                print()

                # Таблица detail
                total_nights_in_detail = 0
                for i, det in enumerate(detail_60):
                    nights = det.get("nights", 0)
                    total_nights_in_detail += nights
                    print(f"    [{i:2}] {det.get('date_begin','?')[:10]} → "
                          f"{det.get('date_end','?')[:10]} | "
                          f"cost={det.get('cost'):<6} | "
                          f"nights={nights} | "
                          f"type={det.get('type')}")

                print(f"\n  Итого ночей в detail: {total_nights_in_detail}")
                print(f"  Средняя цена за ночь: "
                      f"{d60.get('price', 0) / 60 if d60.get('price') else '?':.0f} руб.")

            else:
                errors = obj_60.get("errors", [])
                strategy2_result["errors"] = errors
                print(f"  ОШИБКА от объекта: {errors}")
                print(f"  Полный ответ: {json.dumps(obj_60, ensure_ascii=False, indent=2)[:2000]}")
        else:
            strategy2_result["api_error"] = body_60
            print(f"  API ОШИБКА: {json.dumps(body_60, ensure_ascii=False)[:500]}")

        all_results["strategy_2_sixty_nights"] = strategy2_result

        # ═══════════════════════════════════════════════════════════════
        # СТРАТЕГИЯ 3: Скользящее окно в 2 ночи с шагом 1 день
        # Берём цену первого дня из detail[0].cost
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  СТРАТЕГИЯ 3: Скользящее окно 2 ночи, извлекаем цену 1-го дня")
        print(f"{'─' * 70}\n")

        strategy3_calendar: list[str] = []
        strategy3_prices: list[int] = []
        strategy3_details: list[dict] = []

        for day_offset in range(DAYS_TO_CHECK):
            check_date = today + timedelta(days=day_offset + 1)
            end_date = check_date + timedelta(days=2)

            date_begin = f"{check_date.strftime('%Y-%m-%d')} 14:00:00"
            date_end = f"{end_date.strftime('%Y-%m-%d')} 11:00:00"

            response = await fetch_api(page, LISTING_ID, date_begin, date_end, token, GUESTS)
            body = response.get("body", {})

            if body.get("success") and body.get("data", {}).get("objects"):
                obj = body["data"]["objects"][0]
                if obj.get("success"):
                    d = obj["data"]
                    busy = d.get("busy")
                    detail = d.get("detail", [])

                    if busy == "busy":
                        strategy3_calendar.append("1")
                        strategy3_prices.append(0)
                    else:
                        strategy3_calendar.append("0")
                        # Цена первого дня — detail[0].cost
                        first_day_cost = detail[0].get("cost", 0) if detail else 0
                        strategy3_prices.append(first_day_cost)

                    strategy3_details.append({
                        "date": str(check_date),
                        "busy": busy,
                        "first_day_cost": detail[0].get("cost") if detail else None,
                        "detail_count": len(detail),
                    })
                else:
                    # Ошибка min_nights и т.д. — но success=false НЕ значит "занят"!
                    errors = obj.get("errors", [])
                    # Если ошибка про min_nights — это НЕ занятость
                    strategy3_calendar.append("?")
                    strategy3_prices.append(-1)
                    strategy3_details.append({
                        "date": str(check_date),
                        "errors": errors,
                        "note": "success=false, НЕ означает занятость!",
                    })
            else:
                strategy3_calendar.append("?")
                strategy3_prices.append(-1)

            await asyncio.sleep(0.8)

        all_results["strategy_3_sliding_window"] = {
            "calendar": "".join(strategy3_calendar),
            "prices": ";".join(str(p) for p in strategy3_prices),
            "details": strategy3_details,
        }

        print(f"  Календарь: {''.join(strategy3_calendar)}")
        print(f"  Цены:      {';'.join(str(p) for p in strategy3_prices)}")
        print()
        print(f"  Легенда: 0=свободен, 1=занят, ?=ошибка API (не занятость!)")
        print(f"  Цены:    >0=цена за ночь, 0=занят, -1=ошибка API")

        await browser.close()

    # ─── Итоговая диагностика ───
    print(f"\n{'═' * 70}")
    print(f"  ИТОГОВАЯ ДИАГНОСТИКА")
    print(f"{'═' * 70}")

    # Анализ стратегии 1
    s1 = all_results.get("strategy_1_two_nights", [])
    s1_success = [r for r in s1 if "busy" in r]
    s1_errors = [r for r in s1 if "errors" in r]

    print(f"\n  Стратегия 1 (2 ночи):")
    print(f"    Успешных ответов: {len(s1_success)}/{len(s1)}")
    print(f"    Ошибок min_nights: {len(s1_errors)}")
    if s1_errors:
        print(f"    ⚠ Ошибки: {s1_errors[0].get('errors')}")
        print(f"    → Возможно, min_nights у этого объекта > 2?")

    # Анализ стратегии 2
    s2 = all_results.get("strategy_2_sixty_nights", {})
    if s2.get("detail_count"):
        print(f"\n  Стратегия 2 (60 ночей одним запросом):")
        print(f"    ✓ Работает! detail содержит {s2['detail_count']} записей")
        print(f"    Это ЛУЧШАЯ стратегия: один запрос вместо 60")
        print(f"    Из detail[] можно извлечь цену за каждый день по диапазонам дат")
    elif s2.get("errors"):
        print(f"\n  Стратегия 2 (60 ночей): ОШИБКА — {s2['errors']}")

    # Рекомендация
    print(f"\n{'─' * 70}")
    print(f"  РЕКОМЕНДАЦИЯ ДЛЯ ИСПРАВЛЕНИЯ listing_service.py:")
    print(f"{'─' * 70}")
    print(f"""
  Текущая логика программы (НЕВЕРНАЯ):
    - Запрашивает каждый день отдельно с интервалом 1 ночь
    - Если success=false → считает день "занятым"
    - Это НЕПРАВИЛЬНО: success=false может означать ошибку min_nights

  Варианты исправления:

  ВАРИАНТ A (рекомендуемый): Один запрос на весь диапазон 60 дней.
    - date_begin = завтра 14:00
    - date_end = (завтра + 60 дней) 11:00
    - Из detail[] разбираем цены по диапазонам дат
    - busy/unbusy определяет занятость всего периода
    - Для определения занятости конкретных дней нужен отдельный подход

  ВАРИАНТ B: Скользящее окно с min_nights объекта.
    - Сначала определяем min_nights объекта (из __NUXT__ или ошибки API)
    - Запрашиваем с интервалом = min_nights
    - Берём detail[0].cost как цену первого дня
    - Но: если часть дней в окне занята — ответ будет busy для всего окна

  ВАРИАНТ C (самый надёжный): Комбинированный.
    - Один запрос на 60 дней → получаем цены из detail[]
    - Отдельные запросы по 1 дню с no_time=0 → определяем занятость
    - Если ошибка min_nights → пробуем увеличить окно
    """)

    # Сохранение
    REPORT_PATH.write_text(
        json.dumps(all_results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n  Полный отчёт: {REPORT_PATH.absolute()}")
    print(f"{'═' * 70}\n")


if __name__ == "__main__":
    asyncio.run(main())
