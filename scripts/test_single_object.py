"""Диагностика сбора данных для одного объекта.

Загружает страницу поиска, перехватывает токен API, выполняет:
1. Bulk-запрос на 60 ночей — показывает сырой detail[] и результат PriceParser.
2. Скользящее окно (60 дней) — показывает busy/unbusy для каждого дня.
3. Итоговый календарь и цены (как в основной программе).

Запуск:
    python -m scripts.test_single_object
"""

import asyncio
import json
import sys
from datetime import date, timedelta

from src.config.logger import configure, get_logger
from src.config.settings import Settings
from src.services.browser_service import BrowserService
from src.services.listing.constants import (
    API_PRICES_URL,
    DAYS_COUNT,
    DEFAULT_GUESTS,
)
from src.services.listing.price_parser import PriceParser

# ── Настройки ──────────────────────────────────────────────

# ID объекта для диагностики
TEST_OBJECT_ID: int = 1410491

# Количество ночей в скользящем окне (как в основной программе)
SLIDING_WINDOW_NIGHTS: int = 2

# URL страницы поиска (для перехвата токена)
_DEFAULT_SEARCH_URL: str = (
    "https://sutochno.ru/front/searchapp/search"
    "?type=city&id=397367&term=Санкт-Петербург"
    "&price_per=1&guests_adults=2"
)

# ── Логгер ─────────────────────────────────────────────────

configure(log_level="DEBUG", log_file_path="logs/test_single_object.log")
logger = get_logger("test_single_object")


async def main() -> None:
    """Основная функция диагностики."""
    settings = Settings()
    search_url = settings.search_urls[0] if settings.search_urls else _DEFAULT_SEARCH_URL
    today = date.today()

    print(f"\n{'═' * 70}")
    print(f"  Диагностика объекта ID={TEST_OBJECT_ID}")
    print(f"  Дата начала календаря: {today}")
    print(f"  Скользящее окно: {SLIDING_WINDOW_NIGHTS} ночи")
    print(f"{'═' * 70}\n")

    # ── 1. Запуск браузера и перехват токена ──
    print("[1/5] Запуск браузера и перехват токена...")

    browser = BrowserService(settings=settings)
    token: str | None = None

    try:
        await browser.start()
        page = browser.page

        # Перехватчик токена
        captured_token: list[str] = []

        async def _intercept(route, request):
            if "sutochno.ru/api/json" in request.url and not captured_token:
                t = request.headers.get("token") or request.headers.get("Token")
                if t:
                    captured_token.append(t)
            try:
                await route.continue_()
            except Exception:
                pass

        await page.route("**/api/json/**", _intercept)
        await page.goto(search_url, wait_until="domcontentloaded")

        # Ожидание перехвата
        for _ in range(40):  # 20 секунд
            if captured_token:
                break
            await asyncio.sleep(0.5)

        await page.unroute("**/api/json/**")
        await asyncio.sleep(2)

        if not captured_token:
            print("  ❌ Токен не перехвачен! Проверьте URL поиска.")
            await browser.stop()
            sys.exit(1)

        token = captured_token[0]
        print(f"  ✓ Токен получен (длина: {len(token)})")

        # ── 2. Bulk-запрос на 60 ночей ──
        print(f"\n[2/5] Bulk-запрос на 60 ночей (объект {TEST_OBJECT_ID})...")

        date_begin = f"{today.isoformat()} 14:00:00"
        date_end = f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"

        bulk_result = await page.evaluate(
            """
            async ({apiUrl, objectId, dateBegin, dateEnd, token, guests}) => {
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
                            objects: [objectId],
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
                        }),
                        credentials: 'include'
                    });

                    if (!resp.ok) {
                        return {success: false, error: 'http_' + resp.status, status: resp.status};
                    }

                    const data = await resp.json();
                    return {success: true, raw: data};

                } catch (e) {
                    return {success: false, error: e.message};
                }
            }
            """,
            {
                "apiUrl": API_PRICES_URL,
                "objectId": TEST_OBJECT_ID,
                "dateBegin": date_begin,
                "dateEnd": date_end,
                "token": token,
                "guests": DEFAULT_GUESTS,
            },
        )

        if not bulk_result.get("success"):
            print(f"  ❌ Ошибка bulk-запроса: {bulk_result.get('error')}")
            await browser.stop()
            sys.exit(1)

        raw_data = bulk_result["raw"]
        print(f"  ✓ Ответ получен. success={raw_data.get('success')}")

        # Разбираем ответ
        obj_data = None
        if raw_data.get("data") and raw_data["data"].get("objects"):
            obj_response = raw_data["data"]["objects"][0]
            print(f"  obj.success = {obj_response.get('success')}")
            print(f"  obj.id = {obj_response.get('id')}")

            if obj_response.get("success"):
                obj_data = obj_response["data"]
                print(f"  obj.data.busy = {obj_data.get('busy')}")
                print(f"  obj.data.price = {obj_data.get('price')}")
                print(f"  obj.data.rooms_available = {obj_data.get('rooms_available')}")
                print(f"  obj.data.price_default = {obj_data.get('price_default')}")
            else:
                print(f"  obj.errors = {obj_response.get('errors')}")
                await browser.stop()
                sys.exit(1)
        else:
            print(f"  ❌ Нет data.objects в ответе")
            print(f"  raw_data = {json.dumps(raw_data, ensure_ascii=False, indent=2)[:2000]}")
            await browser.stop()
            sys.exit(1)

        # Выводим detail[]
        detail = obj_data.get("detail", [])
        print(f"\n  detail[] ({len(detail)} записей):")
        print(f"  {'─' * 60}")

        for i, d in enumerate(detail):
            d_type = d.get("type")
            d_cost = d.get("cost")
            d_begin = d.get("date_begin", "")[:10] if d.get("date_begin") else "null"
            d_end = d.get("date_end", "")[:10] if d.get("date_end") else "null"
            d_nights = d.get("nights")
            print(
                f"  [{i:2d}] type={d_type!r:16s} "
                f"cost={d_cost!s:>8s}  "
                f"period={d_begin} → {d_end}  "
                f"nights={d_nights}"
            )

        # ── 3. Парсинг цен через PriceParser ──
        print(f"\n[3/5] Парсинг цен через PriceParser...")

        price_parser = PriceParser()
        prices_60 = price_parser.extract_prices_from_detail(detail, today=today)

        print(f"  Цены (первые 10): {prices_60[:10]}")
        print(f"  Цены (последние 10): {prices_60[-10:]}")
        print(f"  Нулевых цен: {sum(1 for p in prices_60 if p == 0)}")

        # ── 4. Скользящее окно (полное, 60 дней) ──
        busy_status = obj_data.get("busy")

        if busy_status == "busy":
            print(f"\n[4/5] Скользящее окно (60 дней, {SLIDING_WINDOW_NIGHTS} ночи)...")
            print(f"  Объект busy — определяем занятость каждого дня...")
            print(f"  {'─' * 60}")

            calendar: list[int] = []

            for day_offset in range(DAYS_COUNT):
                day = today + timedelta(days=day_offset)
                end_d = day + timedelta(days=SLIDING_WINDOW_NIGHTS)
                d_begin = f"{day.isoformat()} 14:00:00"
                d_end = f"{end_d.isoformat()} 11:00:00"

                day_result = await page.evaluate(
                    """
                    async ({apiUrl, objectId, dateBegin, dateEnd, token, guests}) => {
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
                                    objects: [objectId],
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
                                }),
                                credentials: 'include'
                            });

                            if (!resp.ok) {
                                return {success: false, error: 'http_' + resp.status};
                            }

                            const data = await resp.json();
                            return {success: true, raw: data};

                        } catch (e) {
                            return {success: false, error: e.message};
                        }
                    }
                    """,
                    {
                        "apiUrl": API_PRICES_URL,
                        "objectId": TEST_OBJECT_ID,
                        "dateBegin": d_begin,
                        "dateEnd": d_end,
                        "token": token,
                        "guests": DEFAULT_GUESTS,
                    },
                )

                # Небольшая пауза между запросами
                if (day_offset + 1) % 5 == 0:
                    await asyncio.sleep(0.5)

                # Парсим результат
                day_busy: int = -1  # -1 = ошибка
                day_price: int = 0
                error_info: str = ""

                if not day_result.get("success"):
                    error_info = day_result.get("error", "unknown")
                else:
                    raw = day_result["raw"]
                    if (
                        raw.get("success")
                        and raw.get("data", {}).get("objects")
                    ):
                        obj = raw["data"]["objects"][0]
                        if obj.get("success"):
                            o_data = obj["data"]
                            busy_val = o_data.get("busy")
                            if busy_val == "busy":
                                day_busy = 1
                            elif busy_val == "unbusy":
                                day_busy = 0
                                # Извлекаем цену
                                det = o_data.get("detail", [])
                                for d in det:
                                    if d.get("type") == "season_price" and d.get("cost"):
                                        day_price = int(round(d["cost"]))
                                        break
                                    if d.get("type") == 1 and d.get("cost"):
                                        day_price = int(round(d["cost"]))
                        else:
                            errors = obj.get("errors", [])
                            error_info = str(errors)[:100]
                            # Проверяем min_nights ошибку
                            error_lower = error_info.lower()
                            if any(
                                kw in error_lower
                                for kw in ["min_nights", "минимальн", "суток"]
                            ):
                                day_busy = 0  # min_nights = свободен
                                error_info = f"min_nights ({error_info})"
                    else:
                        error_info = "no_data"

                calendar.append(day_busy)

                # Выводим каждый день
                if day_busy == 1:
                    marker = "🔴 ЗАНЯТ "
                elif day_busy == 0:
                    marker = "🟢 свободен"
                else:
                    marker = "⚠️  ОШИБКА "

                print(
                    f"  день {day_offset:2d} ({day}): "
                    f"{marker} | "
                    f"цена={day_price:>6d}"
                    f"{f' | {error_info}' if error_info else ''}"
                )

            # ── Применяем календарь к ценам (как основная программа) ──
            # Логика из _apply_calendars: если calendar[i] == 1, то price[i] = 0
            final_prices: list[int] = []
            for i in range(DAYS_COUNT):
                if calendar[i] == 1:
                    final_prices.append(0)
                elif calendar[i] == -1:
                    # Ошибка — нормализуем в 0 (свободен), цена из bulk
                    final_prices.append(prices_60[i] if i < len(prices_60) else 0)
                else:
                    final_prices.append(prices_60[i] if i < len(prices_60) else 0)

            # Нормализуем ошибки в календаре
            final_calendar = [0 if c == -1 else c for c in calendar]

        elif busy_status == "unbusy":
            print(f"\n[4/5] Объект unbusy — весь календарь свободен.")
            final_calendar = [0] * DAYS_COUNT
            final_prices = prices_60
            calendar = [0] * DAYS_COUNT
        else:
            print(f"\n[4/5] Неизвестный busy-статус: {busy_status}")
            final_calendar = [0] * DAYS_COUNT
            final_prices = prices_60
            calendar = [0] * DAYS_COUNT

        # ── 5. Итог ──
        busy_days = sum(1 for c in final_calendar if c == 1)
        free_days = sum(1 for c in final_calendar if c == 0)
        error_days = sum(1 for c in calendar if c == -1)
        zero_price_days = sum(1 for p in final_prices if p == 0)
        occupancy = round(busy_days / DAYS_COUNT * 100, 1) if DAYS_COUNT > 0 else 0

        print(f"\n[5/5] Итог:")
        print(f"{'═' * 70}")
        print(f"  Занятость: {occupancy}% ({busy_days} занято, {free_days} свободно, {error_days} ошибок)")
        print(f"  Календарь: {''.join(str(c) for c in final_calendar)}")
        print(f"  Цены:      {';'.join(str(p) for p in final_prices)}")
        print(f"  Нулевых цен: {zero_price_days} (из них {busy_days} = занятые дни)")

        # Визуальная разметка
        print(f"\n  Визуальная карта (🔴=занят, 🟢=свободен):")
        line = ""
        for i, c in enumerate(final_calendar):
            if i % 7 == 0 and i > 0:
                print(f"  {line}")
                line = ""
            day = today + timedelta(days=i)
            sym = "🔴" if c == 1 else "🟢"
            line += f"{sym}{day.day:02d} "
        if line:
            print(f"  {line}")

        print(f"{'═' * 70}\n")

    except Exception as e:
        print(f"\n❌ Непредвиденная ошибка: {type(e).__name__}: {e}")
        logger.error(
            "тест_ошибка",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise
    finally:
        await browser.stop()


if __name__ == "__main__":
    asyncio.run(main())
