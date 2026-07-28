"""Диагностика сбора данных для одного объекта.

Загружает страницу поиска, перехватывает токен API, выполняет:
1. Bulk-запрос на 60 ночей — показывает сырой detail[] и результат PriceParser.
2. Скользящее окно (60 дней) — показывает busy/unbusy для каждого дня.
   При ошибке min_nights — повторяет запрос с требуемым количеством ночей.
3. Итоговый календарь и цены (как в основной программе).

Запуск:
    python -m scripts.test_single_object
"""

import asyncio
import json
import re
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
TEST_OBJECT_ID: int = 265366

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


def _extract_min_nights_from_error(error_text: str) -> int | None:
    """Извлекает требуемое min_nights из текста ошибки API.

    Пример: 'Минимальное количество суток - 8.' → 8

    Args:
        error_text: Текст ошибки из API.

    Returns:
        Число ночей или None если не удалось извлечь.
    """
    numbers = re.findall(r"(\d+)", error_text)
    for num_str in numbers:
        num = int(num_str)
        if 2 <= num <= 999:
            return num
    return None


async def _fetch_day(
    page,
    token: str,
    object_id: int,
    day: date,
    nights: int,
    guests: int = DEFAULT_GUESTS,
) -> dict:
    """Выполняет запрос API для одного дня с указанным количеством ночей.

    Args:
        page: Страница Playwright.
        token: Токен API.
        object_id: ID объекта.
        day: Дата начала.
        nights: Количество ночей.
        guests: Количество гостей.

    Returns:
        Словарь с результатом: {status, busy, price, error, raw_errors, required_nights}.
    """
    end_day = day + timedelta(days=nights)
    date_begin = f"{day.isoformat()} 14:00:00"
    date_end = f"{end_day.isoformat()} 11:00:00"

    result = await page.evaluate(
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
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "token": token,
            "guests": guests,
        },
    )

    # Парсим результат
    if not result.get("success"):
        return {
            "status": "error",
            "busy": None,
            "price": 0,
            "error": result.get("error", "unknown"),
            "raw_errors": [],
            "required_nights": None,
        }

    raw = result["raw"]
    if not raw.get("success") or not raw.get("data", {}).get("objects"):
        return {
            "status": "error",
            "busy": None,
            "price": 0,
            "error": "no_data",
            "raw_errors": [],
            "required_nights": None,
        }

    obj = raw["data"]["objects"][0]

    if obj.get("success"):
        o_data = obj["data"]
        busy_val = o_data.get("busy")
        price = 0

        if busy_val == "unbusy":
            det = o_data.get("detail", [])
            for d in det:
                if d.get("type") == "season_price" and d.get("cost"):
                    price = int(round(d["cost"]))
                    break
                if d.get("type") == 1 and d.get("cost"):
                    price = int(round(d["cost"]))

        return {
            "status": "ok",
            "busy": busy_val,
            "price": price,
            "error": "",
            "raw_errors": [],
            "required_nights": None,
        }

    # Ошибка объекта — извлекаем min_nights
    errors = obj.get("errors", [])
    error_text = str(errors).lower()

    min_nights_keywords = ["min_nights", "минимальн", "суток", "сут."]
    is_min_nights = any(kw in error_text for kw in min_nights_keywords)

    required_nights = None
    if is_min_nights:
        required_nights = _extract_min_nights_from_error(str(errors))

    return {
        "status": "min_nights" if is_min_nights else "obj_error",
        "busy": None,
        "price": 0,
        "error": str(errors)[:150],
        "raw_errors": errors,
        "required_nights": required_nights,
    }


async def main() -> None:
    """Основная функция диагностики с адаптивным min_nights."""
    settings = Settings()
    search_url = (
        settings.search_urls[0] if settings.search_urls else _DEFAULT_SEARCH_URL
    )
    today = date.today()

    print(f"\n{'═' * 70}")
    print(f"  Диагностика объекта ID={TEST_OBJECT_ID}")
    print(f"  Дата начала календаря: {today}")
    print(f"  Скользящее окно: {SLIDING_WINDOW_NIGHTS} ночи (с адаптацией)")
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
                t = (
                    request.headers.get("token")
                    or request.headers.get("Token")
                )
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
        print(
            f"\n[2/5] Bulk-запрос на 60 ночей (объект {TEST_OBJECT_ID})..."
        )

        date_begin = f"{today.isoformat()} 14:00:00"
        date_end = (
            f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"
        )

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
                print(
                    f"  obj.data.rooms_available = "
                    f"{obj_data.get('rooms_available')}"
                )
                print(
                    f"  obj.data.price_default = "
                    f"{obj_data.get('price_default')}"
                )
            else:
                print(f"  obj.errors = {obj_response.get('errors')}")
                await browser.stop()
                sys.exit(1)
        else:
            print("  ❌ Нет data.objects в ответе")
            print(
                f"  raw_data = "
                f"{json.dumps(raw_data, ensure_ascii=False, indent=2)[:2000]}"
            )
            await browser.stop()
            sys.exit(1)

        # Выводим detail[]
        detail = obj_data.get("detail", [])
        print(f"\n  detail[] ({len(detail)} записей):")
        print(f"  {'─' * 60}")

        for i, d in enumerate(detail):
            d_type = d.get("type")
            d_cost = d.get("cost")
            d_begin = (
                d.get("date_begin", "")[:10]
                if d.get("date_begin")
                else "null"
            )
            d_end = (
                d.get("date_end", "")[:10] if d.get("date_end") else "null"
            )
            d_nights = d.get("nights")
            print(
                f"  [{i:2d}] type={d_type!r:16s} "
                f"cost={d_cost!s:>8s}  "
                f"period={d_begin} → {d_end}  "
                f"nights={d_nights}"
            )

        # ── 3. Парсинг цен через PriceParser ──
        print("\n[3/5] Парсинг цен через PriceParser...")

        price_parser = PriceParser()
        prices_60 = price_parser.extract_prices_from_detail(
            detail, today=today
        )

        print(f"  Цены (первые 10): {prices_60[:10]}")
        print(f"  Цены (последние 10): {prices_60[-10:]}")
        print(f"  Нулевых цен: {sum(1 for p in prices_60 if p == 0)}")

        # ── 4. Скользящее окно с адаптивным min_nights ──
        busy_status = obj_data.get("busy")

        if busy_status == "busy":
            print(
                f"\n[4/5] Скользящее окно с адаптивным min_nights "
                f"(60 дней)..."
            )
            print(
                f"  Объект busy — определяем занятость каждого дня..."
            )
            print(
                f"  При ошибке min_nights — повторяем с требуемым nights"
            )
            print(f"  {'─' * 60}")

            calendar: list[int] = []

            for day_offset in range(DAYS_COUNT):
                day = today + timedelta(days=day_offset)

                # Первая попытка с базовым nights
                res = await _fetch_day(
                    page, token, TEST_OBJECT_ID, day,
                    SLIDING_WINDOW_NIGHTS,
                )

                # Небольшая пауза
                if (day_offset + 1) % 5 == 0:
                    await asyncio.sleep(0.3)

                actual_nights_used = SLIDING_WINDOW_NIGHTS
                retry_info = ""

                # Если min_nights — повторяем с требуемым количеством
                if (
                    res["status"] == "min_nights"
                    and res["required_nights"]
                ):
                    required = res["required_nights"]
                    remaining_days = DAYS_COUNT - day_offset

                    # Проверяем, хватит ли дней до конца окна
                    if required <= remaining_days:
                        retry_info = f" -> retry nights={required}"
                        res = await _fetch_day(
                            page, token, TEST_OBJECT_ID, day, required,
                        )
                        actual_nights_used = required
                        await asyncio.sleep(0.3)
                    else:
                        # Не хватает дней — пробуем максимально возможное
                        retry_info = (
                            f" -> retry nights={remaining_days} "
                            f"(не хватает для {required})"
                        )
                        if remaining_days >= 2:
                            res = await _fetch_day(
                                page, token, TEST_OBJECT_ID, day,
                                remaining_days,
                            )
                            actual_nights_used = remaining_days
                            await asyncio.sleep(0.3)

                # Определяем финальный статус дня
                if res["status"] == "ok":
                    if res["busy"] == "busy":
                        calendar.append(1)
                        marker = "🔴 ЗАНЯТ "
                    else:
                        calendar.append(0)
                        marker = "🟢 свободен"
                elif res["status"] == "min_nights":
                    # После retry всё ещё min_nights
                    calendar.append(0)
                    marker = "🟡 min_nights (свободен?)"
                else:
                    calendar.append(-1)
                    marker = "⚠️  ОШИБКА "

                # Формируем строку вывода
                error_part = ""
                if res["error"]:
                    error_part = f" | {res['error'][:80]}"

                print(
                    f"  день {day_offset:2d} ({day}): "
                    f"{marker} | "
                    f"nights={actual_nights_used}"
                    f"{retry_info}"
                    f"{error_part}"
                )

            # ── Применяем календарь к ценам ──
            final_prices: list[int] = []
            for i in range(DAYS_COUNT):
                if calendar[i] == 1:
                    final_prices.append(0)
                else:
                    final_prices.append(
                        prices_60[i] if i < len(prices_60) else 0
                    )

            # Нормализуем ошибки
            final_calendar = [0 if c == -1 else c for c in calendar]

        elif busy_status == "unbusy":
            print("\n[4/5] Объект unbusy — весь календарь свободен.")
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
        occupancy = (
            round(busy_days / DAYS_COUNT * 100, 1)
            if DAYS_COUNT > 0
            else 0
        )

        print(f"\n[5/5] Итог:")
        print(f"{'═' * 70}")
        print(
            f"  Занятость: {occupancy}% "
            f"({busy_days} занято, {free_days} свободно, "
            f"{error_days} ошибок)"
        )
        print(
            f"  Календарь: "
            f"{''.join(str(c) for c in final_calendar)}"
        )
        print(
            f"  Цены:      "
            f"{';'.join(str(p) for p in final_prices)}"
        )

        # Визуальная разметка
        print(
            f"\n  Визуальная карта "
            f"(🔴=занят, 🟢=свободен, 🟡=min_nights):"
        )
        line = ""
        for i, c in enumerate(final_calendar):
            if i % 7 == 0 and i > 0:
                print(f"  {line}")
                line = ""
            day = today + timedelta(days=i)
            if c == 1:
                sym = "🔴"
            else:
                sym = "🟢"
            line += f"{sym}{day.day:02d} "
        if line:
            print(f"  {line}")

        # Сравнение
        print(f"\n{'─' * 70}")
        print("  СРАВНЕНИЕ:")
        print(
            "  Старый результат (без адаптации):  "
            "0% занято (все дни свободны)"
        )
        print(
            f"  Новый результат (с адаптацией):    "
            f"{occupancy}% занято"
        )
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
