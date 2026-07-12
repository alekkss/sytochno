"""Диагностика obj_success=false для getPricesAndAvailabilities.

Полный анализ причин, по которым API sutochno.ru отклоняет запрос
для конкретного объявления. Проверяет все гипотезы:

1. Что именно содержится в obj.errors при разных параметрах
2. Влияние nights (1..60) на ответ API
3. Влияние guests (1..6) на ответ API
4. Влияние rooms_cnt на ответ API (апарт-отели)
5. Данные из window.__NUXT__ (min_nights, тип, rooms)
6. Поведение API для конкретных дат (ближайшие, через неделю, через месяц)
7. Работает ли однодневный запрос с высоким nights
8. Альтернативные эндпоинты (calculateBookingPrice)

Запуск:
    python scripts/diagnose_obj_error.py

Результат:
    data/diagnose_obj_error_report.json
"""

import asyncio
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.async_api import Page, Route, async_playwright


# ═══════════════════════════════════════════════════════════════════════
# Конфигурация — ИЗМЕНИТЕ ПОД СВОЙ СЛУЧАЙ
# ═══════════════════════════════════════════════════════════════════════

# ID объявления, для которого не работает API
LISTING_ID: str = "938089"

# URL карточки
CARD_URL: str = (
    f"https://sutochno.ru/front/searchapp/detail/{LISTING_ID}"
    "?guests_adults=2"
    "&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3"
    "&id=397367&type=city&price_per=1"
)

# Эндпоинты
API_PRICES_URL: str = (
    "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
)
API_CALC_URL: str = (
    "https://sutochno.ru/api/json/objects/calculateBookingPrice"
)
API_CHECK_URL: str = (
    "https://sutochno.ru/api/json/objects/checkBookingAbility"
)

# Дополнительные ID для сравнения (рабочие объявления)
CONTROL_LISTING_IDS: list[str] = ["709383"]

# Пути
DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "diagnose_obj_error_report.json"

# Таймауты
NETWORKIDLE_TIMEOUT_MS: int = 15000
PAGE_READY_TIMEOUT_MS: int = 15000
PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]


# ═══════════════════════════════════════════════════════════════════════
# Утилиты
# ═══════════════════════════════════════════════════════════════════════

def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def header(text: str) -> None:
    print(f"\n{'═' * 80}\n  {text}\n{'═' * 80}")


def section(text: str) -> None:
    print(f"\n{'─' * 70}\n  {text}\n{'─' * 70}\n")


def ok(v: bool) -> str:
    return "✓" if v else "✗"


# ═══════════════════════════════════════════════════════════════════════
# Загрузка страницы с перехватом токена
# ═══════════════════════════════════════════════════════════════════════

async def load_page_and_capture_token(
    page: Page, url: str
) -> tuple[bool, str | None]:
    """Загружает страницу и перехватывает токен из API-запросов."""
    captured: list[str] = []

    async def _handler(route: Route) -> None:
        req = route.request
        if "sutochno.ru/api/json" in req.url:
            t = req.headers.get("token") or req.headers.get("Token")
            if t and not captured:
                captured.append(t)
        await route.continue_()

    await page.route("**/api/json/**", _handler)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        try:
            await page.wait_for_load_state(
                "networkidle", timeout=NETWORKIDLE_TIMEOUT_MS
            )
        except Exception:
            pass
        await asyncio.sleep(2)
    except Exception as e:
        print(f"    [{ts()}] Ошибка загрузки: {e}")
        await page.unroute("**/api/json/**")
        return False, None
    finally:
        await page.unroute("**/api/json/**")

    token = captured[0] if captured else None
    return True, token


# ═══════════════════════════════════════════════════════════════════════
# Универсальный вызов getPricesAndAvailabilities
# ═══════════════════════════════════════════════════════════════════════

async def call_prices_api(
    page: Page,
    token: str,
    object_id: str,
    date_begin: str,
    date_end: str,
    guests: int = 2,
    rooms_cnt: dict | None = None,
) -> dict:
    """Вызывает getPricesAndAvailabilities и возвращает ПОЛНЫЙ ответ."""
    return await page.evaluate(
        """
        async (params) => {
            try {
                const resp = await fetch(params.apiUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'token': params.token,
                        'platform': 'js',
                        'api-version': '1.13'
                    },
                    body: JSON.stringify({
                        objects: [parseInt(params.objectId)],
                        rooms_cnt: params.roomsCnt || {},
                        guests: params.guests,
                        date_begin: params.dateBegin,
                        date_end: params.dateEnd,
                        currency_id: 1,
                        is_pets: 0,
                        documents: 0,
                        target: 0,
                        ages: [],
                        no_time: 1
                    })
                });

                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}

                if (!body) {
                    return {
                        http_status: resp.status,
                        parse_error: true,
                        raw: text ? text.substring(0, 2000) : null
                    };
                }

                // Полная структура ответа
                const result = {
                    http_status: resp.status,
                    api_success: body.success || false,
                    api_errors: body.errors || null,
                    api_message: body.message || null,
                };

                if (body.data && body.data.objects && body.data.objects[0]) {
                    const obj = body.data.objects[0];
                    result.obj_id = obj.id;
                    result.obj_success = obj.success || false;
                    result.obj_errors = obj.errors || null;
                    result.obj_errors_str = obj.errors
                        ? JSON.stringify(obj.errors).substring(0, 1000)
                        : null;

                    if (obj.data) {
                        result.busy = obj.data.busy;
                        result.price = obj.data.price;
                        result.price_default = obj.data.price_default;
                        result.rooms_available = obj.data.rooms_available;
                        result.is_booking_now = obj.data.is_booking_now;
                        result.max_guests = obj.data.max_guests;
                        result.cnt_guests = obj.data.cnt_guests;
                        result.service_fee = obj.data.service_fee;
                        result.bonus = obj.data.bonus;

                        // detail — полный массив
                        result.detail = (obj.data.detail || []).map(d => ({
                            type: d.type,
                            cost: d.cost,
                            cost_old: d.cost_old,
                            nights: d.nights,
                            date_begin: d.date_begin,
                            date_end: d.date_end,
                        }));

                        // Дополнительные поля
                        result.min_nights = obj.data.min_nights;
                        result.max_nights = obj.data.max_nights;
                        result.object_type = obj.data.object_type;
                        result.rooms = obj.data.rooms;

                        // Все ключи obj.data для обнаружения новых полей
                        result.obj_data_keys = Object.keys(obj.data);
                    }

                    // Все ключи obj для обнаружения новых полей
                    result.obj_keys = Object.keys(obj);
                } else {
                    result.no_objects = true;
                    result.raw_data = body.data
                        ? JSON.stringify(body.data).substring(0, 1000)
                        : null;
                }

                return result;

            } catch (e) {
                return {exception: e.message};
            }
        }
        """,
        {
            "apiUrl": API_PRICES_URL,
            "token": token,
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "guests": guests,
            "roomsCnt": rooms_cnt or {},
        },
    )


# ═══════════════════════════════════════════════════════════════════════
# Вызов calculateBookingPrice
# ═══════════════════════════════════════════════════════════════════════

async def call_calculate_api(
    page: Page,
    token: str,
    object_id: str,
    date_begin: str,
    date_end: str,
    guests: int = 2,
) -> dict:
    """Вызывает calculateBookingPrice."""
    return await page.evaluate(
        """
        async (params) => {
            try {
                const resp = await fetch(params.apiUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'token': params.token,
                        'platform': 'js',
                        'api-version': '1.13'
                    },
                    body: JSON.stringify({
                        objectId: parseInt(params.objectId),
                        dateBegin: params.dateBegin,
                        dateEnd: params.dateEnd,
                        guests: params.guests,
                        currencyId: 1,
                        childAges: [],
                        conditions: []
                    })
                });

                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}

                return {
                    http_status: resp.status,
                    success: body ? body.success : false,
                    data: body && body.data
                        ? JSON.stringify(body.data).substring(0, 2000)
                        : null,
                    errors: body ? (body.errors || null) : null,
                    raw: (!body && text)
                        ? text.substring(0, 1000) : null
                };
            } catch (e) {
                return {exception: e.message};
            }
        }
        """,
        {
            "apiUrl": API_CALC_URL,
            "token": token,
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "guests": guests,
        },
    )


# ═══════════════════════════════════════════════════════════════════════
# Вызов checkBookingAbility
# ═══════════════════════════════════════════════════════════════════════

async def call_check_api(
    page: Page,
    token: str,
    object_id: str,
    date_begin: str,
    date_end: str,
    guests: int = 2,
) -> dict:
    """Вызывает checkBookingAbility."""
    return await page.evaluate(
        """
        async (params) => {
            try {
                const resp = await fetch(params.apiUrl, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Accept': 'application/json',
                        'token': params.token,
                        'platform': 'js',
                        'api-version': '1.13'
                    },
                    body: JSON.stringify({
                        objectId: parseInt(params.objectId),
                        dateBegin: params.dateBegin,
                        dateEnd: params.dateEnd,
                        guests: params.guests,
                        currencyId: 1,
                        childAges: [],
                    })
                });

                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}

                return {
                    http_status: resp.status,
                    success: body ? body.success : false,
                    data: body && body.data
                        ? JSON.stringify(body.data).substring(0, 2000)
                        : null,
                    errors: body ? (body.errors || null) : null,
                    raw: (!body && text)
                        ? text.substring(0, 1000) : null
                };
            } catch (e) {
                return {exception: e.message};
            }
        }
        """,
        {
            "apiUrl": API_CHECK_URL,
            "token": token,
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "guests": guests,
        },
    )


# ═══════════════════════════════════════════════════════════════════════
# Извлечение данных из window.__NUXT__
# ═══════════════════════════════════════════════════════════════════════

async def extract_nuxt_data(page: Page, listing_id: str) -> dict:
    """Извлекает данные объявления из window.__NUXT__."""
    return await page.evaluate(
        """
        (listingId) => {
            const result = {
                nuxt_exists: !!window.__NUXT__,
                detail_key: null,
                object_data: null,
            };

            if (!window.__NUXT__) return result;

            const nuxt = window.__NUXT__;
            result.nuxt_top_keys = Object.keys(nuxt);

            // Ищем данные объявления в data
            if (nuxt.data) {
                const dataKeys = Object.keys(nuxt.data);
                result.data_keys = dataKeys;

                // Ищем ключ detail:{ID}
                const detailKey = dataKeys.find(
                    k => k.includes('detail') || k.includes(listingId)
                );

                if (detailKey) {
                    result.detail_key = detailKey;
                    const obj = nuxt.data[detailKey];

                    if (obj && typeof obj === 'object') {
                        // Извлекаем все полезные поля
                        result.object_data = {
                            id: obj.id,
                            type: obj.type,
                            object_type: obj.object_type,
                            object_type_name: obj.object_type_name,
                            rooms_cnt: obj.rooms_cnt,
                            rooms: obj.rooms,
                            min_nights: obj.min_nights,
                            max_nights: obj.max_nights,
                            max_guests: obj.max_guests,
                            is_booking_now: obj.is_booking_now,
                            is_hotel: obj.is_hotel,
                            is_hostel: obj.is_hostel,
                            is_apartment: obj.is_apartment,
                            price: obj.price,
                            price_default: obj.price_default,
                            currency_id: obj.currency_id,

                            // Наличие номеров / комнат
                            rooms_available: obj.rooms_available,
                            rooms_data: obj.rooms_data
                                ? JSON.stringify(obj.rooms_data).substring(0, 2000)
                                : null,
                            hotel_rooms: obj.hotel_rooms
                                ? JSON.stringify(obj.hotel_rooms).substring(0, 2000)
                                : null,

                            // Условия бронирования
                            booking_conditions: obj.booking_conditions,
                            booking_type: obj.booking_type,
                            confirmation_type: obj.confirmation_type,

                            // Все ключи верхнего уровня
                            all_keys: Object.keys(obj),
                        };
                    }
                }

                // Также ищем по всем ключам data
                for (const key of dataKeys) {
                    const val = nuxt.data[key];
                    if (val && typeof val === 'object' && !Array.isArray(val)) {
                        if (val.min_nights !== undefined || val.rooms_cnt !== undefined
                            || val.hotel_rooms !== undefined) {
                            result['extra_data_' + key] = {
                                min_nights: val.min_nights,
                                max_nights: val.max_nights,
                                rooms_cnt: val.rooms_cnt,
                                rooms: val.rooms,
                                type: val.type,
                                object_type: val.object_type,
                                hotel_rooms: val.hotel_rooms
                                    ? JSON.stringify(val.hotel_rooms).substring(0, 500)
                                    : null,
                                keys: Object.keys(val).slice(0, 30),
                            };
                        }
                    }
                }
            }

            // vuex store
            if (nuxt.state) {
                const stateStr = JSON.stringify(nuxt.state).substring(0, 50000);

                // min_nights
                const mnMatch = stateStr.match(/"min_nights"\s*:\s*(\d+)/);
                if (mnMatch) {
                    result.state_min_nights = parseInt(mnMatch[1]);
                }

                // rooms_cnt
                const rcMatch = stateStr.match(
                    /"rooms_cnt"\s*:\s*(\{[^}]*\})/
                );
                if (rcMatch) {
                    result.state_rooms_cnt = rcMatch[1].substring(0, 200);
                }

                // object_type
                const otMatch = stateStr.match(/"object_type"\s*:\s*(\d+)/);
                if (otMatch) {
                    result.state_object_type = parseInt(otMatch[1]);
                }

                // hotel_rooms
                if (stateStr.includes('hotel_rooms')) {
                    result.state_has_hotel_rooms = true;
                }
            }

            // vuex
            if (nuxt.vuex) {
                result.vuex_keys = Object.keys(nuxt.vuex);
                const vuexStr = JSON.stringify(nuxt.vuex).substring(0, 50000);

                const mnMatch2 = vuexStr.match(/"min_nights"\s*:\s*(\d+)/);
                if (mnMatch2) {
                    result.vuex_min_nights = parseInt(mnMatch2[1]);
                }
            }

            return result;
        }
        """,
        listing_id,
    )


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 1: Перебор nights (1, 2, 3, 5, 7, 10, 14, 30, 60)
# ═══════════════════════════════════════════════════════════════════════

async def test_nights_variations(
    page: Page, token: str, object_id: str
) -> list[dict]:
    """Тестирует API с разным количеством ночей."""
    section(f"ТЕСТ 1: Перебор nights для ID {object_id}")

    today = date.today()
    start = today + timedelta(days=3)  # через 3 дня
    nights_list = [1, 2, 3, 4, 5, 7, 10, 14, 21, 30, 45, 60]
    results = []

    for nights in nights_list:
        end = start + timedelta(days=nights)
        d_begin = f"{start.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        errors = resp.get("obj_errors_str", "")

        status = "✓" if obj_ok else "✗"
        print(
            f"    {status} nights={nights:>3}: "
            f"obj_success={obj_ok}, busy={busy}, price={price}, "
            f"errors={errors[:120] if errors else 'none'}"
        )

        results.append({
            "nights": nights,
            "date_begin": d_begin,
            "date_end": d_end,
            **resp,
        })

        await asyncio.sleep(0.3)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 2: Перебор guests (1, 2, 3, 4, 5, 6)
# ═══════════════════════════════════════════════════════════════════════

async def test_guests_variations(
    page: Page, token: str, object_id: str
) -> list[dict]:
    """Тестирует API с разным количеством гостей."""
    section(f"ТЕСТ 2: Перебор guests для ID {object_id}")

    today = date.today()
    start = today + timedelta(days=3)
    end = start + timedelta(days=2)  # 2 ночи
    d_begin = f"{start.isoformat()} 14:00:00"
    d_end = f"{end.isoformat()} 11:00:00"

    guests_list = [1, 2, 3, 4, 5, 6]
    results = []

    for guests in guests_list:
        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=guests
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        errors = resp.get("obj_errors_str", "")

        status = "✓" if obj_ok else "✗"
        print(
            f"    {status} guests={guests}: "
            f"obj_success={obj_ok}, busy={busy}, price={price}, "
            f"errors={errors[:120] if errors else 'none'}"
        )

        results.append({"guests": guests, **resp})
        await asyncio.sleep(0.3)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 3: rooms_cnt варианты (для апарт-отелей)
# ═══════════════════════════════════════════════════════════════════════

async def test_rooms_cnt_variations(
    page: Page, token: str, object_id: str
) -> list[dict]:
    """Тестирует API с разными rooms_cnt."""
    section(f"ТЕСТ 3: Перебор rooms_cnt для ID {object_id}")

    today = date.today()
    start = today + timedelta(days=3)
    end = start + timedelta(days=2)
    d_begin = f"{start.isoformat()} 14:00:00"
    d_end = f"{end.isoformat()} 11:00:00"

    rooms_variants: list[tuple[str, dict]] = [
        ("пустой {}", {}),
        ("{ID: 1}", {object_id: 1}),
        ("{1: 1}", {"1": 1}),
        ("{ID: 2}", {object_id: 2}),
    ]

    results = []

    for label, rooms_cnt in rooms_variants:
        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end,
            guests=2, rooms_cnt=rooms_cnt,
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        rooms_avail = resp.get("rooms_available")
        errors = resp.get("obj_errors_str", "")

        status = "✓" if obj_ok else "✗"
        print(
            f"    {status} rooms_cnt={label}: "
            f"obj_success={obj_ok}, busy={busy}, price={price}, "
            f"rooms_available={rooms_avail}, "
            f"errors={errors[:100] if errors else 'none'}"
        )

        results.append({"rooms_cnt_label": label, "rooms_cnt": rooms_cnt, **resp})
        await asyncio.sleep(0.3)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 4: Разные даты (ближайшие, через неделю, месяц)
# ═══════════════════════════════════════════════════════════════════════

async def test_date_variations(
    page: Page, token: str, object_id: str
) -> list[dict]:
    """Тестирует API для разных дат."""
    section(f"ТЕСТ 4: Разные даты для ID {object_id}")

    today = date.today()
    nights = 2

    date_offsets = [
        ("завтра", 1),
        ("через_3_дня", 3),
        ("через_неделю", 7),
        ("через_2_недели", 14),
        ("через_месяц", 30),
        ("через_45_дней", 45),
        ("через_2_месяца", 60),
        ("через_3_месяца", 90),
    ]

    results = []

    for label, offset in date_offsets:
        start = today + timedelta(days=offset)
        end = start + timedelta(days=nights)
        d_begin = f"{start.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        errors = resp.get("obj_errors_str", "")

        status = "✓" if obj_ok else "✗"
        print(
            f"    {status} {label} ({start}): "
            f"obj_success={obj_ok}, busy={busy}, price={price}, "
            f"errors={errors[:100] if errors else 'none'}"
        )

        results.append({"label": label, "offset_days": offset, **resp})
        await asyncio.sleep(0.3)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 5: Скользящее окно — 10 дней подряд (nights=2)
# ═══════════════════════════════════════════════════════════════════════

async def test_sliding_window_sample(
    page: Page, token: str, object_id: str
) -> list[dict]:
    """Тестирует скользящее окно для первых 10 дней."""
    section(f"ТЕСТ 5: Скользящее окно (10 дней, nights=2) для ID {object_id}")

    today = date.today()
    results = []

    for i in range(10):
        day = today + timedelta(days=i)
        end = day + timedelta(days=2)
        d_begin = f"{day.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        errors = resp.get("obj_errors_str", "")

        status = "✓" if obj_ok else "✗"
        busy_mark = "🔴" if busy == "busy" else ("🟢" if busy == "unbusy" else "❓")
        print(
            f"    {status} день {i} ({day}) {busy_mark}: "
            f"obj_success={obj_ok}, busy={busy}, price={price}, "
            f"errors={errors[:80] if errors else 'none'}"
        )

        results.append({"day_index": i, "date": day.isoformat(), **resp})
        await asyncio.sleep(0.2)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 6: Альтернативные эндпоинты
# ═══════════════════════════════════════════════════════════════════════

async def test_alternative_endpoints(
    page: Page, token: str, object_id: str
) -> dict:
    """Тестирует calculateBookingPrice и checkBookingAbility."""
    section(f"ТЕСТ 6: Альтернативные эндпоинты для ID {object_id}")

    today = date.today()
    start = today + timedelta(days=3)
    end = start + timedelta(days=2)
    d_begin = f"{start.isoformat()} 14:00:00"
    d_end = f"{end.isoformat()} 11:00:00"

    results: dict = {}

    # calculateBookingPrice
    print(f"    [{ts()}] calculateBookingPrice...")
    calc_resp = await call_calculate_api(
        page, token, object_id, d_begin, d_end, guests=2
    )
    results["calculateBookingPrice"] = calc_resp
    print(
        f"    http={calc_resp.get('http_status')}, "
        f"success={calc_resp.get('success')}, "
        f"data={str(calc_resp.get('data', ''))[:200]}"
    )

    await asyncio.sleep(0.3)

    # checkBookingAbility
    print(f"    [{ts()}] checkBookingAbility...")
    check_resp = await call_check_api(
        page, token, object_id, d_begin, d_end, guests=2
    )
    results["checkBookingAbility"] = check_resp
    print(
        f"    http={check_resp.get('http_status')}, "
        f"success={check_resp.get('success')}, "
        f"data={str(check_resp.get('data', ''))[:200]}"
    )

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 7: Контрольное объявление (заведомо рабочее)
# ═══════════════════════════════════════════════════════════════════════

async def test_control_listing(
    page: Page, token: str, control_id: str
) -> dict:
    """Проверяет, работает ли API для контрольного объявления."""
    section(f"ТЕСТ 7: Контрольное объявление ID {control_id}")

    today = date.today()
    start = today + timedelta(days=3)

    results: dict = {"control_id": control_id}

    for nights in [1, 2, 5]:
        end = start + timedelta(days=nights)
        d_begin = f"{start.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, control_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")

        status = "✓" if obj_ok else "✗"
        print(
            f"    {status} nights={nights}: "
            f"obj_success={obj_ok}, busy={busy}, price={price}"
        )

        results[f"nights_{nights}"] = resp
        await asyncio.sleep(0.3)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 8: Bulk-запрос на 60 ночей (как в listing_service)
# ═══════════════════════════════════════════════════════════════════════

async def test_bulk_60(
    page: Page, token: str, object_id: str
) -> dict:
    """Воспроизводит bulk-запрос из listing_service."""
    section(f"ТЕСТ 8: Bulk 60 ночей (как в listing_service) для ID {object_id}")

    today = date.today()
    end = today + timedelta(days=60)
    d_begin = f"{today.isoformat()} 14:00:00"
    d_end = f"{end.isoformat()} 11:00:00"

    resp = await call_prices_api(
        page, token, object_id, d_begin, d_end, guests=2
    )

    obj_ok = resp.get("obj_success", False)
    busy = resp.get("busy")
    price = resp.get("price")
    detail = resp.get("detail", [])
    errors = resp.get("obj_errors_str", "")

    status = "✓" if obj_ok else "✗"
    print(f"    {status} Bulk 60 ночей:")
    print(f"      obj_success: {obj_ok}")
    print(f"      busy: {busy}")
    print(f"      price: {price}")
    print(f"      detail entries: {len(detail)}")
    print(f"      obj_errors: {errors[:300] if errors else 'none'}")
    print(f"      rooms_available: {resp.get('rooms_available')}")
    print(f"      obj_data_keys: {resp.get('obj_data_keys', [])}")
    print(f"      obj_keys: {resp.get('obj_keys', [])}")

    if detail:
        print(f"      detail:")
        for d in detail[:10]:
            print(f"        {d}")

    return resp


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 9: Поиск min_nights через бинарный поиск
# ═══════════════════════════════════════════════════════════════════════

async def test_find_min_nights(
    page: Page, token: str, object_id: str
) -> dict:
    """Находит точное значение min_nights бинарным поиском."""
    section(f"ТЕСТ 9: Поиск min_nights для ID {object_id}")

    today = date.today()
    start = today + timedelta(days=7)  # через неделю

    # Сначала грубый поиск: при каком nights впервые obj_success=true
    first_success_nights: int | None = None

    for nights in [1, 2, 3, 4, 5, 6, 7, 10, 14, 21, 30, 45, 60]:
        end = start + timedelta(days=nights)
        d_begin = f"{start.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        status = "✓" if obj_ok else "✗"
        print(f"    {status} nights={nights}: obj_success={obj_ok}")

        if obj_ok and first_success_nights is None:
            first_success_nights = nights
            break

        await asyncio.sleep(0.2)

    result: dict = {"first_success_nights": first_success_nights}

    if first_success_nights is None:
        print(f"    ✗ obj_success=false для ВСЕХ nights (1..60)")
        print(f"    Причина НЕ в min_nights — другое ограничение!")
        result["conclusion"] = "not_min_nights_issue"
        return result

    if first_success_nights == 1:
        print(f"    ✓ obj_success=true уже при nights=1")
        print(f"    Объект не имеет min_nights ограничения (или min_nights=1)")
        result["conclusion"] = "no_min_nights"
        result["min_nights"] = 1
        return result

    # Бинарный поиск между prev и first_success_nights
    prev_nights = {
        2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6,
        10: 7, 14: 10, 21: 14, 30: 21, 45: 30, 60: 45,
    }
    low = prev_nights.get(first_success_nights, 1)
    high = first_success_nights

    print(f"\n    Бинарный поиск: min_nights ∈ [{low+1}, {high}]")

    while low + 1 < high:
        mid = (low + high) // 2
        end = start + timedelta(days=mid)
        d_begin = f"{start.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )
        obj_ok = resp.get("obj_success", False)

        status = "✓" if obj_ok else "✗"
        print(f"    {status} nights={mid}: obj_success={obj_ok}")

        if obj_ok:
            high = mid
        else:
            low = mid

        await asyncio.sleep(0.2)

    result["min_nights"] = high
    result["conclusion"] = f"min_nights={high}"
    print(f"\n    → НАЙДЕНО: min_nights = {high}")

    return result


# ═══════════════════════════════════════════════════════════════════════
# ТЕСТ 10: Скользящее окно с найденным min_nights
# ═══════════════════════════════════════════════════════════════════════

async def test_sliding_with_found_min_nights(
    page: Page, token: str, object_id: str, min_nights: int
) -> list[dict]:
    """Тестирует скользящее окно с правильным min_nights."""
    section(
        f"ТЕСТ 10: Скользящее окно (10 дней, nights={min_nights}) "
        f"для ID {object_id}"
    )

    today = date.today()
    results = []

    for i in range(10):
        day = today + timedelta(days=i)
        end = day + timedelta(days=min_nights)
        d_begin = f"{day.isoformat()} 14:00:00"
        d_end = f"{end.isoformat()} 11:00:00"

        resp = await call_prices_api(
            page, token, object_id, d_begin, d_end, guests=2
        )

        obj_ok = resp.get("obj_success", False)
        busy = resp.get("busy")
        price = resp.get("price")
        detail = resp.get("detail", [])

        status = "✓" if obj_ok else "✗"
        busy_mark = (
            "🔴" if busy == "busy"
            else ("🟢" if busy == "unbusy" else "❓")
        )

        detail_costs = [
            d.get("cost") for d in detail
            if d.get("type") in ("season_price", 1)
        ]

        print(
            f"    {status} день {i} ({day}) {busy_mark}: "
            f"busy={busy}, price={price}, "
            f"detail_costs={detail_costs}"
        )

        results.append({
            "day_index": i,
            "date": day.isoformat(),
            **resp,
        })
        await asyncio.sleep(0.2)

    return results


# ═══════════════════════════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════════════

async def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    header(f"ДИАГНОСТИКА obj_success=false — ID {LISTING_ID}")
    print(f"  Дата:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  URL:      {CARD_URL[:100]}")
    print(f"  Отчёт:    {REPORT_PATH}")

    report: dict = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "listing_id": LISTING_ID,
            "url": CARD_URL,
            "control_ids": CONTROL_LISTING_IDS,
        },
        "tests": {},
        "summary": {},
    }

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
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

        await context.add_init_script("""
            Object.defineProperty(
                navigator, 'webdriver', {get: () => undefined}
            );
        """)

        page = await context.new_page()
        page.set_default_navigation_timeout(60_000)

        # ── Загрузка страницы и получение токена ──
        header("ЗАГРУЗКА СТРАНИЦЫ И ПЕРЕХВАТ ТОКЕНА")
        loaded, token = await load_page_and_capture_token(page, CARD_URL)

        if not loaded or not token:
            print(f"    ✗ Не удалось загрузить страницу или получить токен!")
            print(f"    loaded={loaded}, token={token}")
            report["error"] = "page_load_or_token_failed"
            REPORT_PATH.write_text(
                json.dumps(report, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
            await browser.close()
            return

        print(f"    ✓ Токен получен: {token[:40]}...")
        report["token"] = token[:50]

        # ── __NUXT__ данные ──
        header("ДАННЫЕ ИЗ window.__NUXT__")
        nuxt_data = await extract_nuxt_data(page, LISTING_ID)
        report["tests"]["nuxt_data"] = nuxt_data

        obj_info = nuxt_data.get("object_data", {})
        if obj_info:
            print(f"    min_nights:       {obj_info.get('min_nights')}")
            print(f"    max_nights:       {obj_info.get('max_nights')}")
            print(f"    max_guests:       {obj_info.get('max_guests')}")
            print(f"    rooms_cnt:        {obj_info.get('rooms_cnt')}")
            print(f"    rooms_available:  {obj_info.get('rooms_available')}")
            print(f"    object_type:      {obj_info.get('object_type')}")
            print(f"    is_hotel:         {obj_info.get('is_hotel')}")
            print(f"    is_hostel:        {obj_info.get('is_hostel')}")
            print(f"    is_apartment:     {obj_info.get('is_apartment')}")
            print(f"    is_booking_now:   {obj_info.get('is_booking_now')}")
            print(f"    booking_type:     {obj_info.get('booking_type')}")
            print(f"    price:            {obj_info.get('price')}")
            print(f"    price_default:    {obj_info.get('price_default')}")
            hotel_rooms = obj_info.get('hotel_rooms')
            print(f"    hotel_rooms:      {(hotel_rooms or 'none')[:200]}")
            rooms_data = obj_info.get('rooms_data')
            print(f"    rooms_data:       {(rooms_data or 'none')[:200]}")
            print(f"    all_keys ({len(obj_info.get('all_keys', []))}): "
                  f"{obj_info.get('all_keys', [])[:20]}")
        else:
            print(f"    Данные объявления в __NUXT__ не найдены")
            print(f"    detail_key: {nuxt_data.get('detail_key')}")
            print(f"    data_keys: {nuxt_data.get('data_keys', [])[:20]}")

        if nuxt_data.get("state_min_nights"):
            print(f"    state.min_nights: {nuxt_data['state_min_nights']}")
        if nuxt_data.get("vuex_min_nights"):
            print(f"    vuex.min_nights: {nuxt_data['vuex_min_nights']}")

        # ── Основные тесты ──
        report["tests"]["nights"] = await test_nights_variations(
            page, token, LISTING_ID
        )
        report["tests"]["guests"] = await test_guests_variations(
            page, token, LISTING_ID
        )
        report["tests"]["rooms_cnt"] = await test_rooms_cnt_variations(
            page, token, LISTING_ID
        )
        report["tests"]["dates"] = await test_date_variations(
            page, token, LISTING_ID
        )
        report["tests"]["sliding_10"] = await test_sliding_window_sample(
            page, token, LISTING_ID
        )
        report["tests"]["alt_endpoints"] = await test_alternative_endpoints(
            page, token, LISTING_ID
        )
        report["tests"]["bulk_60"] = await test_bulk_60(
            page, token, LISTING_ID
        )

        # ── Поиск min_nights ──
        min_nights_result = await test_find_min_nights(
            page, token, LISTING_ID
        )
        report["tests"]["find_min_nights"] = min_nights_result

        found_min = min_nights_result.get("min_nights")
        if found_min and found_min > 1:
            report["tests"]["sliding_with_min_nights"] = (
                await test_sliding_with_found_min_nights(
                    page, token, LISTING_ID, found_min
                )
            )

        # ── Контрольное объявление ──
        for ctrl_id in CONTROL_LISTING_IDS:
            report["tests"][f"control_{ctrl_id}"] = await test_control_listing(
                page, token, ctrl_id
            )

        await browser.close()

    # ═══════════════════════════════════════════════════════════════════
    # ИТОГОВАЯ СВОДКА
    # ═══════════════════════════════════════════════════════════════════

    header("ИТОГОВАЯ СВОДКА")

    summary: dict = {}

    # min_nights
    nuxt_min = (
        (obj_info.get("min_nights") if obj_info else None)
        or nuxt_data.get("state_min_nights")
        or nuxt_data.get("vuex_min_nights")
    )
    api_min = found_min
    summary["nuxt_min_nights"] = nuxt_min
    summary["api_detected_min_nights"] = api_min
    summary["min_nights_conclusion"] = min_nights_result.get("conclusion")

    print(f"  min_nights из __NUXT__:    {nuxt_min}")
    print(f"  min_nights из API-теста:   {api_min}")
    print(f"  Вывод:                     {min_nights_result.get('conclusion')}")

    # Bulk 60 результат
    bulk = report["tests"].get("bulk_60", {})
    summary["bulk_60_obj_success"] = bulk.get("obj_success")
    summary["bulk_60_errors"] = bulk.get("obj_errors_str", "")[:300]

    print(f"\n  Bulk 60 obj_success:       {bulk.get('obj_success')}")
    print(f"  Bulk 60 errors:            {bulk.get('obj_errors_str', '')[:200]}")

    # Скользящее окно
    sliding = report["tests"].get("sliding_10", [])
    if sliding:
        sw_ok = sum(1 for r in sliding if r.get("obj_success"))
        sw_busy = sum(1 for r in sliding if r.get("busy") == "busy")
        sw_free = sum(1 for r in sliding if r.get("busy") == "unbusy")
        summary["sliding_10_success"] = sw_ok
        summary["sliding_10_busy"] = sw_busy
        summary["sliding_10_free"] = sw_free
        print(f"\n  Скольз. окно (10 дн):      "
              f"ok={sw_ok}, busy={sw_busy}, free={sw_free}")

    # Контрольное
    for ctrl_id in CONTROL_LISTING_IDS:
        ctrl = report["tests"].get(f"control_{ctrl_id}", {})
        ctrl_ok = any(
            ctrl.get(f"nights_{n}", {}).get("obj_success")
            for n in [1, 2, 5]
        )
        summary[f"control_{ctrl_id}_works"] = ctrl_ok
        print(f"\n  Контроль ID {ctrl_id}:       {'✓ работает' if ctrl_ok else '✗ не работает'}")

    # Главный вопрос: причина
    print(f"\n  {'═' * 60}")

    if api_min and api_min > 1:
        print(f"  ПРИЧИНА: min_nights = {api_min}")
        print(f"  listing_service запрашивает bulk на 60 ночей →")
        print(f"  API отвечает obj_success=false из-за min_nights.")
        print(f"  Скользящее окно с nights={api_min} должно работать.")
        summary["root_cause"] = f"min_nights={api_min}"
    elif min_nights_result.get("conclusion") == "not_min_nights_issue":
        print(f"  ПРИЧИНА: НЕ min_nights!")
        print(f"  obj_success=false для ВСЕХ nights (1..60).")
        print(f"  Проверьте obj_errors в отчёте для деталей.")
        summary["root_cause"] = "not_min_nights"
    else:
        print(f"  ПРИЧИНА: требует дополнительного анализа")
        print(f"  Смотрите полный отчёт: {REPORT_PATH}")
        summary["root_cause"] = "needs_analysis"

    print(f"  {'═' * 60}")

    report["summary"] = summary

    # ── Сохранение ──
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    print(f"\n  Отчёт сохранён: {REPORT_PATH.absolute()}")
    print(f"  Размер: {REPORT_PATH.stat().st_size / 1024:.1f} КБ")


if __name__ == "__main__":
    asyncio.run(main())
