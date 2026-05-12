"""Диагностика проблемы «цен=0» — пошаговая проверка логики listing_service.

Воспроизводит ТОЧНУЮ логику listing_service.py для проблемных карточек
и на каждом шаге сохраняет сырые данные API, чтобы найти корневую причину.

Проверяемые гипотезы:
1. Токен не перехватывается при загрузке страницы
2. Токен перехватывается, но невалиден (api_false)
3. Bulk-запрос на 60 ночей возвращает ошибку или пустой detail[]
4. detail[] не содержит записей с type="season_price" или type=1
5. Даты в detail[] не покрывают все 60 дней (сдвиг today vs today+1)
6. Скользящее окно возвращает obj_error (min_nights)
7. Токен протухает между шагами

Запуск:
    python scripts/diagnose_zero_prices.py

Результат:
    data/diagnose_zero_prices_report.json
    + вывод в консоль с пошаговой диагностикой
"""

import asyncio
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from playwright.async_api import Page, Request, async_playwright


# ═══════════════════════════════════════════════════════════════════════
# Конфигурация
# ═══════════════════════════════════════════════════════════════════════

# Проблемные карточки из логов (цен=0)
PROBLEM_LISTINGS: list[dict[str, str | int]] = [
    {"id": "1051969", "label": "свободных=14, цен=0"},
    {"id": "926957", "label": "свободных=0, цен=0"},
    {"id": "958767", "label": "свободных=9, цен=0"},
    {"id": "1462937", "label": "свободных=13, цен=0"},
    {"id": "1516995", "label": "свободных=0, цен=0"},
    {"id": "1942899", "label": "свободных=6, цен=0"},
]

# Шаблон URL карточки (как в основной программе)
_CARD_URL_TEMPLATE: str = (
    "https://sutochno.ru/front/searchapp/detail/{listing_id}"
    "?guests_adults=2"
    "&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3"
    "&id=397367&type=city"
    "&price_per=1"
)

# URL API (как в listing_service.py)
_API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Параметры из listing_service.py
_DAYS_COUNT: int = 60
_API_BATCH_SIZE: int = 5
_API_BATCH_DELAY: float = 0.5
_DEFAULT_GUESTS: int = 2
_MIN_NIGHTS_VARIANTS: list[int] = [2, 3, 5, 7]
_NETWORKIDLE_SOFT_TIMEOUT_MS: int = 10000

# Типы записей detail[] (синхронизировано с listing_service.py)
_BASE_PRICE_TYPE_INT: int = 1
_SEASON_PRICE_TYPE: str = "season_price"

# Селекторы готовности страницы (как в listing_service.py)
_PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]
_PAGE_READY_TIMEOUT_MS: int = 15000

# Пути
DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "diagnose_zero_prices_report.json"


# ═══════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════

def _ts() -> str:
    """Текущее время для лога."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _print_header(text: str) -> None:
    """Выводит заголовок секции."""
    print(f"\n{'═' * 70}")
    print(f"  {text}")
    print(f"{'═' * 70}")


def _print_section(text: str) -> None:
    """Выводит заголовок подсекции."""
    print(f"\n{'─' * 70}")
    print(f"  {text}")
    print(f"{'─' * 70}\n")


def _verdict(ok: bool) -> str:
    """Возвращает значок результата."""
    return "✓" if ok else "✗"


# ═══════════════════════════════════════════════════════════════════════
# Шаг 1: Загрузка страницы + перехват токена
# (воспроизводит _goto_and_capture_token + _goto_with_retry)
# ═══════════════════════════════════════════════════════════════════════

async def step1_load_and_capture_token(
    page: Page, url: str, listing_id: str
) -> dict:
    """Загружает страницу карточки и перехватывает токен.

    Воспроизводит логику:
    - listing_service._goto_and_capture_token()
    - listing_service._goto_with_retry()
    - listing_service._wait_for_page_ready()

    Args:
        page: Вкладка браузера.
        url: URL карточки.
        listing_id: ID объявления (для логов).

    Returns:
        Словарь с результатами диагностики шага 1.
    """
    result: dict = {
        "step": "1_load_and_capture_token",
        "listing_id": listing_id,
        "url": url,
        "token": None,
        "page_loaded": False,
        "dom_loaded": False,
        "network_idle": False,
        "page_ready_selector": None,
        "dom_time_sec": 0.0,
        "idle_time_sec": 0.0,
        "api_requests_seen": [],
        "errors": [],
    }

    captured_tokens: list[str] = []
    api_requests_seen: list[dict] = []

    def on_request(request: Request) -> None:
        """Перехватчик запросов — ТОЧНАЯ копия логики listing_service."""
        if captured_tokens:
            return
        req_url = request.url
        if "sutochno.ru/api/json" in req_url:
            token_header = request.headers.get("token")
            api_requests_seen.append({
                "url": req_url.split("?")[0],
                "has_token": bool(token_header),
                "token_preview": token_header[:20] + "..." if token_header else "",
                "time": _ts(),
            })
            if token_header:
                captured_tokens.append(token_header)

    page.on("request", on_request)

    # ── goto с domcontentloaded (как _goto_with_retry) ──
    t0 = time.perf_counter()
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        result["dom_loaded"] = True
        result["dom_time_sec"] = round(time.perf_counter() - t0, 2)
        print(f"    [{_ts()}] {_verdict(True)} domcontentloaded за {result['dom_time_sec']}с")
    except Exception as e:
        result["dom_time_sec"] = round(time.perf_counter() - t0, 2)
        result["errors"].append(f"goto_ошибка: {str(e)[:200]}")
        print(f"    [{_ts()}] {_verdict(False)} goto ОШИБКА за {result['dom_time_sec']}с: {str(e)[:100]}")
        page.remove_listener("request", on_request)
        return result

    # ── networkidle (как _goto_with_retry: мягкий таймаут) ──
    t1 = time.perf_counter()
    try:
        await page.wait_for_load_state(
            "networkidle", timeout=_NETWORKIDLE_SOFT_TIMEOUT_MS
        )
        result["network_idle"] = True
        result["idle_time_sec"] = round(time.perf_counter() - t1, 2)
        print(f"    [{_ts()}] {_verdict(True)} networkidle за {result['idle_time_sec']}с")
    except Exception:
        result["idle_time_sec"] = round(time.perf_counter() - t1, 2)
        print(f"    [{_ts()}] ⚠ networkidle НЕ достигнут за {result['idle_time_sec']}с (продолжаем)")

    # ── Ожидание ключевых элементов (как _wait_for_page_ready) ──
    for selector in _PAGE_READY_SELECTORS:
        try:
            await page.wait_for_selector(selector, timeout=_PAGE_READY_TIMEOUT_MS)
            result["page_ready_selector"] = selector
            print(f"    [{_ts()}] {_verdict(True)} селектор найден: {selector}")
            break
        except Exception:
            continue

    if not result["page_ready_selector"]:
        print(f"    [{_ts()}] ⚠ ни один ключевой селектор не найден (продолжаем)")

    result["page_loaded"] = True

    page.remove_listener("request", on_request)

    result["api_requests_seen"] = api_requests_seen
    result["token"] = captured_tokens[0] if captured_tokens else None

    print(f"    [{_ts()}] API-запросов при загрузке: {len(api_requests_seen)}")
    for req in api_requests_seen[:5]:
        print(f"      [{req['time']}] {req['url']} token={'Да' if req['has_token'] else 'Нет'}")

    if result["token"]:
        print(f"    [{_ts()}] {_verdict(True)} Токен перехвачен: {result['token'][:30]}...")
    else:
        print(f"    [{_ts()}] {_verdict(False)} Токен НЕ перехвачен")

    return result


# ═══════════════════════════════════════════════════════════════════════
# Шаг 2: Валидация токена
# (воспроизводит _validate_token)
# ═══════════════════════════════════════════════════════════════════════

async def step2_validate_token(
    page: Page, object_id: str, token: str
) -> dict:
    """Проверяет работоспособность токена тестовым запросом.

    Воспроизводит ТОЧНУЮ логику listing_service._validate_token():
    - Дата = today + 3 дня
    - Ночей = 2
    - Проверяет data.success (не obj.success — объект может вернуть min_nights ошибку)

    Args:
        page: Вкладка браузера.
        object_id: ID объявления.
        token: Токен для проверки.

    Returns:
        Словарь с результатами диагностики шага 2.
    """
    today = date.today()
    test_date = today + timedelta(days=3)
    end_date = test_date + timedelta(days=2)

    result_diag: dict = {
        "step": "2_validate_token",
        "listing_id": object_id,
        "test_date_begin": f"{test_date.isoformat()} 14:00:00",
        "test_date_end": f"{end_date.isoformat()} 11:00:00",
        "token_valid": False,
        "raw_response": None,
        "reason": None,
    }

    print(f"    Тестовый запрос: {test_date} → {end_date} (2 ночи)")

    api_result = await page.evaluate(
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
                        objects: [parseInt(objectId)],
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

                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}

                if (!resp.ok) return {valid: false, reason: 'http_' + resp.status, body: body, raw: body ? null : text.substring(0, 500)};
                if (!body) return {valid: false, reason: 'json_parse_error', raw: text.substring(0, 500)};
                if (!body.success) return {valid: false, reason: 'api_false', body: body};
                if (!body.data || !body.data.objects || !body.data.objects[0]) {
                    return {valid: false, reason: 'no_objects', body: body};
                }

                const obj = body.data.objects[0];
                return {
                    valid: true,
                    reason: obj.success ? 'obj_success' : 'obj_error_but_token_ok',
                    body: body,
                    obj_success: obj.success,
                    obj_errors: obj.errors || [],
                    obj_data_keys: obj.data ? Object.keys(obj.data) : []
                };

            } catch (e) {
                return {valid: false, reason: 'exception_' + e.message};
            }
        }
        """,
        {
            "apiUrl": _API_PRICES_URL,
            "objectId": object_id,
            "dateBegin": f"{test_date.isoformat()} 14:00:00",
            "dateEnd": f"{end_date.isoformat()} 11:00:00",
            "token": token,
            "guests": _DEFAULT_GUESTS,
        },
    )

    result_diag["token_valid"] = api_result.get("valid", False)
    result_diag["reason"] = api_result.get("reason")
    result_diag["raw_response"] = api_result

    is_valid = result_diag["token_valid"]
    reason = result_diag["reason"]

    print(f"    [{_ts()}] {_verdict(is_valid)} Токен валиден: {is_valid} (причина: {reason})")

    if api_result.get("obj_success") is False:
        print(f"    ⚠ Объект вернул ошибку (но токен ОК): {api_result.get('obj_errors')}")

    return result_diag


# ═══════════════════════════════════════════════════════════════════════
# Шаг 3: Bulk-запрос на 60 ночей
# (воспроизводит _fetch_bulk_prices — ОБНОВЛЁННАЯ ВЕРСИЯ с type=1)
# ═══════════════════════════════════════════════════════════════════════

async def step3_bulk_prices(
    page: Page, object_id: str, token: str
) -> dict:
    """Получает цены одним запросом на 60 ночей.

    Воспроизводит ОБНОВЛЁННУЮ логику listing_service._fetch_bulk_prices():
    - Приоритет: type="season_price" (сезонные цены с диапазонами дат)
    - Fallback: type=1 (базовая цена без дат, применяется ко всем дням)
    - Типы "interval", "dop_persons", "sale" игнорируются

    Args:
        page: Вкладка браузера.
        object_id: ID объявления.
        token: Токен API.

    Returns:
        Словарь с результатами диагностики шага 3.
    """
    today = date.today()
    end_date = today + timedelta(days=_DAYS_COUNT)

    date_begin = f"{today.isoformat()} 14:00:00"
    date_end = f"{end_date.isoformat()} 11:00:00"

    result_diag: dict = {
        "step": "3_bulk_prices",
        "listing_id": object_id,
        "date_begin": date_begin,
        "date_end": date_end,
        "bulk_success": False,
        "busy_status": None,
        "detail_raw": [],
        "detail_season_price_count": 0,
        "detail_other_types": [],
        "base_price_from_type_1": 0,
        "daily_prices_count": 0,
        "daily_prices_sample": {},
        "prices_60_nonzero": 0,
        "prices_60": [],
        "raw_response": None,
        "error": None,
    }

    print(f"    Запрос: {date_begin} → {date_end} (60 ночей)")

    api_result = await page.evaluate(
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
                        objects: [parseInt(objectId)],
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

                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}

                if (!resp.ok) return {success: false, error: 'http_' + resp.status, raw: text ? text.substring(0, 500) : null};
                if (!body) return {success: false, error: 'json_parse_error', raw: text ? text.substring(0, 500) : null};
                if (!body.success) return {success: false, error: 'api_false', body: body};

                if (!body.data || !body.data.objects || !body.data.objects[0]) {
                    return {success: false, error: 'no_objects', body: body};
                }

                const obj = body.data.objects[0];

                if (!obj.success) {
                    return {
                        success: false,
                        error: 'obj_error',
                        obj_errors: obj.errors || [],
                        obj_data: obj.data || null
                    };
                }

                const objData = obj.data;
                return {
                    success: true,
                    busy: objData.busy,
                    detail: objData.detail || [],
                    rooms_available: objData.rooms_available,
                    price: objData.price,
                    is_booking_now: objData.is_booking_now,
                    max_guests: objData.max_guests
                };

            } catch (e) {
                return {success: false, error: 'exception_' + e.message};
            }
        }
        """,
        {
            "apiUrl": _API_PRICES_URL,
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "token": token,
            "guests": _DEFAULT_GUESTS,
        },
    )

    result_diag["raw_response"] = api_result

    if not api_result.get("success"):
        error = api_result.get("error", "unknown")
        result_diag["error"] = error
        print(f"    [{_ts()}] {_verdict(False)} Bulk ОШИБКА: {error}")
        if api_result.get("obj_errors"):
            print(f"    Ошибки объекта: {api_result['obj_errors']}")
        return result_diag

    result_diag["bulk_success"] = True
    result_diag["busy_status"] = api_result.get("busy")

    detail = api_result.get("detail", [])
    result_diag["detail_raw"] = detail

    print(f"    [{_ts()}] {_verdict(True)} Bulk УСПЕХ")
    print(f"    busy={api_result.get('busy')}, price={api_result.get('price')}, "
          f"rooms={api_result.get('rooms_available')}")
    print(f"    detail[] содержит {len(detail)} записей:")

    # ── Анализ detail[] — КЛЮЧЕВАЯ ДИАГНОСТИКА ──
    season_price_entries = []
    other_types: list[str] = []
    base_price: int = 0

    for i, det in enumerate(detail):
        det_type = det.get("type", "?")
        det_cost = det.get("cost", 0)
        det_begin = str(det.get("date_begin", "?"))[:10]
        det_end = str(det.get("date_end", "?"))[:10]
        det_nights = det.get("nights", 0)

        # Определяем, используется ли запись
        if det_type == _SEASON_PRICE_TYPE:
            type_marker = " ← ИСПОЛЬЗУЕТСЯ (season_price)"
            season_price_entries.append(det)
        elif det_type == _BASE_PRICE_TYPE_INT:
            if not base_price and det_cost:
                base_price = int(det_cost)
            type_marker = f" ← ИСПОЛЬЗУЕТСЯ (base_price={base_price})"
        else:
            type_marker = " ← ИГНОРИРУЕТСЯ"
            if det_type not in other_types:
                other_types.append(det_type)

        print(f"      [{i:2}] type={str(det_type):<15} cost={det_cost:<7} "
              f"{det_begin}→{det_end} nights={det_nights}{type_marker}")

    result_diag["detail_season_price_count"] = len(season_price_entries)
    result_diag["detail_other_types"] = other_types
    result_diag["base_price_from_type_1"] = base_price

    print(f"\n    season_price записей: {len(season_price_entries)}")
    print(f"    base_price (type=1):  {base_price}")
    if other_types:
        print(f"    Другие типы (игнорируются): {other_types}")

    # ── Разворачиваем season_price в дневные цены (ТОЧНАЯ КОПИЯ listing_service) ──
    daily_prices: dict[str, int] = {}

    for det in detail:
        if det.get("type") != _SEASON_PRICE_TYPE:
            continue

        d_begin = det.get("date_begin")
        d_end = det.get("date_end")
        cost = det.get("cost", 0)

        if not d_begin or not d_end or not cost:
            print(f"    ⚠ Пропущена запись: begin={d_begin}, end={d_end}, cost={cost}")
            continue

        d_begin_str = str(d_begin)[:10]
        d_end_str = str(d_end)[:10]

        try:
            period_start = date.fromisoformat(d_begin_str)
            period_end = date.fromisoformat(d_end_str)
            current = period_start
            while current <= period_end:
                daily_prices[current.isoformat()] = int(cost)
                current += timedelta(days=1)
        except (ValueError, TypeError) as e:
            print(f"    ⚠ Ошибка парсинга дат: {e}")
            continue

    # ── Формируем массив цен на 60 дней ──
    # Приоритет: season_price (с датами) → type=1 (базовая цена)
    prices_60: list[int] = []
    for i in range(_DAYS_COUNT):
        day = today + timedelta(days=i)
        day_key = day.isoformat()
        price = daily_prices.get(day_key, base_price)
        prices_60.append(price)

    result_diag["daily_prices_count"] = len(daily_prices)
    result_diag["daily_prices_sample"] = dict(list(daily_prices.items())[:15])
    result_diag["prices_60"] = prices_60
    result_diag["prices_60_nonzero"] = sum(1 for p in prices_60 if p > 0)

    print(f"\n    Дневные цены (из season_price): {len(daily_prices)} уникальных дат")
    print(f"    Базовая цена (из type=1): {base_price}")
    print(f"    Массив prices_60: {result_diag['prices_60_nonzero']}/60 ненулевых")

    # ── ДИАГНОСТИКА: почему цены могут быть 0 ──
    if result_diag["prices_60_nonzero"] == 0:
        print(f"\n    {'!' * 50}")
        print(f"    ПРОБЛЕМА НАЙДЕНА: prices_60 содержит 0 ненулевых цен!")
        print(f"    {'!' * 50}")

        if len(season_price_entries) == 0 and base_price == 0:
            print(f"    ПРИЧИНА: НИ season_price, НИ type=1 не содержат цен")
            if len(detail) > 0:
                print(f"    detail[] содержит {len(detail)} записей других типов: {other_types}")
            else:
                print(f"    detail[] пустой — API не вернул разбивку цен")
        elif len(daily_prices) == 0 and base_price == 0:
            print(f"    ПРИЧИНА: season_price записи есть, но парсинг дат не дал результата, "
                  f"и base_price=0")
    elif result_diag["prices_60_nonzero"] == _DAYS_COUNT and base_price > 0 and len(daily_prices) == 0:
        print(f"\n    ✓ Все 60 цен заполнены из base_price (type=1) = {base_price}")

    # Показываем первые 15 дней массива для наглядности
    if prices_60:
        print(f"\n    Первые 15 дней массива prices_60:")
        for i in range(min(15, len(prices_60))):
            day = today + timedelta(days=i)
            marker = " ← today" if i == 0 else ""
            in_daily = daily_prices.get(day.isoformat(), None)
            source = "season" if in_daily is not None else ("base" if base_price > 0 else "нет")
            print(f"      [{i:2}] {day.isoformat()}: prices_60={prices_60[i]}, "
                  f"источник={source}{marker}")

    return result_diag


# ═══════════════════════════════════════════════════════════════════════
# Шаг 4: Скользящее окно (nights=2)
# (воспроизводит _fetch_availability — ОБНОВЛЁННАЯ ВЕРСИЯ с type=1)
# ═══════════════════════════════════════════════════════════════════════

async def step4_sliding_window(
    page: Page, object_id: str, token: str, nights: int = 2
) -> dict:
    """Определяет занятость каждого дня через скользящее окно.

    Воспроизводит ОБНОВЛЁННУЮ логику listing_service._fetch_availability():
    - Извлечение цены: приоритет season_price → fallback type=1
    - Начинает с today (НЕ today+1)
    - Пакеты по 5 запросов
    - Пауза 0.5с между пакетами

    Args:
        page: Вкладка браузера.
        object_id: ID объявления.
        token: Токен API.
        nights: Количество ночей в окне.

    Returns:
        Словарь с результатами диагностики шага 4.
    """
    today = date.today()

    # ── Формируем days_data (ТОЧНАЯ КОПИЯ listing_service) ──
    days_data = []
    for i in range(_DAYS_COUNT):
        day = today + timedelta(days=i)
        end_day = day + timedelta(days=nights)
        days_data.append({
            "date_begin": f"{day.isoformat()} 14:00:00",
            "date_end": f"{end_day.isoformat()} 11:00:00",
        })

    print(f"    Скользящее окно: {_DAYS_COUNT} дней, nights={nights}, пакет={_API_BATCH_SIZE}")
    print(f"    Диапазон: {days_data[0]['date_begin'][:10]} → {days_data[-1]['date_begin'][:10]}")

    t0 = time.perf_counter()

    # ── Пакетные запросы (ОБНОВЛЁННАЯ ВЕРСИЯ — season_price + type=1 fallback) ──
    api_results = await page.evaluate(
        """
        async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
            const results = [];
            const batches = [];

            for (let i = 0; i < daysData.length; i += batchSize) {
                batches.push(daysData.slice(i, i + batchSize));
            }

            for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                const batch = batches[batchIdx];

                const promises = batch.map(async (dayInfo) => {
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
                                objects: [parseInt(objectId)],
                                rooms_cnt: {},
                                guests: guests,
                                date_begin: dayInfo.date_begin,
                                date_end: dayInfo.date_end,
                                currency_id: 1,
                                is_pets: 0,
                                documents: 0,
                                target: 0,
                                ages: [],
                                no_time: 1
                            })
                        });

                        if (!resp.ok) {
                            return {status: 'error', error: 'http_' + resp.status, date: dayInfo.date_begin};
                        }

                        const data = await resp.json();

                        if (!data.success) {
                            return {status: 'error', error: 'api_false', date: dayInfo.date_begin};
                        }

                        if (!data.data || !data.data.objects || !data.data.objects[0]) {
                            return {status: 'error', error: 'no_data', date: dayInfo.date_begin};
                        }

                        const obj = data.data.objects[0];

                        if (!obj.success) {
                            return {
                                status: 'obj_error',
                                errors: obj.errors || [],
                                error_body: JSON.stringify(obj.errors || []).substring(0, 300),
                                date: dayInfo.date_begin
                            };
                        }

                        /* Извлечение цены: приоритет season_price → type=1 */
                        const detail = obj.data.detail || [];
                        let seasonPrice = 0;
                        let basePrice = 0;
                        for (const d of detail) {
                            if (d.type === 'season_price' && d.cost && !seasonPrice) {
                                seasonPrice = Math.round(d.cost);
                            }
                            if (d.type === 1 && d.cost && !basePrice) {
                                basePrice = Math.round(d.cost);
                            }
                        }
                        const price = seasonPrice || basePrice;

                        return {
                            status: 'ok',
                            busy: obj.data.busy === 'busy',
                            busy_raw: obj.data.busy,
                            price: price,
                            price_source: seasonPrice ? 'season_price' : (basePrice ? 'type_1' : 'none'),
                            detail_count: detail.length,
                            detail_types: detail.map(d => d.type),
                            date: dayInfo.date_begin
                        };

                    } catch (e) {
                        return {status: 'error', error: 'exception_' + e.message, date: dayInfo.date_begin};
                    }
                });

                const batchResults = await Promise.all(promises);
                results.push(...batchResults);

                if (batchIdx < batches.length - 1) {
                    await new Promise(resolve => setTimeout(resolve, batchDelay * 1000));
                }
            }

            return results;
        }
        """,
        {
            "objectId": object_id,
            "token": token,
            "guests": _DEFAULT_GUESTS,
            "daysData": days_data,
            "batchSize": _API_BATCH_SIZE,
            "batchDelay": _API_BATCH_DELAY,
            "apiUrl": _API_PRICES_URL,
        },
    )

    elapsed = time.perf_counter() - t0

    # ── Анализ результатов ──
    stats = {
        "ok": 0, "busy": 0, "unbusy": 0,
        "obj_error": 0, "other_error": 0,
        "prices_nonzero": 0,
        "prices_from_season": 0, "prices_from_type1": 0, "prices_from_none": 0,
    }
    calendar: list[int] = []
    prices_from_window: list[int] = []
    error_samples: list[dict] = []

    for day_idx, day_result in enumerate(api_results):
        status = day_result.get("status", "error")

        if status == "ok":
            stats["ok"] += 1
            if day_result.get("busy", False):
                stats["busy"] += 1
                calendar.append(1)
                prices_from_window.append(0)
            else:
                stats["unbusy"] += 1
                calendar.append(0)
                price = day_result.get("price", 0)
                prices_from_window.append(price)
                if price > 0:
                    stats["prices_nonzero"] += 1

                # Отслеживаем источник цены
                source = day_result.get("price_source", "none")
                if source == "season_price":
                    stats["prices_from_season"] += 1
                elif source == "type_1":
                    stats["prices_from_type1"] += 1
                else:
                    stats["prices_from_none"] += 1
        elif status == "obj_error":
            stats["obj_error"] += 1
            calendar.append(-1)
            prices_from_window.append(0)
            if len(error_samples) < 3:
                error_samples.append({
                    "day": day_idx,
                    "date": day_result.get("date", "?")[:10],
                    "errors": day_result.get("errors", []),
                    "error_body": day_result.get("error_body", ""),
                })
        else:
            stats["other_error"] += 1
            calendar.append(-1)
            prices_from_window.append(0)
            if len(error_samples) < 3:
                error_samples.append({
                    "day": day_idx,
                    "date": day_result.get("date", "?")[:10],
                    "error": day_result.get("error", "unknown"),
                })

    result_diag: dict = {
        "step": f"4_sliding_window_nights_{nights}",
        "listing_id": object_id,
        "nights": nights,
        "elapsed_sec": round(elapsed, 2),
        "stats": stats,
        "calendar": calendar,
        "prices_from_window": prices_from_window,
        "error_samples": error_samples,
        "raw_results_sample": api_results[:5],
    }

    print(f"\n    Время: {elapsed:.1f}с")
    print(f"    ok={stats['ok']} (busy={stats['busy']}, unbusy={stats['unbusy']}), "
          f"obj_error={stats['obj_error']}, other_error={stats['other_error']}")
    print(f"    Цены из окна (ненулевых): {stats['prices_nonzero']}/60")
    print(f"    Источники цен: season_price={stats['prices_from_season']}, "
          f"type_1={stats['prices_from_type1']}, нет={stats['prices_from_none']}")

    if error_samples:
        print(f"\n    Примеры ошибок:")
        for es in error_samples:
            print(f"      День {es.get('day')}: {es}")

    # Если много unbusy но цены=0 — это ключевая проблема
    if stats["unbusy"] > 0 and stats["prices_nonzero"] == 0:
        print(f"\n    {'!' * 50}")
        print(f"    ПРОБЛЕМА: {stats['unbusy']} свободных дней, но цен=0!")
        print(f"    Проверяем detail[] первого успешного unbusy-ответа:")
        for dr in api_results:
            if dr.get("status") == "ok" and not dr.get("busy"):
                print(f"      detail_count={dr.get('detail_count')}, "
                      f"types={dr.get('detail_types')}, "
                      f"price_source={dr.get('price_source')}")
                break
        print(f"    {'!' * 50}")

    return result_diag


# ═══════════════════════════════════════════════════════════════════════
# Шаг 5: Итоговая диагностика одной карточки
# ═══════════════════════════════════════════════════════════════════════

async def diagnose_one_listing(
    page: Page, listing_id: str, label: str, listing_index: int, total: int
) -> dict:
    """Полная диагностика одной проблемной карточки.

    Args:
        page: Вкладка браузера.
        listing_id: ID объявления.
        label: Описание проблемы из логов.
        listing_index: Порядковый номер (для вывода).
        total: Общее количество карточек.

    Returns:
        Полный диагностический отчёт по карточке.
    """
    url = _CARD_URL_TEMPLATE.format(listing_id=listing_id)

    _print_header(f"КАРТОЧКА {listing_index}/{total}: ID={listing_id} ({label})")

    report: dict = {
        "listing_id": listing_id,
        "label": label,
        "url": url,
        "diagnosis": None,
        "steps": {},
    }

    # ── ШАГ 1: Загрузка + токен ──
    _print_section("ШАГ 1: Загрузка страницы и перехват токена")
    step1 = await step1_load_and_capture_token(page, url, listing_id)
    report["steps"]["1_load"] = step1

    token = step1.get("token")
    if not token:
        report["diagnosis"] = "ТОКЕН_НЕ_ПЕРЕХВАЧЕН — страница загрузилась, но API-запросов с токеном не было"
        print(f"\n    ДИАГНОЗ: {report['diagnosis']}")
        return report

    # ── ШАГ 2: Валидация токена ──
    _print_section("ШАГ 2: Валидация токена")
    step2 = await step2_validate_token(page, listing_id, token)
    report["steps"]["2_validate"] = step2

    if not step2.get("token_valid"):
        report["diagnosis"] = f"ТОКЕН_НЕВАЛИДЕН — причина: {step2.get('reason')}"
        print(f"\n    ДИАГНОЗ: {report['diagnosis']}")
        return report

    # ── ШАГ 3: Bulk-запрос на 60 ночей ──
    _print_section("ШАГ 3: Bulk-запрос на 60 ночей (цены)")
    step3 = await step3_bulk_prices(page, listing_id, token)
    report["steps"]["3_bulk"] = step3

    # ── ШАГ 4: Скользящее окно nights=2 ──
    _print_section("ШАГ 4: Скользящее окно (nights=2, занятость + цены)")
    step4 = await step4_sliding_window(page, listing_id, token, nights=2)
    report["steps"]["4_window_2"] = step4

    # Если окно с nights=2 дало много ошибок — пробуем nights=3
    if step4["stats"]["obj_error"] > 30:
        _print_section("ШАГ 4b: Скользящее окно (nights=3, адаптация)")
        step4b = await step4_sliding_window(page, listing_id, token, nights=3)
        report["steps"]["4b_window_3"] = step4b

    # ── ИТОГОВЫЙ ДИАГНОЗ ──
    _print_section(f"ИТОГОВЫЙ ДИАГНОЗ: ID={listing_id}")

    diagnosis_parts: list[str] = []

    # Анализ bulk
    if not step3.get("bulk_success"):
        diagnosis_parts.append(f"BULK_ОШИБКА: {step3.get('error')}")
    elif step3.get("prices_60_nonzero", 0) == 0:
        diagnosis_parts.append(
            "BULK_НЕТ_ЦЕН: ни season_price, ни type=1 не дали цен"
        )
    else:
        base_p = step3.get("base_price_from_type_1", 0)
        season_cnt = step3.get("detail_season_price_count", 0)
        source = (
            f"season_price={season_cnt}шт"
            if season_cnt > 0
            else f"type_1={base_p}руб"
        )
        diagnosis_parts.append(
            f"BULK_OK: {step3.get('prices_60_nonzero')}/60 цен ({source})"
        )

    # Анализ скользящего окна
    window_stats = step4.get("stats", {})
    if window_stats.get("obj_error", 0) > 30:
        diagnosis_parts.append(
            f"ОКНО_МАССОВЫЕ_ОШИБКИ: {window_stats.get('obj_error')} obj_error из 60 "
            f"(вероятно min_nights > 2)"
        )
    elif window_stats.get("unbusy", 0) > 0 and window_stats.get("prices_nonzero", 0) == 0:
        diagnosis_parts.append(
            f"ОКНО_ЦЕНЫ_0: {window_stats.get('unbusy')} свободных дней, "
            f"но prices_nonzero=0"
        )
    else:
        diagnosis_parts.append(
            f"ОКНО: ok={window_stats.get('ok')}, "
            f"busy={window_stats.get('busy')}, "
            f"unbusy={window_stats.get('unbusy')}, "
            f"цен={window_stats.get('prices_nonzero')} "
            f"(season={window_stats.get('prices_from_season', 0)}, "
            f"type1={window_stats.get('prices_from_type1', 0)})"
        )

    report["diagnosis"] = " | ".join(diagnosis_parts)

    for part in diagnosis_parts:
        print(f"    → {part}")

    return report


# ═══════════════════════════════════════════════════════════════════════
# Главная функция
# ═══════════════════════════════════════════════════════════════════════

async def main() -> None:
    """Запускает диагностику для всех проблемных карточек."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    _print_header("ДИАГНОСТИКА ПРОБЛЕМЫ «ЦЕНЫ = 0»")
    print(f"  Дата:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Карточек: {len(PROBLEM_LISTINGS)}")
    print(f"  Метод:    воспроизведение логики listing_service.py (с type=1 fallback)")
    print(f"  Отчёт:    {REPORT_PATH}")

    full_report: dict = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "listings_count": len(PROBLEM_LISTINGS),
            "days_count": _DAYS_COUNT,
            "today": date.today().isoformat(),
        },
        "listings": [],
        "summary": {},
    }

    async with async_playwright() as pw:
        # ── Запуск браузера (ТОЧНАЯ КОПИЯ browser_service) ──
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

        # Stealth (ТОЧНАЯ КОПИЯ browser_service)
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()
        page.set_default_navigation_timeout(60000)

        total = len(PROBLEM_LISTINGS)

        for idx, listing_info in enumerate(PROBLEM_LISTINGS, start=1):
            listing_id = str(listing_info["id"])
            label = str(listing_info["label"])

            try:
                listing_report = await diagnose_one_listing(
                    page, listing_id, label, idx, total
                )
                full_report["listings"].append(listing_report)
            except Exception as e:
                print(f"\n    КРИТИЧЕСКАЯ ОШИБКА при диагностике ID={listing_id}: {e}")
                full_report["listings"].append({
                    "listing_id": listing_id,
                    "label": label,
                    "diagnosis": f"КРИТИЧЕСКАЯ_ОШИБКА: {type(e).__name__}: {str(e)[:200]}",
                })

            # Пауза между карточками
            if idx < total:
                delay = 3.0
                print(f"\n    Пауза {delay}с перед следующей карточкой...")
                await asyncio.sleep(delay)

        await browser.close()

    # ── Сводка ──
    _print_header("ОБЩАЯ СВОДКА")

    diagnoses = [r.get("diagnosis", "?") for r in full_report["listings"]]
    summary_counts: dict[str, int] = {}

    for d in diagnoses:
        # Берём первую часть диагноза (до " | ")
        key = d.split(" | ")[0].split(":")[0] if d else "?"
        summary_counts[key] = summary_counts.get(key, 0) + 1

    full_report["summary"] = {
        "diagnoses": diagnoses,
        "summary_counts": summary_counts,
    }

    for listing_report in full_report["listings"]:
        lid = listing_report.get("listing_id", "?")
        diag = listing_report.get("diagnosis", "?")
        print(f"  ID={lid}: {diag}")

    print(f"\n  Группировка причин:")
    for cause, count in sorted(summary_counts.items(), key=lambda x: -x[1]):
        print(f"    {cause}: {count} карточек")

    # ── Сохранение ──
    REPORT_PATH.write_text(
        json.dumps(full_report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\n  Полный отчёт сохранён: {REPORT_PATH.absolute()}")
    _print_header("ДИАГНОСТИКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    asyncio.run(main())
