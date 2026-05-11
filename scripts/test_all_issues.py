"""Комплексный тестовый скрипт: диагностика всех типов ошибок.

Проверяет на одном объявлении:
1. Время загрузки страницы и достижение networkidle
2. Перехват токена из разных источников
3. Ответы API при nights=1, nights=2, nights=3
4. Один большой запрос на 60 ночей
5. Определение реальной занятости vs ошибки min_nights

Запуск:
    python scripts/test_all_issues.py

Результат сохраняется в data/test_all_issues_report.json
"""

import asyncio
import json
import time
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
    "&price_per=1&is_studio=1&is_apartment=1"
)
API_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
GUESTS: int = 2
DAYS_TO_CHECK: int = 15

DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "test_all_issues_report.json"


def _ts() -> str:
    """Текущее время для лога."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


async def fetch_api(page, listing_id: int, date_begin: str, date_end: str, token: str, guests: int) -> dict:
    """Выполняет один запрос к API через fetch() в контексте браузера."""
    return await page.evaluate(
        """
        async ({apiUrl, listingId, dateBegin, dateEnd, token, guests}) => {
            const startTime = Date.now();
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
                const elapsed = Date.now() - startTime;
                const text = await resp.text();
                let body = null;
                try { body = JSON.parse(text); } catch(e) {}
                return {
                    status: resp.status,
                    elapsed_ms: elapsed,
                    body: body,
                    raw_text: body ? null : text.substring(0, 1000),
                    error: null
                };
            } catch (e) {
                return {
                    status: 0,
                    elapsed_ms: Date.now() - startTime,
                    body: null,
                    raw_text: null,
                    error: e.message
                };
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


async def fetch_batch(page, listing_id: int, days_data: list[dict], token: str, guests: int) -> list[dict]:
    """Выполняет пакетный запрос к API (5 параллельно) — имитация основной программы."""
    return await page.evaluate(
        """
        async ({apiUrl, listingId, daysData, token, guests}) => {
            const results = [];
            const batchSize = 5;
            const batchDelay = 500;

            const batches = [];
            for (let i = 0; i < daysData.length; i += batchSize) {
                batches.push(daysData.slice(i, i + batchSize));
            }

            for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                const batch = batches[batchIdx];

                const promises = batch.map(async (dayInfo) => {
                    const startTime = Date.now();
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
                        const elapsed = Date.now() - startTime;
                        const text = await resp.text();
                        let body = null;
                        try { body = JSON.parse(text); } catch(e) {}

                        // Разбор ответа
                        if (!body) return {date_begin: dayInfo.date_begin, status: 'parse_error', elapsed_ms: elapsed};
                        if (!body.success) return {date_begin: dayInfo.date_begin, status: 'api_false', elapsed_ms: elapsed, body: body};

                        const objects = body.data && body.data.objects;
                        if (!objects || !objects[0]) return {date_begin: dayInfo.date_begin, status: 'no_objects', elapsed_ms: elapsed};

                        const obj = objects[0];
                        if (!obj.success) {
                            return {
                                date_begin: dayInfo.date_begin,
                                status: 'obj_error',
                                elapsed_ms: elapsed,
                                errors: obj.errors || [],
                                obj_data: obj.data || null
                            };
                        }

                        const d = obj.data;
                        return {
                            date_begin: dayInfo.date_begin,
                            status: 'ok',
                            elapsed_ms: elapsed,
                            busy: d.busy,
                            price: d.price,
                            detail: d.detail || [],
                            rooms_available: d.rooms_available,
                            is_booking_now: d.is_booking_now
                        };
                    } catch (e) {
                        return {
                            date_begin: dayInfo.date_begin,
                            status: 'exception',
                            elapsed_ms: Date.now() - startTime,
                            error: e.message
                        };
                    }
                });

                const batchResults = await Promise.all(promises);
                results.push(...batchResults);

                if (batchIdx < batches.length - 1) {
                    await new Promise(resolve => setTimeout(resolve, batchDelay));
                }
            }
            return results;
        }
        """,
        {
            "apiUrl": API_URL,
            "listingId": listing_id,
            "daysData": days_data,
            "token": token,
            "guests": guests,
        },
    )


def _safe_str(value, max_len: int = 10) -> str:
    """Безопасно преобразует значение в строку с ограничением длины."""
    if value is None:
        return "None"
    s = str(value)
    return s[:max_len] if len(s) > max_len else s


def _safe_slice(value, end: int) -> str:
    """Безопасно берёт срез строки (защита от None)."""
    if value is None:
        return "?"
    return str(value)[:end]


async def main() -> None:
    """Основная логика комплексного тестового скрипта."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    report: dict = {"meta": {}, "tests": {}}

    print(f"\n{'═' * 70}")
    print(f"  КОМПЛЕКСНАЯ ДИАГНОСТИКА ВСЕХ ОШИБОК")
    print(f"  Объявление: {LISTING_ID}")
    print(f"  Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'═' * 70}")

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

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 1: Загрузка страницы — время и networkidle
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 1: Загрузка страницы и перехват токена")
        print(f"{'─' * 70}\n")

        captured_tokens: list[dict] = []
        api_requests_seen: list[dict] = []

        def on_request(request: Request) -> None:
            if "/api/json/" in request.url:
                token_val = request.headers.get("token", "")
                entry = {
                    "url": request.url.split("?")[0],
                    "token": token_val[:20] + "..." if token_val else "",
                    "time": _ts(),
                }
                api_requests_seen.append(entry)
                if token_val and not captured_tokens:
                    captured_tokens.append({"token": token_val, "time": _ts()})

        page.on("request", on_request)

        # Замер загрузки domcontentloaded
        t0 = time.perf_counter()
        print(f"  [{_ts()}] goto → domcontentloaded...")

        try:
            await page.goto(LISTING_URL, wait_until="domcontentloaded", timeout=30000)
            t_dom = time.perf_counter() - t0
            print(f"  [{_ts()}] ✓ domcontentloaded за {t_dom:.1f}с")
        except Exception as e:
            t_dom = time.perf_counter() - t0
            print(f"  [{_ts()}] ✗ ОШИБКА domcontentloaded за {t_dom:.1f}с: {e}")
            report["tests"]["page_load"] = {"error": str(e), "time": t_dom}

        # Замер networkidle
        t1 = time.perf_counter()
        print(f"  [{_ts()}] Ждём networkidle (макс 15с)...")

        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
            t_idle = time.perf_counter() - t1
            print(f"  [{_ts()}] ✓ networkidle за {t_idle:.1f}с")
        except Exception:
            t_idle = time.perf_counter() - t1
            print(f"  [{_ts()}] ⚠ networkidle НЕ достигнут за {t_idle:.1f}с (продолжаем)")

        # Дополнительное ожидание для перехвата токена
        if not captured_tokens:
            print(f"  [{_ts()}] Токен ещё не перехвачен, ждём 5с...")
            await asyncio.sleep(5)

        if not captured_tokens:
            print(f"  [{_ts()}] Токен всё ещё не перехвачен, прокрутка + ожидание...")
            await page.evaluate("window.scrollBy(0, 500)")
            await asyncio.sleep(5)

        page.remove_listener("request", on_request)

        # Результат теста 1
        token = captured_tokens[0]["token"] if captured_tokens else None

        test1_result = {
            "domcontentloaded_sec": round(t_dom, 2),
            "networkidle_sec": round(t_idle, 2),
            "token_captured": token is not None,
            "token_capture_time": captured_tokens[0]["time"] if captured_tokens else None,
            "api_requests_during_load": len(api_requests_seen),
            "api_requests_details": api_requests_seen[:10],
        }
        report["tests"]["1_page_load"] = test1_result

        print(f"\n  Результат:")
        print(f"    domcontentloaded: {t_dom:.1f}с")
        print(f"    networkidle: {t_idle:.1f}с")
        print(f"    Токен перехвачен: {'✓ Да' if token else '✗ Нет'}")
        print(f"    API-запросов при загрузке: {len(api_requests_seen)}")
        for req in api_requests_seen[:5]:
            print(f"      [{req['time']}] {req['url']}")

        if not token:
            print(f"\n  ✗ КРИТИЧЕСКАЯ ОШИБКА: Токен не получен.")
            print(f"  Пробуем альтернативный способ (localStorage)...")
            token_alt = await page.evaluate("""
                () => {
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        const val = localStorage.getItem(key);
                        if (key && key.toLowerCase().includes('token')) return val;
                    }
                    const cookies = document.cookie.split(';');
                    for (const c of cookies) {
                        if (c.trim().toLowerCase().startsWith('token='))
                            return c.trim().substring(6);
                    }
                    return null;
                }
            """)
            if token_alt:
                token = token_alt
                print(f"  ✓ Токен найден в localStorage: {token[:20]}...")
            else:
                print(f"  ✗ Токен не найден нигде. Тесты API невозможны.")
                await browser.close()
                REPORT_PATH.write_text(
                    json.dumps(report, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                return

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 2: API с nights=1 (текущее поведение программы)
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 2: API запросы с nights=1 (как сейчас в программе)")
        print(f"{'─' * 70}\n")

        today = datetime.now().date()

        days_1night = []
        for i in range(DAYS_TO_CHECK):
            day = today + timedelta(days=i + 1)
            end = day + timedelta(days=1)
            days_1night.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end.isoformat()} 11:00:00",
            })

        print(f"  Отправляем {DAYS_TO_CHECK} запросов (пакетами по 5)...")
        t2_start = time.perf_counter()
        results_1night = await fetch_batch(page, LISTING_ID, days_1night, token, GUESTS)
        t2_elapsed = time.perf_counter() - t2_start

        # Анализ
        stats_1 = {"ok": 0, "obj_error": 0, "busy": 0, "unbusy": 0, "other_error": 0}
        errors_1: list[str] = []

        for r in results_1night:
            s = r.get("status")
            if s == "ok":
                stats_1["ok"] += 1
                if r.get("busy") == "busy":
                    stats_1["busy"] += 1
                else:
                    stats_1["unbusy"] += 1
            elif s == "obj_error":
                stats_1["obj_error"] += 1
                errs = r.get("errors", [])
                if errs:
                    err_text = errs[0] if isinstance(errs[0], str) else str(errs[0])
                    if err_text not in errors_1:
                        errors_1.append(err_text)
            else:
                stats_1["other_error"] += 1

        report["tests"]["2_nights_1"] = {
            "elapsed_sec": round(t2_elapsed, 2),
            "stats": stats_1,
            "unique_errors": errors_1,
            "results": results_1night,
        }

        print(f"  Время: {t2_elapsed:.1f}с")
        print(f"  Результат:")
        print(f"    Успешных (ok):    {stats_1['ok']} (busy={stats_1['busy']}, unbusy={stats_1['unbusy']})")
        print(f"    obj_error:        {stats_1['obj_error']}")
        print(f"    Другие ошибки:    {stats_1['other_error']}")
        print(f"    Тексты ошибок:    {errors_1}")

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 3: API с nights=2 (адаптация min_nights)
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 3: API запросы с nights=2 (адаптация min_nights)")
        print(f"{'─' * 70}\n")

        days_2nights = []
        for i in range(DAYS_TO_CHECK):
            day = today + timedelta(days=i + 1)
            end = day + timedelta(days=2)
            days_2nights.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end.isoformat()} 11:00:00",
            })

        print(f"  Отправляем {DAYS_TO_CHECK} запросов (пакетами по 5)...")
        t3_start = time.perf_counter()
        results_2nights = await fetch_batch(page, LISTING_ID, days_2nights, token, GUESTS)
        t3_elapsed = time.perf_counter() - t3_start

        stats_2 = {"ok": 0, "obj_error": 0, "busy": 0, "unbusy": 0, "other_error": 0}
        errors_2: list[str] = []

        for r in results_2nights:
            s = r.get("status")
            if s == "ok":
                stats_2["ok"] += 1
                if r.get("busy") == "busy":
                    stats_2["busy"] += 1
                else:
                    stats_2["unbusy"] += 1
            elif s == "obj_error":
                stats_2["obj_error"] += 1
                errs = r.get("errors", [])
                if errs:
                    err_text = errs[0] if isinstance(errs[0], str) else str(errs[0])
                    if err_text not in errors_2:
                        errors_2.append(err_text)
            else:
                stats_2["other_error"] += 1

        report["tests"]["3_nights_2"] = {
            "elapsed_sec": round(t3_elapsed, 2),
            "stats": stats_2,
            "unique_errors": errors_2,
            "results": results_2nights,
        }

        print(f"  Время: {t3_elapsed:.1f}с")
        print(f"  Результат:")
        print(f"    Успешных (ok):    {stats_2['ok']} (busy={stats_2['busy']}, unbusy={stats_2['unbusy']})")
        print(f"    obj_error:        {stats_2['obj_error']}")
        print(f"    Другие ошибки:    {stats_2['other_error']}")
        print(f"    Тексты ошибок:    {errors_2}")

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 4: API с nights=3 (для объектов с min_nights=3)
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 4: API запросы с nights=3")
        print(f"{'─' * 70}\n")

        days_3nights = []
        for i in range(DAYS_TO_CHECK):
            day = today + timedelta(days=i + 1)
            end = day + timedelta(days=3)
            days_3nights.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end.isoformat()} 11:00:00",
            })

        print(f"  Отправляем {DAYS_TO_CHECK} запросов (пакетами по 5)...")
        t4_start = time.perf_counter()
        results_3nights = await fetch_batch(page, LISTING_ID, days_3nights, token, GUESTS)
        t4_elapsed = time.perf_counter() - t4_start

        stats_3 = {"ok": 0, "obj_error": 0, "busy": 0, "unbusy": 0, "other_error": 0}
        errors_3: list[str] = []

        for r in results_3nights:
            s = r.get("status")
            if s == "ok":
                stats_3["ok"] += 1
                if r.get("busy") == "busy":
                    stats_3["busy"] += 1
                else:
                    stats_3["unbusy"] += 1
            elif s == "obj_error":
                stats_3["obj_error"] += 1
                errs = r.get("errors", [])
                if errs:
                    err_text = errs[0] if isinstance(errs[0], str) else str(errs[0])
                    if err_text not in errors_3:
                        errors_3.append(err_text)
            else:
                stats_3["other_error"] += 1

        report["tests"]["4_nights_3"] = {
            "elapsed_sec": round(t4_elapsed, 2),
            "stats": stats_3,
            "unique_errors": errors_3,
            "results": results_3nights,
        }

        print(f"  Время: {t4_elapsed:.1f}с")
        print(f"  Результат:")
        print(f"    Успешных (ok):    {stats_3['ok']} (busy={stats_3['busy']}, unbusy={stats_3['unbusy']})")
        print(f"    obj_error:        {stats_3['obj_error']}")
        print(f"    Другие ошибки:    {stats_3['other_error']}")
        print(f"    Тексты ошибок:    {errors_3}")

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 5: Один большой запрос на 60 ночей
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 5: Один запрос на 60 ночей (получение цен одним запросом)")
        print(f"{'─' * 70}\n")

        start_date = today + timedelta(days=1)
        end_date_60 = start_date + timedelta(days=60)
        date_begin_60 = f"{start_date.isoformat()} 14:00:00"
        date_end_60 = f"{end_date_60.isoformat()} 11:00:00"

        print(f"  Запрос: {date_begin_60} → {date_end_60}")
        t5_start = time.perf_counter()
        response_60 = await fetch_api(page, LISTING_ID, date_begin_60, date_end_60, token, GUESTS)
        t5_elapsed = time.perf_counter() - t5_start

        test5_result: dict = {
            "elapsed_sec": round(t5_elapsed, 2),
            "http_status": response_60.get("status"),
            "api_elapsed_ms": response_60.get("elapsed_ms"),
        }

        daily_prices: dict[str, int] = {}

        body_60 = response_60.get("body")
        if body_60 and body_60.get("success"):
            objects = body_60.get("data", {}).get("objects", [])
            if objects and objects[0].get("success"):
                d60 = objects[0]["data"]
                detail_60 = d60.get("detail", [])

                test5_result.update({
                    "busy": d60.get("busy"),
                    "price_total": d60.get("price"),
                    "rooms_available": d60.get("rooms_available"),
                    "detail_count": len(detail_60),
                    "detail": detail_60,
                })

                print(f"  ✓ Успех!")
                print(f"    busy={d60.get('busy')}, total_price={d60.get('price')}")
                print(f"    rooms_available={d60.get('rooms_available')}")
                print(f"    detail содержит {len(detail_60)} записей (сезонные периоды)")
                print(f"    Время ответа API: {response_60.get('elapsed_ms')}мс")

                # Разворачиваем detail в дневные цены
                for det in detail_60:
                    d_begin = det.get("date_begin")
                    d_end = det.get("date_end")
                    cost = det.get("cost", 0)

                    # Защита от None
                    if not d_begin or not d_end or not cost:
                        continue

                    # Берём только первые 10 символов (дата без времени)
                    d_begin_date = str(d_begin)[:10]
                    d_end_date = str(d_end)[:10]

                    if not d_begin_date or not d_end_date:
                        continue

                    try:
                        period_start = datetime.strptime(d_begin_date, "%Y-%m-%d").date()
                        period_end = datetime.strptime(d_end_date, "%Y-%m-%d").date()
                        current = period_start
                        while current <= period_end:
                            daily_prices[current.isoformat()] = int(cost)
                            current += timedelta(days=1)
                    except (ValueError, TypeError):
                        continue

                test5_result["daily_prices_count"] = len(daily_prices)
                test5_result["daily_prices_sample"] = dict(list(daily_prices.items())[:10])

                print(f"\n    Дневные цены (первые 10 из {len(daily_prices)}):")
                for d, p in list(daily_prices.items())[:10]:
                    print(f"      {d}: {p} руб.")

            elif objects and not objects[0].get("success"):
                errs = objects[0].get("errors", [])
                test5_result["obj_errors"] = errs
                print(f"  ✗ Объект вернул ошибку: {errs}")
            else:
                test5_result["error"] = "no_objects"
                print(f"  ✗ Нет объектов в ответе")
        elif body_60:
            test5_result["error"] = f"success=false"
            print(f"  ✗ API вернул success=false")
        else:
            test5_result["error"] = response_60.get("error") or "unknown"
            print(f"  ✗ Ошибка: {test5_result['error']}")

        report["tests"]["5_sixty_nights"] = test5_result

        # ═══════════════════════════════════════════════════════════════
        # ТЕСТ 6: Сравнение стратегий — построение итоговой таблицы
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'─' * 70}")
        print(f"  ТЕСТ 6: Сравнительная таблица стратегий")
        print(f"{'─' * 70}\n")

        print(f"  {'Дата':<12} {'1ночь':<16} {'2ночи':<16} {'3ночи':<16} {'60н(цена)':<10}")
        print(f"  {'─' * 68}")

        comparison_rows: list[dict] = []

        for i in range(DAYS_TO_CHECK):
            day = today + timedelta(days=i + 1)
            day_str = day.isoformat()

            # nights=1
            r1 = results_1night[i] if i < len(results_1night) else {}
            if r1.get("status") == "ok":
                detail_1 = r1.get("detail", [])
                cost_1 = detail_1[0].get("cost", "?") if detail_1 else "?"
                col1 = f"{'B' if r1.get('busy') == 'busy' else 'F'} {cost_1}"
            elif r1.get("status") == "obj_error":
                errs = r1.get("errors", [])
                col1 = "ERR:min_n"
            else:
                col1 = f"ERR:{_safe_str(r1.get('status'), 8)}"

            # nights=2
            r2 = results_2nights[i] if i < len(results_2nights) else {}
            if r2.get("status") == "ok":
                detail_2 = r2.get("detail", [])
                cost_2 = detail_2[0].get("cost", "?") if detail_2 else "?"
                col2 = f"{'B' if r2.get('busy') == 'busy' else 'F'} {cost_2}"
            elif r2.get("status") == "obj_error":
                col2 = "ERR:min_n"
            else:
                col2 = f"ERR:{_safe_str(r2.get('status'), 8)}"

            # nights=3
            r3 = results_3nights[i] if i < len(results_3nights) else {}
            if r3.get("status") == "ok":
                detail_3 = r3.get("detail", [])
                cost_3 = detail_3[0].get("cost", "?") if detail_3 else "?"
                col3 = f"{'B' if r3.get('busy') == 'busy' else 'F'} {cost_3}"
            elif r3.get("status") == "obj_error":
                col3 = "ERR:min_n"
            else:
                col3 = f"ERR:{_safe_str(r3.get('status'), 8)}"

            # 60 nights (из daily_prices)
            col4 = str(daily_prices.get(day_str, "N/A"))

            print(f"  {day_str:<12} {col1:<16} {col2:<16} {col3:<16} {col4:<10}")

            comparison_rows.append({
                "date": day_str,
                "nights_1": col1,
                "nights_2": col2,
                "nights_3": col3,
                "from_60": col4,
            })

        report["tests"]["6_comparison"] = comparison_rows

        # ═══════════════════════════════════════════════════════════════
        # ИТОГОВЫЕ ВЫВОДЫ
        # ═══════════════════════════════════════════════════════════════
        print(f"\n{'═' * 70}")
        print(f"  ИТОГОВЫЕ ВЫВОДЫ И РЕКОМЕНДАЦИИ")
        print(f"{'═' * 70}")

        print(f"\n  📊 Сводка по стратегиям:")
        print(f"    nights=1: ok={stats_1['ok']:2}, errors={stats_1['obj_error']:2} | "
              f"{'✗ ПРОВАЛ' if stats_1['obj_error'] > stats_1['ok'] else '✓ ОК'}")
        print(f"    nights=2: ok={stats_2['ok']:2}, errors={stats_2['obj_error']:2} | "
              f"{'✗ ПРОВАЛ' if stats_2['obj_error'] > stats_2['ok'] else '✓ ОК'}")
        print(f"    nights=3: ok={stats_3['ok']:2}, errors={stats_3['obj_error']:2} | "
              f"{'✗ ПРОВАЛ' if stats_3['obj_error'] > stats_3['ok'] else '✓ ОК'}")
        print(f"    60 ночей: {'✓ Цены получены (' + str(len(daily_prices)) + ' дней)' if daily_prices else '✗ ПРОВАЛ'}")

        # Определяем оптимальную стратегию
        print(f"\n  🏆 РЕКОМЕНДУЕМАЯ СТРАТЕГИЯ:")
        print(f"    ┌─────────────────────────────────────────────────────────────┐")
        print(f"    │ ГИБРИДНЫЙ ПОДХОД (1 + 60 запросов на карточку):             │")
        print(f"    │                                                             │")
        print(f"    │ Шаг 1: Один запрос на 60 ночей → ВСЕ цены из detail[]      │")
        print(f"    │         (разворачиваем сезонные периоды в дневные цены)     │")
        print(f"    │                                                             │")
        print(f"    │ Шаг 2: 60 запросов с nights=2 (пакетами по 5)              │")
        print(f"    │         → определяем занятость каждого дня (busy/unbusy)    │")
        print(f"    │         При ошибке min_nights → пробуем nights=3,5,7        │")
        print(f"    │                                                             │")
        print(f"    │ Результат: точные цены + точная занятость                   │")
        print(f"    └─────────────────────────────────────────────────────────────┘")

        # Проверка: совпадают ли цены из 60-ночного запроса и из скользящего окна
        if daily_prices and stats_2["ok"] > 0:
            print(f"\n  🔍 Проверка совпадения цен:")
            matches = 0
            mismatches = 0
            for i in range(min(DAYS_TO_CHECK, len(results_2nights))):
                day = today + timedelta(days=i + 1)
                day_str = day.isoformat()
                r2 = results_2nights[i]
                if r2.get("status") == "ok" and r2.get("detail"):
                    cost_from_window = r2["detail"][0].get("cost", 0)
                    cost_from_60 = daily_prices.get(day_str, 0)
                    if cost_from_window == cost_from_60:
                        matches += 1
                    else:
                        mismatches += 1
                        print(f"    ⚠ {day_str}: окно={cost_from_window}, 60ночей={cost_from_60}")

            print(f"    Совпадений: {matches}, Расхождений: {mismatches}")
            if mismatches == 0:
                print(f"    ✓ Цены из обеих стратегий ИДЕНТИЧНЫ!")
                print(f"    → Можно использовать ТОЛЬКО запрос на 60 ночей для цен")
                print(f"    → А скользящее окно — только для определения занятости")

        await browser.close()

    # Сохранение отчёта
    report["meta"] = {
        "listing_id": LISTING_ID,
        "timestamp": datetime.now().isoformat(),
        "days_checked": DAYS_TO_CHECK,
    }

    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print(f"\n  📁 Полный отчёт: {REPORT_PATH.absolute()}")
    print(f"{'═' * 70}\n")


if __name__ == "__main__":
    asyncio.run(main())
