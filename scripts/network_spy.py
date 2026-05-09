"""Скрипт-разведчик: перехват сетевых запросов и извлечение Vue-стейта на sutochno.ru.

Открывает одну карточку объявления, записывает все XHR/fetch-запросы
и ответы, а также пытается извлечь данные из Vue-стейта компонентов.
Результаты сохраняются в data/spy_report.json.

Запуск:
    python scripts/network_spy.py <URL_КАРТОЧКИ>

Пример:
    python scripts/network_spy.py https://sutochno.ru/moskva/1234567
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from playwright.async_api import Page, Response, async_playwright


# Директория для результатов
DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "spy_report.json"

# Ключевые слова для фильтрации интересных запросов
_INTERESTING_KEYWORDS: list[str] = [
    "calendar",
    "price",
    "avail",
    "booking",
    "object",
    "detail",
    "cost",
    "tariff",
    "rate",
    "occupancy",
    "schedule",
    "dates",
    "calculation",
    "calc",
    "order",
]


def _is_interesting_url(url: str) -> bool:
    """Определяет, содержит ли URL ключевые слова, связанные с ценами/календарём."""
    url_lower = url.lower()
    return any(kw in url_lower for kw in _INTERESTING_KEYWORDS)


async def run_spy(target_url: str) -> None:
    """Основная логика разведчика.

    Args:
        target_url: URL карточки объявления на sutochno.ru.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Хранилище перехваченных данных
    all_requests: list[dict] = []
    interesting_responses: list[dict] = []

    print(f"\n{'═' * 60}")
    print(f"  РАЗВЕДЧИК СЕТЕВЫХ ЗАПРОСОВ — sutochno.ru")
    print(f"{'═' * 60}")
    print(f"  Цель: {target_url}")
    print(f"  Время: {datetime.now().isoformat()}")
    print(f"{'═' * 60}\n")

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

        # Stealth
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()

        # ─────────────────────────────────────────────────────────────
        # Перехватчик ВСЕХ ответов
        # ─────────────────────────────────────────────────────────────

        async def on_response(response: Response) -> None:
            """Обработчик каждого сетевого ответа."""
            url = response.url
            status = response.status
            resource_type = response.request.resource_type

            # Записываем только XHR, Fetch и документы (не картинки, шрифты, CSS)
            if resource_type not in ("xhr", "fetch", "document", "script"):
                return

            request_entry = {
                "url": url,
                "method": response.request.method,
                "status": status,
                "resource_type": resource_type,
                "content_type": response.headers.get("content-type", ""),
            }

            all_requests.append(request_entry)

            # Если URL содержит интересные ключевые слова — сохраняем тело ответа
            if _is_interesting_url(url) and resource_type in ("xhr", "fetch"):
                print(f"  ★ ИНТЕРЕСНЫЙ: {response.request.method} {url}")
                print(f"    Статус: {status} | Тип: {request_entry['content_type']}")

                try:
                    body = await response.text()
                    # Пробуем распарсить как JSON
                    try:
                        body_json = json.loads(body)
                        body_preview = json.dumps(body_json, ensure_ascii=False, indent=2)[:3000]
                    except (json.JSONDecodeError, ValueError):
                        body_preview = body[:3000]

                    interesting_responses.append({
                        "url": url,
                        "method": response.request.method,
                        "status": status,
                        "content_type": request_entry["content_type"],
                        "body_preview": body_preview,
                        "body_length": len(body),
                    })

                    print(f"    Тело ({len(body)} байт): {body_preview[:200]}...")
                    print()

                except Exception as e:
                    print(f"    Ошибка чтения тела: {e}")

        page.on("response", on_response)

        # ─────────────────────────────────────────────────────────────
        # Загрузка страницы
        # ─────────────────────────────────────────────────────────────

        print("[1/5] Загружаем страницу карточки...")
        await page.goto(target_url, wait_until="domcontentloaded")

        print("[2/5] Ждём загрузки контента (15 сек)...")
        await asyncio.sleep(15)

        # ─────────────────────────────────────────────────────────────
        # Извлечение Vue-стейта
        # ─────────────────────────────────────────────────────────────

        print("[3/5] Извлекаем данные из Vue-стейта...")

        vue_data = await page.evaluate("""
            () => {
                const result = {
                    found_vue_app: false,
                    nuxt_data: null,
                    component_data: [],
                    pinia_stores: null,
                    window_nuxt: null,
                    window_initial_state: null,
                };

                // 1. Проверяем __vue_app__
                const app = document.querySelector('#app')?.__vue_app__;
                if (app) {
                    result.found_vue_app = true;
                }

                // 2. Проверяем window.__NUXT__
                if (window.__NUXT__) {
                    try {
                        const nuxtStr = JSON.stringify(window.__NUXT__, null, 2);
                        result.window_nuxt = nuxtStr.substring(0, 10000);
                    } catch(e) {
                        result.window_nuxt = "ошибка_сериализации: " + e.message;
                    }
                }

                // 3. Проверяем useNuxtApp/payload
                if (window.__NUXT_DATA__ || window.__NUXT_PAYLOAD__) {
                    try {
                        const data = window.__NUXT_DATA__ || window.__NUXT_PAYLOAD__;
                        result.nuxt_data = JSON.stringify(data, null, 2).substring(0, 10000);
                    } catch(e) {
                        result.nuxt_data = "ошибка: " + e.message;
                    }
                }

                // 4. Пробуем достать Pinia stores
                if (app && app._context && app._context.provides) {
                    const provides = app._context.provides;
                    // Ищем Pinia
                    for (const key of Object.getOwnPropertySymbols(provides)) {
                        const val = provides[key];
                        if (val && val._s && val._s instanceof Map) {
                            // Это Pinia
                            const stores = {};
                            val._s.forEach((store, name) => {
                                try {
                                    stores[name] = JSON.parse(JSON.stringify(store.$state));
                                } catch(e) {
                                    stores[name] = "ошибка: " + e.message;
                                }
                            });
                            result.pinia_stores = stores;
                        }
                    }
                }

                // 5. Ищем компоненты с данными о ценах/календаре
                const walkTree = (el, depth = 0) => {
                    if (depth > 15 || result.component_data.length > 20) return;

                    const vnode = el.__vue__;
                    const vueEl = el.__vueParentComponent;

                    if (vueEl && vueEl.setupState) {
                        const state = vueEl.setupState;
                        const keys = Object.keys(state);

                        // Ищем ключи, связанные с ценой/календарём
                        const interestingKeys = keys.filter(k => {
                            const kl = k.toLowerCase();
                            return kl.includes('price') || kl.includes('calendar') ||
                                   kl.includes('cost') || kl.includes('avail') ||
                                   kl.includes('date') || kl.includes('occupied') ||
                                   kl.includes('booking') || kl.includes('tariff') ||
                                   kl.includes('rate') || kl.includes('schedule') ||
                                   kl.includes('day') || kl.includes('night');
                        });

                        if (interestingKeys.length > 0) {
                            const data = {};
                            for (const k of interestingKeys) {
                                try {
                                    data[k] = JSON.parse(JSON.stringify(state[k]));
                                } catch(e) {
                                    data[k] = "не_сериализуем";
                                }
                            }
                            result.component_data.push({
                                component: vueEl.type?.__name || vueEl.type?.name || 'unknown',
                                keys: interestingKeys,
                                data: data,
                            });
                        }
                    }

                    for (const child of el.children || []) {
                        walkTree(child, depth + 1);
                    }
                };

                try {
                    walkTree(document.querySelector('#app'));
                } catch(e) {
                    result.component_data.push({error: e.message});
                }

                // 6. Проверяем window.__INITIAL_STATE__ / window.__DATA__
                const windowKeys = Object.keys(window).filter(k => {
                    const kl = k.toLowerCase();
                    return kl.includes('initial') || kl.includes('state') ||
                           kl.includes('data') || kl.includes('config') ||
                           kl.includes('object') || kl.includes('detail');
                });

                if (windowKeys.length > 0) {
                    const windowData = {};
                    for (const k of windowKeys) {
                        try {
                            const val = window[k];
                            if (val && typeof val === 'object') {
                                windowData[k] = JSON.stringify(val, null, 2).substring(0, 5000);
                            }
                        } catch(e) {}
                    }
                    result.window_initial_state = windowData;
                }

                return result;
            }
        """)

        print(f"    Vue app найден: {vue_data.get('found_vue_app')}")
        print(f"    Pinia stores: {'Да' if vue_data.get('pinia_stores') else 'Нет'}")
        print(f"    Компоненты с данными: {len(vue_data.get('component_data', []))}")
        print(f"    window.__NUXT__: {'Да' if vue_data.get('window_nuxt') else 'Нет'}")
        print()

        # ─────────────────────────────────────────────────────────────
        # Клик по датепикеру для провоцирования API-запросов
        # ─────────────────────────────────────────────────────────────

        print("[4/5] Кликаем по датепикеру для провоцирования API-запросов...")

        # Прокрутка к блоку дат
        await page.evaluate("""
            () => {
                const el = document.querySelector('.sc-detail-dates');
                if (el) el.scrollIntoView({behavior: 'smooth', block: 'center'});
            }
        """)
        await asyncio.sleep(2)

        # Клик на «Заезд»
        checkin_block = await page.query_selector(".sc-detail-dates__item_in")
        if checkin_block:
            await checkin_block.click()
            print("    Кликнули по блоку 'Заезд'")
            await asyncio.sleep(3)

            # Пробуем кликнуть на какой-нибудь свободный день
            free_day = await page.query_selector(
                "td.sc-base-datepicker-day:not(.sc-base-datepicker-day_disabled)"
                ":not(.sc-base-datepicker-day_disabled-both) span"
            )
            if free_day:
                await free_day.click()
                print("    Кликнули по свободному дню (заезд)")
                await asyncio.sleep(2)

                # Кликаем ещё раз для выезда
                free_day_2 = await page.query_selector(
                    "td.sc-base-datepicker-day:not(.sc-base-datepicker-day_disabled)"
                    ":not(.sc-base-datepicker-day_disabled-both)"
                    ":not(.sc-base-datepicker-day_selected) span"
                )
                if free_day_2:
                    await free_day_2.click()
                    print("    Кликнули по свободному дню (выезд)")
                    await asyncio.sleep(5)
        else:
            print("    Блок 'Заезд' не найден")

        # ─────────────────────────────────────────────────────────────
        # Финальный сбор всех данных со страницы
        # ─────────────────────────────────────────────────────────────

        print("[5/5] Собираем итоговый Vue-стейт после взаимодействия...")

        vue_data_after = await page.evaluate("""
            () => {
                const result = {
                    pinia_stores_after: null,
                    component_data_after: [],
                };

                const app = document.querySelector('#app')?.__vue_app__;

                // Pinia
                if (app && app._context && app._context.provides) {
                    const provides = app._context.provides;
                    for (const key of Object.getOwnPropertySymbols(provides)) {
                        const val = provides[key];
                        if (val && val._s && val._s instanceof Map) {
                            const stores = {};
                            val._s.forEach((store, name) => {
                                try {
                                    const stateStr = JSON.stringify(store.$state);
                                    // Сохраняем только сторы с данными > 100 символов
                                    if (stateStr.length > 100) {
                                        stores[name] = JSON.parse(stateStr);
                                    }
                                } catch(e) {
                                    stores[name] = "ошибка: " + e.message;
                                }
                            });
                            result.pinia_stores_after = stores;
                        }
                    }
                }

                // Компоненты
                const walkTree = (el, depth = 0) => {
                    if (depth > 20 || result.component_data_after.length > 30) return;
                    const vueEl = el.__vueParentComponent;
                    if (vueEl && vueEl.setupState) {
                        const state = vueEl.setupState;
                        const keys = Object.keys(state);
                        const interestingKeys = keys.filter(k => {
                            const kl = k.toLowerCase();
                            return kl.includes('price') || kl.includes('calendar') ||
                                   kl.includes('cost') || kl.includes('avail') ||
                                   kl.includes('date') || kl.includes('occupied') ||
                                   kl.includes('booking') || kl.includes('tariff') ||
                                   kl.includes('rate') || kl.includes('schedule') ||
                                   kl.includes('day') || kl.includes('night');
                        });
                        if (interestingKeys.length > 0) {
                            const data = {};
                            for (const k of interestingKeys) {
                                try {
                                    const val = JSON.stringify(state[k]);
                                    if (val && val.length > 2) {
                                        data[k] = JSON.parse(val);
                                    }
                                } catch(e) {
                                    data[k] = "не_сериализуем";
                                }
                            }
                            if (Object.keys(data).length > 0) {
                                result.component_data_after.push({
                                    component: vueEl.type?.__name || vueEl.type?.name || 'unknown',
                                    keys: interestingKeys,
                                    data: data,
                                });
                            }
                        }
                    }
                    for (const child of el.children || []) {
                        walkTree(child, depth + 1);
                    }
                };
                try { walkTree(document.querySelector('#app')); } catch(e) {}

                return result;
            }
        """)

        await browser.close()

    # ─────────────────────────────────────────────────────────────────
    # Сохранение отчёта
    # ─────────────────────────────────────────────────────────────────

    report = {
        "meta": {
            "target_url": target_url,
            "timestamp": datetime.now().isoformat(),
            "total_requests": len(all_requests),
            "interesting_responses": len(interesting_responses),
        },
        "all_xhr_fetch_requests": [
            r for r in all_requests if r["resource_type"] in ("xhr", "fetch")
        ],
        "interesting_responses": interesting_responses,
        "vue_state_initial": vue_data,
        "vue_state_after_interaction": vue_data_after,
    }

    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # ─────────────────────────────────────────────────────────────────
    # Вывод сводки
    # ─────────────────────────────────────────────────────────────────

    print(f"\n{'═' * 60}")
    print(f"  РЕЗУЛЬТАТЫ РАЗВЕДКИ")
    print(f"{'═' * 60}")
    print(f"  Всего XHR/Fetch запросов: {len(report['all_xhr_fetch_requests'])}")
    print(f"  Интересных ответов (с ключевыми словами): {len(interesting_responses)}")
    print()

    if report["all_xhr_fetch_requests"]:
        print("  ── Все XHR/Fetch URL ──")
        for req in report["all_xhr_fetch_requests"]:
            marker = " ★" if _is_interesting_url(req["url"]) else ""
            print(f"    {req['method']:4} {req['status']} {req['url'][:120]}{marker}")
        print()

    if vue_data.get("pinia_stores"):
        print("  ── Pinia Stores (до взаимодействия) ──")
        for name in vue_data["pinia_stores"]:
            store_data = vue_data["pinia_stores"][name]
            size = len(json.dumps(store_data, ensure_ascii=False)) if isinstance(store_data, dict) else 0
            print(f"    • {name} ({size} байт)")
        print()

    if vue_data_after.get("pinia_stores_after"):
        print("  ── Pinia Stores (после взаимодействия) ──")
        for name in vue_data_after["pinia_stores_after"]:
            store_data = vue_data_after["pinia_stores_after"][name]
            size = len(json.dumps(store_data, ensure_ascii=False)) if isinstance(store_data, dict) else 0
            print(f"    • {name} ({size} байт)")
        print()

    if vue_data_after.get("component_data_after"):
        print("  ── Vue-компоненты с данными цен/календаря ──")
        for comp in vue_data_after["component_data_after"]:
            print(f"    • {comp.get('component', '?')}: {comp.get('keys', [])}")
        print()

    print(f"  Полный отчёт сохранён: {REPORT_PATH.absolute()}")
    print(f"{'═' * 60}\n")


def main() -> None:
    """Точка входа скрипта."""
    if len(sys.argv) < 2:
        print("Использование: python scripts/network_spy.py <URL_КАРТОЧКИ>")
        print("Пример: python scripts/network_spy.py https://sutochno.ru/moskva/1234567")
        sys.exit(1)

    target_url = sys.argv[1]

    if "sutochno.ru" not in target_url:
        print("ОШИБКА: URL должен быть страницей на sutochno.ru")
        sys.exit(1)

    asyncio.run(run_spy(target_url))


if __name__ == "__main__":
    main()
