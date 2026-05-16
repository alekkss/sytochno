"""Диагностика проблемы «токен не перехвачен» для ID 1562447.

Проверяемые гипотезы:
1. Страница вообще не отправляет запросов к /api/json/ при загрузке
2. Запросы к API есть, но без заголовка «token»
3. Токен находится в localStorage/sessionStorage, но не передаётся в XHR
4. Токен находится в cookies
5. Страница использует другой URL-паттерн для API
6. Токен появляется только после взаимодействия (клик, скролл)
7. Страница показывает капчу/блокировку/редирект
8. Запросы происходят ДО установки перехватчика (race condition)
9. Токен внедрён в HTML/JS через window.__NUXT__ или inline-скрипт

Запуск:
    python scripts/diagnose_token_capture.py

Результат:
    data/diagnose_token_capture_report.json
    + детальный вывод в консоль
"""

import asyncio
import json
import re
import time
from datetime import datetime
from pathlib import Path

from playwright.async_api import Page, Request, Response, Route, async_playwright


# ═══════════════════════════════════════════════════════════════════════
# Конфигурация
# ═══════════════════════════════════════════════════════════════════════

LISTING_ID: str = "1562447"

_CARD_URL_TEMPLATE: str = (
    "https://sutochno.ru/front/searchapp/detail/{listing_id}"
    "?guests_adults=2"
    "&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3"
    "&id=397367&type=city"
    "&price_per=1"
)

_API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Паттерны для перехвата
_API_URL_PATTERNS: list[str] = [
    "sutochno.ru/api/json",
    "sutochno.ru/api/",
    "/api/json/",
    "/api/",
]

# Селекторы готовности страницы
_PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]
_PAGE_READY_TIMEOUT_MS: int = 15000
_NETWORKIDLE_TIMEOUT_MS: int = 15000

# Пути
DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "diagnose_token_capture_report.json"


# ═══════════════════════════════════════════════════════════════════════
# Вспомогательные функции
# ═══════════════════════════════════════════════════════════════════════

def _ts() -> str:
    """Текущее время для лога."""
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def _print_header(text: str) -> None:
    """Заголовок секции."""
    print(f"\n{'═' * 80}")
    print(f"  {text}")
    print(f"{'═' * 80}")


def _print_section(text: str) -> None:
    """Подзаголовок."""
    print(f"\n{'─' * 80}")
    print(f"  {text}")
    print(f"{'─' * 80}\n")


def _v(ok: bool) -> str:
    """Значок результата."""
    return "✓" if ok else "✗"


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 1: Полный перехват ВСЕХ запросов при загрузке
# ═══════════════════════════════════════════════════════════════════════

async def step1_full_request_interception(page: Page, url: str) -> dict:
    """Перехватывает ВСЕ запросы при загрузке страницы.

    Логирует каждый запрос: URL, метод, заголовки, наличие token.
    Цель — понять, отправляются ли вообще API-запросы.

    Args:
        page: Вкладка браузера.
        url: URL карточки.

    Returns:
        Результаты перехвата.
    """
    _print_section("ШАГ 1: Полный перехват ВСЕХ запросов при загрузке страницы")

    all_requests: list[dict] = []
    api_requests: list[dict] = []
    responses_with_token_header: list[dict] = []
    all_responses: list[dict] = []

    def on_request(request: Request) -> None:
        """Перехватчик ВСЕХ исходящих запросов."""
        req_url = request.url
        method = request.method
        headers = request.headers

        req_info = {
            "time": _ts(),
            "method": method,
            "url": req_url[:200],
            "resource_type": request.resource_type,
            "has_token_header": "token" in headers,
            "token_value": headers.get("token", "")[:50] if "token" in headers else "",
            "has_authorization": "authorization" in headers,
            "content_type": headers.get("content-type", ""),
        }

        all_requests.append(req_info)

        # Проверяем, является ли запрос API-запросом
        is_api = any(pattern in req_url for pattern in _API_URL_PATTERNS)
        if is_api:
            api_req_info = {
                **req_info,
                "all_headers": dict(headers),
            }
            api_requests.append(api_req_info)
            token_status = "ДА" if req_info["has_token_header"] else "НЕТ"
            print(f"    [{_ts()}] 🔵 API-ЗАПРОС: {method} {req_url[:120]}")
            print(f"              token в заголовке: {token_status}")
            if req_info["has_token_header"]:
                print(f"              token: {req_info['token_value']}")

    def on_response(response: Response) -> None:
        """Перехватчик ВСЕХ ответов — ищем token в заголовках ответа."""
        resp_headers = response.headers
        resp_url = response.url

        resp_info = {
            "time": _ts(),
            "url": resp_url[:200],
            "status": response.status,
        }
        all_responses.append(resp_info)

        # Проверяем, есть ли token/set-cookie с токеном в ответе
        if "token" in resp_headers or "x-token" in resp_headers:
            token_in_resp = resp_headers.get("token", resp_headers.get("x-token", ""))
            responses_with_token_header.append({
                **resp_info,
                "token_header": token_in_resp[:50],
                "all_headers": dict(resp_headers),
            })
            print(f"    [{_ts()}] 🟢 ОТВЕТ С ТОКЕНОМ: {resp_url[:100]}")
            print(f"              token: {token_in_resp[:50]}")

    page.on("request", on_request)
    page.on("response", on_response)

    print(f"    [{_ts()}] Перехватчики установлены, начинаю загрузку...")
    print(f"    [{_ts()}] URL: {url}")

    t0 = time.perf_counter()

    # ── Загрузка страницы ──
    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        dom_time = time.perf_counter() - t0
        print(f"    [{_ts()}] {_v(True)} domcontentloaded за {dom_time:.2f}с")
        print(f"    [{_ts()}]   HTTP статус: {response.status if response else '?'}")
        print(f"    [{_ts()}]   URL после загрузки: {page.url[:150]}")
    except Exception as e:
        dom_time = time.perf_counter() - t0
        print(f"    [{_ts()}] {_v(False)} ОШИБКА goto за {dom_time:.2f}с: {e}")
        page.remove_listener("request", on_request)
        page.remove_listener("response", on_response)
        return {
            "step": "1_full_interception",
            "error": str(e)[:300],
            "dom_time": dom_time,
        }

    # ── Ожидаем networkidle ──
    t1 = time.perf_counter()
    network_idle = False
    try:
        await page.wait_for_load_state("networkidle", timeout=_NETWORKIDLE_TIMEOUT_MS)
        network_idle = True
        idle_time = time.perf_counter() - t1
        print(f"    [{_ts()}] {_v(True)} networkidle за {idle_time:.2f}с")
    except Exception:
        idle_time = time.perf_counter() - t1
        print(f"    [{_ts()}] ⚠ networkidle НЕ достигнут за {idle_time:.2f}с")

    # ── Ожидаем ключевые селекторы ──
    found_selector = None
    for selector in _PAGE_READY_SELECTORS:
        try:
            await page.wait_for_selector(selector, timeout=_PAGE_READY_TIMEOUT_MS)
            found_selector = selector
            print(f"    [{_ts()}] {_v(True)} Найден селектор: {selector}")
            break
        except Exception:
            print(f"    [{_ts()}] {_v(False)} Селектор НЕ найден: {selector}")

    # ── Дополнительная пауза для поздних API-запросов ──
    print(f"    [{_ts()}] Ожидаю ещё 5с для поздних запросов...")
    await asyncio.sleep(5)

    page.remove_listener("request", on_request)
    page.remove_listener("response", on_response)

    # ── Статистика ──
    total_time = time.perf_counter() - t0
    print(f"\n    {'─' * 60}")
    print(f"    СТАТИСТИКА ЗАПРОСОВ:")
    print(f"    {'─' * 60}")
    print(f"    Всего запросов:           {len(all_requests)}")
    print(f"    API-запросов (/api/):     {len(api_requests)}")
    print(f"    С заголовком token:       {sum(1 for r in api_requests if r['has_token_header'])}")
    print(f"    Ответов с token:          {len(responses_with_token_header)}")
    print(f"    Время загрузки:           {total_time:.2f}с")

    # Выводим ВСЕ API-запросы
    if api_requests:
        print(f"\n    ВСЕ API-ЗАПРОСЫ ({len(api_requests)}):")
        for i, req in enumerate(api_requests):
            print(f"      [{i+1}] [{req['time']}] {req['method']} {req['url'][:100]}")
            print(f"          token={req['has_token_header']}, "
                  f"content-type={req['content_type']}")
            if req['has_token_header']:
                print(f"          TOKEN: {req['token_value']}")
    else:
        print(f"\n    ⚠ API-ЗАПРОСОВ НЕ ОБНАРУЖЕНО!")
        print(f"    Проверяю все запросы на наличие паттернов sutochno...")
        sutochno_requests = [r for r in all_requests if "sutochno" in r["url"]]
        print(f"    Запросов к sutochno.ru: {len(sutochno_requests)}")
        for i, req in enumerate(sutochno_requests[:20]):
            print(f"      [{i+1}] {req['method']} {req['url'][:120]} "
                  f"[{req['resource_type']}]")

    # Типы ресурсов
    resource_types = {}
    for r in all_requests:
        rt = r["resource_type"]
        resource_types[rt] = resource_types.get(rt, 0) + 1
    print(f"\n    Типы ресурсов: {resource_types}")

    return {
        "step": "1_full_interception",
        "total_requests": len(all_requests),
        "api_requests_count": len(api_requests),
        "api_requests_with_token": sum(1 for r in api_requests if r["has_token_header"]),
        "responses_with_token": len(responses_with_token_header),
        "network_idle": network_idle,
        "found_selector": found_selector,
        "total_time_sec": round(total_time, 2),
        "final_url": page.url,
        "api_requests": api_requests,
        "responses_with_token_header": responses_with_token_header,
        "resource_types": resource_types,
        "all_requests_urls": [r["url"][:150] for r in all_requests],
    }


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 2: Проверка localStorage / sessionStorage / cookies
# ═══════════════════════════════════════════════════════════════════════

async def step2_check_storages(page: Page) -> dict:
    """Проверяет все хранилища браузера на наличие токена.

    Args:
        page: Вкладка браузера (уже загружена).

    Returns:
        Результаты проверки хранилищ.
    """
    _print_section("ШАГ 2: Проверка localStorage / sessionStorage / cookies")

    result: dict = {
        "step": "2_check_storages",
        "localStorage_token": None,
        "sessionStorage_token": None,
        "cookies_token": None,
        "localStorage_all_keys": [],
        "sessionStorage_all_keys": [],
        "cookies_all": [],
        "token_like_values": [],
    }

    # ── localStorage ──
    print(f"    [{_ts()}] Проверяю localStorage...")
    try:
        ls_data = await page.evaluate("""
            () => {
                const data = {};
                for (let i = 0; i < localStorage.length; i++) {
                    const key = localStorage.key(i);
                    data[key] = localStorage.getItem(key);
                }
                return data;
            }
        """)
        result["localStorage_all_keys"] = list(ls_data.keys())
        print(f"    [{_ts()}] localStorage содержит {len(ls_data)} ключей:")

        # Ищем токено-подобные значения
        token_keywords = ["token", "auth", "session", "jwt", "api_key", "access"]
        for key, value in ls_data.items():
            is_token_like = any(kw in key.lower() for kw in token_keywords)
            if is_token_like:
                val_preview = str(value)[:80] if value else "(пусто)"
                print(f"      🔑 {key} = {val_preview}")
                result["token_like_values"].append({
                    "source": "localStorage",
                    "key": key,
                    "value": str(value)[:200] if value else "",
                })
                if "token" in key.lower():
                    result["localStorage_token"] = str(value)[:200] if value else None

        # Показываем все ключи
        for key in sorted(ls_data.keys()):
            is_highlighted = any(kw in key.lower() for kw in token_keywords)
            marker = " 🔑" if is_highlighted else ""
            val_preview = str(ls_data[key])[:60] if ls_data[key] else "(пусто)"
            print(f"      {key} = {val_preview}{marker}")

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка чтения localStorage: {e}")
        result["localStorage_error"] = str(e)[:200]

    # ── sessionStorage ──
    print(f"\n    [{_ts()}] Проверяю sessionStorage...")
    try:
        ss_data = await page.evaluate("""
            () => {
                const data = {};
                for (let i = 0; i < sessionStorage.length; i++) {
                    const key = sessionStorage.key(i);
                    data[key] = sessionStorage.getItem(key);
                }
                return data;
            }
        """)
        result["sessionStorage_all_keys"] = list(ss_data.keys())
        print(f"    [{_ts()}] sessionStorage содержит {len(ss_data)} ключей:")

        for key, value in ss_data.items():
            is_token_like = any(kw in key.lower() for kw in token_keywords)
            marker = " 🔑" if is_token_like else ""
            val_preview = str(value)[:60] if value else "(пусто)"
            print(f"      {key} = {val_preview}{marker}")
            if is_token_like:
                result["token_like_values"].append({
                    "source": "sessionStorage",
                    "key": key,
                    "value": str(value)[:200] if value else "",
                })
                if "token" in key.lower():
                    result["sessionStorage_token"] = str(value)[:200] if value else None

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка чтения sessionStorage: {e}")
        result["sessionStorage_error"] = str(e)[:200]

    # ── Cookies ──
    print(f"\n    [{_ts()}] Проверяю cookies...")
    try:
        cookies = await page.context.cookies()
        result["cookies_all"] = [
            {"name": c["name"], "value": c["value"][:100], "domain": c["domain"]}
            for c in cookies
        ]
        print(f"    [{_ts()}] Cookies: {len(cookies)} шт")

        for c in cookies:
            is_token_like = any(kw in c["name"].lower() for kw in token_keywords)
            marker = " 🔑" if is_token_like else ""
            val_preview = c["value"][:60]
            print(f"      {c['name']} = {val_preview} (domain={c['domain']}){marker}")
            if is_token_like:
                result["token_like_values"].append({
                    "source": "cookie",
                    "key": c["name"],
                    "value": c["value"][:200],
                    "domain": c["domain"],
                })
                if "token" in c["name"].lower():
                    result["cookies_token"] = c["value"][:200]

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка чтения cookies: {e}")
        result["cookies_error"] = str(e)[:200]

    # ── Итог ──
    any_token = (
        result["localStorage_token"]
        or result["sessionStorage_token"]
        or result["cookies_token"]
    )
    print(f"\n    [{_ts()}] ИТОГ:")
    print(f"    localStorage token:  {result['localStorage_token'] or 'НЕ НАЙДЕН'}")
    print(f"    sessionStorage token: {result['sessionStorage_token'] or 'НЕ НАЙДЕН'}")
    print(f"    cookies token:        {result['cookies_token'] or 'НЕ НАЙДЕН'}")
    print(f"    Токено-подобных значений всего: {len(result['token_like_values'])}")

    if any_token:
        print(f"    {_v(True)} Токен НАЙДЕН в хранилищах!")
    else:
        print(f"    {_v(False)} Токен НЕ НАЙДЕН ни в одном хранилище")

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 3: Проверка window.__NUXT__ и inline-скриптов
# ═══════════════════════════════════════════════════════════════════════

async def step3_check_nuxt_and_inline(page: Page) -> dict:
    """Ищет токен в window.__NUXT__, window.__STATE__ и inline-скриптах.

    Args:
        page: Вкладка браузера (уже загружена).

    Returns:
        Результаты поиска.
    """
    _print_section("ШАГ 3: Проверка window.__NUXT__ и глобальных переменных")

    result: dict = {
        "step": "3_nuxt_and_inline",
        "nuxt_exists": False,
        "nuxt_keys": [],
        "token_in_nuxt": None,
        "global_token_vars": {},
        "inline_scripts_with_token": [],
    }

    # ── window.__NUXT__ ──
    print(f"    [{_ts()}] Проверяю window.__NUXT__...")
    try:
        nuxt_info = await page.evaluate("""
            () => {
                if (!window.__NUXT__) return {exists: false};

                const nuxt = window.__NUXT__;
                const info = {
                    exists: true,
                    top_keys: Object.keys(nuxt),
                    data_keys: nuxt.data ? Object.keys(nuxt.data) : [],
                    state_keys: nuxt.state ? Object.keys(nuxt.state) : [],
                };

                // Ищем token в data
                if (nuxt.data) {
                    for (const [key, val] of Object.entries(nuxt.data)) {
                        if (val && typeof val === 'object') {
                            // Ищем поле token или apiToken
                            const valStr = JSON.stringify(val).substring(0, 5000);
                            if (valStr.includes('"token"') || valStr.includes('"apiToken"')
                                || valStr.includes('"api_token"')) {
                                info.token_data_key = key;
                                // Пытаемся извлечь
                                if (val.token) info.token_value = String(val.token).substring(0, 100);
                                if (val.apiToken) info.token_value = String(val.apiToken).substring(0, 100);
                                if (val.api_token) info.token_value = String(val.api_token).substring(0, 100);
                            }
                        }
                    }
                }

                // Ищем token в state
                if (nuxt.state) {
                    const stateStr = JSON.stringify(nuxt.state).substring(0, 10000);
                    const tokenMatch = stateStr.match(/"token"\s*:\s*"([^"]+)"/);
                    if (tokenMatch) {
                        info.token_in_state = tokenMatch[1].substring(0, 100);
                    }
                }

                return info;
            }
        """)

        result["nuxt_exists"] = nuxt_info.get("exists", False)
        result["nuxt_keys"] = nuxt_info.get("top_keys", [])

        if nuxt_info.get("exists"):
            print(f"    [{_ts()}] {_v(True)} window.__NUXT__ существует")
            print(f"      top_keys: {nuxt_info.get('top_keys', [])}")
            print(f"      data_keys: {nuxt_info.get('data_keys', [])[:10]}")
            print(f"      state_keys: {nuxt_info.get('state_keys', [])[:10]}")

            if nuxt_info.get("token_value"):
                result["token_in_nuxt"] = nuxt_info["token_value"]
                print(f"      🔑 ТОКЕН НАЙДЕН в __NUXT__: {nuxt_info['token_value'][:50]}...")
            elif nuxt_info.get("token_in_state"):
                result["token_in_nuxt"] = nuxt_info["token_in_state"]
                print(f"      🔑 ТОКЕН НАЙДЕН в __NUXT__.state: {nuxt_info['token_in_state'][:50]}...")
            else:
                print(f"      Токен в __NUXT__ не обнаружен напрямую")
        else:
            print(f"    [{_ts()}] {_v(False)} window.__NUXT__ НЕ существует")

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка при чтении __NUXT__: {e}")
        result["nuxt_error"] = str(e)[:200]

    # ── Глобальные переменные с token ──
    print(f"\n    [{_ts()}] Проверяю глобальные переменные (window.*)...")
    try:
        global_tokens = await page.evaluate("""
            () => {
                const results = {};
                const candidates = [
                    'token', 'apiToken', 'api_token', 'sessionToken',
                    'AUTH_TOKEN', 'API_TOKEN', '__token', '_token',
                    'sutochnoToken', 'appToken'
                ];

                for (const name of candidates) {
                    if (window[name] !== undefined) {
                        results[name] = String(window[name]).substring(0, 100);
                    }
                }

                // Проверяем window.$nuxt
                if (window.$nuxt) {
                    results['$nuxt_exists'] = true;
                    if (window.$nuxt.$store && window.$nuxt.$store.state) {
                        const state = window.$nuxt.$store.state;
                        const stateStr = JSON.stringify(state).substring(0, 20000);
                        const tokenMatch = stateStr.match(/"token"\s*:\s*"([^"]+)"/);
                        if (tokenMatch) {
                            results['$nuxt_store_token'] = tokenMatch[1].substring(0, 100);
                        }
                        // Ищем в auth модуле
                        if (state.auth && state.auth.token) {
                            results['$nuxt_store_auth_token'] = String(state.auth.token).substring(0, 100);
                        }
                        // Ищем в user модуле
                        if (state.user && state.user.token) {
                            results['$nuxt_store_user_token'] = String(state.user.token).substring(0, 100);
                        }
                    }
                }

                return results;
            }
        """)

        result["global_token_vars"] = global_tokens
        if global_tokens:
            print(f"    [{_ts()}] Найдены глобальные переменные:")
            for key, val in global_tokens.items():
                print(f"      🔑 window.{key} = {val}")
        else:
            print(f"    [{_ts()}] Токен-подобных глобальных переменных не найдено")

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка: {e}")
        result["global_vars_error"] = str(e)[:200]

    # ── Inline-скрипты с token ──
    print(f"\n    [{_ts()}] Ищу token в inline-скриптах...")
    try:
        inline_tokens = await page.evaluate("""
            () => {
                const scripts = document.querySelectorAll('script:not([src])');
                const results = [];

                for (const script of scripts) {
                    const text = script.textContent || '';
                    if (text.length > 50000) continue; // пропускаем огромные скрипты

                    // Ищем паттерны token
                    const patterns = [
                        /["']token["']\s*[=:]\s*["']([^"']{10,100})["']/g,
                        /token\s*[=:]\s*["']([^"']{10,100})["']/g,
                        /apiToken\s*[=:]\s*["']([^"']{10,100})["']/g,
                        /headers\s*[=:]\s*\{[^}]*token[^}]*\}/g,
                    ];

                    for (const pattern of patterns) {
                        const matches = [...text.matchAll(pattern)];
                        for (const match of matches) {
                            results.push({
                                match: match[0].substring(0, 150),
                                captured: match[1] ? match[1].substring(0, 100) : null,
                                context: text.substring(
                                    Math.max(0, match.index - 50),
                                    Math.min(text.length, match.index + match[0].length + 50)
                                ).substring(0, 300),
                                script_length: text.length,
                            });
                        }
                    }
                }

                return results;
            }
        """)

        result["inline_scripts_with_token"] = inline_tokens
        if inline_tokens:
            print(f"    [{_ts()}] Найдено {len(inline_tokens)} упоминаний token в скриптах:")
            for i, item in enumerate(inline_tokens[:10]):
                print(f"      [{i+1}] {item.get('match', '')[:100]}")
                if item.get("captured"):
                    print(f"          captured: {item['captured']}")
        else:
            print(f"    [{_ts()}] Token в inline-скриптах не найден")

    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Ошибка: {e}")
        result["inline_error"] = str(e)[:200]

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 4: Провоцируем API-запрос взаимодействием
# ═══════════════════════════════════════════════════════════════════════

async def step4_trigger_api_by_interaction(page: Page) -> dict:
    """Пытается спровоцировать API-запрос через взаимодействие.

    Гипотеза: токен передаётся только когда пользователь
    взаимодействует со страницей (клик на даты, скролл и т.п.)

    Args:
        page: Вкладка браузера (уже загружена).

    Returns:
        Результаты провокации.
    """
    _print_section("ШАГ 4: Провоцируем API-запрос взаимодействием")

    result: dict = {
        "step": "4_trigger_interaction",
        "api_requests_after_scroll": [],
        "api_requests_after_date_click": [],
        "api_requests_after_booking_click": [],
        "tokens_captured": [],
    }

    captured_after: list[dict] = []

    def on_request_after(request: Request) -> None:
        """Ловим запросы после взаимодействия."""
        req_url = request.url
        if any(p in req_url for p in _API_URL_PATTERNS):
            headers = request.headers
            info = {
                "time": _ts(),
                "method": request.method,
                "url": req_url[:150],
                "has_token": "token" in headers,
                "token": headers.get("token", "")[:80],
            }
            captured_after.append(info)
            token_status = "ДА" if info["has_token"] else "НЕТ"
            print(f"    [{_ts()}] 🔵 API после взаимодействия: {req_url[:100]}")
            print(f"              token: {token_status} {info['token'][:40]}")

    page.on("request", on_request_after)

    # ── Скролл ──
    print(f"    [{_ts()}] Пробую скролл вниз...")
    captured_after.clear()
    try:
        await page.evaluate("window.scrollTo(0, 500)")
        await asyncio.sleep(2)
        await page.evaluate("window.scrollTo(0, 1000)")
        await asyncio.sleep(2)
        result["api_requests_after_scroll"] = list(captured_after)
        print(f"    [{_ts()}] После скролла: {len(captured_after)} API-запросов")
    except Exception as e:
        print(f"    [{_ts()}] Ошибка при скролле: {e}")

    # ── Клик на блок дат ──
    print(f"\n    [{_ts()}] Пробую клик на блок дат (.sc-detail-dates)...")
    captured_after.clear()
    try:
        date_block = page.locator(".sc-detail-dates")
        if await date_block.count() > 0:
            await date_block.first.click()
            await asyncio.sleep(3)
            result["api_requests_after_date_click"] = list(captured_after)
            print(f"    [{_ts()}] После клика на даты: {len(captured_after)} API-запросов")
        else:
            print(f"    [{_ts()}] Блок .sc-detail-dates не найден")
            # Пробуем альтернативные селекторы
            alt_selectors = [
                "[data-testid='dates']",
                ".detail-dates",
                ".booking-dates",
                "button:has-text('Заезд')",
                "button:has-text('даты')",
            ]
            for sel in alt_selectors:
                try:
                    loc = page.locator(sel)
                    if await loc.count() > 0:
                        await loc.first.click()
                        await asyncio.sleep(2)
                        print(f"    [{_ts()}] Клик на {sel}: {len(captured_after)} API-запросов")
                        break
                except Exception:
                    continue
    except Exception as e:
        print(f"    [{_ts()}] Ошибка при клике на даты: {e}")

    # ── Клик на кнопку бронирования ──
    print(f"\n    [{_ts()}] Пробую клик на кнопку бронирования...")
    captured_after.clear()
    try:
        booking_selectors = [
            ".sc-detail-aside-price__cost",
            ".sc-detail-hotel-booking__price-sale",
            "button:has-text('Забронировать')",
            "button:has-text('бронировать')",
            ".booking-button",
            "[data-testid='booking-button']",
        ]
        for sel in booking_selectors:
            try:
                loc = page.locator(sel)
                if await loc.count() > 0:
                    await loc.first.click()
                    await asyncio.sleep(3)
                    result["api_requests_after_booking_click"] = list(captured_after)
                    print(f"    [{_ts()}] Клик на {sel}: {len(captured_after)} API-запросов")
                    break
            except Exception:
                continue
    except Exception as e:
        print(f"    [{_ts()}] Ошибка: {e}")

    page.remove_listener("request", on_request_after)

    # ── Собираем все пойманные токены ──
    all_captured = (
        result["api_requests_after_scroll"]
        + result["api_requests_after_date_click"]
        + result["api_requests_after_booking_click"]
    )
    tokens = [r["token"] for r in all_captured if r.get("has_token") and r.get("token")]
    result["tokens_captured"] = tokens

    print(f"\n    [{_ts()}] ИТОГ: токенов поймано после взаимодействий: {len(tokens)}")
    if tokens:
        print(f"    {_v(True)} Первый токен: {tokens[0][:50]}...")
    else:
        print(f"    {_v(False)} Токенов не поймано даже после взаимодействий")

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 5: Проверка на капчу / блокировку / редирект
# ═══════════════════════════════════════════════════════════════════════

async def step5_check_page_content(page: Page) -> dict:
    """Проверяет, не заблокирована ли страница (капча, 403, редирект).

    Args:
        page: Вкладка браузера.

    Returns:
        Результаты проверки.
    """
    _print_section("ШАГ 5: Проверка содержимого страницы (капча/блокировка/редирект)")

    result: dict = {
        "step": "5_page_content_check",
        "current_url": page.url,
        "is_redirected": False,
        "has_captcha": False,
        "has_error_page": False,
        "page_title": "",
        "body_text_preview": "",
        "key_elements_found": {},
    }

    # ── URL ──
    expected_url_part = f"detail/{LISTING_ID}"
    is_redirected = expected_url_part not in page.url
    result["is_redirected"] = is_redirected
    print(f"    [{_ts()}] Текущий URL: {page.url[:150]}")
    print(f"    [{_ts()}] Ожидаемый фрагмент: {expected_url_part}")
    print(f"    [{_ts()}] Редирект: {'ДА ⚠' if is_redirected else 'НЕТ ✓'}")

    # ── Заголовок страницы ──
    try:
        title = await page.title()
        result["page_title"] = title
        print(f"    [{_ts()}] Title: {title[:100]}")
    except Exception as e:
        print(f"    [{_ts()}] Ошибка получения title: {e}")

    # ── Текст страницы ──
    try:
        body_text = await page.evaluate("""
            () => document.body ? document.body.innerText.substring(0, 3000) : ''
        """)
        result["body_text_preview"] = body_text[:1000]

        # Проверяем на капчу
        captcha_markers = [
            "captcha", "капча", "робот", "подтвердите",
            "cloudflare", "challenge", "verify", "human",
            "access denied", "forbidden", "заблокирован",
        ]
        body_lower = body_text.lower()
        for marker in captcha_markers:
            if marker in body_lower:
                result["has_captcha"] = True
                print(f"    [{_ts()}] ⚠ ОБНАРУЖЕН МАРКЕР БЛОКИРОВКИ: «{marker}»")

        # Проверяем на ошибку
        error_markers = [
            "404", "не найден", "ошибка", "error",
            "страница не найдена", "объявление удалено",
        ]
        for marker in error_markers:
            if marker in body_lower:
                result["has_error_page"] = True
                print(f"    [{_ts()}] ⚠ ОБНАРУЖЕН МАРКЕР ОШИБКИ: «{marker}»")

        if not result["has_captcha"] and not result["has_error_page"]:
            print(f"    [{_ts()}] {_v(True)} Капча/блокировка не обнаружена")

        # Первые 500 символов текста
        print(f"\n    Текст страницы (первые 500 символов):")
        for line in body_text[:500].split("\n")[:15]:
            if line.strip():
                print(f"      {line.strip()[:100]}")

    except Exception as e:
        print(f"    [{_ts()}] Ошибка чтения текста: {e}")

    # ── Ключевые элементы ──
    print(f"\n    [{_ts()}] Проверяю ключевые элементы страницы...")
    key_selectors = {
        "цена": ".sc-detail-aside-price__cost, .sc-detail-hotel-booking__price-sale",
        "даты": ".sc-detail-dates",
        "заголовок": "h1",
        "фото": ".sc-detail-gallery, .swiper",
        "карта": ".sc-detail-map, [class*='map']",
        "отзывы": "[class*='review']",
        "характеристики": ".sc-detail-features, [class*='feature']",
        "описание": ".sc-detail-description, [class*='description']",
        "бронирование": "[class*='booking'], [class*='aside']",
    }

    for name, selector in key_selectors.items():
        try:
            count = await page.locator(selector).count()
            result["key_elements_found"][name] = count
            status = _v(count > 0)
            print(f"      {status} {name}: {count} элементов ({selector[:50]})")
        except Exception:
            result["key_elements_found"][name] = -1
            print(f"      ? {name}: ошибка проверки")

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 6: Ручной API-запрос с разными источниками токена
# ═══════════════════════════════════════════════════════════════════════

async def step6_manual_api_test(page: Page, tokens_found: list[str]) -> dict:
    """Тестирует ручной API-запрос с каждым найденным токеном.

    Args:
        page: Вкладка браузера.
        tokens_found: Список токенов из предыдущих шагов.

    Returns:
        Результаты тестирования.
    """
    _print_section("ШАГ 6: Ручной API-запрос с найденными токенами")

    result: dict = {
        "step": "6_manual_api_test",
        "tokens_tested": [],
        "working_token": None,
    }

    if not tokens_found:
        print(f"    [{_ts()}] Нет токенов для тестирования!")
        print(f"    [{_ts()}] Пробую запрос БЕЗ токена и с пустым токеном...")
        tokens_found = ["", "null", "undefined"]

    from datetime import date, timedelta
    today = date.today()
    test_begin = f"{(today + timedelta(days=3)).isoformat()} 14:00:00"
    test_end = f"{(today + timedelta(days=4)).isoformat()} 11:00:00"

    for i, token in enumerate(tokens_found[:5]):
        print(f"\n    [{_ts()}] Тест #{i+1}: token=«{token[:40]}{'...' if len(token) > 40 else ''}»")

        try:
            api_result = await page.evaluate(
                """
                async ({apiUrl, objectId, dateBegin, dateEnd, token, guests}) => {
                    try {
                        const headers = {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'platform': 'js',
                            'api-version': '1.13'
                        };
                        if (token && token !== 'null' && token !== 'undefined') {
                            headers['token'] = token;
                        }

                        const resp = await fetch(apiUrl, {
                            method: 'POST',
                            headers: headers,
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

                        return {
                            http_status: resp.status,
                            success: body ? body.success : false,
                            has_objects: body && body.data && body.data.objects && body.data.objects.length > 0,
                            obj_success: body && body.data && body.data.objects && body.data.objects[0] ? body.data.objects[0].success : null,
                            busy: body && body.data && body.data.objects && body.data.objects[0] && body.data.objects[0].data ? body.data.objects[0].data.busy : null,
                            price: body && body.data && body.data.objects && body.data.objects[0] && body.data.objects[0].data ? body.data.objects[0].data.price : null,
                            raw_preview: text ? text.substring(0, 500) : null,
                            error: null,
                        };
                    } catch (e) {
                        return {error: e.message, http_status: null, success: false};
                    }
                }
                """,
                {
                    "apiUrl": _API_PRICES_URL,
                    "objectId": LISTING_ID,
                    "dateBegin": test_begin,
                    "dateEnd": test_end,
                    "token": token,
                    "guests": 2,
                },
            )

            test_result = {
                "token": token[:50],
                "http_status": api_result.get("http_status"),
                "api_success": api_result.get("success"),
                "obj_success": api_result.get("obj_success"),
                "busy": api_result.get("busy"),
                "price": api_result.get("price"),
                "error": api_result.get("error"),
            }
            result["tokens_tested"].append(test_result)

            is_ok = api_result.get("success") and api_result.get("obj_success")
            print(f"      HTTP: {api_result.get('http_status')}")
            print(f"      api.success: {api_result.get('success')}")
            print(f"      obj.success: {api_result.get('obj_success')}")
            print(f"      busy: {api_result.get('busy')}")
            print(f"      price: {api_result.get('price')}")

            if is_ok:
                result["working_token"] = token
                print(f"      {_v(True)} ТОКЕН РАБОТАЕТ!")
            elif api_result.get("error"):
                print(f"      {_v(False)} Ошибка: {api_result['error'][:100]}")
            else:
                print(f"      {_v(False)} Не работает")
                if api_result.get("raw_preview"):
                    print(f"      raw: {api_result['raw_preview'][:200]}")

        except Exception as e:
            print(f"      {_v(False)} Исключение: {e}")
            result["tokens_tested"].append({
                "token": token[:50],
                "error": str(e)[:200],
            })

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 7: Race condition — перехват ДО goto
# ═══════════════════════════════════════════════════════════════════════

async def step7_early_interception(context, url: str) -> dict:
    """Проверяет, не уходят ли API-запросы ДО установки перехватчика.

    Создаёт НОВУЮ вкладку с перехватчиком, установленным ЗАРАНЕЕ через
    context.on('request'), и отслеживает все запросы с самого начала.

    Args:
        context: Контекст браузера.
        url: URL карточки.

    Returns:
        Результаты.
    """
    _print_section("ШАГ 7: Ранний перехват (через route) — исключаем race condition")

    result: dict = {
        "step": "7_early_interception",
        "api_requests": [],
        "tokens_found": [],
    }

    api_requests_early: list[dict] = []

    # Устанавливаем route ДО создания страницы — гарантирует перехват всего
    async def route_handler(route: Route) -> None:
        """Перехватываем и пропускаем, но логируем."""
        request = route.request
        req_url = request.url
        headers = request.headers

        if any(p in req_url for p in _API_URL_PATTERNS):
            info = {
                "time": _ts(),
                "method": request.method,
                "url": req_url[:150],
                "has_token": "token" in headers,
                "token": headers.get("token", "")[:80],
            }
            api_requests_early.append(info)
            print(f"    [{_ts()}] 🔵 РАННИЙ API: {request.method} {req_url[:100]}")
            print(f"              token: {'ДА ' + info['token'][:40] if info['has_token'] else 'НЕТ'}")

        await route.continue_()

    # route на ВСЕ запросы
    await context.route("**/*", route_handler)

    # Создаём новую вкладку
    page2 = await context.new_page()
    page2.set_default_navigation_timeout(60000)

    print(f"    [{_ts()}] Новая вкладка создана, route установлен")
    print(f"    [{_ts()}] Загружаю {url[:80]}...")

    try:
        await page2.goto(url, wait_until="domcontentloaded", timeout=30000)
        print(f"    [{_ts()}] domcontentloaded")
    except Exception as e:
        print(f"    [{_ts()}] Ошибка goto: {e}")

    # Ждём networkidle
    try:
        await page2.wait_for_load_state("networkidle", timeout=_NETWORKIDLE_TIMEOUT_MS)
        print(f"    [{_ts()}] networkidle")
    except Exception:
        print(f"    [{_ts()}] networkidle не достигнут")

    # Ещё пауза
    await asyncio.sleep(5)

    await context.unroute("**/*", route_handler)
    await page2.close()

    result["api_requests"] = api_requests_early
    result["tokens_found"] = [r["token"] for r in api_requests_early if r.get("has_token") and r["token"]]

    print(f"\n    [{_ts()}] ИТОГ раннего перехвата:")
    print(f"    API-запросов: {len(api_requests_early)}")
    print(f"    Токенов: {len(result['tokens_found'])}")

    if result["tokens_found"]:
        print(f"    {_v(True)} Токен найден через ранний перехват: {result['tokens_found'][0][:50]}...")
    elif api_requests_early:
        print(f"    ⚠ API-запросы ЕСТЬ, но БЕЗ токена")
    else:
        print(f"    {_v(False)} API-запросов НЕТ даже при раннем перехвате")

    return result


# ═══════════════════════════════════════════════════════════════════════
# ШАГ 8: Проверка — делает ли страница API-запросы вообще
#         (через Network.enable CDP)
# ═══════════════════════════════════════════════════════════════════════

async def step8_cdp_network_log(page: Page, url: str) -> dict:
    """Использует CDP (Chrome DevTools Protocol) для низкоуровневого перехвата.

    Playwright иногда пропускает запросы на уровне page.on('request').
    CDP Network.requestWillBeSent ловит ВСЁ.

    Args:
        page: Вкладка (будет использована для CDP-сессии).
        url: URL карточки.

    Returns:
        Результаты CDP-перехвата.
    """
    _print_section("ШАГ 8: CDP-перехват (Network.requestWillBeSent)")

    result: dict = {
        "step": "8_cdp_network",
        "cdp_requests": [],
        "cdp_api_requests": [],
        "tokens_found": [],
    }

    # Создаём CDP-сессию
    try:
        cdp = await page.context.new_cdp_session(page)
    except Exception as e:
        print(f"    [{_ts()}] {_v(False)} Не удалось создать CDP-сессию: {e}")
        result["error"] = str(e)[:200]
        return result

    cdp_requests: list[dict] = []

    def on_cdp_request(params: dict) -> None:
        """Обработчик CDP Network.requestWillBeSent."""
        request_data = params.get("request", {})
        req_url = request_data.get("url", "")
        method = request_data.get("method", "?")
        headers = request_data.get("headers", {})

        # Нормализуем ключи заголовков к lowercase
        headers_lower = {k.lower(): v for k, v in headers.items()}

        is_api = any(p in req_url for p in _API_URL_PATTERNS)

        info = {
            "time": _ts(),
            "method": method,
            "url": req_url[:200],
            "is_api": is_api,
            "has_token": "token" in headers_lower,
            "token": headers_lower.get("token", "")[:80],
        }
        cdp_requests.append(info)

        if is_api:
            token_msg = f"token={info['token'][:40]}" if info["has_token"] else "БЕЗ ТОКЕНА"
            print(f"    [{_ts()}] 🔵 CDP API: {method} {req_url[:100]} [{token_msg}]")

    cdp.on("Network.requestWillBeSent", on_cdp_request)
    await cdp.send("Network.enable")

    print(f"    [{_ts()}] CDP Network.enable — перехват активен")
    print(f"    [{_ts()}] Перезагружаю страницу...")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        print(f"    [{_ts()}] domcontentloaded")
    except Exception as e:
        print(f"    [{_ts()}] Ошибка goto: {e}")

    try:
        await page.wait_for_load_state("networkidle", timeout=_NETWORKIDLE_TIMEOUT_MS)
        print(f"    [{_ts()}] networkidle")
    except Exception:
        print(f"    [{_ts()}] networkidle не достигнут")

    await asyncio.sleep(5)

    await cdp.send("Network.disable")
    await cdp.detach()

    # Анализ
    api_reqs = [r for r in cdp_requests if r["is_api"]]
    tokens = [r["token"] for r in api_reqs if r["has_token"] and r["token"]]

    result["cdp_requests"] = cdp_requests
    result["cdp_api_requests"] = api_reqs
    result["tokens_found"] = tokens

    print(f"\n    [{_ts()}] ИТОГ CDP:")
    print(f"    Всего запросов через CDP: {len(cdp_requests)}")
    print(f"    API-запросов: {len(api_reqs)}")
    print(f"    Токенов найдено: {len(tokens)}")

    if tokens:
        print(f"    {_v(True)} CDP поймал токен: {tokens[0][:50]}...")
    elif api_reqs:
        print(f"    ⚠ CDP: API-запросы ЕСТЬ ({len(api_reqs)}), но БЕЗ токена!")
        for req in api_reqs[:5]:
            print(f"      {req['method']} {req['url'][:100]}")
    else:
        print(f"    {_v(False)} CDP: ВООБЩЕ нет API-запросов при загрузке!")
        print(f"    Это означает: страница НЕ ДЕЛАЕТ запросов к API при загрузке.")
        print(f"    Токен невозможно перехватить через on('request')!")

    return result


# ═══════════════════════════════════════════════════════════════════════
# Главная функция
# ═══════════════════════════════════════════════════════════════════════

async def main() -> None:
    """Запускает полную диагностику перехвата токена для ID 1562447."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    url = _CARD_URL_TEMPLATE.format(listing_id=LISTING_ID)

    _print_header(f"ДИАГНОСТИКА «ТОКЕН НЕ ПЕРЕХВАЧЕН» — ID {LISTING_ID}")
    print(f"  Дата:     {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  URL:      {url[:100]}")
    print(f"  Отчёт:    {REPORT_PATH}")

    full_report: dict = {
        "meta": {
            "timestamp": datetime.now().isoformat(),
            "listing_id": LISTING_ID,
            "url": url,
        },
        "steps": {},
        "final_diagnosis": None,
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

        # Stealth
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()
        page.set_default_navigation_timeout(60000)

        # ── ШАГ 1 ──
        step1 = await step1_full_request_interception(page, url)
        full_report["steps"]["1_full_interception"] = step1

        # ── ШАГ 2 ──
        step2 = await step2_check_storages(page)
        full_report["steps"]["2_storages"] = step2

        # ── ШАГ 3 ──
        step3 = await step3_check_nuxt_and_inline(page)
        full_report["steps"]["3_nuxt_inline"] = step3

        # ── ШАГ 4 ──
        step4 = await step4_trigger_api_by_interaction(page)
        full_report["steps"]["4_interaction"] = step4

        # ── ШАГ 5 ──
        step5 = await step5_check_page_content(page)
        full_report["steps"]["5_page_content"] = step5

        # ── Собираем все найденные токены ──
        all_tokens: list[str] = []

        # Из шага 1 (перехват запросов)
        if step1.get("api_requests"):
            for req in step1["api_requests"]:
                if req.get("has_token_header") and req.get("token_value"):
                    all_tokens.append(req["token_value"])

        # Из шага 2 (хранилища)
        if step2.get("localStorage_token"):
            all_tokens.append(step2["localStorage_token"])
        if step2.get("sessionStorage_token"):
            all_tokens.append(step2["sessionStorage_token"])
        if step2.get("cookies_token"):
            all_tokens.append(step2["cookies_token"])

        # Из шага 3 (__NUXT__)
        if step3.get("token_in_nuxt"):
            all_tokens.append(step3["token_in_nuxt"])
        for var_name, var_val in step3.get("global_token_vars", {}).items():
            if "token" in var_name.lower() and var_val and var_val not in ("true", "false"):
                all_tokens.append(var_val)

        # Из шага 4 (взаимодействие)
        if step4.get("tokens_captured"):
            all_tokens.extend(step4["tokens_captured"])

        # Дедупликация
        all_tokens = list(dict.fromkeys(all_tokens))

        print(f"\n    {'═' * 60}")
        print(f"    ВСЕГО УНИКАЛЬНЫХ ТОКЕНОВ НАЙДЕНО: {len(all_tokens)}")
        for i, t in enumerate(all_tokens):
            print(f"      [{i+1}] {t[:60]}...")
        print(f"    {'═' * 60}")

        # ── ШАГ 6 ──
        step6 = await step6_manual_api_test(page, all_tokens)
        full_report["steps"]["6_manual_test"] = step6

        # ── ШАГ 7 (новая вкладка с ранним перехватом) ──
        step7 = await step7_early_interception(context, url)
        full_report["steps"]["7_early_interception"] = step7

        # Если шаг 7 нашёл токен, тестируем его тоже
        if step7.get("tokens_found"):
            for t in step7["tokens_found"]:
                if t not in all_tokens:
                    all_tokens.append(t)

        # ── ШАГ 8 (CDP) ──
        step8 = await step8_cdp_network_log(page, url)
        full_report["steps"]["8_cdp"] = step8

        if step8.get("tokens_found"):
            for t in step8["tokens_found"]:
                if t not in all_tokens:
                    all_tokens.append(t)

        await browser.close()

    # ═══════════════════════════════════════════════════════════════════
    # ИТОГОВЫЙ ДИАГНОЗ
    # ═══════════════════════════════════════════════════════════════════

    _print_header("ИТОГОВЫЙ ДИАГНОЗ")

    diagnosis_parts: list[str] = []

    # 1. Были ли API-запросы при загрузке?
    api_count = step1.get("api_requests_count", 0)
    api_with_token = step1.get("api_requests_with_token", 0)
    cdp_api_count = len(step8.get("cdp_api_requests", []))

    if api_count == 0 and cdp_api_count == 0:
        diagnosis_parts.append(
            "КОРНЕВАЯ ПРИЧИНА: Страница НЕ отправляет API-запросов при загрузке. "
            "Перехватчик on('request') не может поймать токен, потому что запросов нет. "
            "Решение: извлекать токен из localStorage/sessionStorage/cookies/__NUXT__."
        )
    elif api_count > 0 and api_with_token == 0:
        diagnosis_parts.append(
            f"API-запросы есть ({api_count}), но БЕЗ заголовка token. "
            "Возможно, токен передаётся другим способом или ещё не инициализирован."
        )
    elif api_with_token > 0:
        diagnosis_parts.append(
            f"Токен перехвачен при загрузке ({api_with_token} запросов с token). "
            "Проблема НЕ воспроизводится в этом запуске — возможен race condition."
        )

    # 2. Есть ли токен в хранилищах?
    storage_token = (
        step2.get("localStorage_token")
        or step2.get("sessionStorage_token")
        or step2.get("cookies_token")
    )
    if storage_token:
        diagnosis_parts.append(f"Токен найден в хранилищах: «{storage_token[:40]}...»")
    else:
        diagnosis_parts.append("Токен НЕ найден ни в localStorage, ни в sessionStorage, ни в cookies.")

    # 3. Есть ли токен в __NUXT__?
    nuxt_token = step3.get("token_in_nuxt")
    if nuxt_token:
        diagnosis_parts.append(f"Токен найден в window.__NUXT__: «{nuxt_token[:40]}...»")

    # 4. Удалось ли спровоцировать API-запрос?
    interaction_tokens = step4.get("tokens_captured", [])
    if interaction_tokens:
        diagnosis_parts.append(
            f"Токен появляется ПОСЛЕ взаимодействия (клик/скролл): «{interaction_tokens[0][:40]}...»"
        )

    # 5. Рабочий токен для API?
    working = step6.get("working_token")
    if working:
        diagnosis_parts.append(f"Найден РАБОЧИЙ токен для API: «{working[:40]}...»")
    else:
        diagnosis_parts.append("РАБОЧИЙ токен не найден ни одним способом.")

    # 6. Страница заблокирована?
    if step5.get("has_captcha"):
        diagnosis_parts.append("⚠ ОБНАРУЖЕНА КАПЧА / БЛОКИРОВКА")
    if step5.get("is_redirected"):
        diagnosis_parts.append(f"⚠ РЕДИРЕКТ на: {step5.get('current_url', '?')[:100]}")

    full_report["final_diagnosis"] = diagnosis_parts

    for i, part in enumerate(diagnosis_parts, 1):
        print(f"  {i}. {part}")

    # ── Рекомендация ──
    print(f"\n  {'─' * 60}")
    print(f"  РЕКОМЕНДАЦИЯ:")
    if api_count == 0 and cdp_api_count == 0:
        print(f"  Для этого объявления необходимо извлекать токен НЕ из перехвата")
        print(f"  запросов, а из альтернативного источника:")
        print(f"  1. localStorage/sessionStorage (если token там есть)")
        print(f"  2. cookies")
        print(f"  3. window.__NUXT__.state")
        print(f"  4. Спровоцировать API-запрос кликом на даты")
        print(f"  5. Дождаться ленивой загрузки (увеличить таймаут ожидания)")
    elif api_count > 0 and api_with_token == 0:
        print(f"  API-запросы есть, но без token. Проверьте:")
        print(f"  1. Не изменился ли формат заголовка (X-Token, Authorization)")
        print(f"  2. Не передаётся ли token как query-параметр")
    print(f"  {'─' * 60}")

    # ── Сохранение отчёта ──
    REPORT_PATH.write_text(
        json.dumps(full_report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    _print_header(f"ОТЧЁТ СОХРАНЁН: {REPORT_PATH.absolute()}")


if __name__ == "__main__":
    asyncio.run(main())
