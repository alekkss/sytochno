"""Финальный пробник getOrdersByObject — вызов с перехваченным токеном.

Фронтенд карточки загружает занятый календарь экшеном setEmploymentCalendar:
    GET /api/json/orders/getOrdersByObject?object_id=<id>
    → data.calendars[] = [{date_begin, date_end}, ...]
Предыдущая попытка вернула 403 «Application authentication failed» —
запрос был отправлен без заголовка token. Здесь токен перехватывается
со страницы (route interception) и передаётся в fetch.

Если эндпоинт ответит 200 — сравниваем calendars[] с октябрём объекта
923051: ожидаем интервалы, покрывающие ночи 1-7, 11-15 и 20-25 октября,
и ОТСУТСТВИЕ ночей 8-10 и 16-19 — это будет прямым доказательством,
что сайт считает их незанятыми (техблок).

Запуск из корня проекта парсера:

    python -m scripts.api_orders_token_probe
"""

import asyncio
import json
import os
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

# ── Конфигурация ──────────────────────────────────────────────

OBJECT_ID: int = 923051
DETAIL_URL: str = f"https://sutochno.ru/front/searchapp/detail/{OBJECT_ID}"
ORDERS_URL: str = "https://sutochno.ru/api/json/orders/getOrdersByObject"

TOKEN_TIMEOUT: float = 30.0
FETCH_TIMEOUT: int = 45

OUT_JSON: Path = Path("data/api_orders_token_probe.json")

# ── JS: GET-запрос с токеном и полным телом ответа ─────────────

_FETCH_GET_JS = """
async ({apiUrl, headers, fetchTimeout}) => {
    try {
        const controller = new AbortController();
        const tid = setTimeout(() => controller.abort(), fetchTimeout * 1000);
        const resp = await fetch(apiUrl, {
            method: 'GET',
            headers: headers,
            credentials: 'include',
            signal: controller.signal
        });
        clearTimeout(tid);
        const text = await resp.text();
        let json = null;
        try { json = JSON.parse(text); } catch (e) { json = null; }
        return {status: resp.status, text: text.substring(0, 30000), json: json};
    } catch (e) {
        return {status: 0, text: String(e.message), json: null};
    }
}
"""

# ── Перехват токена ───────────────────────────────────────────


async def _intercept_token(page: Page) -> str | None:
    """Загружает страницу карточки и перехватывает сессионный токен API.

    Args:
        page: Страница Playwright.

    Returns:
        Токен или None, если перехватить не удалось.
    """
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
        await page.goto(DETAIL_URL, wait_until="domcontentloaded",
                        timeout=60000)
        elapsed = 0.0
        while elapsed < TOKEN_TIMEOUT and not captured:
            await asyncio.sleep(0.5)
            elapsed += 0.5
    except Exception as e:
        print(f"Предупреждение: страница не загрузилась ({str(e)[:120]})")
    finally:
        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass

    return captured[0] if captured else None


# ── Точка входа ───────────────────────────────────────────────


async def main() -> None:
    """Основной сценарий: токен → вызов эндпоинта → разбор calendars."""
    load_dotenv()

    print("=" * 72)
    print("getOrdersByObject с токеном — эталонный календарь занятости")
    print(f"Объект: {OBJECT_ID}")
    print("=" * 72)

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
        page = await context.new_page()

        print("\n[1/2] Загружаю страницу карточки, перехватываю токен...")
        token = await _intercept_token(page)
        if token is None:
            print("      Токен НЕ перехвачен — перезапустите скрипт.")
            await browser.close()
            return
        print(f"      Токен получен (длина {len(token)}).")

        # ── Вызов эндпоинта с токеном ──
        print("\n[2/2] Вызываю orders/getOrdersByObject (с токеном)...")
        url = f"{ORDERS_URL}?object_id={OBJECT_ID}"

        header_variants: list[tuple[str, dict]] = [
            ("token+1.8+platform", {
                "token": token, "api-version": "1.8",
                "platform": "js", "Accept": "application/json",
            }),
            ("token+1.8", {
                "token": token, "api-version": "1.8",
                "Accept": "application/json",
            }),
            ("token+platform", {
                "token": token, "platform": "js",
                "Accept": "application/json",
            }),
        ]

        results: list[dict] = []
        ok_response: dict | None = None

        for name, hdrs in header_variants:
            resp = await page.evaluate(
                _FETCH_GET_JS,
                {"apiUrl": url, "headers": hdrs, "fetchTimeout": FETCH_TIMEOUT},
            )
            results.append({
                "variant": name, "status": resp["status"],
                "response": resp["json"] if resp["json"] else resp["text"][:3000],
            })
            marker = "OK!" if resp["status"] == 200 else "   "
            print(f"  [{marker}] {name}: HTTP {resp['status']} → "
                  f"{resp['text'][:300]}")
            if resp["status"] == 200 and ok_response is None:
                ok_response = resp["json"]
            await asyncio.sleep(1.5)

        await browser.close()

    # ── Разбор ответа ──
    if ok_response is not None:
        data = (ok_response.get("data") or {})
        inner = data.get("data") if isinstance(data, dict) else None
        calendars = inner.get("calendars") if isinstance(inner, dict) else None

        print("\nКАЛЕНДАРИ ЗАНЯТОСТИ (data.calendars[]):")
        if calendars:
            for c in calendars:
                print(f"  {c.get('date_begin')} → {c.get('date_end')}")

            # Быстрая проверка спорных ночей октября
            busy_nights: set[int] = set()
            for c in calendars:
                try:
                    begin = str(c.get("date_begin", ""))[:10]
                    end = str(c.get("date_end", ""))[:10]
                    b = list(map(int, begin.split("-")))
                    e = list(map(int, end.split("-")))
                    from datetime import date as _d, timedelta as _td
                    cur = _d(b[0], b[1], b[2])
                    last = _d(e[0], e[1], e[2])
                    while cur < last:
                        if cur.year == 2026 and cur.month == 10:
                            busy_nights.add(cur.day)
                        cur += _td(days=1)
                except Exception:
                    continue

            checks = [8, 9, 10, 11, 16, 17, 18, 19, 20, 21, 22, 26]
            print("\nПРОВЕРКА ОКТЯБРЯ (ночь занята, если входит в интервал):")
            for day in checks:
                verdict = "ЗАНЯТА" if day in busy_nights else "СВОБОДНА"
                print(f"  2026-10-{day:02d}: {verdict}")

            print("\nОЖИДАНИЕ (правда заказчика):")
            print("  8,9,10 СВОБОДНЫ; 11 ЗАНЯТА; 16-19 СВОБОДНЫ;")
            print("  20-25 ЗАНЯТЫ; 26 СВОБОДНА.")
        else:
            print("  (поле calendars отсутствует — смотрите сырой ответ)")
    else:
        print("\n200 не получено ни в одном варианте — смотрите сырые ответы.")

    try:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(
            json.dumps({"object_id": OBJECT_ID, "results": results},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nРезультаты сохранены: {OUT_JSON}")
    except Exception as e:
        print(f"\nПредупреждение: не удалось сохранить JSON ({e})")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
