"""Скрипт для перехвата тела POST-запроса к getPricesAndAvailabilities.

Запуск:
    python scripts/spy_request_body.py "URL_КАРТОЧКИ"
"""

import asyncio
import json
import sys
from pathlib import Path

from playwright.async_api import Request, Response, async_playwright

DATA_DIR = Path("data")
REPORT_PATH = DATA_DIR / "spy_request_bodies.json"


async def run(target_url: str) -> None:
    """Перехватывает тела запросов к ключевым API-эндпоинтам."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    captured: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
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
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()

        # Перехватываем ВСЕ запросы и ответы к интересным эндпоинтам
        target_endpoints = [
            "getPricesAndAvailabilities",
            "calculateBookingPrice",
            "checkBookingAbility",
        ]

        async def on_request(request: Request) -> None:
            """Перехватывает тело POST-запроса."""
            url = request.url
            if any(ep in url for ep in target_endpoints):
                post_data = request.post_data
                print(f"\n{'─' * 50}")
                print(f"  REQUEST: {request.method} {url}")
                print(f"  POST body: {post_data}")
                captured.append({
                    "type": "request",
                    "url": url,
                    "method": request.method,
                    "post_body": post_data,
                    "headers": dict(request.headers),
                })

        async def on_response(response: Response) -> None:
            """Перехватывает тело ответа."""
            url = response.url
            if any(ep in url for ep in target_endpoints):
                try:
                    body = await response.text()
                    body_json = json.loads(body)
                    print(f"  RESPONSE: {response.status}")
                    print(f"  Body: {json.dumps(body_json, ensure_ascii=False, indent=2)[:2000]}")
                    captured.append({
                        "type": "response",
                        "url": url,
                        "status": response.status,
                        "body": body_json,
                    })
                except Exception as e:
                    print(f"  Ошибка чтения ответа: {e}")

        page.on("request", on_request)
        page.on("response", on_response)

        print(f"Загружаем: {target_url}")
        await page.goto(target_url, wait_until="domcontentloaded")
        print("Ждём 20 секунд для перехвата всех запросов...")
        await asyncio.sleep(20)

        await browser.close()

    # Сохраняем
    REPORT_PATH.write_text(
        json.dumps(captured, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\nСохранено {len(captured)} записей → {REPORT_PATH}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Использование: python scripts/spy_request_body.py \"URL\"")
        sys.exit(1)
    asyncio.run(run(sys.argv[1]))
