"""Инвентаризация API-эндпоинтов sutochno.ru по JS-бандлам фронтенда.

Цель: получить ПОЛНЫЙ список путей /api/json/*, которые знает фронтенд
сайта, вместе с фрагментами кода вызова (в минифицированном коде рядом
с путём видны имена параметров запроса). Это закрывает вопрос «не все
эндпоинты проверили»: вслепую перебирать бессмысленно — роутер сайта
маскирует несуществующие пути ответом 400 «Bad request», как и
существующие с неверным телом.

Алгоритм:
  1. Загружает страницу карточки (SSR HTML) и извлекает все <script src>
     с CDN (cdn.sutochno.ru/.../_nuxt/*.js).
  2. Скачивает каждый бандл (лимиты: до 80 файлов, до 4 МБ на файл).
  3. Ищет вхождения «api/json/...» и ключевые слова (orders, calendar,
     min_nights, restrict и др.), сохраняя контекст ±500 символов.
  4. Печатает сводный список эндпоинтов и сохраняет полный дамп
     с контекстами в data/api_endpoint_inventory.json.

Запуск из корня проекта парсера:

    python -m scripts.api_endpoint_inventory
"""

import asyncio
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright

# ── Конфигурация ──────────────────────────────────────────────

OBJECT_ID: int = 923051
DETAIL_URL: str = f"https://sutochno.ru/front/searchapp/detail/{OBJECT_ID}"

MAX_BUNDLES: int = 80            # максимум скачиваемых JS-файлов
MAX_BUNDLE_BYTES: int = 4_000_000  # максимум размера одного бандла
DOWNLOAD_TIMEOUT_MS: int = 30000

CONTEXT_RADIUS: int = 500        # символов до/после совпадения
MAX_CONTEXTS_PER_ENDPOINT: int = 3
MAX_CONTEXTS_PER_KEYWORD: int = 4
CONTEXT_PRINT: int = 400         # обрезка контекста при печати

OUT_JSON: Path = Path("data/api_endpoint_inventory.json")

# Ключевые слова для поиска «календарных» следов в коде фронтенда
KEYWORDS: tuple[str, ...] = (
    "getOrders", "objectOrders", "busyDates", "freeDates",
    "min_nights", "minNights", "minStay", "min_stay",
    "getCalendar", "calendarApi", "getRestrictions",
    "getAvailabilities", "orders", "restrictions",
)

# ── Извлечение данных из бандлов ──────────────────────────────


def _scan_bundle(text: str, endpoints: dict, keyword_hits: dict) -> None:
    """Ищет эндпоинты и ключевые слова в тексте одного бандла.

    Args:
        text: Содержимое JS-файла (минифицированный код).
        endpoints: Накопитель {путь: [контексты]} — мутируется.
        keyword_hits: Накопитель {ключевое_слово: [контексты]} — мутируется.
    """
    for m in re.finditer(r"api/json/[A-Za-z0-9_/\-.]+", text):
        path = m.group(0).rstrip(".")
        ctx = text[max(0, m.start() - CONTEXT_RADIUS):
                   m.end() + CONTEXT_RADIUS]
        bucket = endpoints.setdefault(path, [])
        if len(bucket) < MAX_CONTEXTS_PER_ENDPOINT:
            bucket.append(ctx)

    for kw in KEYWORDS:
        for m in re.finditer(re.escape(kw), text):
            ctx = text[max(0, m.start() - CONTEXT_RADIUS):
                       m.end() + CONTEXT_RADIUS]
            bucket = keyword_hits.setdefault(kw, [])
            if len(bucket) < MAX_CONTEXTS_PER_KEYWORD:
                bucket.append(ctx)
            break  # по одному контексту за бандл на слово — хватит


# ── Точка входа ───────────────────────────────────────────────


async def main() -> None:
    """Основной сценарий: страница → список бандлов → скан → сводка."""
    load_dotenv()

    print("=" * 72)
    print("Инвентаризация API-эндпоинтов по JS-бандлам фронтенда")
    print(f"Страница-источник: {DETAIL_URL}")
    print("=" * 72)

    endpoints: dict[str, list[str]] = {}
    keyword_hits: dict[str, list[str]] = {}
    bundle_stats: list[dict] = []

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

        # ── Шаг 1: страница карточки → список <script src> ──
        print("\n[1/3] Загружаю страницу карточки...")
        try:
            await page.goto(DETAIL_URL, wait_until="domcontentloaded",
                            timeout=60000)
            await page.wait_for_timeout(4000)
            html = await page.content()
        except Exception as e:
            print(f"ОШИБКА загрузки страницы: {str(e)[:150]}")
            await browser.close()
            return

        srcs = re.findall(r'<script[^>]+src="([^"]+)"', html)
        urls: list[str] = []
        for src in srcs:
            if src.startswith("//"):
                src = "https:" + src
            elif src.startswith("/"):
                src = "https://sutochno.ru" + src
            if ".js" in src and src not in urls:
                urls.append(src)

        print(f"      Найдено JS-файлов: {len(urls)} (лимит {MAX_BUNDLES})")
        urls = urls[:MAX_BUNDLES]

        # ── Шаг 2: скачивание и скан бандлов ──
        print(f"\n[2/3] Скачиваю и сканирую {len(urls)} бандлов...")
        for i, url in enumerate(urls, 1):
            try:
                resp = await context.request.get(
                    url, timeout=DOWNLOAD_TIMEOUT_MS,
                )
                if resp.status != 200:
                    continue
                body = await resp.text()
                if len(body) > MAX_BUNDLE_BYTES:
                    body = body[:MAX_BUNDLE_BYTES]
                _scan_bundle(body, endpoints, keyword_hits)
                bundle_stats.append({
                    "url": url, "bytes": len(body),
                })
            except Exception:
                continue
            if i % 10 == 0 or i == len(urls):
                print(f"      {i}/{len(urls)} обработано, "
                      f"эндпоинтов найдено: {len(endpoints)}")

        await browser.close()

    # ── Шаг 3: сводка ──
    print(f"\n[3/3] Результаты\n")
    print(f"Скачано бандлов: {len(bundle_stats)}")
    print(f"Уникальных эндпоинтов api/json/*: {len(endpoints)}\n")

    print("ПОЛНЫЙ СПИСОК ЭНДПОИНТОВ:")
    for path in sorted(endpoints.keys()):
        print(f"  {path}")

    interesting = [
        p for p in endpoints
        if any(k.lower() in p.lower() for k in (
            "order", "calendar", "busy", "free", "night", "restrict",
            "availab", "date",
        ))
    ]
    print("\nКАЛЕНДАРНО-ПОДОЗРИТЕЛЬНЫЕ ЭНДПОИНТЫ (с контекстом вызова):")
    if not interesting:
        print("  (не найдено)")
    for path in interesting:
        print(f"\n--- {path} ---")
        for ctx in endpoints[path]:
            compact = re.sub(r"\s+", " ", ctx)[:CONTEXT_PRINT * 2]
            print(f"  ...{compact}...")

    print("\nСЛЕДЫ КЛЮЧЕВЫХ СЛОВ В КОДЕ (первые вхождения):")
    for kw, ctxs in sorted(keyword_hits.items()):
        print(f"\n--- {kw} ({len(ctxs)} фрагм.) ---")
        for ctx in ctxs[:2]:
            compact = re.sub(r"\s+", " ", ctx)[:CONTEXT_PRINT]
            print(f"  ...{compact}...")

    try:
        OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
        OUT_JSON.write_text(
            json.dumps({
                "object_id": OBJECT_ID,
                "bundles": bundle_stats,
                "endpoints": {
                    k: [re.sub(r"\s+", " ", c)[:1600] for c in v]
                    for k, v in endpoints.items()
                },
                "keyword_hits": {
                    k: [re.sub(r"\s+", " ", c)[:1600] for c in v]
                    for k, v in keyword_hits.items()
                },
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nПолная инвентаризация сохранена: {OUT_JSON}")
        print("Перешлите файл и вывод консоли для анализа.")
    except Exception as e:
        print(f"\nПредупреждение: не удалось сохранить JSON ({e})")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
