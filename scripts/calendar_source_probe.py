"""Пробник структуры календаря карточки: HTML-контексты + _payload.json.

Часть A — разбор HTML, сохранённого предыдущим пробником
(data/calendar_probe_page.html): ищет спорные даты и ключевые слова
структуры календаря (__NUXT_DATA__, calendar, min-stay и т.п.),
печатает контексты совпадений и сохраняет их в JSON.

Часть B — проверка Nuxt 3 эндпоинта _payload.json для страницы карточки.
Страница карточки рендерится на сервере (SSR), календарь приходит внутри
HTML без отдельных API-запросов. Nuxt обычно умеет отдавать тот же
пейлоад отдельным GET-запросом .../_payload.json — если он доступен,
это дешёвый источник точного посуточного календаря без загрузки страниц.

Запуск из корня проекта парсера:

    python -m scripts.calendar_payload_probe

Результат: сводка в консоли + data/calendar_html_extract.json +
data/calendar_payload_N.json (тела пейлоадов, если получены).
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

HTML_PATH: Path = Path("data/calendar_probe_page.html")
OUT_EXTRACT: Path = Path("data/calendar_html_extract.json")
PAYLOAD_PREFIX: str = "data/calendar_payload"

CONTEXT_RADIUS: int = 700          # символов до/после совпадения
MAX_CONTEXTS_PER_TARGET: int = 6   # максимум контекстов на одну цель
MAX_TOTAL_CONTEXTS: int = 40       # общий лимит сохраняемых контекстов
MAX_PRINT_CONTEXTS: int = 10       # сколько контекстов печатать в консоль
MAX_CONTEXT_PRINT: int = 1100      # обрезка контекста при печати

# Спорные и контрольные даты — маркеры посуточных данных
SEARCH_DATES: tuple[str, ...] = (
    "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11",
    "2026-10-16", "2026-10-17", "2026-10-20",
    "2026-10-21", "2026-10-26",
)

# Ключевые слова структуры календаря (в порядке приоритета вывода)
SEARCH_KEYWORDS: tuple[str, ...] = (
    "__NUXT_DATA__", "window.__NUXT__", "минимальн", "minNights",
    "min_nights", "calendar", "unbusy", "dateBegin", "dayPrices",
    "availableDates", "blockedDates", "occupancy",
)

_TOKEN_TIMEOUT: float = 30.0
_PAYLOAD_CANDIDATES: tuple[str, ...] = (
    f"{DETAIL_URL}/_payload.json",
    f"{DETAIL_URL}/_payload.json?fallback=1",
)

# ── Эвристики ─────────────────────────────────────────────────


def _flags(text: str) -> list[str]:
    """Возвращает метки-признаки календарных данных в тексте.

    Args:
        text: Произвольный текст (HTML или JSON).

    Returns:
        Список меток: упоминания дат, MIN-STAY, CALENDAR, BUSY.
    """
    flags: list[str] = []

    mentioned = [d for d in SEARCH_DATES if d in text]
    if mentioned:
        flags.append("ДАТЫ:" + ",".join(d[5:] for d in mentioned[:6]))

    lowered = text.lower()
    if "минимальн" in lowered or "minnights" in lowered or "min_nights" in lowered:
        flags.append("MIN-STAY")
    if "calendar" in lowered:
        flags.append("CALENDAR")
    if "unbusy" in lowered or '"busy"' in text:
        flags.append("BUSY")

    return flags


# ── Часть A: извлечение контекстов из HTML ────────────────────


def _extract_part(html: str) -> dict[str, dict]:
    """Собирает контексты совпадений по датам и ключевым словам.

    Перекрывающиеся диапазоны объединяются, чтобы не дублировать
    один и тот же фрагмент для соседних совпадений.

    Args:
        html: Полный текст HTML страницы карточки.

    Returns:
        Словарь {цель: {"count": int, "contexts": [str, ...]}}.
    """
    targets: list[str] = list(SEARCH_DATES) + list(SEARCH_KEYWORDS)
    result: dict[str, dict] = {}
    total_saved: int = 0

    for target in targets:
        indices = [m.start() for m in re.finditer(re.escape(target), html)]
        entry: dict = {"count": len(indices), "contexts": []}

        if indices and total_saved < MAX_TOTAL_CONTEXTS:
            # Объединяем перекрывающиеся диапазоны контекстов
            ranges: list[tuple[int, int]] = []
            for idx in indices[: MAX_CONTEXTS_PER_TARGET * 4]:
                start = max(0, idx - CONTEXT_RADIUS)
                end = min(len(html), idx + CONTEXT_RADIUS)
                if ranges and start <= ranges[-1][1]:
                    ranges[-1] = (ranges[-1][0], max(ranges[-1][1], end))
                else:
                    ranges.append((start, end))

            for start, end in ranges[:MAX_CONTEXTS_PER_TARGET]:
                if total_saved >= MAX_TOTAL_CONTEXTS:
                    break
                entry["contexts"].append(html[start:end])
                total_saved += 1

        result[target] = entry

    return result


def _print_part(extract: dict[str, dict]) -> None:
    """Печатает сводку совпадений и наиболее важные контексты."""
    print("\nЧасть A — совпадения в HTML:")
    for target, entry in extract.items():
        print(f"  {target!r}: {entry['count']} совп.")

    print("\nКонтексты (пробелы схлопнуты, обрезаны):")
    shown: int = 0
    for target, entry in extract.items():
        for snippet in entry["contexts"]:
            if shown >= MAX_PRINT_CONTEXTS:
                return
            compact = re.sub(r"\s+", " ", snippet)[:MAX_CONTEXT_PRINT]
            print(f"\n--- {target!r} ---")
            print(f"{compact}")
            shown += 1


# ── Часть B: проверка _payload.json ───────────────────────────


async def _fetch_payloads() -> list[dict]:
    """Загружает страницу (куки и токен), затем запрашивает пейлоады.

    Returns:
        Список результатов: URL, статус, размер, метки, тело.
    """
    results: list[dict] = []

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

        # Перехват токена (на случай, если пейлоад его потребует)
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
            await page.goto(DETAIL_URL, wait_until="domcontentloaded", timeout=60000)
            elapsed = 0.0
            while elapsed < _TOKEN_TIMEOUT and not captured:
                await asyncio.sleep(0.5)
                elapsed += 0.5
        except Exception as e:
            print(f"Предупреждение: страница не загрузилась ({str(e)[:150]})")
        finally:
            try:
                await page.unroute("**/api/json/**")
            except Exception:
                pass

        token = captured[0] if captured else None
        print(f"Токен для пейлоада: {'получен' if token else 'НЕТ — пробуем без него'}")

        headers = {"Accept": "application/json"}
        if token:
            headers["token"] = token

        for i, url in enumerate(_PAYLOAD_CANDIDATES, 1):
            try:
                resp = await context.request.get(url, headers=headers, timeout=30000)
                status = resp.status
                ctype = resp.headers.get("content-type", "")
                body = await resp.text()
            except Exception as e:
                results.append({
                    "url": url, "status": 0, "content_type": "",
                    "length": 0, "flags": [], "body": "",
                    "error": str(e)[:200],
                })
                continue

            results.append({
                "url": url,
                "status": status,
                "content_type": ctype,
                "length": len(body),
                "flags": _flags(body[:200000]),
                "body": body,
            })

            try:
                out = Path(f"{PAYLOAD_PREFIX}_{i}.json")
                out.write_text(body[:500000], encoding="utf-8")
            except Exception as e:
                print(f"Предупреждение: пейлоад {i} не сохранён ({e})")

        await browser.close()

    return results


# ── Точка входа ───────────────────────────────────────────────


async def main() -> None:
    """Основной сценарий: HTML-контексты → проверка пейлоадов → сводка."""
    load_dotenv()

    print("=" * 72)
    print("Пробник структуры календаря (HTML + _payload.json)")
    print(f"Объект: {OBJECT_ID} | {DETAIL_URL}")
    print("=" * 72)

    # ── Часть A ──
    if HTML_PATH.exists():
        html = HTML_PATH.read_text(encoding="utf-8", errors="replace")
        print(f"\n[1/2] HTML: {len(html)} символов, метки: {_flags(html) or 'нет'}")

        extract = _extract_part(html)
        _print_part(extract)

        try:
            OUT_EXTRACT.write_text(
                json.dumps(extract, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"\nКонтексты сохранены: {OUT_EXTRACT}")
        except Exception as e:
            print(f"Предупреждение: JSON контекстов не сохранён ({e})")
    else:
        print(f"\n[1/2] HTML не найден ({HTML_PATH}).")
        print("      Сначала запустите: python -m scripts.calendar_source_probe")

    # ── Часть B ──
    print(f"\n[2/2] Проверка _payload.json ({len(_PAYLOAD_CANDIDATES)} кандидата)...")
    results = await _fetch_payloads()

    for r in results:
        if r.get("error"):
            print(f"  {r['url']} → ОШИБКА: {r['error']}")
            continue

        print(
            f"  {r['url']} → HTTP {r['status']}, {r['length']} байт, "
            f"{r['content_type']}, метки: {r['flags'] or 'нет'}"
        )
        if r["status"] == 200 and r["length"]:
            preview = re.sub(r"\s+", " ", r["body"])[:1500]
            print(f"    начало: {preview}")

    print("\n" + "=" * 72)
    print("ЧТО ИСКАТЬ:")
    print("  1. В контекстах части A — JSON-структуру с датами 2026-10-08 и т.д.:")
    print("     массивы дней с признаками занятости/минимального срока.")
    print("  2. В части B — HTTP 200 у _payload.json: значит точный календарь")
    print("     можно получать одним GET-запросом без полной загрузки страницы.")
    print("=" * 72)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
