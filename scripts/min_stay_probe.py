"""Диагностический скрипт-пробник ограничений минимального срока (min-stay).

Проверяет гипотезу: запрос ровно на 1 ночь однозначно определяет статус дня:
  - busy               → день реально занят (продан);
  - unbusy             → день свободен, ограничений нет;
  - ошибка min_nights  → день свободен, но забронировать нельзя
                         (ограничение минимального срока) — кандидат в «Техблок».

Дополнительно пробует окна в 2/4/5/6 ночей, чтобы увидеть, как ответ API
зависит от длины запрошенного периода (эффект «загрязнения периода»:
свободный день рядом с занятым может получить busy при длинном окне).

Скрипт автономен: не использует сервисы парсера, только Playwright и .env.
Запуск из корня проекта парсера:

    python -m scripts.min_stay_probe

Результат: таблица в консоли + JSON-файл data/min_stay_probe_results.json
(его можно переслать для анализа).
"""

import asyncio
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import Page, async_playwright

# ── Конфигурация (при необходимости правьте здесь) ──────────────────

# ID объявления для проверки
OBJECT_ID: int = 923051

# Даты для пробинга. Спорные — из жалобы заказчика; 21, 22, 26 — контрольные
# (реально продано / реально свободна по словам заказчика).
PROBE_DATES: list[date] = [
    date(2026, 10, 8),    # спорная: свободна, но ограничение
    date(2026, 10, 9),    # спорная
    date(2026, 10, 10),   # спорная
    date(2026, 10, 11),   # спорная
    date(2026, 10, 16),   # спорная: снять можно только начиная с 16
    date(2026, 10, 17),   # спорная: с 17 нельзя (минимум 4–5 суток)
    date(2026, 10, 18),   # спорная
    date(2026, 10, 19),   # спорная
    date(2026, 10, 20),   # спорная
    date(2026, 10, 21),   # контроль: реально продано
    date(2026, 10, 22),   # контроль: реально продано
    date(2026, 10, 26),   # контроль: свободна без ограничений
]

# Варианты длины запрошенного периода (ночей)
NIGHTS_VARIANTS: list[int] = [1, 2, 4, 5, 6]

# Количество гостей в запросе (как в основном проходе парсера)
GUESTS: int = 2

# Пауза между запросами (секунды) — щадящий режим для API
REQUEST_PAUSE: float = 1.2

# Ожидание перехвата токена после загрузки страницы (секунды)
TOKEN_TIMEOUT: float = 30.0

# Таймаут одного fetch-запроса внутри браузера (секунды)
FETCH_TIMEOUT: int = 45

# Файл с результатами
RESULTS_PATH: Path = Path("data/min_stay_probe_results.json")

# URL API цен и занятости. Пробуем взять константу парсера; если импорт
# недоступен — документированное значение из README.
try:
    from src.services.listing.constants import API_PRICES_URL
except Exception:
    API_PRICES_URL = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Ключевые слова для распознавания ошибки min_nights в тексте ответа
_MIN_NIGHTS_KEYWORDS: tuple[str, ...] = (
    "минимальн", "ночей", "ночь", "суток", "min_nights", "nights",
)

_WEEKDAYS: tuple[str, ...] = ("пн", "вт", "ср", "чт", "пт", "сб", "вс")

# ── JS: один запрос getPricesAndAvailabilities для одного объекта ────

_FETCH_JS = """
async ({apiUrl, objectId, dateBegin, dateEnd, token, guests, fetchTimeout}) => {
    try {
        const controller = new AbortController();
        const tid = setTimeout(() => controller.abort(), fetchTimeout * 1000);

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
            credentials: 'include',
            signal: controller.signal
        });

        clearTimeout(tid);

        if (!resp.ok) {
            return {ok: false, stage: 'http', error: 'http_' + resp.status};
        }

        const data = await resp.json();

        if (!data.success) {
            return {ok: false, stage: 'api', error: JSON.stringify(data.errors || [])};
        }

        if (!data.data || !data.data.objects || !data.data.objects[0]) {
            return {ok: false, stage: 'api', error: 'no_objects'};
        }

        const obj = data.data.objects[0];

        if (!obj.success) {
            // Сырой текст ошибки объекта — главный диагностический материал
            return {ok: false, stage: 'object', error: JSON.stringify(obj.errors || [])};
        }

        const d = obj.data;
        return {ok: true, busy: d.busy, price: d.price, price_default: d.price_default};

    } catch (e) {
        if (e.name === 'AbortError') {
            return {ok: false, stage: 'fetch', error: 'fetch_timeout'};
        }
        return {ok: false, stage: 'fetch', error: String(e.message)};
    }
}
"""

# ── Вспомогательные функции ──────────────────────────────────────────


def _extract_required_nights(raw_error: str) -> int | None:
    """Пытается извлечь требуемое количество ночей из текста ошибки.

    Args:
        raw_error: Сырой текст ошибки API (JSON-строка или просто текст).

    Returns:
        Число ночей (2–999) или None, если распознать не удалось.
    """
    lowered = raw_error.lower()
    if not any(kw in lowered for kw in _MIN_NIGHTS_KEYWORDS):
        return None

    for num_str in re.findall(r"(\d+)", raw_error):
        num = int(num_str)
        if 2 <= num <= 999:
            return num

    return None


def _resolve_search_url() -> str:
    """Берёт первую заполненную ссылку поиска из .env.

    Returns:
        URL страницы поиска sutochno.ru.

    Raises:
        SystemExit: Если ни одна SUTOCHNO_SEARCH_URL_* не задана.
    """
    for idx in range(1, 9):
        url = os.getenv(f"SUTOCHNO_SEARCH_URL_{idx}", "").strip()
        if url:
            return url

    raise SystemExit(
        "Ошибка: не найдена ни одна переменная SUTOCHNO_SEARCH_URL_1..8 в .env. "
        "Заполните SUTOCHNO_SEARCH_URL_1 и запустите скрипт повторно."
    )


async def _intercept_token(page: Page, search_url: str) -> str | None:
    """Загружает страницу поиска и перехватывает сессионный токен API.

    Использует тот же механизм, что и парсер: перехват заголовка token
    из исходящих запросов к /api/json/*.

    Args:
        page: Страница Playwright.
        search_url: URL страницы поиска.

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
            # Маршрут уже обработан — безвредно, игнорируем
            pass

    await page.route("**/api/json/**", _intercept)

    try:
        await page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=45000,
        )

        elapsed = 0.0
        while elapsed < TOKEN_TIMEOUT and not captured:
            await asyncio.sleep(0.5)
            elapsed += 0.5
    except Exception as e:
        print(f"      Предупреждение: ошибка загрузки страницы: {e}")
    finally:
        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass

    return captured[0] if captured else None


async def _probe_nights(
    page: Page,
    token: str,
    probe_date: date,
    nights: int,
) -> dict:
    """Выполняет один пробный запрос к API и классифицирует ответ.

    При сетевой ошибке выполняет один повтор (пауза 2 секунды).

    Args:
        page: Страница Playwright (с живой сессией).
        token: Сессионный токен API.
        probe_date: Дата заезда для пробы.
        nights: Количество ночей в запрошенном периоде.

    Returns:
        Словарь с результатом: ok/busy/price или ok/stage/error/required.
    """
    args = {
        "apiUrl": API_PRICES_URL,
        "objectId": OBJECT_ID,
        "dateBegin": f"{probe_date.isoformat()} 14:00:00",
        "dateEnd": f"{(probe_date + timedelta(days=nights)).isoformat()} 11:00:00",
        "token": token,
        "guests": GUESTS,
        "fetchTimeout": FETCH_TIMEOUT,
    }

    result: dict = {}
    for attempt in (1, 2):
        result = await page.evaluate(_FETCH_JS, args)

        # Повторяем только сетевые/HTTP-сбои; ошибки объекта не повторяем —
        # они детерминированы (min_nights и т.п.)
        if result.get("ok") or result.get("stage") not in ("fetch", "http"):
            break

        if attempt == 1:
            await asyncio.sleep(2.0)

    if result.get("ok"):
        return {
            "ok": True,
            "busy": result.get("busy"),
            "price": result.get("price"),
        }

    error_text = str(result.get("error", ""))
    return {
        "ok": False,
        "stage": result.get("stage", "?"),
        "error": error_text,
        "required": _extract_required_nights(error_text),
    }


def _cell(rec: dict) -> str:
    """Формирует компактный текст ячейки таблицы результатов."""
    if rec["ok"]:
        if rec["busy"] == "busy":
            return "BUSY"
        if rec["busy"] == "unbusy":
            price = rec.get("price")
            return f"UNBUSY {price}" if price else "UNBUSY"
        return f"busy={rec['busy']!r}"

    if rec.get("required"):
        return f"MIN {rec['required']}"

    return "ERR"


def _verdict(rec: dict) -> str:
    """Формирует итоговый вердикт по пробе с окном в 1 ночь."""
    if rec["ok"]:
        if rec["busy"] == "busy":
            return "ЗАНЯТ (продан)"
        if rec["busy"] == "unbusy":
            return "СВОБОДЕН (открыт)"
        return "НЕИЗВЕСТНО (неожиданный busy)"

    if rec.get("required"):
        return "СВОБОДЕН С ОГРАНИЧЕНИЕМ → Техблок"

    return "НЕ ОПРЕДЕЛЁН (ошибка запроса)"


# ── Точка входа ──────────────────────────────────────────────────────


async def main() -> None:
    """Основной сценарий: токен → пробинг → таблица → JSON-файл."""
    load_dotenv()

    search_url = _resolve_search_url()
    headless = os.getenv("HEADLESS_MODE", "false").lower() == "true"

    print("=" * 72)
    print("Пробник ограничений минимального срока (min-stay)")
    print(f"Объект: {OBJECT_ID} | дат: {len(PROBE_DATES)} | "
          f"окон: {NIGHTS_VARIANTS} | гостей: {GUESTS}")
    print("=" * 72)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=headless)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1366, "height": 768},
            locale="ru-RU",
        )
        page = await context.new_page()

        # ── Шаг 1: токен ──
        print("\n[1/3] Загружаю страницу поиска и перехватываю токен API...")
        token = await _intercept_token(page, search_url)

        if token is None:
            print("      НЕ УДАЛОСЬ перехватить токен за 30 секунд.")
            print("      Возможные причины: CAPTCHA, изменился формат API,")
            print("      страница не загрузилась. Попробуйте перезапустить")
            print("      скрипт или выставить HEADLESS_MODE=false локально.")
            await browser.close()
            return

        print(f"      Токен получен (длина {len(token)}).")

        # ── Шаг 2: пробинг ──
        total = len(PROBE_DATES) * len(NIGHTS_VARIANTS)
        print(f"\n[2/3] Пробинг: {total} запросов "
              f"(пауза {REQUEST_PAUSE} с между запросами)...")

        results: dict[str, dict[int, dict]] = {}
        done = 0

        for probe_date in PROBE_DATES:
            per_nights: dict[int, dict] = {}
            for nights in NIGHTS_VARIANTS:
                rec = await _probe_nights(page, token, probe_date, nights)
                per_nights[nights] = rec
                done += 1
                await asyncio.sleep(REQUEST_PAUSE)

            results[probe_date.isoformat()] = per_nights
            print(f"      {done}/{total} — {probe_date.strftime('%d.%m')} готово")

        await browser.close()

    # ── Шаг 3: вывод ──
    print(f"\n[3/3] Результаты\n")

    header = "Дата        " + "".join(
        f"| n={n:<9}" for n in NIGHTS_VARIANTS
    )
    print(header)
    print("-" * len(header))

    for date_iso, per_nights in results.items():
        d = date.fromisoformat(date_iso)
        row = (
            f"{d.strftime('%d.%m')} {_WEEKDAYS[d.weekday()]}    "
        )
        for n in NIGHTS_VARIANTS:
            row += f"| {_cell(per_nights[n]):<10}"
        print(row)

    # Сырые тексты ошибок для проб с окном в 1 ночь — главный материал
    print("\nСырые ошибки API (окно n=1, первые 300 символов):")
    has_errors = False
    for date_iso, per_nights in results.items():
        rec = per_nights.get(1)
        if rec is not None and not rec["ok"]:
            has_errors = True
            print(f"  {date_iso}: {rec['error'][:300]}")
    if not has_errors:
        print("  (ошибок нет — все пробы n=1 вернули busy/unbusy)")

    # Итоговый вердикт по гипотезе
    print("\n" + "=" * 72)
    print("ИТОГ по гипотезе (классификация дня по одному запросу n=1):")
    print("=" * 72)
    for date_iso, per_nights in results.items():
        rec = per_nights.get(1)
        verdict = _verdict(rec) if rec is not None else "НЕТ ДАННЫХ"
        print(f"  {date_iso}: {verdict}")

    print("\nКак читать итог:")
    print("  ЗАНЯТ                     → в календаре 1 (Продано)")
    print("  СВОБОДЕН                  → в календаре 0 (Свободен)")
    print("  СВОБОДЕН С ОГРАНИЧЕНИЕМ   → кандидат на новый признак (Техблок)")
    print()
    print("Если спорные даты (08–11, 16–20) дают «СВОБОДЕН С ОГРАНИЧЕНИЕМ»,")
    print("а контрольные 21–22 — «ЗАНЯТ» и 26 — «СВОБОДЕН», гипотеза")
    print("подтверждена: одного запроса nights=1 достаточно для точной")
    print("классификации дня.")

    # ── Сохранение JSON ──
    try:
        RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "generated_at": datetime.now().isoformat(),
            "object_id": OBJECT_ID,
            "guests": GUESTS,
            "nights_variants": NIGHTS_VARIANTS,
            "results": results,
        }
        RESULTS_PATH.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"\nРезультаты сохранены: {RESULTS_PATH}")
    except Exception as e:
        print(f"\nПредупреждение: не удалось сохранить JSON ({e})")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
