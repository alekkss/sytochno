"""Диагностический скрипт: проверка различимости занятого/свободного дня
по тексту ошибки API при разных значениях nights.

Гипотеза: для занятого дня и свободного дня с min_nights-ограничением API
может возвращать РАЗНЫЕ ошибки. Если так — можно по тексту ошибки точно
определить занятость дня одним запросом с nights=1.

Использование:
    python -m scripts.probe_busy_signature 908727
    python -m scripts.probe_busy_signature 908727 --url "https://sutochno.ru/..."
    python -m scripts.probe_busy_signature 908727 --days 30 --max-nights 4
"""

import argparse
import asyncio
import json
import re
import sys
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# Добавляем корень проекта в PYTHONPATH для импортов src.*
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.services.browser_service import BrowserService

logger = get_logger("probe_busy_signature")

# ─────────────────────────────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────────────────────────────

_API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Количество дней для проверки
_DEFAULT_DAYS: int = 60

# Максимальный nights для probe (1, 2, 3, ..., MAX)
_DEFAULT_MAX_NIGHTS: int = 4

# Параллельность запросов
_BATCH_SIZE: int = 5
_BATCH_DELAY_SEC: float = 0.5

# Количество гостей
_DEFAULT_GUESTS: int = 2

# Каталог отчётов
_REPORT_DIR: Path = PROJECT_ROOT / "data" / "diagnostics"

# Селекторы готовности страницы
_PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]
_PAGE_READY_TIMEOUT_MS: int = 15000
_NETWORKIDLE_TIMEOUT_MS: int = 10000


# ─────────────────────────────────────────────────────────────────────
# Перехват токена (повторяет логику из listing_service.py)
# ─────────────────────────────────────────────────────────────────────


async def _goto_and_capture_token(page: Page, url: str) -> tuple[bool, str | None]:
    """Загружает страницу и перехватывает токен из заголовков запросов к API."""
    captured: list[str] = []

    async def _route_handler(route):
        req = route.request
        if "sutochno.ru/api/json" in req.url:
            token = req.headers.get("token") or req.headers.get("Token")
            if token and not captured:
                captured.append(token)
        await route.continue_()

    await page.route("**/api/json/**", _route_handler)

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        try:
            await page.wait_for_load_state("networkidle", timeout=_NETWORKIDLE_TIMEOUT_MS)
        except Exception:
            pass

        # Ждём появления ключевого селектора (любого)
        for selector in _PAGE_READY_SELECTORS:
            try:
                await page.wait_for_selector(selector, timeout=_PAGE_READY_TIMEOUT_MS)
                break
            except Exception:
                continue

        # Дополнительная пауза для асинхронных API-запросов
        await asyncio.sleep(1.5)
    finally:
        await page.unroute("**/api/json/**")

    return True, (captured[0] if captured else None)


# ─────────────────────────────────────────────────────────────────────
# Probe-запросы
# ─────────────────────────────────────────────────────────────────────


async def _probe_one_day(
    page: Page,
    object_id: str,
    token: str,
    guests: int,
    day: date,
    nights: int,
) -> dict[str, Any]:
    """Отправляет один запрос для указанного дня и nights, возвращает сырой ответ."""
    date_begin = f"{day.isoformat()} 14:00:00"
    date_end = f"{(day + timedelta(days=nights)).isoformat()} 11:00:00"

    result = await page.evaluate(
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

                if (!resp.ok) {
                    return {
                        http_status: resp.status,
                        api_success: false,
                        obj_success: null,
                        obj_errors: ['http_' + resp.status],
                        busy: null,
                        price: null
                    };
                }

                const data = await resp.json();

                const apiSuccess = !!data.success;
                const obj = (data.data && data.data.objects && data.data.objects[0]) || null;

                if (!obj) {
                    return {
                        http_status: 200,
                        api_success: apiSuccess,
                        obj_success: null,
                        obj_errors: ['no_object_in_response'],
                        busy: null,
                        price: null
                    };
                }

                const objSuccess = !!obj.success;
                const objErrors = obj.errors || [];
                const objData = obj.data || {};

                return {
                    http_status: 200,
                    api_success: apiSuccess,
                    obj_success: objSuccess,
                    obj_errors: objErrors,
                    busy: objData.busy || null,
                    price: objData.price || null,
                    price_default: objData.price_default || null,
                    rooms_available: (typeof objData.rooms_available === 'number')
                        ? objData.rooms_available : null,
                    is_booking_now: (typeof objData.is_booking_now === 'boolean')
                        ? objData.is_booking_now : null
                };

            } catch (e) {
                return {
                    http_status: 0,
                    api_success: false,
                    obj_success: null,
                    obj_errors: ['exception_' + e.message],
                    busy: null,
                    price: null
                };
            }
        }
        """,
        {
            "apiUrl": _API_PRICES_URL,
            "objectId": object_id,
            "dateBegin": date_begin,
            "dateEnd": date_end,
            "token": token,
            "guests": guests,
        },
    )

    return {
        "date": day.isoformat(),
        "nights": nights,
        "date_begin": date_begin,
        "date_end": date_end,
        **result,
    }


async def _probe_day_batch(
    page: Page,
    object_id: str,
    token: str,
    guests: int,
    days: list[date],
    nights: int,
) -> list[dict[str, Any]]:
    """Параллельный probe батча дней для одного значения nights."""
    results: list[dict[str, Any]] = []
    for i in range(0, len(days), _BATCH_SIZE):
        chunk = days[i : i + _BATCH_SIZE]
        chunk_results = await asyncio.gather(
            *[
                _probe_one_day(page, object_id, token, guests, day, nights)
                for day in chunk
            ]
        )
        results.extend(chunk_results)
        if i + _BATCH_SIZE < len(days):
            await asyncio.sleep(_BATCH_DELAY_SEC)
    return results


# ─────────────────────────────────────────────────────────────────────
# Анализ сигнатур
# ─────────────────────────────────────────────────────────────────────


_NUMBER_RE = re.compile(r"\d+")


def _normalize_error_text(errors: list[Any]) -> str:
    """Приводит список ошибок к нормализованному виду (цифры → '<N>')."""
    if not errors:
        return ""
    text = " | ".join(str(e) for e in errors)
    return _NUMBER_RE.sub("<N>", text).strip().lower()


def _signature_for_response(resp: dict[str, Any]) -> str:
    """Сигнатура одного ответа: «обнаруживаемый класс ответа»."""
    if resp.get("http_status") != 200:
        return f"http:{resp.get('http_status')}"
    if not resp.get("api_success"):
        return "api_false"
    if resp.get("obj_success") is None:
        return "no_object"
    if resp.get("obj_success"):
        busy = resp.get("busy") or "null"
        return f"ok_busy:{busy}"
    # obj_success = False → главный кейс
    norm = _normalize_error_text(resp.get("obj_errors", []))
    return f"obj_error:{norm}" if norm else "obj_error:<empty>"


def _analyze_per_day(
    matrix: dict[int, list[dict[str, Any]]],
    days: list[date],
) -> list[dict[str, Any]]:
    """По каждому дню собирает картину ответов при разных nights."""
    analysis: list[dict[str, Any]] = []

    for idx, day in enumerate(days):
        row: dict[str, Any] = {
            "day_index": idx,
            "date": day.isoformat(),
            "by_nights": {},
        }

        # Кратко: для каждого nights — сигнатура + ключевые поля
        first_success_nights: int | None = None
        first_success_busy: str | None = None

        for nights, results in matrix.items():
            resp = results[idx]
            sig = _signature_for_response(resp)
            row["by_nights"][str(nights)] = {
                "signature": sig,
                "obj_success": resp.get("obj_success"),
                "busy": resp.get("busy"),
                "price": resp.get("price"),
                "obj_errors": resp.get("obj_errors"),
            }
            if resp.get("obj_success") and first_success_nights is None:
                first_success_nights = nights
                first_success_busy = resp.get("busy")

        row["first_success_nights"] = first_success_nights
        row["first_success_busy"] = first_success_busy
        analysis.append(row)

    return analysis


def _build_summary(
    matrix: dict[int, list[dict[str, Any]]],
    per_day: list[dict[str, Any]],
) -> dict[str, Any]:
    """Сводная аналитика по всем дням и nights."""
    # 1. Уникальные сигнатуры ответов с подсчётом
    signature_counter: Counter[str] = Counter()
    for nights, results in matrix.items():
        for resp in results:
            signature_counter[_signature_for_response(resp)] += 1

    # 2. Корреляция: для каждого дня — каков минимальный nights, при котором успех,
    # и какой busy при этом
    first_success_nights_dist: Counter[str] = Counter()
    busy_when_success: Counter[str] = Counter()
    days_never_success: int = 0

    for row in per_day:
        fsn = row["first_success_nights"]
        if fsn is None:
            days_never_success += 1
            first_success_nights_dist["never"] += 1
        else:
            first_success_nights_dist[str(fsn)] += 1
            busy_when_success[row["first_success_busy"] or "null"] += 1

    # 3. Гипотеза: при nights=1 какие сигнатуры встречаются?
    nights_1_signatures: Counter[str] = Counter()
    if 1 in matrix:
        for resp in matrix[1]:
            nights_1_signatures[_signature_for_response(resp)] += 1

    # 4. Возможна ли классификация по nights=1?
    # Для каждого дня сравниваем сигнатуру nights=1 с реальным busy (по first_success)
    classification: dict[str, Counter[str]] = {}
    if 1 in matrix:
        for idx, row in enumerate(per_day):
            sig_n1 = _signature_for_response(matrix[1][idx])
            real_busy = row["first_success_busy"] or "unknown"
            classification.setdefault(sig_n1, Counter())[real_busy] += 1

    classification_serializable = {
        sig: dict(counts) for sig, counts in classification.items()
    }

    return {
        "signatures_overall": dict(signature_counter),
        "first_success_nights_distribution": dict(first_success_nights_dist),
        "busy_distribution_when_success": dict(busy_when_success),
        "days_never_success": days_never_success,
        "nights_1_signatures": dict(nights_1_signatures),
        "classification_by_nights_1_signature": classification_serializable,
    }


# ─────────────────────────────────────────────────────────────────────
# Главная функция probe
# ─────────────────────────────────────────────────────────────────────


async def probe_listing(
    listing_id: str,
    url: str,
    days_count: int,
    max_nights: int,
    guests: int,
) -> dict[str, Any]:
    """Полный probe-проход по одному объявлению."""
    settings = Settings.load()
    browser_service = BrowserService(settings=settings)

    started_at = datetime.now(timezone.utc).isoformat()

    try:
        await browser_service.start()

        # Прогрев: открываем главную, чтобы получить cookies
        await browser_service.navigate("https://sutochno.ru")
        await asyncio.sleep(3.0)

        page = browser_service.page

        logger.info("probe_загрузка_страницы", step=f"id={listing_id}")
        loaded, token = await _goto_and_capture_token(page, url)

        if not loaded:
            return {
                "error": "page_not_loaded",
                "listing_id": listing_id,
                "url": url,
            }

        if not token:
            return {
                "error": "token_not_captured",
                "listing_id": listing_id,
                "url": url,
            }

        logger.info("probe_токен_получен", step=f"id={listing_id}, длина={len(token)}")

        today = date.today()
        days = [today + timedelta(days=i) for i in range(days_count)]

        # ── Запуск probe для каждого значения nights ──
        matrix: dict[int, list[dict[str, Any]]] = {}

        for nights in range(1, max_nights + 1):
            logger.info(
                "probe_проход",
                step=f"id={listing_id}, nights={nights}/{max_nights}, дней={days_count}",
            )
            results = await _probe_day_batch(
                page=page,
                object_id=listing_id,
                token=token,
                guests=guests,
                days=days,
                nights=nights,
            )
            matrix[nights] = results

            # Промежуточная статистика
            ok_cnt = sum(1 for r in results if r.get("obj_success"))
            err_cnt = sum(1 for r in results if r.get("obj_success") is False)
            logger.info(
                "probe_проход_итог",
                step=f"nights={nights}",
                total=f"ok={ok_cnt}, obj_error={err_cnt}",
            )

        # ── Анализ ──
        per_day = _analyze_per_day(matrix, days)
        summary = _build_summary(matrix, per_day)

        finished_at = datetime.now(timezone.utc).isoformat()

        return {
            "meta": {
                "started_at": started_at,
                "finished_at": finished_at,
                "listing_id": listing_id,
                "url": url,
                "days_count": days_count,
                "max_nights": max_nights,
                "guests": guests,
                "today": today.isoformat(),
                "token_prefix": token[:8] + "..." if token else None,
            },
            "summary": summary,
            "per_day": per_day,
            "raw_matrix": {
                str(nights): results for nights, results in matrix.items()
            },
        }

    finally:
        try:
            await asyncio.wait_for(browser_service.stop(), timeout=15.0)
        except Exception as e:
            logger.warning("probe_ошибка_остановки", error=str(e))


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────


def _build_default_url(listing_id: str) -> str:
    """Конструирует дефолтный URL карточки sutochno.ru по listing_id."""
    return (
        f"https://sutochno.ru/front/searchapp/detail/{listing_id}"
        f"?guests_adults=2&id={listing_id}&type=apartment&price_per=1"
    )


def _save_report(report: dict[str, Any], listing_id: str) -> Path:
    """Сохраняет отчёт в data/diagnostics/."""
    _REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = _REPORT_DIR / f"probe_busy_signature_{listing_id}_{ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return path


def _print_summary(report: dict[str, Any]) -> None:
    """Печатает краткую сводку в stdout для быстрого визуального анализа."""
    summary = report.get("summary", {})

    print("\n" + "=" * 70)
    print("📊 СВОДКА PROBE")
    print("=" * 70)

    print("\n🔹 Сигнатуры ответов (всего обращений по всем nights):")
    for sig, cnt in sorted(
        summary.get("signatures_overall", {}).items(),
        key=lambda kv: -kv[1],
    ):
        print(f"   {cnt:>5}  {sig}")

    print("\n🔹 Распределение минимального nights, при котором успех:")
    for nights_key, cnt in sorted(
        summary.get("first_success_nights_distribution", {}).items(),
        key=lambda kv: (kv[0] == "never", kv[0]),
    ):
        print(f"   nights={nights_key:>5}  →  дней: {cnt}")

    print("\n🔹 Busy при первом успехе:")
    for busy_val, cnt in sorted(
        summary.get("busy_distribution_when_success", {}).items(),
        key=lambda kv: -kv[1],
    ):
        print(f"   busy={busy_val:>10}  →  дней: {cnt}")

    print("\n🔹 Сигнатуры при nights=1:")
    for sig, cnt in sorted(
        summary.get("nights_1_signatures", {}).items(),
        key=lambda kv: -kv[1],
    ):
        print(f"   {cnt:>5}  {sig}")

    print("\n🔹 КЛАССИФИКАЦИЯ: nights=1 сигнатура → реальная занятость")
    print("   (Если сигнатура однозначно мапится в busy/unbusy → гипотеза подтверждена!)")
    for sig, dist in summary.get("classification_by_nights_1_signature", {}).items():
        total = sum(dist.values())
        print(f"\n   Сигнатура: {sig}  (всего {total} дней)")
        for busy_val, cnt in sorted(dist.items(), key=lambda kv: -kv[1]):
            pct = (cnt / total * 100) if total else 0
            print(f"      → реально {busy_val:>10} : {cnt:>3} дней ({pct:5.1f}%)")

    print("\n" + "=" * 70 + "\n")


async def _main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Probe: проверка различимости занятого/свободного дня по тексту "
            "ошибки API при разных значениях nights."
        )
    )
    parser.add_argument("listing_id", help="ID объявления sutochno.ru")
    parser.add_argument(
        "--url",
        default=None,
        help="Полный URL карточки (если не указан — будет сконструирован)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=_DEFAULT_DAYS,
        help=f"Количество дней для probe (по умолчанию {_DEFAULT_DAYS})",
    )
    parser.add_argument(
        "--max-nights",
        type=int,
        default=_DEFAULT_MAX_NIGHTS,
        help=f"Максимальный nights для probe (по умолчанию {_DEFAULT_MAX_NIGHTS})",
    )
    parser.add_argument(
        "--guests",
        type=int,
        default=_DEFAULT_GUESTS,
        help=f"Количество гостей (по умолчанию {_DEFAULT_GUESTS})",
    )
    args = parser.parse_args()

    listing_id: str = args.listing_id
    url: str = args.url or _build_default_url(listing_id)

    logger.info(
        "probe_запуск",
        step=f"id={listing_id}",
        total=f"дней={args.days}, max_nights={args.max_nights}, guests={args.guests}",
    )

    try:
        report = await probe_listing(
            listing_id=listing_id,
            url=url,
            days_count=args.days,
            max_nights=args.max_nights,
            guests=args.guests,
        )
    except Exception as e:
        logger.warning(
            "probe_ошибка",
            error=str(e),
            error_type=type(e).__name__,
        )
        return 1

    if "error" in report:
        logger.warning("probe_неуспех", step=str(report.get("error")))
        return 2

    report_path = _save_report(report, listing_id)
    logger.info("probe_отчёт_сохранён", path=str(report_path))

    _print_summary(report)

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))