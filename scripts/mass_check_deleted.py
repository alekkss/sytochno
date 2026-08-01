"""Массовая проверка объявлений на удаление через API sutochno.ru.

Логика полностью из batch_enrichment_service.py:
- route "**/api/json/**" → headers.get("token")
- API: /api/json/objects/getPricesAndAvailabilities
- fetch с credentials: 'include'

Использование:
    python -m scripts.mass_check_deleted          # dry-run
    python -m scripts.mass_check_deleted --delete # с удалением
"""

import asyncio
import json
import os
import random
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, Page

# ─── Конфигурация ────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "data" / "sutochno_listings.db"
PROXY_FILE = PROJECT_ROOT / "data" / "proxies.txt"
REPORT_DIR = PROJECT_ROOT / "data" / "reports"
ENV_FILE = PROJECT_ROOT / ".env"

# Загрузка .env
load_dotenv(ENV_FILE)

WORKERS = 20
GOTO_TIMEOUT_MS = 45_000
TOKEN_INTERCEPT_TIMEOUT = 20.0
TOKEN_POLL_INTERVAL = 0.5
POST_LOAD_WAIT = 3.0
REQUEST_DELAY = (0.3, 0.8)
MAX_RETRIES = 2
DAYS_COUNT = 60
FETCH_TIMEOUT_SECONDS = 30
EVALUATE_TIMEOUT = 45.0

# API URL (из constants.py парсера)
API_PRICES_URL = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# URL для загрузки страницы и перехвата токена (из .env, как делает парсер)
SEARCH_URL = os.getenv("SUTOCHNO_SEARCH_URL_1", "")
if not SEARCH_URL:
    # Fallback — пробуем любой заполненный URL
    for i in range(1, 9):
        url = os.getenv(f"SUTOCHNO_SEARCH_URL_{i}", "")
        if url:
            SEARCH_URL = url
            break

# Stealth (из browser_service.py)
_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--single-process",
    "--js-flags=--max-old-space-size=256",
    "--disable-renderer-backgrounding",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-gpu",
    "--disable-features=PaintHolding,ImageDecodeService",
    "--blink-settings=imagesEnabled=false",
]

_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
    window.chrome = {runtime: {}};
"""

_CONTEXT_OPTIONS = {
    "viewport": {"width": 1920, "height": 1080},
    "locale": "ru-RU",
    "timezone_id": "Europe/Moscow",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


# ─── Прокси ──────────────────────────────────────────────────────────────────


def parse_proxy(line: str) -> dict:
    """Парсит ip:port:login:password в формат Playwright."""
    clean = line.strip().replace("http://", "").replace("https://", "")
    parts = clean.split(":")
    if len(parts) == 4:
        return {
            "server": f"http://{parts[0]}:{parts[1]}",
            "username": parts[2],
            "password": parts[3],
        }
    elif len(parts) == 2:
        return {"server": f"http://{parts[0]}:{parts[1]}"}
    raise ValueError(f"Неизвестный формат прокси: {line}")


def load_proxies() -> list[dict]:
    if not PROXY_FILE.exists():
        print(f"[ERROR] Файл прокси не найден: {PROXY_FILE}")
        sys.exit(1)
    proxies = []
    for line in PROXY_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            try:
                proxies.append(parse_proxy(line))
            except ValueError:
                pass
    if not proxies:
        print("[ERROR] Файл прокси пуст")
        sys.exit(1)
    print(f"[INFO] Загружено прокси: {len(proxies)}")
    return proxies


def load_all_external_ids() -> list[str]:
    if not DB_PATH.exists():
        print(f"[ERROR] БД не найдена: {DB_PATH}")
        sys.exit(1)
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.execute("SELECT external_id FROM listings ORDER BY id")
    ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    print(f"[INFO] Объявлений в БД: {len(ids)}")
    return ids


# ─── Удаление из БД ─────────────────────────────────────────────────────────


def delete_from_db(deleted_ids: set[str]) -> None:
    if not deleted_ids:
        return
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    ids_list = list(deleted_ids)
    batch_size = 500
    total_listings = total_snapshots = total_prices = 0

    for i in range(0, len(ids_list), batch_size):
        batch = ids_list[i:i + batch_size]
        ph = ",".join("?" * len(batch))

        rows = cursor.execute(
            f"SELECT id FROM listing_snapshots WHERE external_id IN ({ph})", batch
        ).fetchall()
        snap_ids = [r[0] for r in rows]

        if snap_ids:
            sp_ph = ",".join("?" * len(snap_ids))
            cursor.execute(f"DELETE FROM snapshot_prices WHERE snapshot_id IN ({sp_ph})", snap_ids)
            total_prices += cursor.rowcount

        cursor.execute(f"DELETE FROM listing_snapshots WHERE external_id IN ({ph})", batch)
        total_snapshots += cursor.rowcount
        cursor.execute(f"DELETE FROM listings WHERE external_id IN ({ph})", batch)
        total_listings += cursor.rowcount

    conn.commit()
    conn.close()
    print(f"\n[DELETE] Удалено:")
    print(f"  listings:          {total_listings}")
    print(f"  listing_snapshots: {total_snapshots}")
    print(f"  snapshot_prices:   {total_prices}")


# ─── Получение токена (ТОЧНО как в _start_and_get_token) ─────────────────────


async def get_token_and_page(proxy: dict, worker_id: int) -> tuple | None:
    """Запускает браузер → грузит search_url → перехватывает токен из route."""
    pw = await async_playwright().start()

    try:
        browser = await pw.chromium.launch(
            headless=True,
            proxy=proxy,
            args=_BROWSER_ARGS,
            ignore_default_args=["--enable-automation"],
        )
    except Exception as e:
        print(f"    [W{worker_id:02d}] Ошибка запуска браузера: {str(e)[:100]}")
        await pw.stop()
        return None

    context = await browser.new_context(**_CONTEXT_OPTIONS)
    await context.add_init_script(_STEALTH_SCRIPT)
    page = await context.new_page()
    page.set_default_navigation_timeout(GOTO_TIMEOUT_MS)

    # Перехват токена — ТОЧНО как в batch_enrichment_service._start_and_get_token
    captured_token: list[str] = []

    async def _intercept(route, request):
        url = request.url
        if "sutochno.ru/api/json" in url and not captured_token:
            token = request.headers.get("token") or request.headers.get("Token")
            if token:
                captured_token.append(token)
        try:
            await route.continue_()
        except Exception as e:
            if "Route is already handled" not in str(e):
                raise

    await page.route("**/api/json/**", _intercept)

    try:
        await page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=GOTO_TIMEOUT_MS)
    except Exception as e:
        print(f"    [W{worker_id:02d}] Ошибка навигации: {str(e)[:120]}")
        try:
            await page.unroute("**/api/json/**")
        except Exception:
            pass
        await browser.close()
        await pw.stop()
        return None

    # Ожидание токена
    elapsed = 0.0
    while elapsed < TOKEN_INTERCEPT_TIMEOUT:
        if captured_token:
            break
        await asyncio.sleep(TOKEN_POLL_INTERVAL)
        elapsed += TOKEN_POLL_INTERVAL

    try:
        await page.unroute("**/api/json/**")
    except Exception:
        pass

    # Post-load wait (как в парсере)
    await asyncio.sleep(POST_LOAD_WAIT)

    if not captured_token:
        print(f"    [W{worker_id:02d}] Токен не перехвачен за {elapsed:.1f}с")
        await browser.close()
        await pw.stop()
        return None

    return pw, browser, context, page, captured_token[0]


# ─── Проверка batch (ТОЧНО как _fetch_batch) ─────────────────────────────────


async def check_batch_ids(
    page: Page, token: str, object_ids: list[str], check_in: str, check_out: str
) -> dict[str, bool | None]:
    """Проверяет пачку ID. Возвращает {id: True=удалён, False=жив, None=ошибка}."""
    date_begin = f"{check_in} 14:00:00"
    date_end = f"{check_out} 11:00:00"
    int_ids = [int(x) for x in object_ids]

    try:
        raw_result = await asyncio.wait_for(
            page.evaluate(
                """
                async ({apiUrl, objectIds, dateBegin, dateEnd, token, guests, fetchTimeout}) => {
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
                                objects: objectIds,
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
                            return {success: false, error: 'http_' + resp.status};
                        }

                        const data = await resp.json();

                        if (!data.success) {
                            return {success: false, error: 'api_false'};
                        }

                        if (!data.data || !data.data.objects) {
                            return {success: false, error: 'no_objects_array'};
                        }

                        const results = [];
                        for (const obj of data.data.objects) {
                            if (obj.success) {
                                results.push({object_id: obj.id, success: true});
                            } else {
                                results.push({
                                    object_id: obj.id,
                                    success: false,
                                    error_text: JSON.stringify(obj.errors || [])
                                });
                            }
                        }
                        return {success: true, results: results};

                    } catch (e) {
                        if (e.name === 'AbortError') {
                            return {success: false, error: 'fetch_timeout'};
                        }
                        return {success: false, error: e.message};
                    }
                }
                """,
                {
                    "apiUrl": API_PRICES_URL,
                    "objectIds": int_ids,
                    "dateBegin": date_begin,
                    "dateEnd": date_end,
                    "token": token,
                    "guests": 2,
                    "fetchTimeout": FETCH_TIMEOUT_SECONDS,
                },
            ),
            timeout=EVALUATE_TIMEOUT,
        )
    except Exception as e:
        return {oid: None for oid in object_ids}

    results: dict[str, bool | None] = {}

    if not raw_result.get("success"):
        error = raw_result.get("error", "")
        # Общая ошибка no_objects_array = ВСЕ ID в пачке удалены?
        # Нет — это ошибка структуры ответа. Помечаем как None.
        if "no_objects_array" in error:
            # API не вернул массив objects — все ID скорее всего удалены
            return {oid: True for oid in object_ids}
        return {oid: None for oid in object_ids}

    api_results = raw_result.get("results", [])
    returned_ids = set()

    for item in api_results:
        obj_id = str(item.get("object_id", ""))
        returned_ids.add(obj_id)

        if item.get("success"):
            results[obj_id] = False  # Живое
        else:
            error_text = item.get("error_text", "").lower()
            if "no_objects" in error_text or "not_found" in error_text:
                results[obj_id] = True  # Удалено
            else:
                # min_nights, guests и прочее — объявление ЖИВОЕ
                results[obj_id] = False

    # ID которые не вернулись в ответе — удалены
    for oid in object_ids:
        if oid not in results:
            results[oid] = True

    return results


# ─── Воркер ──────────────────────────────────────────────────────────────────


async def worker(
    worker_id: int,
    proxy: dict,
    queue: asyncio.Queue,
    deleted_set: set,
    checked_counter: list,
    error_counter: list,
    total_count: int,
    lock: asyncio.Lock,
) -> None:
    """Воркер: браузер → токен → проверка пачками по 50."""
    result = await get_token_and_page(proxy, worker_id)
    if result is None:
        proxy_short = proxy["server"].replace("http://", "")
        print(f"  [W{worker_id:02d}] FAIL (прокси: {proxy_short})")
        return

    pw, browser, context, page, token = result
    proxy_short = proxy["server"].replace("http://", "")
    print(f"  [W{worker_id:02d}] OK — токен: {token[:8]}... (прокси: {proxy_short})")

    today = date.today()
    check_in = today.isoformat()
    check_out = (today + timedelta(days=DAYS_COUNT)).isoformat()

    while True:
        # Берём пачку ID из очереди
        batch: list[str] = []
        for _ in range(50):
            try:
                ext_id = queue.get_nowait()
                batch.append(ext_id)
            except asyncio.QueueEmpty:
                break

        if not batch:
            break

        # Проверяем пачку целиком (как batch_enrichment)
        results = await check_batch_ids(page, token, batch, check_in, check_out)

        async with lock:
            for ext_id, is_deleted in results.items():
                checked_counter[0] += 1
                if is_deleted is True:
                    deleted_set.add(ext_id)
                elif is_deleted is None:
                    error_counter[0] += 1

            if checked_counter[0] % 500 == 0 or checked_counter[0] == total_count:
                print(
                    f"  [PROGRESS] {checked_counter[0]}/{total_count} | "
                    f"Удалено: {len(deleted_set)} | Ошибок: {error_counter[0]}"
                )

        # Пауза между пачками (как BATCH_PAUSE в парсере)
        await asyncio.sleep(random.uniform(*REQUEST_DELAY))

    # Корректное закрытие (как в browser_service.stop)
    try:
        await page.goto("about:blank", timeout=3000, wait_until="commit")
    except Exception:
        pass
    try:
        await context.close()
    except Exception:
        pass
    await asyncio.sleep(0.5)
    try:
        await browser.close()
    except Exception:
        pass
    await asyncio.sleep(2.0)
    try:
        await pw.stop()
    except Exception:
        pass


# ─── Валидация прокси ─────────────────────────────────────────────────────────


async def _validate_proxy_impl(proxy: dict) -> bool:
    """Внутренняя проверка прокси с обязательным закрытием ресурсов."""
    pw = await async_playwright().start()
    browser = None
    context = None
    page = None
    try:
        browser = await pw.chromium.launch(
            headless=True,
            proxy=proxy,
            args=_BROWSER_ARGS[:6],
            timeout=20000,          # таймаут на запуск браузера
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="ru-RU",
            user_agent=_CONTEXT_OPTIONS["user_agent"],
        )
        await context.add_init_script(_STEALTH_SCRIPT)
        page = await context.new_page()
        page.set_default_navigation_timeout(15000)
        await page.goto("https://sutochno.ru", wait_until="domcontentloaded")
        content = await page.content()
        return len(content) > 1000
    except (asyncio.CancelledError, Exception):
        # При отмене или ошибке просто перебрасываем, чтобы finally закрыл ресурсы
        raise
    finally:
        # Закрываем всё в обратном порядке
        if page:
            try:
                await page.close()
            except Exception:
                pass
        if context:
            try:
                await context.close()
            except Exception:
                pass
        if browser:
            try:
                await browser.close()
            except Exception:
                pass
        try:
            await pw.stop()
        except Exception:
            pass


async def validate_proxy(proxy: dict) -> bool:
    """Проверка прокси с общим таймаутом."""
    try:
        return await asyncio.wait_for(_validate_proxy_impl(proxy), timeout=45.0)
    except asyncio.TimeoutError:
        return False


async def validate_proxies(proxies: list[dict], needed: int = 20) -> list[dict]:
    print(f"\n[INFO] Валидация прокси (нужно {needed} рабочих)...")
    random.shuffle(proxies)
    valid = []
    semaphore = asyncio.Semaphore(3)

    async def check(p):
        async with semaphore:
            return p, await validate_proxy(p)

    for i in range(0, len(proxies), 6):
        batch = proxies[i:i + 6]
        results = await asyncio.gather(*[check(p) for p in batch])
        for proxy, ok in results:
            if ok:
                valid.append(proxy)
        if len(valid) >= needed:
            break

    print(f"[INFO] Рабочих прокси: {len(valid)}")
    return valid[:needed]


# ─── Main ────────────────────────────────────────────────────────────────────


async def main(do_delete: bool = False) -> None:
    print("=" * 60)
    print("  МАССОВАЯ ПРОВЕРКА УДАЛЁННЫХ ОБЪЯВЛЕНИЙ")
    print("=" * 60)

    if not SEARCH_URL:
        print("[ERROR] Не найден SUTOCHNO_SEARCH_URL_* в .env!")
        print(f"        Проверьте файл: {ENV_FILE}")
        sys.exit(1)

    print(f"[INFO] Search URL: {SEARCH_URL[:80]}...")
    print(f"[INFO] API URL: {API_PRICES_URL}")

    proxies = load_proxies()
    all_ids = load_all_external_ids()

    if not all_ids:
        print("[INFO] БД пуста.")
        return

    valid_proxies = await validate_proxies(proxies, needed=WORKERS)
    if not valid_proxies:
        print("[ERROR] Нет рабочих прокси!")
        return

    actual_workers = min(WORKERS, len(valid_proxies))
    # Оценка: 50 ID за ~1 сек + пауза 0.5с → ~33 ID/сек на воркер
    est_minutes = len(all_ids) / (actual_workers * 33) / 60
    print(f"\n[INFO] Воркеров: {actual_workers}")
    print(f"[INFO] Объявлений: {len(all_ids)}")
    print(f"[INFO] Проверка пачками по 50 ID")
    print(f"[INFO] Примерное время: {est_minutes:.0f} мин\n")

    queue: asyncio.Queue = asyncio.Queue()
    for ext_id in all_ids:
        await queue.put(ext_id)

    deleted_set: set = set()
    checked_counter = [0]
    error_counter = [0]
    lock = asyncio.Lock()

    # Запуск воркеров с задержкой (как _WORKER_START_DELAY в парсере)
    tasks = []
    for i in range(actual_workers):
        if i > 0:
            await asyncio.sleep(2.0)
        task = asyncio.create_task(
            worker(i, valid_proxies[i], queue, deleted_set,
                   checked_counter, error_counter, len(all_ids), lock)
        )
        tasks.append(task)

    await asyncio.gather(*tasks)

    # Результаты
    print("\n" + "=" * 60)
    print("  РЕЗУЛЬТАТ")
    print("=" * 60)
    print(f"  Проверено:    {checked_counter[0]}/{len(all_ids)}")
    print(f"  Удалённых:    {len(deleted_set)}")
    print(f"  Ошибок:       {error_counter[0]}")
    print(f"  Активных:     {checked_counter[0] - len(deleted_set) - error_counter[0]}")

    if deleted_set:
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_file = REPORT_DIR / f"deleted_listings_{date.today().isoformat()}.txt"
        report_file.write_text(
            "\n".join(sorted(deleted_set, key=lambda x: int(x))),
            encoding="utf-8",
        )
        print(f"\n  Отчёт: {report_file}")
        print(f"  Удалённые ID (первые 30):")
        for ext_id in sorted(deleted_set, key=lambda x: int(x))[:30]:
            print(f"    - {ext_id}")
        if len(deleted_set) > 30:
            print(f"    ... и ещё {len(deleted_set) - 30}")

        if do_delete:
            print("\n[DELETE] Удаление из БД...")
            delete_from_db(deleted_set)
            print("[DELETE] Готово!")
        else:
            print("\n[INFO] Dry-run. Для удаления:")
            print("       python -m scripts.mass_check_deleted --delete")
    else:
        print("\n  Удалённых объявлений не найдено.")


if __name__ == "__main__":
    do_delete = "--delete" in sys.argv
    asyncio.run(main(do_delete=do_delete))
