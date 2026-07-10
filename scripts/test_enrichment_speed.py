"""Диагностический скрипт — измерение скорости и стабильности Этапа 2.

Для каждой карточки из списка ID:
1. Загружает страницу и перехватывает токен — замеряет время.
2. Делает тестовый API-запрос (getPricesAndAvailabilities на 2 ночи) — замеряет время.
3. Логирует причину ошибки при неудаче.

Данные НЕ собираются, снимки НЕ создаются — чистая диагностика.

Запуск:
    python -m scripts.test_enrichment_speed
"""

import asyncio
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

# ── Добавляем корень проекта в sys.path ──
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config.logger import configure, get_logger
from src.config.settings import Settings
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService
from src.services.proxy_service import ProxyService

logger = get_logger("test_speed")

# ── Список ID карточек для тестирования ──
TEST_IDS: list[str] = [
    "2298659", "2305672", "20353", "728489", "2056536",
    "1915947", "1686733", "1862243", "2047403", "2271993",
    "1735249", "2325412", "956337", "2069377", "2205242",
    "1534367", "2288071", "2223254", "966819", "1856774",
    "2179010", "1491671", "2257776", "1351541", "573675",
    "1534230", "1642380", "2025468", "762965", "2223620",
    "1060959", "1337691", "2083706", "1073321", "2175550",
    "1931754", "2078662", "1883791", "2000444", "1279831",
    "471475", "1419503", "1185431", "1873281", "2220427",
    "2303295", "982831", "2010577", "2056435", "1741035",
    "1595421", "1513877", "1410491", "653397", "1344789",
    "1899116", "1978873", "1229857", "1395421", "1286229",
    "843453", "2065466", "1223705", "2015659", "2242484",
    "2042902", "2089219", "854247", "706483", "2029771",
    "1914067", "1671358", "2276195", "2263360", "1529294",
    "1957161", "26885", "2300621", "2295779", "1066651",
    "1961988", "2216752", "2274177", "1069721", "2076215",
    "1093725", "1497144", "287676", "2120791", "886753",
    "2319996", "954561", "1911872", "1437111", "2308892",
    "1543587", "1405925", "1723130", "2294684", "1732863",
]

# Максимальное количество карточек для теста (из TEST_IDS).
# Установите 0 для обработки всех.
MAX_TEST_CARDS: int = 100

# URL шаблон для карточки (прямой URL фронтенда)
_ENRICHMENT_URL_TEMPLATE: str = (
    "https://sutochno.ru/front/searchapp/detail/{object_id}"
)

# URL API для тестового запроса
_API_PRICES_URL: str = (
    "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"
)

# Таймаут ожидания токена после domcontentloaded (секунды)
_TOKEN_WAIT_SECONDS: float = 10.0

# Интервал поллинга токена (секунды)
_TOKEN_POLL_INTERVAL: float = 0.2

# ── Параметры плавности запуска ──

# Максимальное количество одновременно запускаемых браузеров.
_MAX_CONCURRENT_BROWSER_LAUNCHES: int = 3

# Пауза между последовательными запусками браузеров внутри семафора (сек).
_BROWSER_LAUNCH_STAGGER_SECONDS: float = 2.0

# Задержка между стартом вкладок внутри воркера (секунды).
_TAB_START_DELAY_SECONDS: float = 2.0

# Пауза между порциями вкладок (секунды).
_INTER_CHUNK_PAUSE_SECONDS: float = 1.0

# ── Параметры завершения ──

# Максимальное время ожидания завершения всех pending-задач
# после gather при выходе из main() (секунды).
# Playwright внутри создаёт фоновые asyncio.Task для обработки
# CDP-событий — при закрытии 80 браузеров часть этих задач
# остаётся в pending и блокирует asyncio.run(). Этот таймаут
# гарантирует, что скрипт завершится за предсказуемое время.
_SHUTDOWN_PENDING_TIMEOUT: float = 10.0

# Максимальное количество одновременных остановок браузеров.
# Playwright.stop() закрывает pipe к Node.js-драйверу — если
# 80 процессов Node.js закрываются одновременно, ОС может
# исчерпать файловые дескрипторы или замедлить обработку сигналов.
# Ограничение до 5 одновременных остановок предотвращает это.
_MAX_CONCURRENT_STOPS: int = 5


# ── Модель результата диагностики ──


@dataclass
class CardDiagResult:
    """Результат диагностики одной карточки."""

    object_id: str
    worker_idx: int = 0
    is_first_card: bool = False

    # Этап 1: Загрузка страницы
    page_loaded: bool = False
    page_load_time_s: float = 0.0
    page_error: str = ""

    # Этап 2: Перехват токена
    token_captured: bool = False
    token_wait_time_s: float = 0.0

    # Этап 3: API-запрос
    api_success: bool = False
    api_response_time_s: float = 0.0
    api_error: str = ""
    api_busy_status: str = ""

    # Общее время
    total_time_s: float = 0.0


@dataclass
class WorkerStats:
    """Статистика одного воркера."""

    worker_idx: int
    proxy: str
    cards_total: int = 0
    cards_page_ok: int = 0
    cards_token_ok: int = 0
    cards_api_ok: int = 0
    errors_by_reason: dict[str, int] = field(default_factory=dict)
    page_load_times: list[float] = field(default_factory=list)
    token_wait_times: list[float] = field(default_factory=list)
    api_response_times: list[float] = field(default_factory=list)


# ── Классификация сетевых ошибок ──

_ERROR_CLASSIFIERS: list[tuple[str, str]] = [
    ("ERR_TUNNEL_CONNECTION_FAILED", "ERR_TUNNEL_CONNECTION_FAILED"),
    ("ERR_PROXY_CONNECTION_FAILED", "ERR_PROXY_CONNECTION_FAILED"),
    ("ERR_TIMED_OUT", "ERR_TIMED_OUT"),
    ("ERR_CONNECTION_RESET", "ERR_CONNECTION_RESET"),
    ("ERR_CONNECTION_REFUSED", "ERR_CONNECTION_REFUSED"),
    ("ERR_CONNECTION_CLOSED", "ERR_CONNECTION_CLOSED"),
    ("ERR_EMPTY_RESPONSE", "ERR_EMPTY_RESPONSE"),
    ("Timeout", "navigation_timeout"),
]


def _classify_page_error(error_msg: str, timeout_ms: int) -> str:
    """Классифицирует ошибку загрузки страницы."""
    for marker, label in _ERROR_CLASSIFIERS:
        if marker in error_msg:
            if label == "navigation_timeout":
                return f"timeout_{timeout_ms}ms"
            return label

    if "net::" in error_msg:
        for part in error_msg.split():
            if part.startswith("net::"):
                return part
        return f"net_error: {error_msg[:80]}"

    return f"{error_msg[:100]}"


# ── Диагностика одной карточки ──


async def diagnose_card(
    page: "any",  # type: ignore[name-defined]
    object_id: str,
    navigation_timeout_ms: int,
    worker_idx: int = 0,
    is_first_card: bool = False,
) -> CardDiagResult:
    """Диагностирует одну карточку: загрузка, токен, API-запрос."""
    result = CardDiagResult(
        object_id=object_id,
        worker_idx=worker_idx,
        is_first_card=is_first_card,
    )
    card_start = time.perf_counter()

    url = _ENRICHMENT_URL_TEMPLATE.format(object_id=object_id)
    captured_token: list[str] = []

    async def _route_handler(route: "any") -> None:  # type: ignore[name-defined]
        req = route.request
        if "sutochno.ru/api/json" in req.url:
            token = req.headers.get("token") or req.headers.get("Token")
            if token and not captured_token:
                captured_token.append(token)
        try:
            await route.continue_()
        except Exception:
            pass

    try:
        await page.route("**/api/json/**", _route_handler)
    except Exception as e:
        result.page_error = f"route_setup: {type(e).__name__}"
        result.total_time_s = time.perf_counter() - card_start
        return result

    # ── Этап 1: Загрузка страницы ──
    page_start = time.perf_counter()
    try:
        await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=navigation_timeout_ms,
        )
        result.page_loaded = True
        result.page_load_time_s = time.perf_counter() - page_start

        current_url = page.url
        if "sutochno.ru" not in current_url:
            result.page_loaded = False
            result.page_error = f"redirect: {current_url[:80]}"

    except Exception as e:
        result.page_load_time_s = time.perf_counter() - page_start
        result.page_error = _classify_page_error(str(e), navigation_timeout_ms)

    # ── Этап 2: Ожидание токена ──
    if result.page_loaded:
        token_start = time.perf_counter()

        if not captured_token:
            elapsed = 0.0
            while elapsed < _TOKEN_WAIT_SECONDS:
                if captured_token:
                    break
                await asyncio.sleep(_TOKEN_POLL_INTERVAL)
                elapsed += _TOKEN_POLL_INTERVAL

        result.token_wait_time_s = time.perf_counter() - token_start
        result.token_captured = bool(captured_token)

    try:
        await page.unroute("**/api/json/**")
    except Exception:
        pass

    # ── Этап 3: Тестовый API-запрос ──
    if result.token_captured:
        token = captured_token[0]
        today = date.today()
        date_begin = f"{today} 14:00:00"
        date_end = f"{today + timedelta(days=2)} 11:00:00"

        api_body = {
            "objects": [int(object_id)],
            "rooms_cnt": {},
            "guests": 2,
            "date_begin": date_begin,
            "date_end": date_end,
            "currency_id": 1,
            "is_pets": 0,
            "documents": 0,
            "target": 0,
            "ages": [],
            "no_time": 1,
        }

        api_start = time.perf_counter()

        try:
            api_result = await page.evaluate("""
                async ({url, body, token}) => {
                    try {
                        const resp = await fetch(url, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'token': token,
                                'platform': 'js',
                                'api-version': '1.13'
                            },
                            body: JSON.stringify(body),
                            credentials: 'include'
                        });
                        const text = await resp.text();
                        try {
                            const data = JSON.parse(text);
                            return {success: true, status: resp.status, data: data};
                        } catch(e) {
                            return {success: false, error: 'JSON parse error', raw: text.substring(0, 300)};
                        }
                    } catch(e) {
                        return {success: false, error: e.message};
                    }
                }
            """, {"url": _API_PRICES_URL, "body": api_body, "token": token})

            result.api_response_time_s = time.perf_counter() - api_start

            if api_result.get("success"):
                data = api_result.get("data", {})
                if isinstance(data, dict) and data.get("success"):
                    objects = data.get("data", {}).get("objects", [])
                    if objects and isinstance(objects, list):
                        obj_data = objects[0]
                        if isinstance(obj_data, dict):
                            inner = obj_data.get("data", {})
                            if isinstance(inner, dict):
                                result.api_success = True
                                result.api_busy_status = inner.get("busy", "unknown")
                            else:
                                result.api_error = "no_inner_data"
                        else:
                            result.api_error = "invalid_object_format"
                    else:
                        result.api_error = "no_objects"
                else:
                    errors = data.get("errors", []) if isinstance(data, dict) else []
                    result.api_error = f"api_error: {str(errors)[:150]}"
            else:
                result.api_error = api_result.get("error", "unknown_fetch_error")

        except Exception as e:
            result.api_response_time_s = time.perf_counter() - api_start
            result.api_error = f"{type(e).__name__}: {str(e)[:100]}"

    result.total_time_s = time.perf_counter() - card_start

    first_marker = " [ПЕРВАЯ]" if is_first_card else ""
    if result.api_success:
        logger.debug(
            "карточка_ок",
            step=f"id={object_id}, в={worker_idx}{first_marker}, "
                 f"стр={result.page_load_time_s:.1f}с, "
                 f"токен={result.token_wait_time_s:.1f}с, "
                 f"api={result.api_response_time_s:.1f}с, "
                 f"busy={result.api_busy_status}",
        )
    else:
        error = result.page_error or result.api_error or "token_not_captured"
        logger.debug(
            "карточка_ошибка",
            step=f"id={object_id}, в={worker_idx}{first_marker}, причина={error}",
        )

    return result


# ── Глобальный семафор запуска браузеров ──

_launch_semaphore: asyncio.Semaphore | None = None
_launch_lock: asyncio.Lock | None = None
_last_launch_time: float = 0.0


async def _acquire_launch_slot(worker_idx: int) -> None:
    """Ожидает слот для запуска браузера с плавной задержкой."""
    global _last_launch_time  # noqa: PLW0603

    assert _launch_semaphore is not None
    assert _launch_lock is not None

    await _launch_semaphore.acquire()

    async with _launch_lock:
        now = time.monotonic()
        since_last = now - _last_launch_time
        if since_last < _BROWSER_LAUNCH_STAGGER_SECONDS:
            wait = _BROWSER_LAUNCH_STAGGER_SECONDS - since_last
            logger.debug(
                "ожидание_слота_запуска",
                step=f"в={worker_idx}, пауза={wait:.1f}с",
            )
            await asyncio.sleep(wait)
        _last_launch_time = time.monotonic()

    logger.info(
        "слот_запуска_получен",
        step=f"в={worker_idx}",
    )


def _release_launch_slot() -> None:
    """Освобождает слот запуска после старта браузера."""
    assert _launch_semaphore is not None
    _launch_semaphore.release()


# ── Воркер ──


async def run_worker(
    worker_idx: int,
    card_ids: list[str],
    settings: Settings,
    proxy: ProxyConfig | None = None,
    max_tabs: int = 1,
) -> tuple[list[CardDiagResult], BrowserService]:
    """Воркер — запускает браузер и сразу идёт на карточки (без прогрева).

    Первая карточка каждого воркера устанавливает TCP/TLS-туннель
    через прокси — она будет медленнее остальных. Это даёт реальную
    картину «холодного старта» vs «горячего туннеля».

    ВАЖНО: возвращает BrowserService вместе с результатами.
    Остановка браузера выполняется централизованно в main() —
    это предотвращает гонку exception_handler'ов при параллельном
    закрытии 80 Playwright-инстансов.

    Args:
        worker_idx: Номер воркера.
        card_ids: Список ID карточек.
        settings: Настройки приложения.
        proxy: Прокси (None = без прокси).
        max_tabs: Количество параллельных вкладок.

    Returns:
        Кортеж (список результатов диагностики, BrowserService).
    """
    results: list[CardDiagResult] = []
    browser_service = BrowserService(settings=settings)
    nav_timeout = settings.navigation_timeout
    proxy_label = str(proxy) if proxy else "без_прокси"
    launch_slot_held = False
    cards_processed = 0

    try:
        # ── Фаза 1: Ожидание слота запуска ──
        await _acquire_launch_slot(worker_idx)
        launch_slot_held = True

        # ── Фаза 2: Запуск браузера (без прогрева) ──
        logger.info(
            "воркер_запуск_браузера",
            step=f"в={worker_idx}, прокси={proxy_label}, "
                 f"карточек={len(card_ids)}, без_прогрева=да",
        )

        await browser_service.start(proxy=proxy)

        # Освобождаем слот сразу после запуска браузера —
        # следующий воркер может начинать запуск
        _release_launch_slot()
        launch_slot_held = False

        logger.info(
            "воркер_браузер_готов",
            step=f"в={worker_idx}, прокси={proxy_label}",
        )

        # ── Фаза 3: Обработка карточек (первая = холодный старт) ──
        for chunk_start in range(0, len(card_ids), max_tabs):
            chunk = card_ids[chunk_start: chunk_start + max_tabs]

            async def _process_tab(
                idx: int, card_id: str,
            ) -> CardDiagResult:
                page = None
                try:
                    if idx > 0:
                        await asyncio.sleep(_TAB_START_DELAY_SECONDS)
                    page = await browser_service.create_page()

                    # Помечаем первую карточку воркера
                    is_first = (cards_processed + idx == 0)

                    return await diagnose_card(
                        page=page,
                        object_id=card_id,
                        navigation_timeout_ms=nav_timeout,
                        worker_idx=worker_idx,
                        is_first_card=is_first,
                    )
                except Exception as e:
                    return CardDiagResult(
                        object_id=card_id,
                        worker_idx=worker_idx,
                        is_first_card=(cards_processed + idx == 0),
                        page_error=f"tab_error: {type(e).__name__}",
                    )
                finally:
                    if page is not None:
                        try:
                            await browser_service.close_page(page)
                        except Exception:
                            pass

            tasks = [
                _process_tab(idx, card_id)
                for idx, card_id in enumerate(chunk)
            ]

            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, res in enumerate(chunk_results):
                if isinstance(res, CardDiagResult):
                    results.append(res)
                elif isinstance(res, BaseException):
                    results.append(CardDiagResult(
                        object_id=chunk[i] if i < len(chunk) else "?",
                        worker_idx=worker_idx,
                        page_error=f"gather_error: {type(res).__name__}",
                    ))

            cards_processed += len(chunk)
            await browser_service.close_all_pages()

            ok_count = sum(1 for r in results if r.api_success)
            err_count = sum(1 for r in results if r.page_error)
            logger.info(
                "воркер_прогресс",
                step=f"в={worker_idx}, {cards_processed}/{len(card_ids)}, "
                     f"ok={ok_count}, err={err_count}",
            )

            if chunk_start + max_tabs < len(card_ids):
                await asyncio.sleep(_INTER_CHUNK_PAUSE_SECONDS)

    except Exception as e:
        logger.error(
            "воркер_критическая_ошибка",
            error=str(e)[:200],
            error_type=type(e).__name__,
            step=f"в={worker_idx}",
        )
    finally:
        if launch_slot_held:
            _release_launch_slot()

        # НЕ останавливаем браузер здесь — возвращаем его для
        # централизованной остановки в main(). Это предотвращает
        # гонку exception_handler'ов при параллельном закрытии.

    ok_total = sum(1 for r in results if r.api_success)
    logger.info(
        "воркер_завершён",
        step=f"в={worker_idx}, всего={len(results)}, "
             f"ok={ok_total}, err={len(results) - ok_total}",
    )
    return (results, browser_service)


# ── Централизованная остановка браузеров ──


async def _stop_all_browsers(
    browsers: list[tuple[BrowserService, int]],
) -> None:
    """Останавливает все браузеры последовательными пачками.

    Playwright устанавливает exception_handler на event loop при
    каждом start() и восстанавливает при stop(). Параллельный вызов
    80 stop() создаёт гонку на loop.set_exception_handler() — один
    handler восстанавливает original, который на самом деле является
    handler'ом другого инстанса. Это может привести к зависанию
    playwright.stop() или к необработанным Future.

    Ограничение через семафор (_MAX_CONCURRENT_STOPS) гарантирует,
    что одновременно закрывается не более N Playwright-процессов.
    Последовательная остановка безопаснее, но слишком медленна
    при 80 браузерах (80 × 3с = 4 минуты). 5 одновременных — компромисс.

    Args:
        browsers: Список кортежей (BrowserService, worker_idx).
    """
    if not browsers:
        return

    total = len(browsers)
    logger.info(
        "централизованная_остановка_браузеров",
        step=f"всего={total}, параллельно={_MAX_CONCURRENT_STOPS}",
    )

    stop_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_STOPS)
    stopped = 0

    async def _stop_one(browser_svc: BrowserService, w_idx: int) -> None:
        nonlocal stopped
        async with stop_semaphore:
            try:
                await asyncio.wait_for(
                    browser_svc.stop(),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "остановка_браузера_таймаут",
                    step=f"в={w_idx}",
                )
            except Exception as e:
                logger.debug(
                    "ошибка_остановки_браузера",
                    error=str(e)[:100],
                    step=f"в={w_idx}",
                )
            finally:
                stopped += 1
                if stopped % 10 == 0 or stopped == total:
                    logger.info(
                        "прогресс_остановки_браузеров",
                        step=f"{stopped}/{total}",
                    )

    stop_tasks = [
        asyncio.create_task(
            _stop_one(bsvc, w_idx),
            name=f"stop-{w_idx}",
        )
        for bsvc, w_idx in browsers
    ]

    # Ждём завершения всех с жёстким таймаутом
    done, pending = await asyncio.wait(
        stop_tasks,
        timeout=_SHUTDOWN_PENDING_TIMEOUT + total * 0.5,
    )

    if pending:
        logger.warning(
            "остановка_не_завершилась_вовремя",
            step=f"завершено={len(done)}, зависло={len(pending)}",
        )
        for task in pending:
            task.cancel()
        # Даём отменённым задачам финализироваться
        await asyncio.wait(pending, timeout=3.0)

    logger.info(
        "все_браузеры_остановлены",
        step=f"всего={total}",
    )


# ── Очистка event loop от осиротевших задач ──


async def _cleanup_pending_tasks() -> None:
    """Отменяет все pending-задачи в event loop, кроме текущей.

    После параллельной остановки 80 Playwright-инстансов в event loop
    могут остаться незавершённые задачи (CDP-обработчики, pipe-читатели,
    callback'и от Node.js). asyncio.run() не завершится, пока эти задачи
    не будут отменены — это и вызывает зависание скрипта.
    """
    current_task = asyncio.current_task()
    all_tasks = asyncio.all_tasks()

    pending = [
        t for t in all_tasks
        if t is not current_task and not t.done()
    ]

    if not pending:
        return

    logger.info(
        "очистка_pending_задач",
        step=f"осталось={len(pending)}",
    )

    for task in pending:
        task.cancel()

    # Ждём завершения отменённых задач
    results = await asyncio.gather(*pending, return_exceptions=True)

    cancelled_count = sum(
        1 for r in results
        if isinstance(r, asyncio.CancelledError)
    )
    error_count = sum(
        1 for r in results
        if isinstance(r, BaseException) and not isinstance(r, asyncio.CancelledError)
    )

    logger.info(
        "pending_задачи_очищены",
        step=f"отменено={cancelled_count}, ошибок={error_count}",
    )


# ── Вывод сводной таблицы ──


def print_summary(all_results: list[CardDiagResult], elapsed: float) -> None:
    """Выводит сводную таблицу диагностики в консоль."""
    total = len(all_results)
    if total == 0:
        print("\n  Нет результатов для отображения.\n")
        return

    page_ok = sum(1 for r in all_results if r.page_loaded)
    token_ok = sum(1 for r in all_results if r.token_captured)
    api_ok = sum(1 for r in all_results if r.api_success)

    page_times = [r.page_load_time_s for r in all_results if r.page_loaded]
    token_times = [r.token_wait_time_s for r in all_results if r.token_captured]
    api_times = [r.api_response_time_s for r in all_results if r.api_success]

    # Отдельно первые карточки (холодный старт)
    first_cards = [r for r in all_results if r.is_first_card]
    first_page_times = [r.page_load_time_s for r in first_cards if r.page_loaded]
    other_page_times = [
        r.page_load_time_s for r in all_results
        if r.page_loaded and not r.is_first_card
    ]

    error_reasons: dict[str, int] = {}
    for r in all_results:
        if r.page_error:
            error_reasons[r.page_error] = error_reasons.get(r.page_error, 0) + 1
        elif not r.token_captured and r.page_loaded:
            error_reasons["token_not_captured"] = (
                error_reasons.get("token_not_captured", 0) + 1
            )
        elif r.api_error:
            reason = r.api_error.split(":")[0] if ":" in r.api_error else r.api_error
            error_reasons[reason] = error_reasons.get(reason, 0) + 1

    busy_stats: dict[str, int] = {}
    for r in all_results:
        if r.api_busy_status:
            busy_stats[r.api_busy_status] = busy_stats.get(r.api_busy_status, 0) + 1

    worker_data: dict[int, WorkerStats] = {}
    for r in all_results:
        if r.worker_idx not in worker_data:
            worker_data[r.worker_idx] = WorkerStats(
                worker_idx=r.worker_idx, proxy=""
            )
        ws = worker_data[r.worker_idx]
        ws.cards_total += 1
        if r.page_loaded:
            ws.cards_page_ok += 1
            ws.page_load_times.append(r.page_load_time_s)
        if r.token_captured:
            ws.cards_token_ok += 1
            ws.token_wait_times.append(r.token_wait_time_s)
        if r.api_success:
            ws.cards_api_ok += 1
            ws.api_response_times.append(r.api_response_time_s)
        if r.page_error:
            ws.errors_by_reason[r.page_error] = (
                ws.errors_by_reason.get(r.page_error, 0) + 1
            )

    sep = "=" * 72

    print(f"\n{sep}")
    print("  ДИАГНОСТИКА ЭТАПА 2 — СВОДКА (без прогрева)")
    print(sep)

    print(f"\n  Общее время:         {elapsed:.1f} сек ({elapsed / 60:.1f} мин)")
    print(f"  Всего карточек:      {total}")
    print(f"  Страница загружена:  {page_ok}/{total} "
          f"({page_ok / total * 100:.1f}%)")
    print(f"  Токен перехвачен:    {token_ok}/{total} "
          f"({token_ok / total * 100:.1f}%)")
    print(f"  API-запрос успешен:  {api_ok}/{total} "
          f"({api_ok / total * 100:.1f}%)")

    def _stats_line(times: list[float], label: str) -> str:
        if not times:
            return f"  {label}: нет данных"
        mn = min(times)
        mx = max(times)
        avg = statistics.mean(times)
        med = statistics.median(times)
        p95 = sorted(times)[int(len(times) * 0.95)] if len(times) >= 20 else mx
        return (
            f"  {label}: "
            f"мин={mn:.2f}с, медиана={med:.2f}с, среднее={avg:.2f}с, "
            f"p95={p95:.2f}с, макс={mx:.2f}с"
        )

    print(f"\n{'─' * 72}")
    print("  ВРЕМЯ ОПЕРАЦИЙ")
    print(f"{'─' * 72}")
    print(_stats_line(page_times, "Загрузка страницы (все)    "))
    print(_stats_line(api_times, "API-запрос                 "))
    print(_stats_line(token_times, "Ожидание токена            "))

    # ── Сравнение холодного старта и горячего туннеля ──
    print(f"\n{'─' * 72}")
    print("  ХОЛОДНЫЙ СТАРТ vs ГОРЯЧИЙ ТУННЕЛЬ")
    print(f"{'─' * 72}")
    print(_stats_line(first_page_times, "Первая карточка (холодная) "))
    print(_stats_line(other_page_times, "Остальные (горячий туннель)"))

    first_ok = sum(1 for r in first_cards if r.page_loaded)
    first_total = len(first_cards)
    first_err = first_total - first_ok
    print(f"\n  Первых карточек: {first_total}, "
          f"загрузилось: {first_ok} ({first_ok / first_total * 100:.1f}%), "
          f"ошибок: {first_err}")

    if busy_stats:
        print(f"\n{'─' * 72}")
        print("  СТАТУС ЗАНЯТОСТИ (из успешных API-ответов)")
        print(f"{'─' * 72}")
        for status, count in sorted(busy_stats.items(), key=lambda x: -x[1]):
            print(f"  {status}: {count}")

    if error_reasons:
        print(f"\n{'─' * 72}")
        print("  ПРИЧИНЫ ОШИБОК")
        print(f"{'─' * 72}")
        for reason, count in sorted(error_reasons.items(), key=lambda x: -x[1]):
            pct = count / total * 100
            bar_len = min(int(pct / 2), 50)
            bar = "█" * bar_len + "░" * (50 - bar_len)
            print(f"  {count:>4} ({pct:>5.1f}%) {bar} {reason}")

    if len(worker_data) > 1:
        print(f"\n{'─' * 72}")
        print("  СТАТИСТИКА ПО ВОРКЕРАМ")
        print(f"{'─' * 72}")
        header = (
            f"  {'В':>3} {'Карт':>5} {'СтрОК':>6} {'ТокОК':>6} "
            f"{'ApiОК':>6} {'СтрМед':>7} {'ApiМед':>7} {'Ошиб':>5}"
        )
        print(header)
        print(f"  {'─' * 3} {'─' * 5} {'─' * 6} {'─' * 6} "
              f"{'─' * 6} {'─' * 7} {'─' * 7} {'─' * 5}")

        for ws in sorted(worker_data.values(), key=lambda x: x.worker_idx):
            page_med = (
                f"{statistics.median(ws.page_load_times):.1f}с"
                if ws.page_load_times else "—"
            )
            api_med = (
                f"{statistics.median(ws.api_response_times):.1f}с"
                if ws.api_response_times else "—"
            )
            err_count = ws.cards_total - ws.cards_api_ok
            print(
                f"  {ws.worker_idx:>3} {ws.cards_total:>5} "
                f"{ws.cards_page_ok:>6} {ws.cards_token_ok:>6} "
                f"{ws.cards_api_ok:>6} {page_med:>7} {api_med:>7} "
                f"{err_count:>5}"
            )

        worst_workers = sorted(
            worker_data.values(),
            key=lambda x: x.cards_total - x.cards_api_ok,
            reverse=True,
        )[:5]

        if worst_workers and any(w.errors_by_reason for w in worst_workers):
            print(f"\n  Топ-5 воркеров по ошибкам:")
            for ws in worst_workers:
                if ws.errors_by_reason:
                    top_err = max(
                        ws.errors_by_reason.items(), key=lambda x: x[1]
                    )
                    print(
                        f"    в={ws.worker_idx}: "
                        f"{ws.cards_total - ws.cards_api_ok} ошибок, "
                        f"главная: {top_err[0]} ({top_err[1]})"
                    )

    slowest = sorted(
        [r for r in all_results if r.page_loaded],
        key=lambda x: x.page_load_time_s,
        reverse=True,
    )[:10]

    if slowest:
        print(f"\n{'─' * 72}")
        print("  ТОП-10 САМЫХ МЕДЛЕННЫХ ЗАГРУЗОК СТРАНИЦ")
        print(f"{'─' * 72}")
        for r in slowest:
            token_s = (
                f"токен={r.token_wait_time_s:.1f}с"
                if r.token_captured else "токен=нет"
            )
            api_s = (
                f"api={r.api_response_time_s:.1f}с"
                if r.api_success
                else f"api_err={r.api_error[:25]}"
            )
            first_tag = " [1я]" if r.is_first_card else ""
            print(
                f"  id={r.object_id:>10} | стр={r.page_load_time_s:.1f}с"
                f" | {token_s} | {api_s} | в={r.worker_idx}{first_tag}"
            )

    print(f"\n{sep}\n")


# ── Точка входа ──


async def main() -> None:
    """Основная функция диагностики."""
    global _launch_semaphore, _launch_lock, _last_launch_time  # noqa: PLW0603

    configure(
        log_level="DEBUG",
        log_file_path="logs/test_enrichment_speed.log",
    )

    settings = Settings.load()

    card_ids = TEST_IDS[:MAX_TEST_CARDS] if MAX_TEST_CARDS > 0 else TEST_IDS
    total_cards = len(card_ids)

    print(f"\n  Диагностика Этапа 2 (БЕЗ ПРОГРЕВА): {total_cards} карточек")
    print(f"  Таймаут навигации: {settings.navigation_timeout} мс")
    print(f"  Ожидание токена: {_TOKEN_WAIT_SECONDS} сек")
    print(f"  Прогрев: ОТКЛЮЧЁН (первая карточка = холодный старт)")

    # ── Загрузка и проверка прокси ──
    proxies: list[ProxyConfig] = []
    if settings.use_proxy:
        proxy_service = ProxyService(settings=settings)
        raw_proxies = proxy_service.load_proxies()
        print(f"  Загружено прокси: {len(raw_proxies)}")
        print(f"  Проверка прокси...")
        proxies = await proxy_service.check_proxies(raw_proxies)
        print(f"  Рабочих прокси: {len(proxies)}")

        if not proxies:
            print("  Нет рабочих прокси. Запуск без прокси.")

    # ── Конфигурация воркеров ──
    max_tabs = settings.max_tabs

    if proxies:
        max_workers = min(len(proxies), settings.max_proxy_workers)
        active_proxies: list[ProxyConfig | None] = list(proxies[:max_workers])

        chunks: list[list[str]] = [[] for _ in range(max_workers)]
        for idx, card_id in enumerate(card_ids):
            chunks[idx % max_workers].append(card_id)

        print(f"  Воркеров: {max_workers}")
        print(f"  Вкладок на воркер: {max_tabs}")
        print(f"  Карточек на воркер: ~{total_cards // max_workers}")
    else:
        active_proxies = [None]
        chunks = [card_ids]
        max_workers = 1
        print(f"  Режим: без прокси, 1 воркер, {max_tabs} вкладок")

    print(f"  Одновременный запуск браузеров: {_MAX_CONCURRENT_BROWSER_LAUNCHES}")
    print(f"  Stagger между запусками: {_BROWSER_LAUNCH_STAGGER_SECONDS} сек")
    print()

    # ── Инициализация синхронизации ──
    _launch_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_BROWSER_LAUNCHES)
    _launch_lock = asyncio.Lock()
    _last_launch_time = 0.0

    # ── Запуск всех воркеров сразу ──
    overall_start = time.perf_counter()

    worker_tasks: list[asyncio.Task] = []
    for i in range(max_workers):
        proxy = active_proxies[i] if i < len(active_proxies) else None
        chunk = chunks[i] if i < len(chunks) else []

        if not chunk:
            continue

        task = asyncio.create_task(
            run_worker(
                worker_idx=i + 1,
                card_ids=chunk,
                settings=settings,
                proxy=proxy,
                max_tabs=max_tabs,
            ),
            name=f"diag-worker-{i + 1}",
        )
        worker_tasks.append(task)

    logger.info(
        "все_задачи_созданы",
        step=f"воркеров={len(worker_tasks)}, "
             f"семафор={_MAX_CONCURRENT_BROWSER_LAUNCHES}, "
             f"прогрев=отключён",
    )

    # ── Ожидание завершения ──
    all_results: list[CardDiagResult] = []
    browsers_to_stop: list[tuple[BrowserService, int]] = []

    if worker_tasks:
        task_results = await asyncio.gather(*worker_tasks, return_exceptions=True)

        for i, res in enumerate(task_results):
            worker_idx = i + 1
            if isinstance(res, tuple) and len(res) == 2:
                card_results, browser_svc = res
                all_results.extend(card_results)
                browsers_to_stop.append((browser_svc, worker_idx))
            elif isinstance(res, BaseException):
                logger.error(
                    "воркер_исключение",
                    error=str(res)[:200],
                    error_type=type(res).__name__,
                    step=f"в={worker_idx}",
                )

    overall_elapsed = time.perf_counter() - overall_start

    # ── Вывод сводки ПЕРЕД остановкой браузеров ──
    # Сводка выводится сразу — пользователь видит результаты,
    # пока браузеры останавливаются в фоне.
    print_summary(all_results, overall_elapsed)

    # ── CSV ──
    csv_path = Path("data/diag_enrichment_speed_no_warmup.csv")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(
            "object_id,worker,is_first,page_loaded,page_time_s,page_error,"
            "token_captured,token_time_s,"
            "api_success,api_time_s,api_error,api_busy,total_time_s\n"
        )
        for r in all_results:
            pe = r.page_error.replace(",", ";").replace('"', "'")
            ae = r.api_error.replace(",", ";").replace('"', "'")
            f.write(
                f"{r.object_id},{r.worker_idx},{r.is_first_card},"
                f"{r.page_loaded},{r.page_load_time_s:.3f},{pe},"
                f"{r.token_captured},{r.token_wait_time_s:.3f},"
                f"{r.api_success},{r.api_response_time_s:.3f},{ae},"
                f"{r.api_busy_status},{r.total_time_s:.3f}\n"
            )

    print(f"  Детальные результаты: {csv_path}")
    print(f"  Логи: logs/test_enrichment_speed.log\n")

    # ── Централизованная остановка всех браузеров ──
    # Выполняется ПОСЛЕ вывода сводки и CSV — пользователь уже
    # получил результаты. Остановка может занять 30–60 секунд
    # при 80 браузерах, но это нормально.
    if browsers_to_stop:
        print(f"  Остановка {len(browsers_to_stop)} браузеров...")
        await _stop_all_browsers(browsers_to_stop)
        print("  Все браузеры остановлены.\n")

    # ── Очистка осиротевших задач в event loop ──
    # После закрытия 80 Playwright-инстансов в event loop могут
    # остаться pending-задачи (CDP-обработчики, pipe-читатели).
    # Без этой очистки asyncio.run() зависнет навсегда.
    await _cleanup_pending_tasks()


if __name__ == "__main__":
    asyncio.run(main())
