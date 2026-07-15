"""Batch-обогащение карточек через API без загрузки страниц карточек.

Двухфазный подход:
  Фаза 1 — Batch bulk: пачками по BATCH_SIZE ID отправляет
  getPricesAndAvailabilities на 60 ночей. Для каждого объекта получает
  busy-статус и detail[] с ценами. Объекты с unbusy — полностью готовы.

  Фаза 2 — Batch скользящее окно: для объектов с busy="busy"
  определяет занятость каждого дня. Для каждого дня (0–59) отправляет
  batch-запрос с пачкой busy-ID. Группирует объекты по требуемому
  min_nights и обрабатывает каждую группу отдельно.

Параллельный режим (enrich_batch_parallel):
  Запускает N прокси-браузеров, каждый загружает страницу поиска для
  получения токена и сессии, затем обрабатывает свою порцию объектов.
  При сбое прокси — автоматическая замена через ProxyService.
  Необработанные объекты перераспределяются между живыми воркерами.

Обработка ошибок:
  - min_nights >= 60 → enrichment_skip_reason = "min_nights_exceeded".
  - no_objects → enrichment_skip_reason = "object_not_found".
  - ошибка guests → повтор с guests=1.
  - сбой прокси → замена прокси, повтор загрузки страницы.
  - протухший токен → возврат необогащённых карточек для fallback.
  - сетевые ошибки скользящего окна → до _SLIDING_RETRY_ROUNDS повторов
    только по ошибочным ячейкам с паузой между раундами.
"""

import asyncio
import re
import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService
from src.services.listing.constants import (
    API_PRICES_URL,
    DAYS_COUNT,
    DEFAULT_GUESTS,
    FALLBACK_GUESTS,
    GUESTS_ERROR_KEYWORDS,
    MIN_NIGHTS_ERROR_KEYWORDS,
    MIN_NIGHTS_VARIANTS,
    WORKER_STOP_TIMEOUT,
    format_duration,
    safe_stop_browser,
)
from src.services.listing.price_parser import PriceParser

if TYPE_CHECKING:
    from src.config.settings import Settings
    from src.services.proxy_service import ProxyService

logger = get_logger("batch_enrichment")

# ── Константы ──────────────────────────────────────────────

# Количество ID в одном batch-запросе к API
BATCH_SIZE: int = 50

# Пауза между batch-запросами (секунды)
BATCH_PAUSE: float = 0.5

# Таймаут одного fetch-запроса внутри браузера (секунды)
_FETCH_TIMEOUT_SECONDS: int = 60

# Таймаут page.evaluate для batch-запроса (секунды)
_EVALUATE_TIMEOUT: float = 120.0

# Порог min_nights, выше которого карточка необогащаема
_MIN_NIGHTS_SKIP_THRESHOLD: int = 60

# Количество ночей по умолчанию для скользящего окна
_DEFAULT_SLIDING_NIGHTS: int = 2

# Количество retry-раундов для ошибочных ячеек скользящего окна.
# После основного прохода и адаптации min_nights — дополнительные
# проходы только по оставшимся ячейкам со статусом -1.
# Эти ошибки — сетевые (таймаут прокси, обрыв соединения),
# а не логические — повторный запрос часто их устраняет.
_SLIDING_RETRY_ROUNDS: int = 1

# Пауза между retry-раундами скользящего окна (секунды).
# Даёт прокси/серверу «отдышаться» после серии таймаутов.
_SLIDING_RETRY_PAUSE: float = 3.0

# ── Константы для параллельного режима ─────────────────────

# Таймаут ожидания перехвата токена после загрузки страницы (секунды)
_TOKEN_INTERCEPT_TIMEOUT: float = 20.0

# Интервал поллинга перехвата токена (секунды)
_TOKEN_POLL_INTERVAL: float = 0.5

# Дополнительное ожидание после загрузки страницы (секунды)
_POST_LOAD_WAIT: float = 3.0

# Максимальное количество попыток получения токена для одного воркера
_MAX_TOKEN_ATTEMPTS: int = 3

# Пауза между попытками получения токена (секунды)
_TOKEN_RETRY_PAUSE: float = 3.0

# Задержка между стартом воркеров (секунды)
_WORKER_START_DELAY: float = 2.0

# Заголовки, которые не нужно передавать из перехвата
_SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}


@dataclass
class _ObjectBulkResult:
    """Результат bulk-запроса для одного объекта.

    Attributes:
        object_id: ID объявления.
        success: Успешен ли запрос для этого объекта.
        busy: Статус занятости ("busy", "unbusy" или None при ошибке).
        detail: Массив detail[] из ответа API.
        error_text: Текст ошибки (если success=False).
        is_fatal: Фатальная ошибка (повтор бессмыслен).
        skip_reason: Причина пропуска для enrichment_skip_reason.
    """

    object_id: int
    success: bool = False
    busy: str | None = None
    detail: list[dict] = field(default_factory=list)
    error_text: str = ""
    is_fatal: bool = False
    skip_reason: str | None = None


@dataclass
class _WorkerResult:
    """Результат работы одного прокси-воркера.

    Attributes:
        worker_idx: Номер воркера.
        processed_ids: ID объектов, обработанных этим воркером.
        bulk_results: Результаты фазы 1 (bulk).
        calendars: Результаты фазы 2 (скользящее окно).
        duration: Время работы в секундах.
        failed: True если воркер упал (не смог получить токен).
        browser_service: Браузер воркера (для корректной остановки).
    """

    worker_idx: int
    processed_ids: list[int] = field(default_factory=list)
    bulk_results: list[_ObjectBulkResult] = field(default_factory=list)
    calendars: dict[int, list[int]] = field(default_factory=dict)
    duration: float = 0.0
    failed: bool = False
    browser_service: BrowserService | None = None


class BatchEnrichmentService:
    """Сервис batch-обогащения карточек через API.

    Поддерживает два режима:
    - enrich_batch() — один браузер, без прокси.
    - enrich_batch_parallel() — N прокси-браузеров параллельно.

    Переиспользует PriceParser для разворачивания detail[] в дневные
    цены — та же логика, что и в HybridStrategy.
    """

    def __init__(self, price_parser: PriceParser | None = None) -> None:
        """Инициализирует сервис.

        Args:
            price_parser: Парсер цен. Если None — создаётся новый.
        """
        self._price_parser = price_parser or PriceParser()

    # ══════════════════════════════════════════════════════════
    #  Параллельный режим (прокси-воркеры)
    # ══════════════════════════════════════════════════════════

    async def enrich_batch_parallel(
        self,
        settings: "Settings",
        listings: list[RawListing],
        proxies: list[ProxyConfig],
        search_url: str,
        proxy_service: "ProxyService | None" = None,
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через прокси-браузеры.

        Каждый воркер:
        1. Запускает браузер через прокси.
        2. Загружает страницу поиска → перехватывает токен.
        3. Обрабатывает свою порцию ID (bulk + скользящее окно).

        При сбое прокси — замена через proxy_service. Необработанные
        объекты перераспределяются между живыми воркерами в retry-раунде.

        Args:
            settings: Настройки приложения.
            listings: Список карточек для обогащения.
            proxies: Список рабочих прокси.
            search_url: URL страницы поиска (для загрузки и перехвата токена).
            proxy_service: Сервис прокси (для замены при сбоях).

        Returns:
            Тот же список карточек с заполненными данными.
        """
        if not listings or not proxies:
            return listings

        today = date.today()
        start_time = time.perf_counter()
        total = len(listings)

        # Индекс: external_id → RawListing
        listings_map: dict[str, RawListing] = {
            l.external_id: l for l in listings
        }

        all_ids = [int(l.external_id) for l in listings]
        max_workers = min(len(proxies), settings.max_proxy_workers)
        active_proxies = proxies[:max_workers]

        logger.info(
            "batch_parallel_начало",
            step=f"карточек={total}, воркеров={max_workers}, "
                 f"batch_size={BATCH_SIZE}",
        )

        # ── Распределяем ID между воркерами ──
        chunks = self._distribute_ids(all_ids, max_workers)

        # ── Запускаем воркеры с задержкой ──
        tasks: list[asyncio.Task] = []
        for idx, (chunk, proxy) in enumerate(zip(chunks, active_proxies)):
            if idx > 0:
                await asyncio.sleep(_WORKER_START_DELAY)

            task = asyncio.create_task(
                self._worker(
                    settings=settings,
                    object_ids=chunk,
                    proxy=proxy,
                    worker_idx=idx + 1,
                    search_url=search_url,
                    proxy_service=proxy_service,
                    today=today,
                ),
                name=f"batch-worker-{idx + 1}",
            )
            tasks.append(task)

        # ── Ожидаем завершения всех воркеров ──
        results_raw = await asyncio.gather(*tasks, return_exceptions=True)

        # ── Обрабатываем результаты ──
        all_worker_results: list[_WorkerResult] = []
        failed_ids: list[int] = []

        for i, result in enumerate(results_raw):
            if isinstance(result, BaseException):
                logger.warning(
                    "batch_воркер_исключение",
                    error=str(result)[:200],
                    error_type=type(result).__name__,
                    step=f"воркер={i + 1}",
                )
                # Все ID этого воркера — необработанные
                if i < len(chunks):
                    failed_ids.extend(chunks[i])
                continue

            all_worker_results.append(result)

            if result.failed:
                failed_ids.extend(result.processed_ids)
            else:
                # Применяем результаты bulk
                self._apply_bulk_results(
                    result.bulk_results, listings_map, today,
                )
                # Применяем результаты скользящего окна
                self._apply_calendars(result.calendars, listings_map)

        # ── Останавливаем браузеры воркеров ──
        stop_tasks = []
        for wr in all_worker_results:
            if wr.browser_service is not None:
                stop_tasks.append(
                    safe_stop_browser(wr.browser_service, wr.worker_idx)
                )
        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        elapsed = time.perf_counter() - start_time

        # ── Статистика ──
        final_enriched = sum(
            1 for l in listings
            if (l.calendar_60_days and any(c == 1 for c in l.calendar_60_days))
            or (l.prices_60_days and any(p > 0 for p in l.prices_60_days))
        )
        final_fatal = sum(
            1 for l in listings if l.enrichment_skip_reason is not None
        )

        # Сводка по воркерам
        for wr in all_worker_results:
            status = "УПАЛ" if wr.failed else "ОК"
            logger.info(
                "batch_воркер_сводка",
                step=f"воркер={wr.worker_idx}, статус={status}, "
                     f"объектов={len(wr.processed_ids)}, "
                     f"время={format_duration(wr.duration)}",
            )

        logger.info(
            "batch_parallel_завершено",
            step=f"время={format_duration(elapsed)}, "
                 f"обогащено={final_enriched}, "
                 f"фатальных={final_fatal}, "
                 f"для_fallback={total - final_enriched - final_fatal}, "
                 f"упавших_ids={len(failed_ids)}",
        )

        return listings

    async def _worker(
        self,
        settings: "Settings",
        object_ids: list[int],
        proxy: ProxyConfig,
        worker_idx: int,
        search_url: str,
        proxy_service: "ProxyService | None",
        today: date,
    ) -> _WorkerResult:
        """Один прокси-воркер: запуск браузера → токен → обработка порции.

        При сбое загрузки страницы или перехвата токена:
        1. Повторяет попытку (до _MAX_TOKEN_ATTEMPTS).
        2. При стабильном сбое — запрашивает замену прокси.
        3. Если замена не помогла — возвращает результат с failed=True.

        Args:
            settings: Настройки приложения.
            object_ids: Порция ID для обработки.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).
            search_url: URL страницы поиска.
            proxy_service: Сервис прокси (для замены).
            today: Дата начала календаря.

        Returns:
            Результат работы воркера.
        """
        worker_start = time.perf_counter()
        result = _WorkerResult(
            worker_idx=worker_idx,
            processed_ids=list(object_ids),
        )

        if not object_ids:
            return result

        current_proxy = proxy
        browser_service = BrowserService(settings=settings)
        result.browser_service = browser_service

        # ── Получение токена с retry и заменой прокси ──
        token: str | None = None
        page: Page | None = None

        for attempt in range(1, _MAX_TOKEN_ATTEMPTS + 1):
            token, page = await self._start_and_get_token(
                browser_service=browser_service,
                proxy=current_proxy,
                search_url=search_url,
                worker_idx=worker_idx,
                attempt=attempt,
            )

            if token is not None and page is not None:
                break

            # Сбой — пробуем замену прокси
            if proxy_service is not None and attempt < _MAX_TOKEN_ATTEMPTS:
                logger.warning(
                    "batch_воркер_замена_прокси",
                    step=f"воркер={worker_idx}, попытка={attempt}/{_MAX_TOKEN_ATTEMPTS}",
                )

                replacement = await proxy_service.get_replacement_proxy(
                    current_proxy=current_proxy,
                )

                if replacement is not None:
                    current_proxy = replacement
                    logger.info(
                        "batch_воркер_новая_прокси",
                        step=f"воркер={worker_idx}, прокси={replacement}",
                    )
                else:
                    logger.warning(
                        "batch_воркер_замена_не_найдена",
                        step=f"воркер={worker_idx}",
                    )
                    break

            await asyncio.sleep(_TOKEN_RETRY_PAUSE)

        if token is None or page is None:
            result.failed = True
            result.duration = time.perf_counter() - worker_start
            logger.warning(
                "batch_воркер_не_получил_токен",
                step=f"воркер={worker_idx}, попыток={_MAX_TOKEN_ATTEMPTS}",
            )
            return result

        logger.info(
            "batch_воркер_готов",
            step=f"воркер={worker_idx}, объектов={len(object_ids)}, "
                 f"прокси={current_proxy}",
        )

        # ── Фаза 1: Batch bulk ──
        result.bulk_results = await self._phase_bulk(
            page, token, object_ids, today,
        )

        # Определяем busy-ID для фазы 2
        busy_ids: list[int] = []
        for br in result.bulk_results:
            if br.success and br.busy == "busy":
                busy_ids.append(br.object_id)

        # ── Фаза 2: Batch скользящее окно ──
        if busy_ids:
            result.calendars = await self._phase_sliding_window(
                page, token, busy_ids, today,
            )

        result.duration = time.perf_counter() - worker_start

        logger.info(
            "batch_воркер_завершён",
            step=f"воркер={worker_idx}, "
                 f"bulk={len(result.bulk_results)}, "
                 f"calendars={len(result.calendars)}, "
                 f"время={format_duration(result.duration)}",
        )

        return result

    async def _start_and_get_token(
        self,
        browser_service: BrowserService,
        proxy: ProxyConfig,
        search_url: str,
        worker_idx: int,
        attempt: int,
    ) -> tuple[str | None, Page | None]:
        """Запускает браузер, загружает страницу поиска, перехватывает токен.

        Route handler безопасно обрабатывает «Route is already handled!» —
        эта ошибка возникает когда page.goto() таймаутит и Playwright
        отменяет pending-запросы, а route handler всё ещё пытается
        продолжить уже отменённый запрос.

        После загрузки route снимается через page.unroute() —
        это критически важно, иначе route handler будет перехватывать
        fetch()-запросы из _fetch_batch() и вызывать «Route is already
        handled!» при последующих page.evaluate().

        Args:
            browser_service: Сервис браузера.
            proxy: Прокси для запуска.
            search_url: URL страницы поиска.
            worker_idx: Номер воркера (для логов).
            attempt: Номер попытки.

        Returns:
            Кортеж (token, page) или (None, None) при ошибке.
        """
        try:
            # Останавливаем предыдущий браузер (если был)
            try:
                await browser_service.stop()
            except Exception:
                pass

            await browser_service.start(proxy=proxy)
            page = browser_service.page

            # Перехватываем токен
            captured_token: list[str] = []

            async def _intercept(route, request):
                url = request.url
                if "sutochno.ru/api/json" in url and not captured_token:
                    token = (
                        request.headers.get("token")
                        or request.headers.get("Token")
                    )
                    if token:
                        captured_token.append(token)
                # При таймауте page.goto() Playwright отменяет pending-запросы.
                # Если route.continue_() вызывается для уже отменённого запроса —
                # выбрасывается «Route is already handled!». Это не ошибка логики —
                # безопасно игнорируем.
                try:
                    await route.continue_()
                except Exception as e:
                    if "Route is already handled" not in str(e):
                        raise

            await page.route("**/api/json/**", _intercept)

            logger.debug(
                "batch_воркер_загрузка_страницы",
                step=f"воркер={worker_idx}, попытка={attempt}",
            )

            # Таймаут навигации: SPA sutochno.ru через медленные прокси
            # загружается за 15–40 секунд. 45 секунд — компромисс
            # между скоростью и стабильностью.
            await page.goto(
                search_url,
                wait_until="domcontentloaded",
                timeout=45000,
            )

            # Ожидание перехвата токена
            elapsed = 0.0
            while elapsed < _TOKEN_INTERCEPT_TIMEOUT:
                if captured_token:
                    break
                await asyncio.sleep(_TOKEN_POLL_INTERVAL)
                elapsed += _TOKEN_POLL_INTERVAL

            # Снимаем route handler — критически важно!
            # Без этого handler будет перехватывать fetch()-запросы
            # из _fetch_batch() и вызывать «Route is already handled!»
            # при последующих page.evaluate().
            try:
                await page.unroute("**/api/json/**")
            except Exception:
                pass

            # Дополнительное ожидание для стабилизации сессии
            await asyncio.sleep(_POST_LOAD_WAIT)

            if not captured_token:
                logger.warning(
                    "batch_воркер_токен_не_перехвачен",
                    step=f"воркер={worker_idx}, попытка={attempt}, "
                         f"ожидание={elapsed:.1f}с",
                )
                return None, None

            logger.info(
                "batch_воркер_токен_получен",
                step=f"воркер={worker_idx}, попытка={attempt}, "
                     f"ожидание={elapsed:.1f}с",
            )

            return captured_token[0], page

        except Exception as e:
            logger.warning(
                "batch_воркер_ошибка_старта",
                error=str(e)[:200],
                error_type=type(e).__name__,
                step=f"воркер={worker_idx}, попытка={attempt}",
            )
            # Снимаем route при ошибке — чтобы не мешал при retry
            try:
                if browser_service._page is not None:  # noqa: SLF001
                    await browser_service.page.unroute("**/api/json/**")
            except Exception:
                pass
            return None, None

    # ══════════════════════════════════════════════════════════
    #  Однопоточный режим (один браузер, без прокси)
    # ══════════════════════════════════════════════════════════

    async def enrich_batch(
        self,
        page: Page,
        token: str,
        listings: list[RawListing],
    ) -> list[RawListing]:
        """Обогащает карточки batch-запросами через один браузер (без прокси).

        Args:
            page: Страница Playwright (со страницы поиска, с живой сессией).
            token: Сессионный токен API (перехвачен со страницы поиска).
            listings: Список карточек для обогащения.

        Returns:
            Тот же список карточек с заполненными данными.
        """
        if not listings:
            return listings

        today = date.today()
        start_time = time.perf_counter()
        total = len(listings)

        logger.info(
            "batch_обогащение_начало",
            step=f"карточек={total}, batch_size={BATCH_SIZE}",
        )

        listings_map: dict[str, RawListing] = {
            l.external_id: l for l in listings
        }

        all_ids = [int(l.external_id) for l in listings]

        # ── Фаза 1: Batch bulk ──
        bulk_results = await self._phase_bulk(page, token, all_ids, today)

        busy_ids: list[int] = []
        self._apply_bulk_results(bulk_results, listings_map, today)

        for br in bulk_results:
            if br.success and br.busy == "busy":
                busy_ids.append(br.object_id)

        enriched_count = sum(
            1 for l in listings
            if l.calendar_60_days and l.calendar_60_days == [0] * DAYS_COUNT
            and l.prices_60_days and any(p > 0 for p in l.prices_60_days)
        )
        fatal_count = sum(
            1 for l in listings if l.enrichment_skip_reason is not None
        )

        logger.info(
            "batch_фаза_1_завершена",
            step=f"unbusy={enriched_count}, busy={len(busy_ids)}, "
                 f"фатальных={fatal_count}",
        )

        # ── Фаза 2: Batch скользящее окно ──
        if busy_ids:
            calendars = await self._phase_sliding_window(
                page, token, busy_ids, today,
            )
            self._apply_calendars(calendars, listings_map)

        elapsed = time.perf_counter() - start_time

        final_enriched = sum(
            1 for l in listings
            if (l.calendar_60_days and any(c == 1 for c in l.calendar_60_days))
            or (l.prices_60_days and any(p > 0 for p in l.prices_60_days))
        )
        final_fatal = sum(
            1 for l in listings if l.enrichment_skip_reason is not None
        )
        final_empty = total - final_enriched - final_fatal

        logger.info(
            "batch_обогащение_завершено",
            step=f"время={format_duration(elapsed)}, "
                 f"обогащено={final_enriched}, "
                 f"фатальных={final_fatal}, "
                 f"для_fallback={final_empty}",
        )

        return listings

    # ══════════════════════════════════════════════════════════
    #  Применение результатов к карточкам
    # ══════════════════════════════════════════════════════════

    def _apply_bulk_results(
        self,
        bulk_results: list[_ObjectBulkResult],
        listings_map: dict[str, RawListing],
        today: date,
    ) -> None:
        """Применяет результаты bulk-запросов к карточкам.

        Args:
            bulk_results: Результаты фазы 1.
            listings_map: Индекс external_id → RawListing.
            today: Дата начала календаря.
        """
        for result in bulk_results:
            ext_id = str(result.object_id)
            listing = listings_map.get(ext_id)
            if listing is None:
                continue

            if result.skip_reason is not None:
                listing.enrichment_skip_reason = result.skip_reason
                continue

            if not result.success:
                continue

            prices_60 = self._price_parser.extract_prices_from_detail(
                result.detail, today=today,
            )

            if result.busy == "unbusy":
                listing.calendar_60_days = [0] * DAYS_COUNT
                listing.prices_60_days = prices_60
            elif result.busy == "busy":
                listing.prices_60_days = prices_60

    def _apply_calendars(
        self,
        calendars: dict[int, list[int]],
        listings_map: dict[str, RawListing],
    ) -> None:
        """Применяет результаты скользящего окна к карточкам.

        Args:
            calendars: Словарь {object_id: calendar_60_days}.
            listings_map: Индекс external_id → RawListing.
        """
        for obj_id, calendar in calendars.items():
            ext_id = str(obj_id)
            listing = listings_map.get(ext_id)
            if listing is None:
                continue

            listing.calendar_60_days = calendar

            if listing.prices_60_days:
                listing.prices_60_days = [
                    0 if i < len(calendar) and calendar[i] == 1
                    else listing.prices_60_days[i]
                    for i in range(min(DAYS_COUNT, len(listing.prices_60_days)))
                ]

    # ══════════════════════════════════════════════════════════
    #  Фаза 1: Batch bulk
    # ══════════════════════════════════════════════════════════

    async def _phase_bulk(
        self,
        page: Page,
        token: str,
        all_ids: list[int],
        today: date,
    ) -> list[_ObjectBulkResult]:
        """Отправляет batch bulk-запросы на 60 ночей.

        Args:
            page: Страница Playwright.
            token: Токен API.
            all_ids: Все ID объявлений.
            today: Дата начала календаря.

        Returns:
            Список результатов для каждого объекта.
        """
        date_begin = f"{today.isoformat()} 14:00:00"
        date_end = f"{(today + timedelta(days=DAYS_COUNT)).isoformat()} 11:00:00"

        all_results: list[_ObjectBulkResult] = []
        total_batches = (len(all_ids) + BATCH_SIZE - 1) // BATCH_SIZE

        for batch_idx in range(0, len(all_ids), BATCH_SIZE):
            batch = all_ids[batch_idx: batch_idx + BATCH_SIZE]
            batch_num = batch_idx // BATCH_SIZE + 1

            results = await self._fetch_batch(
                page, token, batch, date_begin, date_end,
                guests=DEFAULT_GUESTS,
            )

            retry_with_reduced_guests: list[int] = []

            for result in results:
                classified = self._classify_result(result)

                if classified.skip_reason is not None or classified.success:
                    all_results.append(classified)
                elif self._is_guests_error(classified.error_text):
                    retry_with_reduced_guests.append(classified.object_id)
                else:
                    all_results.append(classified)

            if retry_with_reduced_guests:
                retry_results = await self._fetch_batch(
                    page, token, retry_with_reduced_guests,
                    date_begin, date_end, guests=FALLBACK_GUESTS,
                )
                for result in retry_results:
                    all_results.append(self._classify_result(result))

            if batch_num < total_batches:
                await asyncio.sleep(BATCH_PAUSE)

            if batch_num % 10 == 0 or batch_num == total_batches:
                logger.info(
                    "batch_bulk_прогресс",
                    step=f"пачка={batch_num}/{total_batches}, "
                         f"обработано={len(all_results)}",
                )

        return all_results

    # ══════════════════════════════════════════════════════════
    #  Фаза 2: Batch скользящее окно
    # ══════════════════════════════════════════════════════════

    async def _phase_sliding_window(
        self,
        page: Page,
        token: str,
        busy_ids: list[int],
        today: date,
    ) -> dict[int, list[int]]:
        """Определяет занятость каждого дня для busy-объектов.

        После основного прохода и адаптации min_nights выполняет
        до _SLIDING_RETRY_ROUNDS повторных проходов только по ячейкам
        со статусом -1 (сетевые ошибки). Это устраняет временные
        сбои прокси без перезапуска браузера.

        Args:
            page: Страница Playwright.
            token: Токен API.
            busy_ids: ID объектов со статусом busy.
            today: Дата начала календаря.

        Returns:
            Словарь {object_id: calendar_60_days}.
        """
        logger.info(
            "batch_скользящее_окно_начало",
            step=f"объектов={len(busy_ids)}, дней={DAYS_COUNT}",
        )

        calendars: dict[int, list[int]] = {
            obj_id: [-1] * DAYS_COUNT for obj_id in busy_ids
        }

        # Основная группа с nights=2
        await self._sliding_window_for_group(
            page, token, list(busy_ids), _DEFAULT_SLIDING_NIGHTS, today, calendars,
        )

        # Адаптация min_nights для объектов с полным провалом
        failed_ids = [
            obj_id for obj_id, cal in calendars.items()
            if all(c == -1 for c in cal)
        ]

        if failed_ids:
            logger.info(
                "batch_скользящее_окно_адаптация",
                step=f"не_определённых={len(failed_ids)}",
            )

            for nights in MIN_NIGHTS_VARIANTS:
                if nights <= _DEFAULT_SLIDING_NIGHTS:
                    continue

                still_failed = [
                    obj_id for obj_id in failed_ids
                    if all(c == -1 for c in calendars[obj_id])
                ]
                if not still_failed:
                    break

                await self._sliding_window_for_group(
                    page, token, still_failed, nights, today, calendars,
                )

        # ── Retry ошибочных ячеек (сетевые сбои) ──
        for retry_round in range(1, _SLIDING_RETRY_ROUNDS + 1):
            # Подсчитываем оставшиеся ошибочные ячейки
            error_count = sum(
                1 for cal in calendars.values()
                for c in cal if c == -1
            )

            if error_count == 0:
                break

            # Определяем объекты с хотя бы одной ошибочной ячейкой
            retry_ids = [
                obj_id for obj_id, cal in calendars.items()
                if any(c == -1 for c in cal)
            ]

            logger.info(
                "batch_скользящее_окно_retry",
                step=f"раунд={retry_round}/{_SLIDING_RETRY_ROUNDS}, "
                     f"ошибочных_дней={error_count}, "
                     f"объектов={len(retry_ids)}",
            )

            # Пауза перед retry — даём прокси/серверу восстановиться
            await asyncio.sleep(_SLIDING_RETRY_PAUSE)

            # Повторный проход только по ошибочным ячейкам
            # (метод _sliding_window_for_group уже пропускает ячейки != -1)
            await self._sliding_window_for_group(
                page, token, retry_ids, _DEFAULT_SLIDING_NIGHTS, today, calendars,
            )

            # Проверяем, помог ли retry
            new_error_count = sum(
                1 for cal in calendars.values()
                for c in cal if c == -1
            )

            resolved = error_count - new_error_count
            logger.info(
                "batch_скользящее_окно_retry_результат",
                step=f"раунд={retry_round}, "
                     f"было_ошибок={error_count}, "
                     f"исправлено={resolved}, "
                     f"осталось={new_error_count}",
            )

            # Если retry не исправил ни одной ячейки — прекращаем
            # (проблема не временная, дальнейшие попытки бессмысленны)
            if resolved == 0:
                logger.info(
                    "batch_скользящее_окно_retry_стоп",
                    step=f"раунд={retry_round}, нет_прогресса",
                )
                break

        # Нормализация оставшихся ошибок
        error_count = 0
        for obj_id, cal in calendars.items():
            errors = sum(1 for c in cal if c == -1)
            if errors > 0:
                error_count += errors
            calendars[obj_id] = [0 if c == -1 else c for c in cal]

        if error_count > 0:
            logger.warning(
                "batch_скользящее_окно_ошибки",
                step=f"ошибочных_дней={error_count}",
            )

        logger.info(
            "batch_скользящее_окно_завершено",
            step=f"объектов={len(busy_ids)}",
        )

        return calendars

    async def _sliding_window_for_group(
        self,
        page: Page,
        token: str,
        group_ids: list[int],
        nights: int,
        today: date,
        calendars: dict[int, list[int]],
    ) -> None:
        """Обрабатывает скользящее окно для группы объектов.

        Args:
            page: Страница Playwright.
            token: Токен API.
            group_ids: ID объектов в группе.
            nights: Количество ночей в окне.
            today: Дата начала календаря.
            calendars: Общий словарь календарей (мутируется).
        """
        for day_offset in range(DAYS_COUNT):
            day = today + timedelta(days=day_offset)
            end_day = day + timedelta(days=nights)
            date_begin = f"{day.isoformat()} 14:00:00"
            date_end = f"{end_day.isoformat()} 11:00:00"

            pending_ids = [
                obj_id for obj_id in group_ids
                if calendars[obj_id][day_offset] == -1
            ]
            if not pending_ids:
                continue

            for batch_start in range(0, len(pending_ids), BATCH_SIZE):
                batch = pending_ids[batch_start: batch_start + BATCH_SIZE]

                results = await self._fetch_batch(
                    page, token, batch, date_begin, date_end,
                    guests=DEFAULT_GUESTS,
                )

                for raw in results:
                    obj_id = raw.get("object_id")
                    if obj_id is None or obj_id not in calendars:
                        continue

                    if raw.get("success"):
                        busy = raw.get("busy")
                        if busy == "busy":
                            calendars[obj_id][day_offset] = 1
                        elif busy == "unbusy":
                            calendars[obj_id][day_offset] = 0
                    else:
                        error_text = raw.get("error_text", "")
                        if self._is_min_nights_error(error_text):
                            calendars[obj_id][day_offset] = 0

                if batch_start + BATCH_SIZE < len(pending_ids):
                    await asyncio.sleep(BATCH_PAUSE)

            if day_offset < DAYS_COUNT - 1:
                await asyncio.sleep(BATCH_PAUSE)

            if (day_offset + 1) % 10 == 0:
                logger.info(
                    "batch_скользящее_окно_прогресс",
                    step=f"день={day_offset + 1}/{DAYS_COUNT}, "
                         f"nights={nights}, объектов={len(group_ids)}",
                )

    # ══════════════════════════════════════════════════════════
    #  Низкоуровневый fetch
    # ══════════════════════════════════════════════════════════

    async def _fetch_batch(
        self,
        page: Page,
        token: str,
        object_ids: list[int],
        date_begin: str,
        date_end: str,
        guests: int = DEFAULT_GUESTS,
    ) -> list[dict]:
        """Выполняет один batch-запрос getPricesAndAvailabilities.

        Args:
            page: Страница Playwright.
            token: Токен API.
            object_ids: Список ID объявлений.
            date_begin: Дата начала периода.
            date_end: Дата конца периода.
            guests: Количество гостей.

        Returns:
            Список словарей с результатом для каждого объекта.
        """
        try:
            raw_result = await asyncio.wait_for(
                page.evaluate(
                    """
                    async ({apiUrl, objectIds, dateBegin, dateEnd, token,
                            guests, fetchTimeout}) => {
                        try {
                            const controller = new AbortController();
                            const tid = setTimeout(
                                () => controller.abort(), fetchTimeout * 1000
                            );

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
                                return {
                                    success: false,
                                    error: 'http_' + resp.status
                                };
                            }

                            const data = await resp.json();

                            if (!data.success) {
                                return {
                                    success: false,
                                    error: 'api_false',
                                    errors: data.errors || []
                                };
                            }

                            if (!data.data || !data.data.objects) {
                                return {success: false, error: 'no_objects_array'};
                            }

                            const results = [];
                            for (const obj of data.data.objects) {
                                if (obj.success) {
                                    results.push({
                                        object_id: obj.id,
                                        success: true,
                                        busy: obj.data.busy,
                                        detail: obj.data.detail || [],
                                        price: obj.data.price,
                                        price_default: obj.data.price_default,
                                        rooms_available: obj.data.rooms_available
                                    });
                                } else {
                                    results.push({
                                        object_id: obj.id,
                                        success: false,
                                        error_text: JSON.stringify(
                                            obj.errors || []
                                        )
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
                        "objectIds": object_ids,
                        "dateBegin": date_begin,
                        "dateEnd": date_end,
                        "token": token,
                        "guests": guests,
                        "fetchTimeout": _FETCH_TIMEOUT_SECONDS,
                    },
                ),
                timeout=_EVALUATE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "batch_evaluate_таймаут",
                step=f"ids={len(object_ids)}, таймаут={_EVALUATE_TIMEOUT}с",
            )
            return [
                {"object_id": oid, "success": False, "error_text": "evaluate_timeout"}
                for oid in object_ids
            ]
        except Exception as e:
            logger.warning(
                "batch_evaluate_исключение",
                error=str(e)[:200],
                error_type=type(e).__name__,
            )
            return [
                {"object_id": oid, "success": False, "error_text": str(e)[:100]}
                for oid in object_ids
            ]

        if not raw_result.get("success"):
            error = raw_result.get("error", "unknown")
            logger.warning(
                "batch_fetch_ошибка",
                step=f"ids={len(object_ids)}, ошибка={error}",
            )
            return [
                {"object_id": oid, "success": False, "error_text": error}
                for oid in object_ids
            ]

        return raw_result.get("results", [])

    # ══════════════════════════════════════════════════════════
    #  Классификация и утилиты
    # ══════════════════════════════════════════════════════════

    def _classify_result(self, raw: dict) -> _ObjectBulkResult:
        """Классифицирует результат API для одного объекта."""
        obj_id = raw.get("object_id", 0)

        if raw.get("success"):
            return _ObjectBulkResult(
                object_id=obj_id,
                success=True,
                busy=raw.get("busy"),
                detail=raw.get("detail", []),
            )

        error_text = raw.get("error_text", "").lower()

        if "no_objects" in error_text:
            return _ObjectBulkResult(
                object_id=obj_id,
                is_fatal=True,
                skip_reason="object_not_found",
                error_text=error_text,
            )

        min_nights_value = self._extract_min_nights(error_text)
        if min_nights_value is not None and min_nights_value >= _MIN_NIGHTS_SKIP_THRESHOLD:
            return _ObjectBulkResult(
                object_id=obj_id,
                is_fatal=True,
                skip_reason="min_nights_exceeded",
                error_text=error_text,
            )

        return _ObjectBulkResult(
            object_id=obj_id,
            error_text=error_text,
        )

    @staticmethod
    def _distribute_ids(all_ids: list[int], worker_count: int) -> list[list[int]]:
        """Распределяет ID поровну между воркерами.

        Args:
            all_ids: Все ID для распределения.
            worker_count: Количество воркеров.

        Returns:
            Список списков ID — порция для каждого воркера.
        """
        chunks: list[list[int]] = [[] for _ in range(worker_count)]
        for idx, obj_id in enumerate(all_ids):
            chunks[idx % worker_count].append(obj_id)
        return chunks

    @staticmethod
    def _is_min_nights_error(error_text: str) -> bool:
        """Проверяет, является ли ошибка связанной с min_nights."""
        text = error_text.lower()
        return any(kw in text for kw in MIN_NIGHTS_ERROR_KEYWORDS)

    @staticmethod
    def _is_guests_error(error_text: str) -> bool:
        """Проверяет, является ли ошибка связанной с вместимостью гостей."""
        text = error_text.lower()
        return any(kw in text for kw in GUESTS_ERROR_KEYWORDS)

    @staticmethod
    def _extract_min_nights(error_text: str) -> int | None:
        """Извлекает значение min_nights из текста ошибки."""
        if not error_text:
            return None

        is_min_nights = any(
            kw in error_text for kw in MIN_NIGHTS_ERROR_KEYWORDS
        )
        if not is_min_nights:
            return None

        numbers = re.findall(r"(\d+)", error_text)
        for num_str in numbers:
            num = int(num_str)
            if 2 <= num <= 999:
                return num

        return None
