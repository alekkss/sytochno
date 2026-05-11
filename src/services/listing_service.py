"""Сервис парсинга карточки объявления — извлечение календаря занятости и цен через API."""

import asyncio
import re
import time
from datetime import date, timedelta

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService

logger = get_logger("listing")

# URL внутреннего API для получения цен и занятости
_API_PRICES_URL: str = "https://sutochno.ru/api/json/objects/getPricesAndAvailabilities"

# Количество дней в одном пакетном запросе к API
_API_BATCH_SIZE: int = 5

# Пауза между пакетами запросов (секунды) — защита от rate-limit
_API_BATCH_DELAY: float = 0.5

# Максимальное количество попыток загрузки страницы карточки
_MAX_GOTO_RETRIES: int = 3

# Пауза между повторными попытками загрузки (секунды)
_GOTO_RETRY_DELAY: float = 5.0

# Таймаут остановки одного прокси-браузера (секунды)
_WORKER_STOP_TIMEOUT: float = 15.0

# Таймаут мягкого ожидания networkidle (мс)
_NETWORKIDLE_SOFT_TIMEOUT_MS: int = 10000

# Селекторы, подтверждающие что карточка загрузилась
_PAGE_READY_SELECTORS: list[str] = [
    ".sc-detail-dates",
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]

# Таймаут ожидания готовности страницы (мс)
_PAGE_READY_TIMEOUT_MS: int = 15000

# Количество гостей по умолчанию (используется в API-запросе)
_DEFAULT_GUESTS: int = 2

# Максимальное количество retry при полном провале
_MAX_API_RETRIES: int = 2

# Пауза перед повторной попыткой после перезагрузки страницы (секунды)
_RELOAD_WAIT_SECONDS: float = 15.0

# Варианты min_nights для адаптивного запроса (по возрастанию)
_MIN_NIGHTS_VARIANTS: list[int] = [2, 3, 5, 7]

# Количество дней для анализа
_DAYS_COUNT: int = 60

# Ключевые слова в ответе API, указывающие на ограничение min_nights
_MIN_NIGHTS_ERROR_KEYWORDS: list[str] = [
    "min_nights",
    "minimum_nights",
    "минимальный срок",
    "минимальное количество суток",
    "минимальное количество",
    "минимум",
    "суток",
    "сут.",
    "nights_min",
    "min_duration",
    "minimalnoe_kolichestvo",
    "minimum_stay",
    "min_stay",
]


def _format_duration(seconds: float) -> str:
    """Форматирует длительность в секундах в человекочитаемый вид.

    Args:
        seconds: Длительность в секундах.

    Returns:
        Строка вида «Xм Yс» или «Yс» если менее минуты.
    """
    total_seconds = int(seconds)
    minutes = total_seconds // 60
    secs = total_seconds % 60

    if minutes > 0:
        return f"{minutes}м {secs}с"
    return f"{secs}с"


async def _safe_stop_browser(browser_service: BrowserService, worker_idx: int) -> None:
    """Безопасно останавливает прокси-браузер с таймаутом.

    Args:
        browser_service: Экземпляр BrowserService для остановки.
        worker_idx: Номер воркера (для логов).
    """
    try:
        await asyncio.wait_for(
            browser_service.stop(),
            timeout=_WORKER_STOP_TIMEOUT,
        )
        logger.info(
            "воркер_браузер_остановлен",
            step=f"воркер={worker_idx}",
        )
    except asyncio.TimeoutError:
        logger.warning(
            "воркер_браузер_таймаут_остановки",
            step=f"воркер={worker_idx}, лимит={_WORKER_STOP_TIMEOUT}с",
        )
    except Exception as e:
        logger.warning(
            "воркер_ошибка_остановки_браузера",
            error=str(e),
            error_type=type(e).__name__,
            step=f"воркер={worker_idx}",
        )


class ListingService:
    """Сервис парсинга карточки объявления на sutochno.ru.

    Использует гибридную стратегию получения данных через API:

    Шаг 1 (быстрый): Один запрос на 60 ночей → все цены из detail[].
        - Фильтрует только type="season_price" (исключает скидки "interval").
        - Разворачивает сезонные периоды в дневные цены.
        - Если busy="unbusy" → все дни свободны, готово за 1 запрос (~600мс).

    Шаг 2 (при необходимости): 60 запросов с nights=min_nights → занятость.
        - Выполняется ТОЛЬКО если Шаг 1 вернул busy="busy".
        - Определяет занят/свободен каждый конкретный день.
        - При ошибке min_nights → адаптирует количество ночей.

    Поддерживает три режима обработки:
    - Последовательный: одна вкладка, карточки по очереди.
    - Параллельные вкладки: N вкладок в одном браузере.
    - Прокси + вкладки: M браузеров × N вкладок в каждом.
    """

    def __init__(self, settings: Settings, browser_service: BrowserService) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
        """
        self._settings = settings
        self._browser = browser_service

    # ─────────────────────────────────────────────────────────────────────
    # Загрузка страницы с перехватом токена
    # ─────────────────────────────────────────────────────────────────────

    async def _goto_and_capture_token(self, page: Page, url: str) -> tuple[bool, str | None]:
        """Загружает страницу карточки и перехватывает токен API из исходящих запросов.

        Args:
            page: Вкладка браузера.
            url: URL карточки.

        Returns:
            Кортеж (страница_загружена, токен_или_None).
        """
        captured_token: list[str] = []

        def on_request(request: "any") -> None:  # type: ignore[name-defined]
            """Синхронный обработчик — перехватывает токен из заголовков."""
            if captured_token:
                return
            req_url = request.url
            if "sutochno.ru/api/json" in req_url:
                token_header = request.headers.get("token")
                if token_header:
                    captured_token.append(token_header)

        page.on("request", on_request)

        try:
            loaded = await self._goto_with_retry(page, url)
        finally:
            page.remove_listener("request", on_request)

        token = captured_token[0] if captured_token else None

        if token:
            logger.debug(
                "токен_перехвачен",
                step=f"длина={len(token)}, источник=request_header",
            )
        else:
            logger.debug("токен_не_перехвачен_при_загрузке")

        return loaded, token

    async def _goto_with_retry(self, page: Page, url: str) -> bool:
        """Загружает страницу карточки с повторными попытками при сетевых ошибках.

        Args:
            page: Вкладка браузера.
            url: URL карточки.

        Returns:
            True если страница загружена, False — если все попытки исчерпаны.
        """
        for attempt in range(1, _MAX_GOTO_RETRIES + 1):
            try:
                logger.debug(
                    "goto_попытка",
                    step=f"попытка={attempt}/{_MAX_GOTO_RETRIES}",
                    path=url,
                )

                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                try:
                    await page.wait_for_load_state(
                        "networkidle", timeout=_NETWORKIDLE_SOFT_TIMEOUT_MS
                    )
                except Exception:
                    logger.debug(
                        "networkidle_не_достигнут_продолжаем",
                        step=f"попытка={attempt}",
                    )

                page_ready = await self._wait_for_page_ready(page)
                if page_ready:
                    logger.debug("страница_готова", step=f"попытка={attempt}")
                    return True

                logger.debug(
                    "элементы_не_найдены_но_продолжаем",
                    step=f"попытка={attempt}",
                )
                return True

            except Exception as e:
                error_msg = str(e)
                is_network_error = any(
                    err in error_msg
                    for err in [
                        "ERR_TIMED_OUT",
                        "ERR_CONNECTION_RESET",
                        "ERR_CONNECTION_CLOSED",
                        "ERR_CONNECTION_REFUSED",
                        "ERR_PROXY_CONNECTION_FAILED",
                        "ERR_TUNNEL_CONNECTION_FAILED",
                        "NS_ERROR_NET_RESET",
                    ]
                )

                if is_network_error and attempt < _MAX_GOTO_RETRIES:
                    logger.warning(
                        "сетевая_ошибка_повтор",
                        error=error_msg[:200],
                        step=f"попытка={attempt}/{_MAX_GOTO_RETRIES}",
                    )
                    await asyncio.sleep(_GOTO_RETRY_DELAY)
                    continue

                logger.warning(
                    "goto_не_удался",
                    error=error_msg[:200],
                    error_type=type(e).__name__,
                    step=f"попытка={attempt}/{_MAX_GOTO_RETRIES}",
                )
                return False

        return False

    async def _wait_for_page_ready(self, page: Page) -> bool:
        """Ожидает появления ключевых элементов на странице карточки.

        Args:
            page: Вкладка браузера.

        Returns:
            True если хотя бы один ключевой элемент найден.
        """
        for selector in _PAGE_READY_SELECTORS:
            try:
                await page.wait_for_selector(selector, timeout=_PAGE_READY_TIMEOUT_MS)
                return True
            except Exception:
                continue
        return False

    # ─────────────────────────────────────────────────────────────────────
    # Шаг 1: Один запрос на 60 ночей — получение всех цен
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_bulk_prices(
        self, page: Page, object_id: str, token: str
    ) -> tuple[str | None, list[int], bool]:
        """Получает все цены одним запросом на 60 ночей.

        Отправляет один запрос с date_begin=завтра, date_end=завтра+60.
        Из ответа извлекает:
        - detail[] с type="season_price" → дневные цены
        - busy → общий статус занятости на весь период
        - При ошибке min_nights → возвращает None (нужна адаптация)

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.

        Returns:
            Кортеж (busy_status, prices_60_days, success).
            busy_status: "unbusy"/"busy"/None (None = ошибка).
            prices_60_days: Список из 60 цен (0 если нет данных для дня).
            success: True если запрос успешен.
        """
        today = date.today()
        start_date = today
        end_date = today + timedelta(days=_DAYS_COUNT)
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        date_begin = f"{start_date.isoformat()} 14:00:00"
        date_end = f"{end_date.isoformat()} 11:00:00"

        logger.debug(
            "запрос_цен_bulk_60",
            step=f"id={object_id}, период={start_date}→{end_date}",
        )

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
                        return {success: false, error: 'http_' + resp.status};
                    }

                    const data = await resp.json();

                    if (!data.success) {
                        return {success: false, error: 'api_false'};
                    }

                    if (!data.data || !data.data.objects || !data.data.objects[0]) {
                        return {success: false, error: 'no_objects'};
                    }

                    const obj = data.data.objects[0];

                    if (!obj.success) {
                        return {
                            success: false,
                            error: 'obj_error',
                            errors: obj.errors || []
                        };
                    }

                    const objData = obj.data;
                    return {
                        success: true,
                        busy: objData.busy,
                        detail: objData.detail || [],
                        rooms_available: objData.rooms_available
                    };

                } catch (e) {
                    return {success: false, error: 'exception_' + e.message};
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

        if not result.get("success"):
            error = result.get("error", "unknown")
            errors_list = result.get("errors", [])
            logger.debug(
                "bulk_запрос_ошибка",
                step=f"id={object_id}, ошибка={error}, errors={errors_list}",
            )
            return None, [0] * _DAYS_COUNT, False

        busy_status = result.get("busy")
        detail = result.get("detail", [])

        # Разворачиваем detail[] в дневные цены
        # Берём ТОЛЬКО записи с type="season_price" (исключаем "interval" — скидки)
        daily_prices: dict[str, int] = {}

        for det in detail:
            det_type = det.get("type", "")
            if det_type != "season_price":
                continue

            d_begin = det.get("date_begin")
            d_end = det.get("date_end")
            cost = det.get("cost", 0)

            if not d_begin or not d_end or not cost:
                continue

            # Извлекаем дату (первые 10 символов)
            d_begin_str = str(d_begin)[:10]
            d_end_str = str(d_end)[:10]

            try:
                period_start = date.fromisoformat(d_begin_str)
                period_end = date.fromisoformat(d_end_str)
                current = period_start
                while current <= period_end:
                    daily_prices[current.isoformat()] = int(cost)
                    current += timedelta(days=1)
            except (ValueError, TypeError):
                continue

        # Формируем массив цен на 60 дней
        prices_60: list[int] = []
        for i in range(_DAYS_COUNT):
            day = today + timedelta(days=i)
            price = daily_prices.get(day.isoformat(), 0)
            prices_60.append(price)

        prices_filled = sum(1 for p in prices_60 if p > 0)

        logger.debug(
            "bulk_цены_получены",
            step=f"id={object_id}, busy={busy_status}, "
                 f"цен_заполнено={prices_filled}/60, "
                 f"detail_записей={len(detail)}, "
                 f"season_price_записей={len(daily_prices)}",
        )

        return busy_status, prices_60, True

    # ─────────────────────────────────────────────────────────────────────
    # Шаг 2: Определение занятости каждого дня (скользящее окно)
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_availability(
        self, page: Page, object_id: str, token: str, nights: int = 2
    ) -> tuple[list[int], list[dict[str, str | int]]]:
        """Определяет занятость каждого дня через скользящее окно.

        Для каждого из 60 дней отправляет запрос с окном в nights ночей.
        Из ответа берёт только busy-статус (цены уже получены в Шаге 1).

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            nights: Количество ночей в окне (по умолчанию 2).

        Returns:
            Кортеж (calendar_60_days, errors_details).
            calendar: 0=свободен, 1=занят, -1=ошибка.
        """
        today = date.today()
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        days_data = []
        for i in range(_DAYS_COUNT):
            day = today + timedelta(days=i)
            end_day = day + timedelta(days=nights)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end_day.isoformat()} 11:00:00",
            })

        logger.debug(
            "запрос_занятости",
            step=f"id={object_id}, дней=60, ночей={nights}, пакет={_API_BATCH_SIZE}",
        )

        result = await page.evaluate(
            """
            async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
                const results = [];
                const batches = [];

                for (let i = 0; i < daysData.length; i += batchSize) {
                    batches.push(daysData.slice(i, i + batchSize));
                }

                for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                    const batch = batches[batchIdx];

                    const promises = batch.map(async (dayInfo) => {
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
                                    date_begin: dayInfo.date_begin,
                                    date_end: dayInfo.date_end,
                                    currency_id: 1,
                                    is_pets: 0,
                                    documents: 0,
                                    target: 0,
                                    ages: [],
                                    no_time: 1
                                })
                            });

                            if (!resp.ok) {
                                return {status: 'error', error: 'http_' + resp.status};
                            }

                            const data = await resp.json();

                            if (!data.success || !data.data || !data.data.objects || !data.data.objects[0]) {
                                return {status: 'error', error: 'no_data'};
                            }

                            const obj = data.data.objects[0];

                            if (!obj.success) {
                                return {
                                    status: 'obj_error',
                                    errors: obj.errors || [],
                                    error_body: JSON.stringify(obj.errors || []).substring(0, 300)
                                };
                            }

                            return {
                                status: 'ok',
                                busy: obj.data.busy === 'busy'
                            };

                        } catch (e) {
                            return {status: 'error', error: 'exception_' + e.message};
                        }
                    });

                    const batchResults = await Promise.all(promises);
                    results.push(...batchResults);

                    if (batchIdx < batches.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, batchDelay * 1000));
                    }
                }

                return results;
            }
            """,
            {
                "objectId": object_id,
                "token": token,
                "guests": guests,
                "daysData": days_data,
                "batchSize": _API_BATCH_SIZE,
                "batchDelay": _API_BATCH_DELAY,
                "apiUrl": _API_PRICES_URL,
            },
        )

        calendar: list[int] = []
        errors_details: list[dict[str, str | int]] = []

        for day_idx, day_result in enumerate(result):
            status = day_result.get("status", "error")

            if status == "ok":
                calendar.append(1 if day_result.get("busy", False) else 0)
            elif status == "obj_error":
                errors_details.append({
                    "day": day_idx,
                    "error": "obj_error",
                    "errors": str(day_result.get("errors", [])),
                    "error_body": day_result.get("error_body", ""),
                })
                calendar.append(-1)
            else:
                errors_details.append({
                    "day": day_idx,
                    "error": day_result.get("error", "unknown"),
                })
                calendar.append(-1)

        free_days = sum(1 for c in calendar if c == 0)
        busy_days = sum(1 for c in calendar if c == 1)
        error_days = sum(1 for c in calendar if c == -1)

        logger.debug(
            "занятость_результат",
            step=f"id={object_id}, ночей={nights}",
            total=f"свободных={free_days}, занятых={busy_days}, ошибок={error_days}",
        )

        return calendar, errors_details

    # ─────────────────────────────────────────────────────────────────────
    # Определение min_nights из ошибок API
    # ─────────────────────────────────────────────────────────────────────

    def _detect_min_nights(self, errors_details: list[dict[str, str | int]]) -> int | None:
        """Анализирует ошибки API и определяет минимальный срок бронирования.

        Args:
            errors_details: Список словарей с подробностями об ошибках.

        Returns:
            Значение min_nights или None если не удалось определить.
        """
        if not errors_details:
            return None

        sample_errors = errors_details[:3]

        for error_info in sample_errors:
            error_body = str(error_info.get("error_body", "")).lower()
            error_code = str(error_info.get("error", "")).lower()
            errors_list = str(error_info.get("errors", "")).lower()

            combined_text = f"{error_body} {error_code} {errors_list}"

            is_min_nights_error = any(
                keyword in combined_text
                for keyword in _MIN_NIGHTS_ERROR_KEYWORDS
            )

            if is_min_nights_error:
                numbers = re.findall(r"(\d+)", combined_text)
                for num_str in numbers:
                    num = int(num_str)
                    if 2 <= num <= 30:
                        logger.info(
                            "min_nights_обнаружен",
                            step=f"min_nights={num}",
                        )
                        return num

                logger.info("min_nights_предположительно", step="пробуем=2")
                return 2

        error_count = len(errors_details)
        if error_count >= 55:
            unique_errors = set(str(e.get("error", "")) for e in errors_details)
            if len(unique_errors) <= 2:
                logger.info(
                    "массовая_ошибка_предполагаем_min_nights",
                    step=f"ошибок={error_count}/60",
                )
                return 2

        return None

    # ─────────────────────────────────────────────────────────────────────
    # Основная логика: гибридная стратегия
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_with_hybrid_strategy(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает календарь и цены гибридной стратегией.

        Алгоритм:
        1. Один запрос на 60 ночей → все цены из detail[type="season_price"].
        2. Если busy="unbusy" → все 60 дней свободны, готово за 1 запрос.
        3. Если busy="busy" → скользящее окно nights=2 для определения занятости.
        4. При ошибке min_nights → адаптация (nights=3, 5, 7).
        5. При полном провале → перезагрузка страницы + retry.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки (для перезагрузки при retry).

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        # ══════════════════════════════════════════════════════════════
        # Шаг 1: Один запрос на 60 ночей → цены
        # ══════════════════════════════════════════════════════════════
        busy_status, prices_60, bulk_success = await self._fetch_bulk_prices(
            page, object_id, current_token
        )

        if not bulk_success:
            # Bulk-запрос не удался (возможно min_nights для всего периода)
            # Переходим к классической стратегии со скользящим окном
            logger.info(
                "bulk_запрос_не_удался_переход_к_скользящему_окну",
                step=f"id={object_id}",
            )
            return await self._fetch_with_sliding_window(
                page, object_id, current_token, url
            )

        prices_filled = sum(1 for p in prices_60 if p > 0)

        # ══════════════════════════════════════════════════════════════
        # Шаг 2: Определение занятости
        # ══════════════════════════════════════════════════════════════

        if busy_status == "unbusy":
            # Весь период свободен — все 60 дней свободны!
            calendar_60 = [0] * _DAYS_COUNT
            logger.info(
                "все_дни_свободны_bulk",
                step=f"id={object_id}, цен={prices_filled}/60",
            )
            return calendar_60, prices_60

        # busy="busy" — нужно определить какие именно дни заняты
        logger.debug(
            "busy_период_определяем_занятость_по_дням",
            step=f"id={object_id}, busy={busy_status}",
        )

        # Скользящее окно для определения занятости
        calendar_60, errors_details = await self._fetch_availability(
            page, object_id, current_token, nights=2
        )

        error_days = sum(1 for c in calendar_60 if c == -1)

        # Если есть ошибки min_nights — адаптируем
        if error_days > 0:
            detected_min_nights = self._detect_min_nights(errors_details)

            if detected_min_nights is not None and detected_min_nights > 2:
                nights_to_try = [n for n in _MIN_NIGHTS_VARIANTS if n >= detected_min_nights]
                if detected_min_nights not in nights_to_try:
                    nights_to_try.insert(0, detected_min_nights)

                for nights in nights_to_try:
                    logger.info(
                        "адаптация_занятости",
                        step=f"id={object_id}, ночей={nights}",
                    )

                    calendar_retry, errors_retry = await self._fetch_availability(
                        page, object_id, current_token, nights=nights
                    )

                    error_days_retry = sum(1 for c in calendar_retry if c == -1)

                    if error_days_retry < error_days:
                        calendar_60 = calendar_retry
                        errors_details = errors_retry
                        error_days = error_days_retry

                        if error_days_retry == 0:
                            break

            # Нормализуем оставшиеся ошибки: -1 → 0 (ошибка ≠ занятость)
            if error_days > 0:
                calendar_60 = [0 if c == -1 else c for c in calendar_60]
                logger.info(
                    "ошибки_занятости_нормализованы",
                    step=f"id={object_id}, заменено={error_days}",
                )

        # Обнуляем цены для занятых дней
        final_prices: list[int] = []
        for i in range(_DAYS_COUNT):
            if calendar_60[i] == 1:
                final_prices.append(0)
            else:
                final_prices.append(prices_60[i])

        free_days = sum(1 for c in calendar_60 if c == 0)
        busy_days = sum(1 for c in calendar_60 if c == 1)

        logger.info(
            "гибридная_стратегия_завершена",
            step=f"id={object_id}",
            total=f"свободных={free_days}, занятых={busy_days}, цен={prices_filled}",
        )

        return calendar_60, final_prices

    # ─────────────────────────────────────────────────────────────────────
    # Запасная стратегия: скользящее окно (если bulk не работает)
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_with_sliding_window(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает данные через скользящее окно (запасная стратегия).

        Используется когда bulk-запрос на 60 ночей не удался.
        Отправляет 60 запросов с nights=2 (или больше при min_nights).
        Из каждого ответа берёт busy-статус И цену первого дня.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки (для перезагрузки).

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        # Начинаем с nights=2 (покрывает min_nights=2)
        for nights in _MIN_NIGHTS_VARIANTS:
            logger.info(
                "скользящее_окно",
                step=f"id={object_id}, ночей={nights}",
            )

            calendar, prices, errors_details = await self._fetch_prices_and_availability(
                page, object_id, current_token, nights=nights
            )

            error_days = sum(1 for c in calendar if c == -1)

            if error_days == 0:
                return calendar, prices

            if error_days < 55:
                # Частичный успех — нормализуем ошибки
                calendar = [0 if c == -1 else c for c in calendar]
                logger.info(
                    "частичный_результат_нормализован",
                    step=f"id={object_id}, ночей={nights}, ошибок={error_days}",
                )
                return calendar, prices

            # Проверяем, связана ли ошибка с min_nights
            detected = self._detect_min_nights(errors_details)
            if detected is None or detected <= nights:
                # Ошибка не связана с min_nights — не имеет смысла увеличивать
                break

        # Последняя попытка: перезагрузка
        for retry_attempt in range(1, _MAX_API_RETRIES + 1):
            logger.info(
                "перезагрузка_для_скользящего_окна",
                step=f"id={object_id}, попытка={retry_attempt}",
            )

            await asyncio.sleep(_RELOAD_WAIT_SECONDS)
            loaded, new_token = await self._goto_and_capture_token(page, url)

            if not loaded or not new_token:
                continue

            current_token = new_token
            await self._browser.random_delay()

            calendar, prices, _ = await self._fetch_prices_and_availability(
                page, object_id, current_token, nights=2
            )

            error_days = sum(1 for c in calendar if c == -1)
            if error_days < 60:
                calendar = [0 if c == -1 else c for c in calendar]
                return calendar, prices

        # Полный провал
        logger.warning(
            "скользящее_окно_провал",
            step=f"id={object_id}",
        )
        return [0] * _DAYS_COUNT, [0] * _DAYS_COUNT

    async def _fetch_prices_and_availability(
        self, page: Page, object_id: str, token: str, nights: int = 2
    ) -> tuple[list[int], list[int], list[dict[str, str | int]]]:
        """Получает и занятость, и цены через скользящее окно.

        Для каждого дня берёт busy-статус и detail[0].cost (цена первого дня).
        Используется как запасная стратегия, когда bulk не работает.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            nights: Количество ночей в окне.

        Returns:
            Кортеж (calendar, prices, errors_details).
        """
        today = date.today()
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        days_data = []
        for i in range(_DAYS_COUNT):
            day = today + timedelta(days=i)
            end_day = day + timedelta(days=nights)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end_day.isoformat()} 11:00:00",
            })

        result = await page.evaluate(
            """
            async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
                const results = [];
                const batches = [];

                for (let i = 0; i < daysData.length; i += batchSize) {
                    batches.push(daysData.slice(i, i + batchSize));
                }

                for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                    const batch = batches[batchIdx];

                    const promises = batch.map(async (dayInfo) => {
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
                                    date_begin: dayInfo.date_begin,
                                    date_end: dayInfo.date_end,
                                    currency_id: 1,
                                    is_pets: 0,
                                    documents: 0,
                                    target: 0,
                                    ages: [],
                                    no_time: 1
                                })
                            });

                            if (!resp.ok) {
                                return {status: 'error', error: 'http_' + resp.status};
                            }

                            const data = await resp.json();

                            if (!data.success || !data.data || !data.data.objects || !data.data.objects[0]) {
                                return {status: 'error', error: 'no_data'};
                            }

                            const obj = data.data.objects[0];

                            if (!obj.success) {
                                return {
                                    status: 'obj_error',
                                    errors: obj.errors || [],
                                    error_body: JSON.stringify(obj.errors || []).substring(0, 300)
                                };
                            }

                            const objData = obj.data;
                            const isBusy = objData.busy === 'busy';
                            let price = 0;

                            // Цена первого дня: первая запись detail с type="season_price"
                            if (objData.detail && objData.detail.length > 0) {
                                for (const det of objData.detail) {
                                    if (det.type === 'season_price' && det.cost) {
                                        price = det.cost;
                                        break;
                                    }
                                }
                            }

                            return {
                                status: 'ok',
                                busy: isBusy,
                                price: Math.round(price)
                            };

                        } catch (e) {
                            return {status: 'error', error: 'exception_' + e.message};
                        }
                    });

                    const batchResults = await Promise.all(promises);
                    results.push(...batchResults);

                    if (batchIdx < batches.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, batchDelay * 1000));
                    }
                }

                return results;
            }
            """,
            {
                "objectId": object_id,
                "token": token,
                "guests": guests,
                "daysData": days_data,
                "batchSize": _API_BATCH_SIZE,
                "batchDelay": _API_BATCH_DELAY,
                "apiUrl": _API_PRICES_URL,
            },
        )

        calendar: list[int] = []
        prices: list[int] = []
        errors_details: list[dict[str, str | int]] = []

        for day_idx, day_result in enumerate(result):
            status = day_result.get("status", "error")

            if status == "ok":
                if day_result.get("busy", False):
                    calendar.append(1)
                    prices.append(0)
                else:
                    calendar.append(0)
                    prices.append(day_result.get("price", 0))
            elif status == "obj_error":
                errors_details.append({
                    "day": day_idx,
                    "error": "obj_error",
                    "errors": str(day_result.get("errors", [])),
                    "error_body": day_result.get("error_body", ""),
                })
                calendar.append(-1)
                prices.append(0)
            else:
                errors_details.append({
                    "day": day_idx,
                    "error": day_result.get("error", "unknown"),
                })
                calendar.append(-1)
                prices.append(0)

        return calendar, prices, errors_details

    # ─────────────────────────────────────────────────────────────────────
    # Публичные методы обогащения
    # ─────────────────────────────────────────────────────────────────────

    async def enrich_listing(self, listing: RawListing, page: Page | None = None) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Args:
            listing: Объявление с базовыми данными из каталога.
            page: Вкладка для работы. Если None — используется основная.

        Returns:
            Объявление с заполненными calendar_60_days и prices_60_days.
        """
        active_page = page if page is not None else self._browser.page
        start_time = time.perf_counter()

        logger.info(
            "парсинг_карточки",
            path=listing.url,
            step=f"id={listing.external_id}",
        )

        try:
            loaded, token = await self._goto_and_capture_token(active_page, listing.url)

            if not loaded:
                logger.warning(
                    "страница_не_загрузилась",
                    step=f"id={listing.external_id}",
                )
                return listing

            if not token:
                logger.warning(
                    "токен_не_получен_пропуск_карточки",
                    step=f"id={listing.external_id}",
                )
                return listing

            await self._browser.random_delay()

            calendar, prices = await self._fetch_with_hybrid_strategy(
                active_page, listing.external_id, token, listing.url
            )

            listing.calendar_60_days = calendar
            listing.prices_60_days = prices

            logger.info(
                "карточка_обогащена",
                step=f"id={listing.external_id}",
                total=f"свободных={sum(1 for c in calendar if c == 0)}, "
                      f"занятых={sum(1 for c in calendar if c == 1)}, "
                      f"цен={sum(1 for p in prices if p > 0)}",
            )

        except Exception as e:
            logger.warning(
                "ошибка_парсинга_карточки",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}",
            )

        elapsed = time.perf_counter() - start_time
        logger.info(
            "карточка_завершена",
            step=f"id={listing.external_id}",
            total=f"{_format_duration(elapsed)}",
        )

        return listing

    async def enrich_listings(self, listings: list[RawListing]) -> list[RawListing]:
        """Обогащает список объявлений последовательно.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days и prices_60_days.
        """
        total = len(listings)
        for idx, listing in enumerate(listings, start=1):
            logger.info(
                "обработка_карточки",
                current=idx,
                total=total,
            )
            await self.enrich_listing(listing)
            await self._browser.random_delay()

        return listings

    async def enrich_listings_tabbed(self, listings: list[RawListing]) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько вкладок в одном браузере.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days и prices_60_days.
        """
        max_tabs = self._settings.max_tabs
        tab_delay_ms = self._settings.tab_delay_ms
        total = len(listings)

        logger.info(
            "запуск_параллельных_вкладок",
            step=f"вкладок={max_tabs}",
            total=total,
        )

        semaphore = asyncio.Semaphore(max_tabs)
        navigation_lock = asyncio.Lock()
        processed_count = 0
        count_lock = asyncio.Lock()

        async def _process_one(listing: RawListing) -> None:
            """Обрабатывает одну карточку в отдельной вкладке."""
            nonlocal processed_count

            async with semaphore:
                page = await self._browser.create_page()

                try:
                    async with navigation_lock:
                        await asyncio.sleep(tab_delay_ms / 1000.0)

                        loaded, token = await self._goto_and_capture_token(
                            page, listing.url
                        )

                    if not loaded:
                        logger.warning(
                            "страница_не_загрузилась_вкладка",
                            step=f"id={listing.external_id}",
                        )
                    elif not token:
                        logger.warning(
                            "токен_не_получен_вкладка",
                            step=f"id={listing.external_id}",
                        )
                    else:
                        await self._browser.random_delay()

                        try:
                            calendar, prices = await self._fetch_with_hybrid_strategy(
                                page, listing.external_id, token, listing.url
                            )
                            listing.calendar_60_days = calendar
                            listing.prices_60_days = prices

                            logger.info(
                                "карточка_обработана_вкладкой",
                                step=f"id={listing.external_id}",
                                total=f"свободных={sum(1 for c in calendar if c == 0)}, "
                                      f"цен={sum(1 for p in prices if p > 0)}",
                            )

                        except Exception as e:
                            logger.warning(
                                "ошибка_обработки_вкладки",
                                error=str(e),
                                error_type=type(e).__name__,
                                step=f"id={listing.external_id}",
                            )

                finally:
                    await self._browser.close_page(page)

                async with count_lock:
                    processed_count += 1
                    current = processed_count

                logger.info(
                    "прогресс_вкладок",
                    current=current,
                    total=total,
                )

        tasks = [_process_one(listing) for listing in listings]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        error_count = sum(1 for r in results if isinstance(r, Exception))
        if error_count > 0:
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.warning(
                        "ошибка_в_задаче_вкладки",
                        error=str(result),
                        error_type=type(result).__name__,
                        step=f"карточка={idx + 1}",
                    )

        logger.info(
            "параллельные_вкладки_завершены",
            total=total,
            step=f"ошибок={error_count}",
        )

        return listings

    # ─────────────────────────────────────────────────────────────────────
    # Параллельная обработка через прокси
    # ─────────────────────────────────────────────────────────────────────

    @staticmethod
    async def enrich_listings_parallel(
        settings: Settings,
        listings: list[RawListing],
        proxies: list[ProxyConfig],
    ) -> list[RawListing]:
        """Обогащает карточки параллельно через несколько прокси-браузеров.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек для обработки.
            proxies: Список рабочих прокси.

        Returns:
            Список обогащённых карточек.
        """
        from src.services.proxy_service import ProxyService

        chunks = ProxyService.distribute_listings(listings, len(proxies))

        logger.info(
            "параллельная_обработка",
            total=len(listings),
            step=f"прокси={len(proxies)}, вкладок_на_прокси={settings.max_tabs}",
        )

        parallel_start = time.perf_counter()

        tasks = [
            ListingService._worker(settings, chunk, proxy, worker_idx)
            for worker_idx, (chunk, proxy) in enumerate(zip(chunks, proxies), start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        parallel_elapsed = time.perf_counter() - parallel_start

        all_enriched: list[RawListing] = []
        worker_stats: list[tuple[int, int, float]] = []
        browsers_to_stop: list[tuple[BrowserService, int]] = []

        for worker_idx, result in enumerate(results, start=1):
            if isinstance(result, Exception):
                logger.warning(
                    "воркер_завершился_с_ошибкой",
                    error=str(result),
                    error_type=type(result).__name__,
                    step=f"воркер={worker_idx}",
                )
            elif isinstance(result, tuple) and len(result) == 3:
                enriched_list, duration, browser_svc = result
                all_enriched.extend(enriched_list)
                worker_stats.append((worker_idx, len(enriched_list), duration))
                browsers_to_stop.append((browser_svc, worker_idx))

        if browsers_to_stop:
            logger.info("остановка_прокси_браузеров", total=len(browsers_to_stop))
            for browser_svc, w_idx in browsers_to_stop:
                await _safe_stop_browser(browser_svc, w_idx)
            logger.info("все_прокси_браузеры_остановлены")

        if worker_stats:
            logger.info("─" * 50)
            logger.info("сводка_по_воркерам", total=len(worker_stats))

            for w_idx, w_cards, w_duration in worker_stats:
                avg_per_card = w_duration / w_cards if w_cards > 0 else 0.0
                logger.info(
                    "время_воркера",
                    step=f"воркер={w_idx}",
                    total=f"карточек={w_cards}, время={_format_duration(w_duration)}, "
                          f"среднее={_format_duration(avg_per_card)}/карточка",
                )

            fastest = min(worker_stats, key=lambda x: x[2])
            slowest = max(worker_stats, key=lambda x: x[2])
            total_cards = sum(c for _, c, _ in worker_stats)

            logger.info(
                "итого_параллельная_обработка",
                step=f"карточек={total_cards}, воркеров={len(worker_stats)}",
                total=f"общее_время={_format_duration(parallel_elapsed)}, "
                      f"быстрейший=воркер_{fastest[0]}({_format_duration(fastest[2])}), "
                      f"медленнейший=воркер_{slowest[0]}({_format_duration(slowest[2])})",
            )
            logger.info("─" * 50)

        logger.info(
            "параллельная_обработка_завершена",
            total=len(all_enriched),
        )

        return all_enriched

    @staticmethod
    async def _worker(
        settings: Settings,
        listings: list[RawListing],
        proxy: ProxyConfig,
        worker_idx: int,
    ) -> tuple[list[RawListing], float, BrowserService]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер.

        Args:
            settings: Настройки приложения.
            listings: Порция карточек для этого воркера.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).

        Returns:
            Кортеж (список карточек, время работы, browser_service).
        """
        if not listings:
            return ([], 0.0, BrowserService(settings=settings))

        worker_start = time.perf_counter()
        browser_service = BrowserService(settings=settings)

        try:
            await browser_service.start(proxy=proxy)

            logger.info(
                "воркер_запущен",
                step=f"воркер={worker_idx}",
                total=len(listings),
            )

            await browser_service.navigate("https://sutochno.ru")
            await browser_service.scroll_page()
            await asyncio.sleep(10)

            logger.info("воркер_прогрет", step=f"воркер={worker_idx}")

            listing_service = ListingService(
                settings=settings,
                browser_service=browser_service,
            )

            await listing_service.enrich_listings_tabbed(listings)

            worker_elapsed = time.perf_counter() - worker_start

            logger.info(
                "воркер_завершил_обработку",
                step=f"воркер={worker_idx}",
                total=f"карточек={len(listings)}, время={_format_duration(worker_elapsed)}",
            )

            return (listings, worker_elapsed, browser_service)

        except Exception as e:
            worker_elapsed = time.perf_counter() - worker_start
            logger.warning(
                "ошибка_воркера",
                error=str(e),
                error_type=type(e).__name__,
                step=f"воркер={worker_idx}",
            )
            return (listings, worker_elapsed, browser_service)
