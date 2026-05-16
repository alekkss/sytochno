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
_RELOAD_WAIT_SECONDS: float = 10.0
# Варианты min_nights для адаптивного запроса (по возрастанию).
# Расширен до 30 — встречаются объекты с min_nights=10, 14, 30.
_MIN_NIGHTS_VARIANTS: list[int] = [2, 3, 4, 5, 6, 7, 10, 14, 30]
# Количество дней для анализа
_DAYS_COUNT: int = 60
# Порог ошибок, после которого скользящее окно считается провалившимся
_ERROR_THRESHOLD: int = 30
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
# Типы записей detail[], содержащие базовую цену за сутки.
# API возвращает type="season_price" (сезонные цены с диапазонами дат)
# или type=1 (числовой тип — единая базовая цена без дат).
# Типы "interval" (скидки за длительность), "dop_persons" (доплата за гостей),
# "sale" (акции) НЕ являются базовой ценой и игнорируются.
_BASE_PRICE_TYPE_INT: int = 1
_SEASON_PRICE_TYPE: str = "season_price"


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

    Гибридная стратегия:

    1. Валидация токена: один тестовый запрос (nights=2, один день).
       Если токен невалиден — перезагрузка страницы.

    2. Bulk-запрос на 60 ночей → все цены из detail[].
       Приоритет: type="season_price" (сезонные цены с диапазонами дат).
       Fallback: type=1 (единая базовая цена, применяется ко всем дням
       без сезонной цены).
       Если busy="unbusy" → все дни свободны, готово за 1 запрос.

    3. Если bulk вернул busy="busy" → скользящее окно для занятости.

    4. Если bulk вернул api_false → токен протух или аномалия.
       Перезагрузка страницы + повтор с новым токеном.

    5. При массовых ошибках в скользящем окне (>30 из 60) →
       перезагрузка страницы + повтор (НЕ нормализация как "свободен").
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
        captured_token: list[str] = []

        async def _route_handler(route):
            req = route.request
            # Перехватываем только запросы к внутреннему API
            if "sutochno.ru/api/json" in req.url:
                token = req.headers.get("token") or req.headers.get("Token")
                if token and not captured_token:
                    captured_token.append(token)
            # Обязательно продолжаем выполнение запроса, чтобы страница работала штатно
            await route.continue_()

        # page.route надёжнее page.on('request'): гарантирует перехват
        # даже для запросов из iframe, service workers или асинхронных init-скриптов
        await page.route("**/api/json/**", _route_handler)

        try:
            loaded = await self._goto_with_retry(page, url)
            # Небольшая пауза после загрузки: API-запросы часто уходят
            # асинхронно сразу после DOMContentLoaded / networkidle
            await asyncio.sleep(1.0)
        finally:
            await page.unroute("**/api/json/**")

        token = captured_token[0] if captured_token else None

        if token:
            logger.debug(
                "токен_перехвачен",
                step=f"длина={len(token)}, источник=route_interception",
            )
        else:
            logger.warning("токен_не_перехвачен_при_загрузке")

        return loaded, token

    async def _goto_with_retry(self, page: Page, url: str) -> bool:
        """Загружает страницу карточки с повторными попытками.

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
                        "Timeout",
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
    # Валидация токена: тестовый запрос
    # ─────────────────────────────────────────────────────────────────────

    async def _validate_token(self, page: Page, object_id: str, token: str) -> bool:
        """Проверяет работоспособность токена одним тестовым запросом.

        Отправляет запрос на 2 ночи для завтрашнего дня.
        Если получен ответ с success=true (на уровне data.objects) — токен валиден.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен для проверки.

        Returns:
            True если токен работает, False — если невалиден.
        """
        today = date.today()
        test_date = today + timedelta(days=3)
        end_date = test_date + timedelta(days=2)
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

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

                    if (!resp.ok) return {valid: false, reason: 'http_' + resp.status};

                    const data = await resp.json();
                    if (!data.success) return {valid: false, reason: 'api_false'};
                    if (!data.data || !data.data.objects || !data.data.objects[0]) {
                        return {valid: false, reason: 'no_objects'};
                    }

                    // Объект может вернуть ошибку min_nights — это ОК, токен валиден
                    return {valid: true};

                } catch (e) {
                    return {valid: false, reason: 'exception_' + e.message};
                }
            }
            """,
            {
                "apiUrl": _API_PRICES_URL,
                "objectId": object_id,
                "dateBegin": f"{test_date.isoformat()} 14:00:00",
                "dateEnd": f"{end_date.isoformat()} 11:00:00",
                "token": token,
                "guests": guests,
            },
        )

        is_valid = result.get("valid", False)

        if not is_valid:
            logger.warning(
                "токен_невалиден",
                step=f"id={object_id}, причина={result.get('reason', '?')}",
            )
        else:
            logger.debug(
                "токен_валиден",
                step=f"id={object_id}",
            )

        return is_valid

    # ─────────────────────────────────────────────────────────────────────
    # Шаг 1: Один запрос на 60 ночей — получение всех цен
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_bulk_prices(
        self, page: Page, object_id: str, token: str
    ) -> tuple[str | None, list[int], bool]:
        """Получает все цены одним запросом на 60 ночей.

        Обрабатывает два формата ценовых записей в detail[]:

        1. type="season_price" — сезонные цены с диапазонами дат
           (date_begin, date_end заполнены). Каждая запись покрывает
           конкретный период. Разворачиваются в дневные цены.

        2. type=1 (числовой) — единая базовая цена за сутки.
           Поля date_begin/date_end = null. Применяется ко всем дням,
           не покрытым записями season_price (fallback).

        Записи type="interval" (скидки за длительность), "dop_persons"
        (доплата за гостей), "sale" (акции) игнорируются — это не базовая
        цена за сутки.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.

        Returns:
            Кортеж (busy_status, prices_60_days, success).
        """
        today = date.today()
        end_date = today + timedelta(days=_DAYS_COUNT)
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        date_begin = f"{today.isoformat()} 14:00:00"
        date_end = f"{end_date.isoformat()} 11:00:00"

        logger.debug(
            "запрос_цен_bulk",
            step=f"id={object_id}, период={today}→{end_date}",
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
            logger.debug(
                "bulk_запрос_ошибка",
                step=f"id={object_id}, ошибка={error}, errors={result.get('errors', [])}",
            )
            return None, [0] * _DAYS_COUNT, False

        busy_status = result.get("busy")
        detail = result.get("detail", [])

        # ── Извлекаем базовую цену из type=1 (fallback) ──
        base_price: int = 0
        for det in detail:
            if det.get("type") == _BASE_PRICE_TYPE_INT and det.get("cost"):
                base_price = int(det["cost"])
                break

        # ── Разворачиваем season_price в дневные цены ──
        daily_prices: dict[str, int] = {}

        for det in detail:
            if det.get("type") != _SEASON_PRICE_TYPE:
                continue

            d_begin = det.get("date_begin")
            d_end = det.get("date_end")
            cost = det.get("cost", 0)

            if not d_begin or not d_end or not cost:
                continue

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

        # ── Формируем массив цен на 60 дней ──
        # Приоритет: season_price (с датами) → type=1 (базовая цена)
        prices_60: list[int] = []
        for i in range(_DAYS_COUNT):
            day = today + timedelta(days=i)
            day_key = day.isoformat()
            price = daily_prices.get(day_key, base_price)
            prices_60.append(price)

        prices_filled = sum(1 for p in prices_60 if p > 0)

        logger.debug(
            "bulk_цены_получены",
            step=f"id={object_id}, busy={busy_status}, цен={prices_filled}/60, "
                 f"season_price={len(daily_prices)}, base_price={base_price}",
        )

        return busy_status, prices_60, True

    # ─────────────────────────────────────────────────────────────────────
    # Шаг 2: Определение занятости каждого дня (скользящее окно)
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_availability(
        self, page: Page, object_id: str, token: str, nights: int = 2
    ) -> tuple[list[int], list[dict[str, str | int]]]:
        """Определяет занятость каждого дня через скользящее окно.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            nights: Количество ночей в окне.

        Returns:
            Кортеж (calendar_60_days, errors_details).
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
            step=f"id={object_id}, ночей={nights}, пакет={_API_BATCH_SIZE}",
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

                            if (!data.success) {
                                return {status: 'error', error: 'api_false'};
                            }

                            if (!data.data || !data.data.objects || !data.data.objects[0]) {
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
                                busy: obj.data.busy === 'busy',
                                price: (() => {
                                    const detail = obj.data.detail || [];
                                    /* Приоритет: season_price → type=1 (базовая цена) */
                                    let seasonPrice = 0;
                                    let basePrice = 0;
                                    for (const d of detail) {
                                        if (d.type === 'season_price' && d.cost && !seasonPrice) {
                                            seasonPrice = Math.round(d.cost);
                                        }
                                        if (d.type === 1 && d.cost && !basePrice) {
                                            basePrice = Math.round(d.cost);
                                        }
                                    }
                                    return seasonPrice || basePrice;
                                })()
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

        return calendar, errors_details

    # ─────────────────────────────────────────────────────────────────────
    # Определение min_nights из ошибок API
    # ─────────────────────────────────────────────────────────────────────

    def _detect_min_nights(self, errors_details: list[dict[str, str | int]]) -> int | None:
        """Определяет min_nights из текстов ошибок API.

        Args:
            errors_details: Список ошибок.

        Returns:
            Значение min_nights или None.
        """
        if not errors_details:
            return None

        for error_info in errors_details[:3]:
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
                        logger.info("min_nights_обнаружен", step=f"min_nights={num}")
                        return num
                return 2

        if len(errors_details) >= 55:
            unique_errors = set(str(e.get("error", "")) for e in errors_details)
            if len(unique_errors) <= 2:
                return 2

        return None

    # ─────────────────────────────────────────────────────────────────────
    # Основная логика: гибридная стратегия с валидацией токена
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_with_hybrid_strategy(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает календарь и цены гибридной стратегией.

        Алгоритм:
        1. Валидация токена (тестовый запрос).
        2. Bulk-запрос на 60 ночей → цены.
        3. Если unbusy → готово.
        4. Если busy → скользящее окно для занятости.
        5. При ошибках → перезагрузка + retry с новым токеном.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки.

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        # ── Валидация токена ──
        token_valid = await self._validate_token(page, object_id, current_token)

        if not token_valid:
            # Токен невалиден — перезагружаем страницу
            logger.info(
                "токен_невалиден_перезагрузка",
                step=f"id={object_id}",
            )
            new_token = await self._reload_and_get_token(page, url, object_id)
            if not new_token:
                logger.warning(
                    "не_удалось_получить_валидный_токен",
                    step=f"id={object_id}",
                )
                return [0] * _DAYS_COUNT, [0] * _DAYS_COUNT
            current_token = new_token

        # ── Шаг 1: Bulk-запрос на 60 ночей → цены ──
        busy_status, prices_60, bulk_success = await self._fetch_bulk_prices(
            page, object_id, current_token
        )

        if not bulk_success:
            # Bulk не удался — возможно токен протух между валидацией и запросом
            # Или API не поддерживает bulk для этого объекта
            logger.info(
                "bulk_не_удался_пробуем_перезагрузку",
                step=f"id={object_id}",
            )

            # Перезагружаем и пробуем ещё раз
            new_token = await self._reload_and_get_token(page, url, object_id)
            if new_token:
                current_token = new_token
                busy_status, prices_60, bulk_success = await self._fetch_bulk_prices(
                    page, object_id, current_token
                )

            if not bulk_success:
                # Bulk точно не работает — переходим к скользящему окну
                logger.info(
                    "bulk_окончательно_не_удался_скользящее_окно",
                    step=f"id={object_id}",
                )
                return await self._full_sliding_window(
                    page, object_id, current_token, url
                )

        # ── Шаг 2: Определение занятости ──
        if busy_status == "unbusy":
            # Все дни свободны!
            calendar_60 = [0] * _DAYS_COUNT
            logger.info(
                "все_дни_свободны_bulk",
                step=f"id={object_id}, цен={sum(1 for p in prices_60 if p > 0)}/60",
            )
            return calendar_60, prices_60

        # busy="busy" — нужно определить какие дни заняты
        calendar_60 = await self._determine_availability(
            page, object_id, current_token, url
        )

        # Объединяем: обнуляем цены для занятых дней
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
            total=f"свободных={free_days}, занятых={busy_days}, "
                  f"цен={sum(1 for p in final_prices if p > 0)}",
        )

        return calendar_60, final_prices

    async def _determine_availability(
        self, page: Page, object_id: str, token: str, url: str
    ) -> list[int]:
        """Определяет занятость каждого дня с адаптацией min_nights и retry.

        Обрабатывает динамические ограничения min_nights (когда разные даты
        требуют разное минимальное количество суток). Ошибки типа
        «Минимальное количество суток - N» трактуются как доступность (0),
        так как они указывают на правила бронирования, а не на фактическую
        занятость объекта.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.

        Returns:
            Список из 60 значений: 0=свободен, 1=занят.
        """
        current_token = token
        best_calendar: list[int] = [-1] * _DAYS_COUNT
        best_unresolved: int = _DAYS_COUNT

        for nights in _MIN_NIGHTS_VARIANTS:
            calendar, errors_details = await self._fetch_availability(
                page, object_id, current_token, nights=nights
            )

            # Разрешаем ошибки: min_nights -> 0 (свободно), остальные -> -1
            resolved = list(calendar)
            unresolved_count = 0

            for idx, status in enumerate(calendar):
                if status == -1:
                    is_min_nights_err = False
                    for err_info in errors_details:
                        if err_info.get("day") == idx:
                            err_text = str(err_info.get("errors", "")) + " " + str(err_info.get("error", ""))
                            if any(kw in err_text.lower() for kw in _MIN_NIGHTS_ERROR_KEYWORDS):
                                is_min_nights_err = True
                                break

                    if is_min_nights_err:
                        resolved[idx] = 0  # Считаем свободным (ограничение бронирования)
                    else:
                        unresolved_count += 1

            if unresolved_count < best_unresolved:
                best_calendar = resolved
                best_unresolved = unresolved_count

            if unresolved_count == 0:
                return resolved

            if unresolved_count <= 5:
                return resolved

            if unresolved_count >= _ERROR_THRESHOLD:
                new_token = await self._reload_and_get_token(page, url, object_id)
                if new_token:
                    current_token = new_token
                else:
                    break

        return [0 if c == -1 else c for c in best_calendar]

    async def _full_sliding_window(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает и цены, и занятость через скользящее окно (fallback).

        Используется когда bulk-запрос полностью не работает.
        Скользящее окно возвращает и busy-статус, и цену первого дня.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        for nights in _MIN_NIGHTS_VARIANTS:
            logger.info(
                "скользящее_окно_полное",
                step=f"id={object_id}, ночей={nights}",
            )

            calendar, errors_details = await self._fetch_availability(
                page, object_id, current_token, nights=nights
            )

            error_days = sum(1 for c in calendar if c == -1)

            if error_days == 0:
                # Данные получены — извлекаем цены из результатов
                busy_status, prices_60, bulk_ok = await self._fetch_bulk_prices(
                    page, object_id, current_token
                )
                if bulk_ok and sum(1 for p in prices_60 if p > 0) > 0:
                    final_prices = [
                        0 if calendar[i] == 1 else prices_60[i]
                        for i in range(_DAYS_COUNT)
                    ]
                    return calendar, final_prices

                return await self._sliding_window_with_prices(
                    page, object_id, current_token, nights
                )

            if error_days < _ERROR_THRESHOLD:
                # Частичный успех — нормализуем и пробуем bulk для цен
                calendar_norm = [0 if c == -1 else c for c in calendar]
                _, prices_60, bulk_ok = await self._fetch_bulk_prices(
                    page, object_id, current_token
                )
                if bulk_ok:
                    final_prices = [
                        0 if calendar_norm[i] == 1 else prices_60[i]
                        for i in range(_DAYS_COUNT)
                    ]
                    return calendar_norm, final_prices

                return await self._sliding_window_with_prices(
                    page, object_id, current_token, nights
                )

            # Много ошибок — проверяем min_nights и продолжаем цикл
            detected = self._detect_min_nights(errors_details)

            if detected is not None and detected > nights:
                # min_nights явно больше — пробуем следующий вариант
                logger.info(
                    "скользящее_окно_адаптация",
                    step=f"id={object_id}, текущий={nights}, нужен={detected}",
                )
                continue

            # detected <= nights или None — API может занижать min_nights.
            # Пробуем следующий вариант nights перед перезагрузкой.
            if nights < _MIN_NIGHTS_VARIANTS[-1]:
                logger.debug(
                    "скользящее_окно_следующий_вариант",
                    step=f"id={object_id}, текущий={nights}, ошибок={error_days}",
                )
                continue

            # Последний вариант — перезагрузка как крайняя мера
            new_token = await self._reload_and_get_token(page, url, object_id)
            if new_token:
                current_token = new_token
                continue
            break

        # Полный провал
        logger.warning(
            "полный_провал_нет_данных",
            step=f"id={object_id}",
        )
        return [0] * _DAYS_COUNT, [0] * _DAYS_COUNT

    async def _sliding_window_with_prices(
        self, page: Page, object_id: str, token: str, nights: int
    ) -> tuple[list[int], list[int]]:
        """Скользящее окно с извлечением цен из каждого ответа.

        Используется как последний fallback — получает и занятость, и цены.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            nights: Количество ночей в окне.

        Returns:
            Кортеж (calendar, prices).
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

                            if (!resp.ok) return {status: 'error'};

                            const data = await resp.json();

                            if (!data.success || !data.data || !data.data.objects || !data.data.objects[0]) {
                                return {status: 'error'};
                            }

                            const obj = data.data.objects[0];
                            if (!obj.success) return {status: 'obj_error'};

                            const objData = obj.data;
                            let price = 0;
                            if (objData.detail) {
                                /* Приоритет: season_price → type=1 (базовая цена) */
                                let seasonPrice = 0;
                                let basePrice = 0;
                                for (const d of objData.detail) {
                                    if (d.type === 'season_price' && d.cost && !seasonPrice) {
                                        seasonPrice = Math.round(d.cost);
                                    }
                                    if (d.type === 1 && d.cost && !basePrice) {
                                        basePrice = Math.round(d.cost);
                                    }
                                }
                                price = seasonPrice || basePrice;
                            }

                            return {
                                status: 'ok',
                                busy: objData.busy === 'busy',
                                price: price
                            };

                        } catch (e) {
                            return {status: 'error'};
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

        for day_result in result:
            if day_result.get("status") == "ok":
                if day_result.get("busy", False):
                    calendar.append(1)
                    prices.append(0)
                else:
                    calendar.append(0)
                    prices.append(day_result.get("price", 0))
            else:
                # Ошибка → свободен (лучше не терять данные)
                calendar.append(0)
                prices.append(0)

        return calendar, prices

    # ─────────────────────────────────────────────────────────────────────
    # Перезагрузка страницы и получение нового токена
    # ─────────────────────────────────────────────────────────────────────

    async def _reload_and_get_token(
        self, page: Page, url: str, object_id: str
    ) -> str | None:
        """Перезагружает страницу и получает новый токен.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов).

        Returns:
            Новый токен или None.
        """
        await asyncio.sleep(_RELOAD_WAIT_SECONDS)

        loaded, new_token = await self._goto_and_capture_token(page, url)

        if not loaded:
            logger.warning(
                "перезагрузка_не_удалась",
                step=f"id={object_id}",
            )
            return None

        if not new_token:
            logger.warning(
                "токен_не_получен_после_перезагрузки",
                step=f"id={object_id}",
            )
            return None

        await self._browser.random_delay()

        logger.debug(
            "новый_токен_получен",
            step=f"id={object_id}",
        )

        return new_token

    # ─────────────────────────────────────────────────────────────────────
    # Публичные методы обогащения
    # ─────────────────────────────────────────────────────────────────────

    async def enrich_listing(self, listing: RawListing, page: Page | None = None) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Args:
            listing: Объявление с базовыми данными из каталога.
            page: Вкладка для работы.

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
        """Обогащает карточки параллельно через несколько вкладок.

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
            listings: Полный список карточек.
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