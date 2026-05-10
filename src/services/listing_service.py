"""Сервис парсинга карточки объявления — извлечение календаря занятости и цен через API."""

import asyncio
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

# Максимальное количество retry при полном провале (все 60 дней — ошибки)
_MAX_API_RETRIES: int = 2

# Пауза перед повторной попыткой после перезагрузки страницы (секунды)
_RELOAD_WAIT_SECONDS: float = 15.0

# Варианты min_nights для адаптивного запроса (по возрастанию)
_MIN_NIGHTS_VARIANTS: list[int] = [2, 3, 5, 7]

# Ключевые слова в ответе API, указывающие на ограничение min_nights
_MIN_NIGHTS_ERROR_KEYWORDS: list[str] = [
    "min_nights",
    "minimum_nights",
    "минимальный срок",
    "минимум",
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

    Использует внутреннее API sutochno.ru для получения цен и занятости
    вместо кликов по датепикеру. Это в 10-20 раз быстрее и надёжнее.

    Алгоритм:
    1. Загружает страницу карточки, перехватывая токен из исходящих запросов сайта.
    2. Вызывает API getPricesAndAvailabilities через fetch() в контексте страницы.
    3. Из ответа формирует calendar_60_days и prices_60_days.
    4. При ошибках (min_nights, rate-limit) адаптирует запрос и повторяет.

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

        Подписывается на событие 'request' ДО загрузки страницы.
        При загрузке карточки сайт автоматически отправляет запросы к
        /api/json/ (getPricesAndAvailabilities, checkBookingAbility и т.д.)
        с заголовком 'token'. Мы перехватываем этот заголовок.

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

        # Подписываемся ДО загрузки страницы
        page.on("request", on_request)

        try:
            loaded = await self._goto_with_retry(page, url)
        finally:
            # Отписываемся после загрузки
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

                # Мягкое ожидание networkidle
                try:
                    await page.wait_for_load_state(
                        "networkidle", timeout=_NETWORKIDLE_SOFT_TIMEOUT_MS
                    )
                except Exception:
                    logger.debug(
                        "networkidle_не_достигнут_продолжаем",
                        step=f"попытка={attempt}",
                    )

                # Ждём ключевых элементов
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
    # Получение календаря и цен через API
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_prices_via_api(
        self, page: Page, object_id: str, token: str, nights: int = 1
    ) -> tuple[list[int], list[int], list[dict[str, str | int]]]:
        """Получает календарь занятости и цены через внутреннее API sutochno.ru.

        Выполняет пакетные fetch-запросы к getPricesAndAvailabilities
        прямо в контексте страницы (сохраняя cookies и сессию).

        Для каждого дня из 60 отправляет запрос с date_begin=день 14:00,
        date_end=день+nights 11:00. Из ответа:
        - busy == "unbusy" → день свободен, цена = detail[0].cost
        - иначе → день занят, цена = 0

        Запросы группируются в пакеты по _API_BATCH_SIZE для контроля нагрузки.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления на sutochno.ru.
            token: Сессионный токен API.
            nights: Количество ночей в запросе (по умолчанию 1, увеличивается
                    при ограничении min_nights).

        Returns:
            Кортеж (calendar_60_days, prices_60_days, errors_details).
            errors_details — список словарей с подробностями об ошибках API
            (для диагностики).
        """
        today = date.today()
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        # Формируем массив дат для запросов
        days_data = []
        for i in range(60):
            day = today + timedelta(days=i)
            end_day = day + timedelta(days=nights)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end_day.isoformat()} 11:00:00",
            })

        logger.debug(
            "запрос_цен_через_api",
            step=f"id={object_id}, дней=60, пакет={_API_BATCH_SIZE}, ночей={nights}",
        )

        # Выполняем пакетные запросы через page.evaluate
        result = await page.evaluate(
            """
            async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
                const results = [];

                // Разбиваем на пакеты
                const batches = [];
                for (let i = 0; i < daysData.length; i += batchSize) {
                    batches.push(daysData.slice(i, i + batchSize));
                }

                for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                    const batch = batches[batchIdx];

                    // Запускаем все запросы в пакете параллельно
                    const promises = batch.map(async (dayInfo) => {
                        try {
                            const body = {
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
                            };

                            const resp = await fetch(apiUrl, {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                    'Accept': 'application/json',
                                    'token': token,
                                    'platform': 'js',
                                    'api-version': '1.13'
                                },
                                body: JSON.stringify(body)
                            });

                            const responseText = await resp.text();

                            if (!resp.ok) {
                                return {
                                    busy: true,
                                    price: 0,
                                    error: 'http_' + resp.status,
                                    error_body: responseText.substring(0, 500)
                                };
                            }

                            let data;
                            try {
                                data = JSON.parse(responseText);
                            } catch (parseErr) {
                                return {
                                    busy: true,
                                    price: 0,
                                    error: 'json_parse_error',
                                    error_body: responseText.substring(0, 500)
                                };
                            }

                            if (!data.success) {
                                return {
                                    busy: true,
                                    price: 0,
                                    error: 'api_success_false',
                                    error_body: responseText.substring(0, 500)
                                };
                            }

                            if (!data.data || !data.data.objects || !data.data.objects[0]) {
                                return {
                                    busy: true,
                                    price: 0,
                                    error: 'no_data',
                                    error_body: responseText.substring(0, 500)
                                };
                            }

                            const obj = data.data.objects[0];

                            if (!obj.success) {
                                return {
                                    busy: true,
                                    price: 0,
                                    error: 'obj_not_success',
                                    error_body: JSON.stringify(obj).substring(0, 500)
                                };
                            }

                            const objData = obj.data;
                            const isBusy = objData.busy !== 'unbusy';
                            let price = 0;

                            if (!isBusy && objData.detail && objData.detail.length > 0) {
                                price = objData.detail[0].cost || 0;
                            }

                            return {busy: isBusy, price: Math.round(price)};

                        } catch (e) {
                            return {
                                busy: true,
                                price: 0,
                                error: 'exception_' + e.message,
                                error_body: e.stack ? e.stack.substring(0, 300) : ''
                            };
                        }
                    });

                    const batchResults = await Promise.all(promises);
                    results.push(...batchResults);

                    // Пауза между пакетами (кроме последнего)
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

        # Парсим результаты
        calendar: list[int] = []
        prices: list[int] = []
        errors_details: list[dict[str, str | int]] = []

        for day_idx, day_result in enumerate(result):
            if day_result.get("error"):
                errors_details.append({
                    "day": day_idx,
                    "error": day_result.get("error", ""),
                    "error_body": day_result.get("error_body", ""),
                })

            if day_result.get("busy", True):
                calendar.append(1)
                prices.append(0)
            else:
                calendar.append(0)
                price = day_result.get("price", 0)
                prices.append(int(price) if price else 0)

        # Статистика
        free_days = sum(1 for c in calendar if c == 0)
        busy_days = sum(1 for c in calendar if c == 1)
        errors_count = len(errors_details)

        logger.debug(
            "api_результат",
            step=f"id={object_id}, ночей={nights}",
            total=f"свободных={free_days}, занятых={busy_days}, ошибок={errors_count}",
        )

        return calendar, prices, errors_details

    # ─────────────────────────────────────────────────────────────────────
    # Определение min_nights из ошибок API
    # ─────────────────────────────────────────────────────────────────────

    def _detect_min_nights(self, errors_details: list[dict[str, str | int]]) -> int | None:
        """Анализирует ошибки API и пытается определить минимальный срок бронирования.

        Ищет в телах ответов ключевые слова, указывающие на ограничение min_nights.
        Также пытается извлечь конкретное число из текста ошибки.

        Args:
            errors_details: Список словарей с подробностями об ошибках.

        Returns:
            Предполагаемое значение min_nights или None если не удалось определить.
        """
        if not errors_details:
            return None

        # Проверяем первые 3 ошибки (достаточно для определения паттерна)
        sample_errors = errors_details[:3]

        for error_info in sample_errors:
            error_body = str(error_info.get("error_body", "")).lower()
            error_code = str(error_info.get("error", "")).lower()

            # Проверяем ключевые слова
            is_min_nights_error = any(
                keyword in error_body or keyword in error_code
                for keyword in _MIN_NIGHTS_ERROR_KEYWORDS
            )

            if is_min_nights_error:
                # Пытаемся извлечь число из текста
                import re

                numbers = re.findall(r"(\d+)", error_body)
                for num_str in numbers:
                    num = int(num_str)
                    if 2 <= num <= 30:
                        logger.info(
                            "min_nights_обнаружен_в_ответе",
                            step=f"min_nights={num}",
                        )
                        return num

                # Ключевое слово найдено, но число не извлечено — пробуем 2
                logger.info(
                    "min_nights_предположительно",
                    step="ключевое_слово_найдено, пробуем=2",
                )
                return 2

        # Если все 60 запросов вернули одинаковую ошибку 'obj_not_success' или
        # 'api_success_false' — вероятно min_nights ограничение
        unique_errors = set(str(e.get("error", "")) for e in errors_details)
        if len(errors_details) == 60 and len(unique_errors) <= 2:
            # Все ошибки одинаковые — предполагаем min_nights
            first_body = str(errors_details[0].get("error_body", ""))
            logger.info(
                "все_60_дней_одинаковая_ошибка_предполагаем_min_nights",
                step=f"ошибка={list(unique_errors)}, первый_ответ={first_body[:200]}",
            )
            return 2

        return None

    # ─────────────────────────────────────────────────────────────────────
    # Retry-логика с адаптацией min_nights и перезагрузкой
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_with_retry(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает календарь и цены с retry-логикой и адаптацией min_nights.

        Алгоритм:
        1. Первый запрос с nights=1.
        2. Если все 60 дней — ошибки:
           a) Логирует тела ответов API для диагностики.
           b) Пытается определить min_nights из ошибок.
           c) Если min_nights найден — повторяет запрос с увеличенным периодом.
           d) Если не помогло — перезагружает страницу, ждёт 15 секунд,
              перехватывает новый токен и повторяет.
        3. Максимум _MAX_API_RETRIES попыток с перезагрузкой.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки (для перезагрузки).

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        # ── Попытка 1: стандартный запрос (1 ночь) ──
        calendar, prices, errors_details = await self._fetch_prices_via_api(
            page, object_id, current_token, nights=1
        )

        errors_count = len(errors_details)
        free_days = sum(1 for c in calendar if c == 0)

        # Если есть хотя бы 1 свободный день — результат валиден
        if free_days > 0 or errors_count < 60:
            return calendar, prices

        # ── Все 60 дней = ошибки. Логируем подробности. ──
        logger.warning(
            "все_60_дней_ошибка_начинаем_диагностику",
            step=f"id={object_id}, ошибок={errors_count}",
        )

        # Логируем первые 3 тела ответов для диагностики
        for i, err_info in enumerate(errors_details[:3]):
            logger.debug(
                "api_ошибка_подробности",
                step=f"id={object_id}, день={err_info.get('day')}, "
                     f"ошибка={err_info.get('error')}",
                path=str(err_info.get("error_body", ""))[:300],
            )

        # ── Попытка адаптации: определяем min_nights ──
        detected_min_nights = self._detect_min_nights(errors_details)

        if detected_min_nights is not None:
            # Пробуем варианты min_nights
            nights_to_try = [n for n in _MIN_NIGHTS_VARIANTS if n >= detected_min_nights]
            if detected_min_nights not in nights_to_try:
                nights_to_try.insert(0, detected_min_nights)

            for nights in nights_to_try:
                logger.info(
                    "повтор_с_увеличенным_периодом",
                    step=f"id={object_id}, ночей={nights}",
                )

                calendar, prices, errors_details = await self._fetch_prices_via_api(
                    page, object_id, current_token, nights=nights
                )

                errors_count = len(errors_details)
                free_days = sum(1 for c in calendar if c == 0)

                if free_days > 0 or errors_count < 60:
                    logger.info(
                        "адаптация_min_nights_успешна",
                        step=f"id={object_id}, ночей={nights}, свободных={free_days}",
                    )
                    return calendar, prices

                logger.debug(
                    "адаптация_не_помогла",
                    step=f"id={object_id}, ночей={nights}, ошибок={errors_count}",
                )

        # ── Retry с перезагрузкой страницы ──
        for retry_attempt in range(1, _MAX_API_RETRIES + 1):
            logger.info(
                "перезагрузка_страницы_для_повтора",
                step=f"id={object_id}, попытка={retry_attempt}/{_MAX_API_RETRIES}",
            )

            # Ждём перед перезагрузкой
            logger.debug(
                "ожидание_перед_перезагрузкой",
                step=f"пауза={_RELOAD_WAIT_SECONDS}с",
            )
            await asyncio.sleep(_RELOAD_WAIT_SECONDS)

            # Перезагружаем страницу и перехватываем новый токен
            loaded, new_token = await self._goto_and_capture_token(page, url)

            if not loaded:
                logger.warning(
                    "перезагрузка_не_удалась",
                    step=f"id={object_id}, попытка={retry_attempt}",
                )
                continue

            if not new_token:
                logger.warning(
                    "токен_не_получен_после_перезагрузки",
                    step=f"id={object_id}, попытка={retry_attempt}",
                )
                continue

            current_token = new_token
            await self._browser.random_delay()

            # Определяем с каким количеством ночей пробовать
            nights_for_retry = detected_min_nights if detected_min_nights else 1

            calendar, prices, errors_details = await self._fetch_prices_via_api(
                page, object_id, current_token, nights=nights_for_retry
            )

            errors_count = len(errors_details)
            free_days = sum(1 for c in calendar if c == 0)

            if free_days > 0 or errors_count < 60:
                logger.info(
                    "retry_после_перезагрузки_успешен",
                    step=f"id={object_id}, попытка={retry_attempt}, "
                         f"свободных={free_days}",
                )
                return calendar, prices

            logger.warning(
                "retry_после_перезагрузки_не_помог",
                step=f"id={object_id}, попытка={retry_attempt}, ошибок={errors_count}",
            )

            # Логируем ошибки после retry
            for i, err_info in enumerate(errors_details[:2]):
                logger.debug(
                    "api_ошибка_после_retry",
                    step=f"id={object_id}, день={err_info.get('day')}, "
                         f"ошибка={err_info.get('error')}",
                    path=str(err_info.get("error_body", ""))[:300],
                )

        # ── Все попытки исчерпаны ──
        logger.warning(
            "все_попытки_исчерпаны_данные_не_получены",
            step=f"id={object_id}",
            total=f"retry={_MAX_API_RETRIES}, min_nights_пробовали="
                  f"{detected_min_nights or 'не_определён'}",
        )

        return calendar, prices

    # ─────────────────────────────────────────────────────────────────────
    # Публичные методы обогащения
    # ─────────────────────────────────────────────────────────────────────

    async def enrich_listing(self, listing: RawListing, page: Page | None = None) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Алгоритм:
        1. Загружает страницу карточки, перехватывая токен из запросов сайта.
        2. Вызывает API с retry-логикой для получения цен и занятости на 60 дней.
        3. Заполняет calendar_60_days и prices_60_days.

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
            # Шаг 1: Загружаем страницу и перехватываем токен
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

            # Шаг 2: Вызываем API с retry-логикой
            calendar, prices = await self._fetch_with_retry(
                active_page, listing.external_id, token, listing.url
            )

            listing.calendar_60_days = calendar
            listing.prices_60_days = prices

            logger.info(
                "карточка_обогащена_через_api",
                step=f"id={listing.external_id}",
                total=f"календарь={len(calendar)}, цены={len(prices)}, "
                      f"свободных={sum(1 for c in calendar if c == 0)}",
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

                        # Загружаем страницу и перехватываем токен
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
                            # Используем retry-логику вместо прямого вызова API
                            calendar, prices = await self._fetch_with_retry(
                                page, listing.external_id, token, listing.url
                            )
                            listing.calendar_60_days = calendar
                            listing.prices_60_days = prices

                            logger.info(
                                "карточка_обработана_вкладкой",
                                step=f"id={listing.external_id}",
                                total=f"свободных={sum(1 for c in calendar if c == 0)}",
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

        # Остановка браузеров
        if browsers_to_stop:
            logger.info("остановка_прокси_браузеров", total=len(browsers_to_stop))
            for browser_svc, w_idx in browsers_to_stop:
                await _safe_stop_browser(browser_svc, w_idx)
            logger.info("все_прокси_браузеры_остановлены")

        # Сводка
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

            # Прогрев
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
