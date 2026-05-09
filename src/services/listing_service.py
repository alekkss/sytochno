"""Сервис парсинга карточки объявления — извлечение календаря занятости и цен через API."""

import asyncio
import json
import time
from datetime import date, timedelta

from playwright.async_api import Page, Response

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

# Таймаут перехвата токена из сетевых запросов (секунды)
_TOKEN_INTERCEPT_TIMEOUT: float = 10.0

# Количество гостей по умолчанию (используется в API-запросе)
_DEFAULT_GUESTS: int = 2


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
    1. Загружает страницу карточки в браузере.
    2. Перехватывает сессионный токен из сетевых запросов.
    3. Вызывает API getPricesAndAvailabilities через fetch() в контексте страницы.
    4. Из ответа формирует calendar_60_days и prices_60_days.

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
    # Загрузка страницы карточки с retry и перехватом токена
    # ─────────────────────────────────────────────────────────────────────

    async def _goto_with_retry(self, page: Page, url: str) -> bool:
        """Загружает страницу карточки с повторными попытками при сетевых ошибках.

        Стратегия:
        1. goto с wait_until="domcontentloaded" (быстро).
        2. Мягкое ожидание networkidle (не блокирует при таймауте).
        3. Ожидание ключевых элементов карточки.

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

                # Страница загрузилась частично — пробуем работать
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

    async def _intercept_token(self, page: Page) -> str | None:
        """Извлекает сессионный токен API из контекста страницы.

        Пробует несколько источников:
        1. localStorage/sessionStorage
        2. Cookie
        3. Meta-тег или глобальная переменная
        4. Перехват из уже выполненных XHR-запросов (через Performance API)

        Args:
            page: Вкладка браузера.

        Returns:
            Токен (строка) или None если не найден.
        """
        token = await page.evaluate("""
            () => {
                // 1. Пробуем localStorage
                const keys = ['token', 'api_token', 'authToken', 'user_token'];
                for (const key of keys) {
                    const val = localStorage.getItem(key);
                    if (val) return val;
                }

                // 2. Пробуем sessionStorage
                for (const key of keys) {
                    const val = sessionStorage.getItem(key);
                    if (val) return val;
                }

                // 3. Пробуем cookie
                const cookies = document.cookie.split(';');
                for (const cookie of cookies) {
                    const [name, value] = cookie.trim().split('=');
                    if (name === 'token' || name === 'api_token') {
                        return decodeURIComponent(value);
                    }
                }

                // 4. Пробуем window.__NUXT__ или глобальные переменные
                if (window.__NUXT__ && window.__NUXT__.config) {
                    const config = window.__NUXT__.config;
                    if (config.token) return config.token;
                    if (config.public && config.public.token) return config.public.token;
                }

                // 5. Пробуем найти в DOM (meta-теги)
                const meta = document.querySelector('meta[name="api-token"]');
                if (meta) return meta.getAttribute('content');

                return null;
            }
        """)

        if token:
            logger.debug("токен_найден_в_хранилище", step=f"длина={len(token)}")
            return token

        logger.debug("токен_не_найден_в_хранилище_пробуем_перехват")
        return None

    async def _intercept_token_from_requests(self, page: Page) -> str | None:
        """Перехватывает токен, провоцируя сетевой запрос.

        Выполняет лёгкий запрос к API (getCurrencies) и перехватывает
        заголовок 'token' из исходящего запроса.

        Args:
            page: Вкладка браузера.

        Returns:
            Токен или None.
        """
        captured_token: list[str] = []

        async def capture_request(request: "any") -> None:  # type: ignore[name-defined]
            """Перехватывает токен из заголовков запроса."""
            if "sutochno.ru/api/json" in request.url:
                token_header = request.headers.get("token")
                if token_header and not captured_token:
                    captured_token.append(token_header)

        page.on("request", capture_request)

        try:
            # Провоцируем запрос к API
            await page.evaluate("""
                () => {
                    return fetch('/api/json/currencies/getList', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                        },
                        body: JSON.stringify({})
                    }).then(r => r.status).catch(() => 0);
                }
            """)

            # Ждём перехвата
            for _ in range(20):
                if captured_token:
                    break
                await asyncio.sleep(0.5)

        finally:
            page.remove_listener("request", capture_request)

        if captured_token:
            logger.debug("токен_перехвачен_из_запроса", step=f"длина={len(captured_token[0])}")
            return captured_token[0]

        logger.debug("токен_не_удалось_перехватить")
        return None

    async def _get_api_token(self, page: Page) -> str | None:
        """Получает токен API любым доступным способом.

        Комбинирует несколько стратегий извлечения токена.

        Args:
            page: Вкладка браузера.

        Returns:
            Токен или None.
        """
        # Способ 1: из хранилища/cookies
        token = await self._intercept_token(page)
        if token:
            return token

        # Способ 2: перехват из сетевого запроса
        token = await self._intercept_token_from_requests(page)
        if token:
            return token

        logger.warning("api_токен_не_найден")
        return None

    # ─────────────────────────────────────────────────────────────────────
    # Основной метод: получение календаря и цен через API
    # ─────────────────────────────────────────────────────────────────────

    async def _fetch_prices_via_api(
        self, page: Page, object_id: str, token: str
    ) -> tuple[list[int], list[int]]:
        """Получает календарь занятости и цены через внутреннее API sutochno.ru.

        Выполняет пакетные fetch-запросы к getPricesAndAvailabilities
        прямо в контексте страницы (сохраняя cookies и сессию).

        Для каждого дня из 60 отправляет запрос с date_begin=день 14:00,
        date_end=следующий_день 11:00 (одна ночь). Из ответа:
        - busy == "unbusy" → день свободен, цена = detail[0].cost
        - иначе → день занят, цена = 0

        Запросы группируются в пакеты по _API_BATCH_SIZE для контроля нагрузки.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления на sutochno.ru.
            token: Сессионный токен API.

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        today = date.today()
        guests = self._settings.guests if hasattr(self._settings, "guests") else _DEFAULT_GUESTS

        # Формируем массив дат для запросов
        days_data = []
        for i in range(60):
            day = today + timedelta(days=i)
            next_day = day + timedelta(days=1)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{next_day.isoformat()} 11:00:00",
            })

        logger.debug(
            "запрос_цен_через_api",
            step=f"id={object_id}, дней=60, пакет={_API_BATCH_SIZE}",
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

                            if (!resp.ok) {
                                return {busy: true, price: 0, error: resp.status};
                            }

                            const data = await resp.json();

                            if (!data.success || !data.data || !data.data.objects ||
                                !data.data.objects[0]) {
                                return {busy: true, price: 0, error: 'no_data'};
                            }

                            const obj = data.data.objects[0];

                            if (!obj.success) {
                                return {busy: true, price: 0, error: 'obj_not_success'};
                            }

                            const objData = obj.data;
                            const isBusy = objData.busy !== 'unbusy';
                            let price = 0;

                            if (!isBusy && objData.detail && objData.detail.length > 0) {
                                price = objData.detail[0].cost || 0;
                            }

                            return {busy: isBusy, price: Math.round(price)};

                        } catch (e) {
                            return {busy: true, price: 0, error: e.message};
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

        for day_result in result:
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
        errors = sum(1 for r in result if r.get("error"))

        logger.debug(
            "api_результат",
            step=f"id={object_id}",
            total=f"свободных={free_days}, занятых={busy_days}, ошибок={errors}",
        )

        return calendar, prices

    # ─────────────────────────────────────────────────────────────────────
    # Публичные методы обогащения
    # ─────────────────────────────────────────────────────────────────────

    async def enrich_listing(self, listing: RawListing, page: Page | None = None) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Новый алгоритм:
        1. Загружает страницу карточки.
        2. Извлекает сессионный токен API.
        3. Вызывает API для получения цен и занятости на 60 дней.
        4. Заполняет calendar_60_days и prices_60_days.

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
            # Шаг 1: Загружаем страницу
            loaded = await self._goto_with_retry(active_page, listing.url)
            if not loaded:
                logger.warning(
                    "страница_не_загрузилась",
                    step=f"id={listing.external_id}",
                )
                return listing

            await self._browser.random_delay()

            # Шаг 2: Получаем токен API
            token = await self._get_api_token(active_page)
            if not token:
                logger.warning(
                    "токен_не_получен_пропуск_карточки",
                    step=f"id={listing.external_id}",
                )
                return listing

            # Шаг 3: Вызываем API для получения календаря и цен
            calendar, prices = await self._fetch_prices_via_api(
                active_page, listing.external_id, token
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
                        loaded = await self._goto_with_retry(page, listing.url)

                    if not loaded:
                        logger.warning(
                            "страница_не_загрузилась_вкладка",
                            step=f"id={listing.external_id}",
                        )
                    else:
                        await self._browser.random_delay()

                        try:
                            # Получаем токен
                            token = await self._get_api_token(page)
                            if token:
                                calendar, prices = await self._fetch_prices_via_api(
                                    page, listing.external_id, token
                                )
                                listing.calendar_60_days = calendar
                                listing.prices_60_days = prices

                                logger.info(
                                    "карточка_обработана_вкладкой",
                                    step=f"id={listing.external_id}",
                                    total=f"свободных={sum(1 for c in calendar if c == 0)}",
                                )
                            else:
                                logger.warning(
                                    "токен_не_получен_вкладка",
                                    step=f"id={listing.external_id}",
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
