"""Сервис парсинга каталога — сбор объявлений через внутреннее API sutochno.ru.

Двухфазный подход:
  Фаза A — searchObjectsOnMap: для каждой ссылки поиска загружает страницу
  в браузере, перехватывает реальный URL API от фронтенда, затем через fetch()
  с пагинацией offset += 50 собирает ID всех объявлений.

  Фаза B — searchObjectsByLocation: по собранным ID пачками по 50
  запрашивает полные данные (название, цена, рейтинг, отзывы, площадь,
  гости, адрес, метро, быстрое бронирование).

При сбое (сетевая ошибка, блокировка IP, протухший токен):
  1. Закрывает текущий браузер.
  2. Открывает новый через прокси из пула.
  3. Загружает ту же страницу → перехватывает новый URL/токен.
  4. Продолжает с того же offset (не с начала).
  5. Если прокси тоже упала — берёт следующую из пула.
  6. Если пул исчерпан — возвращает то, что собрано.

При серверной ошибке 5xx (500, 502, 503 и т.д.):
  Парсер переходит в режим ожидания: ждёт 10 минут и повторяет
  тот же запрос. Повторяет до исчерпания лимита (200 минут).
  Это ошибка сервера, а не клиента — данные не теряются.

После завершения scrape_catalog() браузер НЕ закрывается — страница
и токен остаются живыми для batch-обогащения. Вызывающий код сам
управляет жизненным циклом браузера.
"""

import asyncio
import re
from datetime import datetime, timezone

from playwright.async_api import Error as PlaywrightError
from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService

logger = get_logger("scraper")

# ── Константы ──────────────────────────────────────────────

# Размер страницы API (объектов за один запрос)
_API_PAGE_SIZE: int = 50

# Пауза между API-запросами fetch() (секунды)
_PAUSE_BETWEEN_API: float = 0.5

# Таймаут ожидания перехвата API-запроса searchObjectsOnMap (секунды).
# После goto(domcontentloaded) фронтенд отправляет API-запрос
# в первые 3-10 секунд. 20 секунд — достаточно с запасом.
# Если за это время API не перехвачен — страница не загрузилась корректно.
_API_INTERCEPT_TIMEOUT: float = 20.0

# Интервал проверки перехвата API URL (секунды)
_API_INTERCEPT_POLL_INTERVAL: float = 0.5

# Пауза между обработкой ссылок поиска (секунды)
_PAUSE_BETWEEN_URLS: float = 3.0

# Максимальное количество ошибок подряд до переключения на прокси
_MAX_CONSECUTIVE_ERRORS: int = 3

# Максимальное количество перезапусков браузера с прокси за одну ссылку
_MAX_PROXY_RESTARTS: int = 5

# Количество повторных попыток для одной пачки перед пропуском
_BATCH_RETRY_ATTEMPTS: int = 2

# Пауза перед повторной попыткой пачки (секунды)
_BATCH_RETRY_PAUSE: float = 2.0

# Пауза после переключения прокси в Фазе B (секунды)
_PROXY_SWITCH_PAUSE: float = 3.0

# ── Ожидание при серверных ошибках 5xx ──
# Пауза ожидания при ошибке 5xx (секунды) — 10 минут
_SERVER_ERROR_WAIT_SECONDS: int = 600

# Максимальное суммарное время ожидания серверных ошибок (минуты)
_SERVER_ERROR_MAX_WAIT_MINUTES: int = 200

# Маркеры ошибок API, при которых выполняется переключение на прокси
_API_ERROR_MARKERS: tuple[str, ...] = (
    "Bad request",
    "Application authentication failed",
    "ERR_TIMED_OUT",
    "ERR_CONNECTION_REFUSED",
    "ERR_CONNECTION_RESET",
    "net::",
    "Target crashed",
    "Target closed",
    "Browser closed",
    "Connection closed",
)

# URL эндпоинта для получения полных данных объявлений
_API_SEARCH_BY_LOCATION: str = (
    "https://sutochno.ru/api/json/search/searchObjectsByLocation"
)

# Заголовки браузера, которые не нужно передавать в fetch()
_SKIP_HEADERS: set[str] = {
    "host", "connection", "content-length",
    "accept-encoding", "sec-fetch-dest",
    "sec-fetch-mode", "sec-fetch-site",
    "sec-ch-ua", "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
}


async def _safe_route_continue(route) -> None:
    """Безопасно продолжает route, подавляя ошибки Playwright.

    Playwright может выбросить ``Route.continue_: Route is already handled!``,
    если маршрут уже был завершён (страница закрылась, ушла в навигацию,
    ``unroute`` уже снял обработчик и т.п.). Такие исключения безвредны —
    они возникают в event listener'е самого Playwright и не влияют
    на собранные данные. Логируем на уровне DEBUG и подавляем.

    Args:
        route: Объект Playwright ``Route``.
    """
    try:
        await route.continue_()
    except PlaywrightError as e:
        # "Route is already handled", "Target page/context closed" и т.п.
        logger.debug(
            "route_continue_подавлено",
            error=str(e)[:200],
            error_type=type(e).__name__,
        )
    except Exception as e:
        # Любая другая ошибка внутри listener'а не должна ронять Playwright
        logger.debug(
            "route_continue_неожиданная_ошибка",
            error=str(e)[:200],
            error_type=type(e).__name__,
        )


async def _safe_unroute(page: Page, pattern: str) -> None:
    """Безопасно снимает route-обработчик, подавляя ошибки.

    Если страница уже закрыта или ушла в навигацию, ``unroute`` может
    бросить исключение — оно не влияет на дальнейшую работу.

    Args:
        page: Страница Playwright.
        pattern: Шаблон URL, ранее переданный в ``page.route``.
    """
    try:
        await page.unroute(pattern)
    except PlaywrightError as e:
        logger.debug(
            "unroute_подавлено",
            pattern=pattern,
            error=str(e)[:200],
        )
    except Exception as e:
        logger.debug(
            "unroute_неожиданная_ошибка",
            pattern=pattern,
            error=str(e)[:200],
        )


class ScraperService:
    """Сервис парсинга каталога sutochno.ru через внутреннее API.

    Обходит несколько URL поиска, для каждого перехватывает API-запрос
    от фронтенда и использует его для пагинации. Извлекает полные данные
    объявлений через второй API-эндпоинт.

    Дедупликация выполняется по external_id в процессе сбора.
    При сбоях автоматически переключается на прокси.
    При серверных ошибках 5xx — ожидает восстановления сервера.

    После завершения scrape_catalog() браузер остаётся запущенным —
    страница и токен доступны для batch-обогащения.
    """

    def __init__(
        self,
        settings: Settings,
        browser_service: BrowserService,
        proxies: list[ProxyConfig] | None = None,
    ) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
            proxies: Пул рабочих прокси для fallback при блокировке.
        """
        self._settings = settings
        self._browser = browser_service
        self._proxies: list[ProxyConfig] = proxies or []
        self._proxy_index: int = 0
        self._seen_ids: set[int] = set()
        self._duplicates_count: int = 0
        # Суммарное время ожидания серверных ошибок 5xx за текущий прогон (секунды)
        self._server_error_total_wait: int = 0

    def _get_next_proxy(
        self, exclude: ProxyConfig | None = None,
    ) -> ProxyConfig | None:
        """Возвращает следующую прокси из пула, пропуская исключённую.

        Args:
            exclude: Прокси, которую нужно пропустить.

        Returns:
            Следующая прокси или None, если пул пуст.
        """
        if not self._proxies:
            return None

        for _ in range(len(self._proxies)):
            proxy = self._proxies[self._proxy_index % len(self._proxies)]
            self._proxy_index += 1
            if proxy != exclude:
                return proxy

        return self._proxies[0]

    async def _wait_for_server_recovery(self, context: str) -> bool:
        """Ожидает восстановления сервера при ошибке 5xx.

        Ждёт _SERVER_ERROR_WAIT_SECONDS (10 минут). Проверяет лимит
        суммарного ожидания (_SERVER_ERROR_MAX_WAIT_MINUTES).

        Args:
            context: Контекст вызова для логирования (например, "фаза_A" или "фаза_B").

        Returns:
            True — можно повторить запрос.
            False — лимит ожидания исчерпан, нужно прекращать.
        """
        max_wait_seconds = _SERVER_ERROR_MAX_WAIT_MINUTES * 60

        if self._server_error_total_wait >= max_wait_seconds:
            logger.error(
                "лимит_ожидания_5xx_исчерпан",
                context=context,
                total_waited_minutes=self._server_error_total_wait // 60,
                limit_minutes=_SERVER_ERROR_MAX_WAIT_MINUTES,
            )
            return False

        wait_minutes = _SERVER_ERROR_WAIT_SECONDS // 60
        remaining_minutes = (
            (max_wait_seconds - self._server_error_total_wait) // 60
        )

        logger.warning(
            "ошибка_сервера_5xx_ожидание",
            context=context,
            wait_minutes=wait_minutes,
            total_waited_minutes=self._server_error_total_wait // 60,
            remaining_minutes=remaining_minutes,
        )

        await asyncio.sleep(_SERVER_ERROR_WAIT_SECONDS)
        self._server_error_total_wait += _SERVER_ERROR_WAIT_SECONDS

        logger.info(
            "ожидание_5xx_завершено_повтор_запроса",
            context=context,
            total_waited_minutes=self._server_error_total_wait // 60,
        )

        return True

    async def scrape_catalog(self) -> tuple[list[RawListing], str | None]:
        """Основной метод — обходит все URL поиска и собирает объявления.

        Для каждой ссылки:
        1. Загружает страницу в браузере (для сессии и токена).
        2. Перехватывает URL searchObjectsOnMap от фронтенда.
        3. Через fetch() с пагинацией собирает все ID.
        4. Через searchObjectsByLocation получает полные данные.

        После завершения браузер НЕ закрывается — страница с живой
        сессией и токен доступны для batch-обогащения карточек.

        Returns:
            Кортеж из двух элементов:
            - Список уникальных объявлений со всех ссылок.
            - Токен API (перехвачен из заголовков) или None, если
              токен не удалось получить.
        """
        self._seen_ids.clear()
        self._duplicates_count = 0
        self._server_error_total_wait = 0

        all_ids: list[int] = []
        api_headers: dict[str, str] | None = None
        current_proxy: ProxyConfig | None = None

        logger.info(
            "начало_парсинга_каталога",
            urls_count=len(self._settings.search_urls),
        )

        # ── Фаза A: Сбор ID по всем ссылкам ──
        for url_index, search_url in enumerate(self._settings.search_urls, 1):
            logger.info(
                "начало_обхода_ссылки",
                url_index=url_index,
                urls_total=len(self._settings.search_urls),
                url=search_url[:100],
            )

            ids_from_url, api_headers, current_proxy = await self._collect_ids_for_url(
                search_url=search_url,
                url_index=url_index,
                current_proxy=current_proxy,
                previous_headers=api_headers,
            )

            all_ids.extend(ids_from_url)

            logger.info(
                "ссылка_обработана",
                url_index=url_index,
                new_ids=len(ids_from_url),
                total_ids=len(all_ids),
                total_unique=len(self._seen_ids),
            )

            if url_index < len(self._settings.search_urls):
                await asyncio.sleep(_PAUSE_BETWEEN_URLS)

        logger.info(
            "сбор_id_завершён",
            total_unique=len(self._seen_ids),
            total_with_duplicates=len(all_ids),
            duplicates=self._duplicates_count,
        )

        if not all_ids:
            logger.warning("не_собрано_ни_одного_id")
            return [], None

        # ── Фаза B: Получение полных данных ──
        if api_headers is None:
            logger.error("нет_заголовков_api_для_получения_данных")
            return [], None

        # Нужна живая страница для fetch() — если браузер закрыт, открываем
        page = await self._ensure_browser_page(current_proxy)
        if page is None:
            logger.error("не_удалось_открыть_браузер_для_получения_данных")
            return [], None

        listings = await self._fetch_full_listings(
            page=page,
            ids=all_ids,
            headers=api_headers,
            current_proxy=current_proxy,
        )

        # ── Извлекаем токен из перехваченных заголовков ──
        captured_token = api_headers.get("token") or api_headers.get("Token")

        if captured_token:
            logger.info(
                "токен_api_извлечён_из_каталога",
                step=f"длина={len(captured_token)}",
            )
        else:
            logger.warning(
                "токен_api_не_найден_в_заголовках",
                step=f"ключи={list(api_headers.keys())}",
            )

        # Браузер НЕ закрываем — страница и токен нужны для batch-обогащения.
        # Вызывающий код (pipeline в __main__.py) сам управляет жизненным циклом.

        logger.info(
            "парсинг_каталога_завершён",
            total_listings=len(listings),
            total_ids=len(all_ids),
            duplicates=self._duplicates_count,
            token_available=captured_token is not None,
            server_error_wait_minutes=self._server_error_total_wait // 60,
        )

        return listings, captured_token

    # ── Фаза A: Сбор ID ──────────────────────────────────────

    async def _collect_ids_for_url(
        self,
        search_url: str,
        url_index: int,
        current_proxy: ProxyConfig | None,
        previous_headers: dict[str, str] | None,
    ) -> tuple[list[int], dict[str, str] | None, ProxyConfig | None]:
        """Собирает ID объявлений для одной ссылки поиска.

        При сбое переключается на прокси и продолжает с того же offset.
        При серверной ошибке 5xx — ждёт 10 минут и повторяет запрос.

        Args:
            search_url: URL страницы поиска.
            url_index: Номер ссылки (для логов).
            current_proxy: Текущая прокси (None = без прокси).
            previous_headers: Заголовки от предыдущей ссылки (для переиспользования).

        Returns:
            Кортеж (список новых ID, актуальные заголовки, текущая прокси).
        """
        new_ids: list[int] = []
        offset: int = 0
        consecutive_errors: int = 0
        proxy_restarts: int = 0
        api_headers = previous_headers
        map_url: str | None = None

        while True:
            # Если нет перехваченного URL — загружаем страницу
            if map_url is None:
                load_result = await self._load_page_and_intercept(
                    search_url=search_url,
                    proxy=current_proxy,
                )

                if load_result is None:
                    # Не удалось загрузить — пробуем прокси
                    switched = await self._switch_to_proxy(
                        current_proxy=current_proxy,
                        url_index=url_index,
                        proxy_restarts=proxy_restarts,
                    )
                    if switched is None:
                        logger.warning(
                            "не_удалось_загрузить_ссылку_пропуск",
                            url_index=url_index,
                        )
                        break

                    current_proxy, proxy_restarts = switched
                    continue

                map_url, api_headers = load_result

            # Пагинация через fetch()
            page = self._browser.page
            paginated_url = _replace_offset(map_url, offset)

            result = await _fetch_get(page, paginated_url, api_headers)

            # ── Проверяем серверную ошибку 5xx ──
            if _is_server_error(result):
                status_code = _get_http_status(result)
                logger.warning(
                    "серверная_ошибка_5xx_фаза_A",
                    url_index=url_index,
                    offset=offset,
                    status=status_code,
                )

                can_retry = await self._wait_for_server_recovery(
                    context=f"фаза_A_url{url_index}_offset{offset}",
                )

                if not can_retry:
                    logger.error(
                        "лимит_ожидания_5xx_фаза_A_прерывание",
                        url_index=url_index,
                        offset=offset,
                        collected=len(new_ids),
                    )
                    break

                # Повторяем тот же запрос — не меняем offset, не считаем
                # как consecutive_error
                continue

            # ── Проверяем обычные ошибки (клиентские, сетевые) ──
            if _is_error_response(result):
                consecutive_errors += 1
                error_msg = _extract_error_message(result)

                logger.warning(
                    "ошибка_api_запроса",
                    url_index=url_index,
                    offset=offset,
                    error=error_msg,
                    consecutive=consecutive_errors,
                )

                if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                    # Переключаемся на прокси
                    switched = await self._switch_to_proxy(
                        current_proxy=current_proxy,
                        url_index=url_index,
                        proxy_restarts=proxy_restarts,
                    )
                    if switched is None:
                        logger.warning(
                            "прокси_исчерпаны_продолжаем_с_собранными",
                            url_index=url_index,
                            collected=len(new_ids),
                        )
                        break

                    current_proxy, proxy_restarts = switched
                    consecutive_errors = 0
                    map_url = None  # Перехватим новый URL после перезагрузки
                    continue

                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            # Успешный ответ — сбрасываем счётчик ошибок
            consecutive_errors = 0

            objects = _extract_objects(result.get("data"))

            if not objects:
                logger.debug(
                    "пустой_ответ_конец_пагинации",
                    url_index=url_index,
                    offset=offset,
                )
                break

            # Извлекаем ID с дедупликацией
            page_new = 0
            page_dups = 0

            for obj in objects:
                obj_id = obj.get("id")
                if obj_id is None:
                    continue
                if obj_id in self._seen_ids:
                    page_dups += 1
                    self._duplicates_count += 1
                    continue
                self._seen_ids.add(obj_id)
                new_ids.append(obj_id)
                page_new += 1

            logger.debug(
                "страница_api_обработана",
                url_index=url_index,
                offset=offset,
                received=len(objects),
                new=page_new,
                duplicates=page_dups,
            )

            # Последняя страница
            if len(objects) < _API_PAGE_SIZE:
                break

            offset += _API_PAGE_SIZE
            await asyncio.sleep(_PAUSE_BETWEEN_API)

        return new_ids, api_headers, current_proxy

    async def _load_page_and_intercept(
        self,
        search_url: str,
        proxy: ProxyConfig | None,
    ) -> tuple[str, dict[str, str]] | None:
        """Загружает страницу и перехватывает URL searchObjectsOnMap.

        Стратегия загрузки оптимизирована для скорости:
        1. goto с wait_until="domcontentloaded" — не ждёт изображения/шрифты.
        2. Активное ожидание перехвата API URL с коротким таймаутом
           (_API_INTERCEPT_TIMEOUT) — если фронтенд не отправил запрос
           за 20 секунд, страница загрузилась некорректно.

        Это сокращает время одной неудачной попытки с ~60-75 секунд
        до ~20-25 секунд, что критично при переборе нескольких прокси.

        Args:
            search_url: URL страницы поиска.
            proxy: Прокси для браузера (None = без прокси).

        Returns:
            Кортеж (перехваченный URL API, заголовки) или None при ошибке.
        """
        captured: dict = {"url": None, "headers": None}

        try:
            # Запускаем браузер (или перезапускаем с новой прокси)
            await self._browser.stop()
            await self._browser.start(proxy=proxy)

            page = self._browser.page

            # Перехватчик searchObjectsOnMap.
            # Вся логика listener'а обёрнута в try/except: любые исключения
            # внутри route-обработчика Playwright всплывают в его event loop
            # и печатаются как "Error occurred in event listener" в stderr.
            # Особенно важно защитить route.continue_() — если маршрут уже
            # был обработан (страница ушла в навигацию, unroute успел
            # снять обработчик и т.п.), Playwright бросит
            # "Route.continue_: Route is already handled!".
            async def _intercept(route, request):
                try:
                    url = request.url
                    if "searchObjectsOnMap" in url and captured["url"] is None:
                        captured["url"] = url
                        captured["headers"] = dict(request.headers)
                except Exception as e:
                    logger.debug(
                        "intercept_чтение_запроса_ошибка",
                        error=str(e)[:200],
                    )
                await _safe_route_continue(route)

            await page.route("**/api/json/**", _intercept)

            # Загрузка страницы — domcontentloaded: DOM готов, скрипты
            # начали выполняться. Не ждём загрузки изображений/шрифтов/аналитики.
            # API-запрос searchObjectsOnMap отправляется фронтендом
            # через 3-10 секунд после domcontentloaded.
            await page.goto(search_url, wait_until="domcontentloaded")

            # Активное ожидание перехвата API URL с таймаутом.
            # Вместо фиксированного sleep(15) — проверяем каждые 0.5 секунд,
            # завершаем сразу после перехвата. Если за _API_INTERCEPT_TIMEOUT
            # не перехвачен — страница загрузилась некорректно.
            elapsed = 0.0
            while elapsed < _API_INTERCEPT_TIMEOUT:
                if captured["url"] is not None:
                    break
                await asyncio.sleep(_API_INTERCEPT_POLL_INTERVAL)
                elapsed += _API_INTERCEPT_POLL_INTERVAL

            # Снимаем перехватчик (чтобы не мешал fetch).
            # Оборачиваем в safe-хелпер: если страница уже уходит
            # в навигацию/закрытие, unroute может бросить исключение.
            await _safe_unroute(page, "**/api/json/**")

            if not captured["url"]:
                logger.warning(
                    "searchObjectsOnMap_не_перехвачен",
                    url=search_url[:100],
                    waited=f"{elapsed:.1f}с",
                )
                return None

            # Фильтруем заголовки
            api_headers = {
                k: v for k, v in captured["headers"].items()
                if k.lower() not in _SKIP_HEADERS
            }

            logger.info(
                "api_url_перехвачен",
                api_url=captured["url"][:120],
                waited=f"{elapsed:.1f}с",
            )

            return captured["url"], api_headers

        except Exception as e:
            logger.warning(
                "ошибка_загрузки_страницы",
                error=str(e)[:300],
                error_type=type(e).__name__,
            )
            return None

    async def _switch_to_proxy(
        self,
        current_proxy: ProxyConfig | None,
        url_index: int,
        proxy_restarts: int,
    ) -> tuple[ProxyConfig, int] | None:
        """Переключается на следующую прокси из пула.

        Args:
            current_proxy: Текущая прокси (None = без прокси).
            url_index: Номер ссылки (для логов).
            proxy_restarts: Количество перезапусков для этой ссылки.

        Returns:
            Кортеж (новая прокси, обновлённый счётчик) или None если исчерпаны.
        """
        if proxy_restarts >= _MAX_PROXY_RESTARTS:
            logger.warning(
                "лимит_перезапусков_прокси_достигнут",
                url_index=url_index,
                restarts=proxy_restarts,
            )
            return None

        next_proxy = self._get_next_proxy(exclude=current_proxy)

        if next_proxy is None:
            logger.warning(
                "пул_прокси_пуст",
                url_index=url_index,
            )
            return None

        logger.info(
            "переключение_на_прокси",
            url_index=url_index,
            proxy=str(next_proxy),
            restart_num=proxy_restarts + 1,
        )

        await self._browser.stop()
        await asyncio.sleep(2)

        return next_proxy, proxy_restarts + 1

    async def _ensure_browser_page(
        self, proxy: ProxyConfig | None,
    ) -> Page | None:
        """Проверяет наличие живой страницы, при необходимости перезапускает.

        Args:
            proxy: Прокси для браузера.

        Returns:
            Страница Playwright или None при ошибке.
        """
        try:
            if await self._browser.is_alive():
                return self._browser.page
        except Exception:
            pass

        try:
            await self._browser.stop()
            await self._browser.start(proxy=proxy)
            return self._browser.page
        except Exception as e:
            logger.error(
                "не_удалось_запустить_браузер",
                error=str(e)[:300],
            )
            return None

    # ── Фаза B: Получение полных данных ──────────────────────

    async def _fetch_full_listings(
        self,
        page: Page,
        ids: list[int],
        headers: dict[str, str],
        current_proxy: ProxyConfig | None = None,
    ) -> list[RawListing]:
        """Получает полные данные объявлений пачками по 50 ID.

        При серверных ошибках 5xx — ждёт 10 минут и повторяет запрос.
        При клиентских/сетевых ошибках:
        1. Повторяет пачку до _BATCH_RETRY_ATTEMPTS раз с паузой.
        2. При _MAX_CONSECUTIVE_ERRORS подряд — переключается на прокси,
           перезапускает браузер и продолжает с той же пачки.
        3. Если прокси исчерпаны — возвращает то, что собрано.

        Args:
            page: Страница Playwright для fetch().
            ids: Список ID объявлений.
            headers: Заголовки API.
            current_proxy: Текущая прокси (для fallback при блокировке).

        Returns:
            Список RawListing с заполненными полями.
        """
        listings: list[RawListing] = []
        total_batches = (len(ids) + _API_PAGE_SIZE - 1) // _API_PAGE_SIZE
        consecutive_errors: int = 0
        proxy_restarts: int = 0
        skipped_batches: int = 0

        logger.info(
            "начало_получения_полных_данных",
            total_ids=len(ids),
            total_batches=total_batches,
        )

        batch_index = 0

        while batch_index < len(ids):
            batch = ids[batch_index: batch_index + _API_PAGE_SIZE]
            batch_num = batch_index // _API_PAGE_SIZE + 1

            ids_params = "&".join(f"ids[]={oid}" for oid in batch)
            url = (
                f"{_API_SEARCH_BY_LOCATION}"
                f"?{ids_params}"
                f"&max_guests=2&relevance=pairs&currencyId=1"
            )

            # ── Retry для отдельной пачки ──
            batch_success = False

            for attempt in range(_BATCH_RETRY_ATTEMPTS):
                result = await _fetch_get(page, url, headers)

                # ── Проверяем серверную ошибку 5xx ──
                if _is_server_error(result):
                    status_code = _get_http_status(result)
                    logger.warning(
                        "серверная_ошибка_5xx_фаза_B",
                        batch=f"{batch_num}/{total_batches}",
                        status=status_code,
                    )

                    can_retry = await self._wait_for_server_recovery(
                        context=f"фаза_B_пачка{batch_num}",
                    )

                    if not can_retry:
                        logger.error(
                            "лимит_ожидания_5xx_фаза_B_прерывание",
                            batch=f"{batch_num}/{total_batches}",
                            collected=len(listings),
                        )
                        return listings

                    # Повторяем тот же attempt — не инкрементируем счётчик
                    # (выходим из for и повторим while с тем же batch_index)
                    break

                if not _is_error_response(result):
                    # Успешный ответ — сбрасываем счётчик ошибок
                    consecutive_errors = 0
                    batch_success = True

                    objects = _extract_objects(result.get("data"))

                    if objects:
                        for obj in objects:
                            listing = _parse_api_object(obj)
                            if listing is not None:
                                listings.append(listing)

                    break

                # Обычная ошибка (не 5xx) — логируем с деталями
                error_msg = _extract_error_message(result)
                raw_preview = result.get("raw", "")

                logger.warning(
                    "ошибка_получения_данных_пачки",
                    batch=f"{batch_num}/{total_batches}",
                    attempt=f"{attempt + 1}/{_BATCH_RETRY_ATTEMPTS}",
                    error=error_msg,
                    raw_preview=raw_preview[:200] if raw_preview else "",
                    consecutive=consecutive_errors + 1,
                )

                consecutive_errors += 1

                # ── Порог ошибок → переключение на прокси ──
                if consecutive_errors >= _MAX_CONSECUTIVE_ERRORS:
                    logger.warning(
                        "порог_ошибок_фазы_B_переключение_прокси",
                        consecutive=consecutive_errors,
                        batch=f"{batch_num}/{total_batches}",
                        collected=len(listings),
                    )

                    switch_result = await self._restart_browser_for_phase_b(
                        current_proxy=current_proxy,
                        proxy_restarts=proxy_restarts,
                    )

                    if switch_result is None:
                        # Прокси исчерпаны — возвращаем собранное
                        logger.warning(
                            "прокси_исчерпаны_фаза_B_возврат_собранного",
                            collected=len(listings),
                            remaining_batches=total_batches - batch_num + 1,
                        )
                        return listings

                    page, headers, current_proxy, proxy_restarts = switch_result
                    consecutive_errors = 0
                    # Не инкрементируем batch_index — повторяем ту же пачку
                    break

                # Пауза перед retry пачки
                if attempt < _BATCH_RETRY_ATTEMPTS - 1:
                    await asyncio.sleep(_BATCH_RETRY_PAUSE)
            else:
                # for завершился без break — все attempt исчерпаны
                if not batch_success and consecutive_errors < _MAX_CONSECUTIVE_ERRORS:
                    # Все retry пачки исчерпаны, но порог прокси не достигнут —
                    # пропускаем пачку
                    skipped_batches += 1
                    logger.warning(
                        "пачка_пропущена_после_retry",
                        batch=f"{batch_num}/{total_batches}",
                        skipped_ids=len(batch),
                        total_skipped_batches=skipped_batches,
                    )

            # Если была ошибка 5xx и мы вышли через break —
            # не двигаем batch_index, повторяем ту же пачку
            if _is_server_error(result) and not batch_success:
                continue

            # Переходим к следующей пачке только при успехе или пропуске
            # (не при переключении прокси — тогда повторяем текущую)
            if batch_success or consecutive_errors < _MAX_CONSECUTIVE_ERRORS:
                batch_index += _API_PAGE_SIZE

            if batch_num % 50 == 0 or batch_index >= len(ids):
                logger.info(
                    "прогресс_получения_данных",
                    batch=f"{batch_num}/{total_batches}",
                    listings_collected=len(listings),
                    skipped_batches=skipped_batches,
                )

            await asyncio.sleep(_PAUSE_BETWEEN_API)

        if skipped_batches > 0:
            logger.warning(
                "фаза_B_завершена_с_пропусками",
                total_listings=len(listings),
                skipped_batches=skipped_batches,
                skipped_ids_approx=skipped_batches * _API_PAGE_SIZE,
            )

        return listings

    async def _restart_browser_for_phase_b(
        self,
        current_proxy: ProxyConfig | None,
        proxy_restarts: int,
    ) -> tuple[Page, dict[str, str], ProxyConfig | None, int] | None:
        """Перезапускает браузер с новой прокси для Фазы B.

        Загружает страницу sutochno.ru для получения сессии,
        перехватывает свежие заголовки API.

        Args:
            current_proxy: Текущая прокси.
            proxy_restarts: Количество перезапусков.

        Returns:
            Кортеж (page, headers, proxy, restarts) или None если прокси исчерпаны.
        """
        if proxy_restarts >= _MAX_PROXY_RESTARTS:
            logger.warning(
                "лимит_перезапусков_прокси_фаза_B",
                restarts=proxy_restarts,
            )
            return None

        next_proxy = self._get_next_proxy(exclude=current_proxy)

        if next_proxy is None:
            logger.warning("пул_прокси_пуст_фаза_B")
            return None

        logger.info(
            "переключение_прокси_фаза_B",
            proxy=str(next_proxy),
            restart_num=proxy_restarts + 1,
        )

        await self._browser.stop()
        await asyncio.sleep(_PROXY_SWITCH_PAUSE)

        # Открываем новый браузер через прокси
        # и загружаем главную страницу для получения сессии
        try:
            await self._browser.start(proxy=next_proxy)
            page = self._browser.page

            # Перехватываем свежие заголовки из любого API-запроса.
            # Тот же защитный шаблон, что и в _intercept выше: любые
            # исключения внутри listener'а глушим, route.continue_()
            # обёрнут в _safe_route_continue.
            fresh_headers: dict[str, str] | None = None

            async def _capture_headers(route, request):
                nonlocal fresh_headers
                try:
                    if fresh_headers is None:
                        fresh_headers = {
                            k: v for k, v in request.headers.items()
                            if k.lower() not in _SKIP_HEADERS
                        }
                except Exception as e:
                    logger.debug(
                        "capture_headers_чтение_ошибка",
                        error=str(e)[:200],
                    )
                await _safe_route_continue(route)

            await page.route("**/api/json/**", _capture_headers)

            # Загружаем любую страницу sutochno.ru для инициализации сессии
            await page.goto(
                "https://sutochno.ru",
                wait_until="domcontentloaded",
            )

            # Ожидаем перехвата заголовков
            elapsed = 0.0
            while elapsed < _API_INTERCEPT_TIMEOUT:
                if fresh_headers is not None:
                    break
                await asyncio.sleep(_API_INTERCEPT_POLL_INTERVAL)
                elapsed += _API_INTERCEPT_POLL_INTERVAL

            await _safe_unroute(page, "**/api/json/**")

            if fresh_headers is None:
                logger.warning(
                    "не_удалось_перехватить_заголовки_фаза_B",
                    proxy=str(next_proxy),
                )
                # Используем базовые заголовки — fetch может работать
                # благодаря cookies сессии
                fresh_headers = {
                    "accept": "application/json, text/plain, */*",
                    "accept-language": "ru-RU,ru;q=0.9",
                }

            logger.info(
                "прокси_готова_фаза_B",
                proxy=str(next_proxy),
                headers_captured=fresh_headers is not None,
            )

            return page, fresh_headers, next_proxy, proxy_restarts + 1

        except Exception as e:
            logger.warning(
                "ошибка_запуска_прокси_фаза_B",
                proxy=str(next_proxy),
                error=str(e)[:300],
            )
            # Рекурсивно пробуем следующую прокси
            return await self._restart_browser_for_phase_b(
                current_proxy=next_proxy,
                proxy_restarts=proxy_restarts + 1,
            )


# ── Вспомогательные функции (модульный уровень) ──────────────


def _replace_offset(url: str, new_offset: int) -> str:
    """Заменяет параметр offset в URL через regex.

    Не перекодирует квадратные скобки и другие параметры —
    сохраняет оригинальный формат URL от фронтенда.

    Args:
        url: Исходный URL.
        new_offset: Новое значение offset.

    Returns:
        URL с заменённым offset.
    """
    result = re.sub(r"offset=\d+", f"offset={new_offset}", url)

    if "offset=" not in result:
        separator = "&" if "?" in result else "?"
        result = f"{result}{separator}offset={new_offset}"

    return result


async def _fetch_get(page: Page, url: str, headers: dict[str, str]) -> dict:
    """Выполняет GET-запрос через fetch() в контексте браузера.

    Args:
        page: Страница Playwright.
        url: URL запроса.
        headers: Заголовки запроса.

    Returns:
        Словарь с ключами success, status, data/error.
    """
    try:
        result = await page.evaluate("""
            async ({url, headers}) => {
                try {
                    const resp = await fetch(url, {
                        method: 'GET',
                        headers: headers,
                        credentials: 'include'
                    });
                    const text = await resp.text();
                    try {
                        return {
                            success: true,
                            status: resp.status,
                            data: JSON.parse(text)
                        };
                    } catch (e) {
                        return {
                            success: false,
                            status: resp.status,
                            error: 'JSON parse error',
                            raw: text.substring(0, 500)
                        };
                    }
                } catch (e) {
                    return {success: false, error: e.message};
                }
            }
        """, {"url": url, "headers": headers})
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


def _is_server_error(result: dict) -> bool:
    """Проверяет, является ли ответ серверной ошибкой 5xx.

    Серверные ошибки (500, 502, 503, 504 и т.д.) означают проблему
    на стороне сервера. Данные не потеряны — нужно подождать
    и повторить запрос.

    Args:
        result: Результат _fetch_get.

    Returns:
        True если HTTP-статус 5xx.
    """
    status = _get_http_status(result)
    return status is not None and 500 <= status < 600


def _get_http_status(result: dict) -> int | None:
    """Извлекает HTTP-статус из ответа fetch.

    Args:
        result: Результат _fetch_get.

    Returns:
        HTTP-статус или None, если статус не доступен.
    """
    status = result.get("status")
    if isinstance(status, int):
        return status
    return None


def _is_error_response(result: dict) -> bool:
    """Проверяет, является ли ответ ошибочным (кроме 5xx).

    Сначала исключает серверные ошибки 5xx — они обрабатываются
    отдельно через _is_server_error. Затем проверяет клиентские
    и сетевые ошибки.

    Args:
        result: Результат _fetch_get.

    Returns:
        True если ответ содержит ошибку (не 5xx).
    """
    # 5xx обрабатывается отдельно — здесь не считаем ошибкой
    if _is_server_error(result):
        return False

    if not result.get("success"):
        return True

    data = result.get("data")
    if isinstance(data, dict):
        if data.get("success") is False:
            return True
        errors = data.get("errors")
        if errors and isinstance(errors, list) and len(errors) > 0:
            return True

    return False


def _extract_error_message(result: dict) -> str:
    """Извлекает текст ошибки из ответа API.

    Args:
        result: Результат _fetch_get.

    Returns:
        Строка с описанием ошибки.
    """
    # Ошибка fetch
    if not result.get("success"):
        return result.get("error", "неизвестная ошибка fetch")

    # Ошибка API
    data = result.get("data", {})
    if isinstance(data, dict):
        errors = data.get("errors", [])
        if errors:
            return str(errors)

    return "неизвестная ошибка API"


def _extract_objects(data) -> list[dict] | None:
    """Извлекает массив объектов из ответа API.

    Пробует разные структуры: data, data.objects, data.data и т.д.

    Args:
        data: Parsed JSON ответа.

    Returns:
        Список объектов или None.
    """
    if isinstance(data, list) and data:
        return data

    if not isinstance(data, dict):
        return None

    for key in ("objects", "items", "results", "list", "data"):
        val = data.get(key)
        if isinstance(val, list) and val:
            return val
        if isinstance(val, dict):
            for subkey in ("objects", "items", "results", "list"):
                subval = val.get(subkey)
                if isinstance(subval, list) and subval:
                    return subval

    return None


def _parse_api_object(obj: dict) -> RawListing | None:
    """Преобразует объект из searchObjectsByLocation в RawListing.

    Извлекает все доступные поля из ответа API, включая координаты
    (lat, lng), количество комнат (rooms) и тип жилья (property_type).

    Args:
        obj: Словарь объекта из API.

    Returns:
        RawListing или None, если обязательные поля отсутствуют.
    """
    external_id = obj.get("id")
    title = obj.get("title", "")

    if not external_id or not title:
        return None

    external_id_str = str(external_id)

    # Цена
    prices = obj.get("prices", {})
    per_day = prices.get("perDay", {}) if isinstance(prices, dict) else {}
    price_per_night = per_day.get("value") if isinstance(per_day, dict) else None

    # Рейтинг и отзывы
    rating_data = obj.get("rating", {})
    rating: float | None = None
    review_count: int | None = None

    if isinstance(rating_data, dict):
        raw_rating = rating_data.get("value")
        if raw_rating is not None:
            try:
                rating = round(float(raw_rating), 1)
            except (ValueError, TypeError):
                pass
        review_count = rating_data.get("count")

    # Свойства объекта
    props = obj.get("properties", {})
    area_m2: int | None = None
    guests: int | None = None
    has_instant_booking: bool = False
    rooms: int | None = None

    if isinstance(props, dict):
        area_m2 = props.get("area")
        guests = props.get("maxGuests")
        has_instant_booking = bool(props.get("bookingNow", False))
        raw_rooms = props.get("rooms")
        if raw_rooms is not None:
            try:
                rooms = int(raw_rooms)
            except (ValueError, TypeError):
                pass

    # Тип жилья (верхнеуровневое поле «type»: «Квартира», «Комната», «Дом» и т.д.)
    property_type: str | None = None
    raw_type = obj.get("type")
    if raw_type is not None:
        property_type = str(raw_type).strip() or None

    # Координаты и адрес
    location = obj.get("location", {})
    address: str | None = None
    lat: float | None = None
    lng: float | None = None

    if isinstance(location, dict):
        addr_data = location.get("address", {})
        if isinstance(addr_data, dict):
            address = addr_data.get("title")

        # Координаты из location.lat / location.lng
        raw_lat = location.get("lat")
        raw_lng = location.get("lng")
        if raw_lat is not None and raw_lng is not None:
            try:
                lat = float(raw_lat)
                lng = float(raw_lng)
            except (ValueError, TypeError):
                pass

    # Метро
    metro_station: str | None = None

    if isinstance(location, dict):
        relations = location.get("relations", {})
        if isinstance(relations, dict):
            metro_data = relations.get("metro", {})
            if isinstance(metro_data, dict):
                metro_title = metro_data.get("title", "")
                metro_dist = metro_data.get("distance")
                if metro_title:
                    metro_station = (
                        f"{metro_title}, {metro_dist} м"
                        if metro_dist
                        else metro_title
                    )

    # URL
    url = f"https://sutochno.ru/{external_id_str}"

    try:
        return RawListing(
            external_id=external_id_str,
            title=title.strip(),
            url=url,
            price_per_night=price_per_night,
            rating=rating,
            review_count=review_count,
            area_m2=area_m2,
            guests=guests,
            address=address,
            metro_station=metro_station,
            has_instant_booking=has_instant_booking,
            lat=lat,
            lng=lng,
            rooms=rooms,
            property_type=property_type,
        )
    except ValueError as e:
        logger.debug(
            "ошибка_создания_listing",
            external_id=external_id_str,
            error=str(e),
        )
        return None
