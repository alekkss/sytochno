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
"""

import asyncio
import re
from datetime import datetime, timezone

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

# Пауза после загрузки страницы — ожидание фронтенд API-запросов (секунды)
_PAUSE_AFTER_PAGE_LOAD: float = 10.0

# Пауза между обработкой ссылок поиска (секунды)
_PAUSE_BETWEEN_URLS: float = 3.0

# Максимальное количество ошибок подряд до переключения на прокси
_MAX_CONSECUTIVE_ERRORS: int = 3

# Максимальное количество перезапусков браузера с прокси за одну ссылку
_MAX_PROXY_RESTARTS: int = 5

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


class ScraperService:
    """Сервис парсинга каталога sutochno.ru через внутреннее API.

    Обходит несколько URL поиска, для каждого перехватывает API-запрос
    от фронтенда и использует его для пагинации. Извлекает полные данные
    объявлений через второй API-эндпоинт.

    Дедупликация выполняется по external_id в процессе сбора.
    При сбоях автоматически переключается на прокси.
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

    async def scrape_catalog(self) -> list[RawListing]:
        """Основной метод — обходит все URL поиска и собирает объявления.

        Для каждой ссылки:
        1. Загружает страницу в браузере (для сессии и токена).
        2. Перехватывает URL searchObjectsOnMap от фронтенда.
        3. Через fetch() с пагинацией собирает все ID.
        4. Через searchObjectsByLocation получает полные данные.

        Returns:
            Список уникальных объявлений со всех ссылок.
        """
        self._seen_ids.clear()
        self._duplicates_count = 0

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
            return []

        # ── Фаза B: Получение полных данных ──
        if api_headers is None:
            logger.error("нет_заголовков_api_для_получения_данных")
            return []

        # Нужна живая страница для fetch() — если браузер закрыт, открываем
        page = await self._ensure_browser_page(current_proxy)
        if page is None:
            logger.error("не_удалось_открыть_браузер_для_получения_данных")
            return []

        listings = await self._fetch_full_listings(
            page=page,
            ids=all_ids,
            headers=api_headers,
        )

        # Закрываем браузер после сбора
        await self._browser.stop()

        logger.info(
            "парсинг_каталога_завершён",
            total_listings=len(listings),
            total_ids=len(all_ids),
            duplicates=self._duplicates_count,
        )

        return listings

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

            # Проверяем ошибки
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

            # Перехватчик searchObjectsOnMap
            async def _intercept(route, request):
                url = request.url
                if "searchObjectsOnMap" in url and captured["url"] is None:
                    captured["url"] = url
                    captured["headers"] = dict(request.headers)
                await route.continue_()

            await page.route("**/api/json/**", _intercept)

            # Загрузка страницы
            await page.goto(search_url, wait_until="networkidle")

            try:
                await page.wait_for_selector(
                    ".card[data-observe-id]", timeout=30000,
                )
            except Exception:
                pass

            # Ждём пока фронтенд завершит API-запросы
            await asyncio.sleep(_PAUSE_AFTER_PAGE_LOAD)

            # Снимаем перехватчик (чтобы не мешал fetch)
            await page.unroute("**/api/json/**")

            if not captured["url"]:
                logger.warning(
                    "searchObjectsOnMap_не_перехвачен",
                    url=search_url[:100],
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
    ) -> list[RawListing]:
        """Получает полные данные объявлений пачками по 50 ID.

        Args:
            page: Страница Playwright для fetch().
            ids: Список ID объявлений.
            headers: Заголовки API.

        Returns:
            Список RawListing с заполненными полями.
        """
        listings: list[RawListing] = []
        total_batches = (len(ids) + _API_PAGE_SIZE - 1) // _API_PAGE_SIZE

        logger.info(
            "начало_получения_полных_данных",
            total_ids=len(ids),
            total_batches=total_batches,
        )

        for i in range(0, len(ids), _API_PAGE_SIZE):
            batch = ids[i: i + _API_PAGE_SIZE]
            batch_num = i // _API_PAGE_SIZE + 1

            ids_params = "&".join(f"ids[]={oid}" for oid in batch)
            url = (
                f"{_API_SEARCH_BY_LOCATION}"
                f"?{ids_params}"
                f"&max_guests=2&relevance=pairs&currencyId=1"
            )

            result = await _fetch_get(page, url, headers)

            if _is_error_response(result):
                error_msg = _extract_error_message(result)
                logger.warning(
                    "ошибка_получения_данных_пачки",
                    batch=f"{batch_num}/{total_batches}",
                    error=error_msg,
                )
                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            objects = _extract_objects(result.get("data"))

            if not objects:
                logger.debug(
                    "пустая_пачка",
                    batch=f"{batch_num}/{total_batches}",
                )
                await asyncio.sleep(_PAUSE_BETWEEN_API)
                continue

            for obj in objects:
                listing = _parse_api_object(obj)
                if listing is not None:
                    listings.append(listing)

            if batch_num % 50 == 0 or batch_num == total_batches:
                logger.info(
                    "прогресс_получения_данных",
                    batch=f"{batch_num}/{total_batches}",
                    listings_collected=len(listings),
                )

            await asyncio.sleep(_PAUSE_BETWEEN_API)

        return listings


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


def _is_error_response(result: dict) -> bool:
    """Проверяет, является ли ответ ошибочным.

    Args:
        result: Результат _fetch_get.

    Returns:
        True если ответ содержит ошибку.
    """
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

    if isinstance(props, dict):
        area_m2 = props.get("area")
        guests = props.get("maxGuests")
        has_instant_booking = bool(props.get("bookingNow", False))

    # Адрес
    location = obj.get("location", {})
    address: str | None = None

    if isinstance(location, dict):
        addr_data = location.get("address", {})
        if isinstance(addr_data, dict):
            address = addr_data.get("title")

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
        )
    except ValueError as e:
        logger.debug(
            "ошибка_создания_listing",
            external_id=external_id_str,
            error=str(e),
        )
        return None
