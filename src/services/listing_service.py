"""Сервис парсинга карточки объявления — извлечение календаря занятости и цен."""

import asyncio
import re
import time
from datetime import date, timedelta

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService

logger = get_logger("listing")

# Маппинг русских названий месяцев к номерам
_MONTH_MAP: dict[str, int] = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}

# CSS-селекторы элементов с ценой (в порядке приоритета)
_PRICE_SELECTORS: list[str] = [
    ".sc-detail-aside-price__cost",
    ".sc-detail-hotel-booking__price-sale",
]

# Селектор ошибки минимального количества суток
_MIN_NIGHTS_ERROR_SELECTOR: str = ".sc-detail-aside-booking__info-error-text"

# CSS-классы, означающие полную недоступность дня (занят или прошедший)
_DISABLED_FULL: str = "sc-base-datepicker-day_disabled-both"
_DISABLED_PAST: str = "sc-base-datepicker-day_disabled"

# CSS-классы граничных дней (кликабельны, считаются свободными)
_DISABLED_LEFT: str = "sc-base-datepicker-day_disabled-left"
_DISABLED_RIGHT: str = "sc-base-datepicker-day_disabled-right"


def _is_day_disabled(class_attr: str) -> bool:
    """Определяет, является ли день полностью недоступным (занят или прошёл).

    Семантика CSS-классов датепикера sutochno.ru:
    - ``_disabled-both`` — занят полностью (ни заезд, ни выезд).
    - ``_disabled`` (без суффикса) — прошедший день.
    - ``_disabled-left`` — граничный: предыдущий гость выезжает, день СВОБОДЕН.
    - ``_disabled-right`` — граничный: следующий гость заезжает, день СВОБОДЕН.

    Дни с ``_disabled-left`` и ``_disabled-right`` кликабельны и считаются свободными.

    Args:
        class_attr: Значение атрибута class у ячейки дня.

    Returns:
        True — день недоступен (занят/прошёл), False — день свободен (можно кликнуть).
    """
    classes = class_attr.split()

    # Проверяем полную недоступность
    if _DISABLED_FULL in classes:
        return True

    # Проверяем прошедший день: класс _disabled без суффикса.
    # Нужно точное совпадение, чтобы не поймать _disabled-left / _disabled-right
    if _DISABLED_PAST in classes:
        return True

    return False


class ListingService:
    """Сервис парсинга карточки объявления на sutochno.ru.

    Заходит в каждое объявление, открывает календарь и считывает
    занятость на 60 дней (0 — свободен, 1 — занят), а затем
    собирает цены за сутки для каждого свободного дня.
    """

    def __init__(self, settings: Settings, browser_service: BrowserService) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
        """
        self._settings = settings
        self._browser = browser_service

    async def enrich_listing(self, listing: RawListing) -> RawListing:
        """Обогащает объявление данными календаря занятости и ценами.

        Переходит на страницу объявления, открывает датепикер,
        сбрасывает даты, считывает занятость на 60 дней,
        затем собирает цены по каждому свободному дню.
        Замеряет и логирует время обработки карточки.

        Args:
            listing: Объявление с базовыми данными из каталога.

        Returns:
            Объявление с заполненными calendar_60_days и prices_60_days.
        """
        start_time = time.perf_counter()

        logger.info(
            "парсинг_карточки",
            path=listing.url,
            step=f"id={listing.external_id}",
        )

        try:
            # Переходим на страницу объявления
            logger.debug(
                "переход_на_страницу",
                step=f"id={listing.external_id}",
                path=listing.url,
            )
            await self._browser.navigate(listing.url)
            await self._browser.random_delay()

            logger.debug(
                "страница_загружена",
                step=f"id={listing.external_id}",
            )

            # Открываем календарь и считываем занятость
            calendar = await self._extract_calendar()
            listing.calendar_60_days = calendar

            logger.info(
                "календарь_собран",
                step=f"id={listing.external_id}",
                total=len(calendar),
            )

            # Собираем цены по дням
            prices = await self._extract_prices(calendar)
            listing.prices_60_days = prices

            logger.info(
                "цены_собраны",
                step=f"id={listing.external_id}",
                total=len(prices),
            )

        except Exception as e:
            logger.warning(
                "ошибка_парсинга_карточки",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}",
            )

        elapsed = time.perf_counter() - start_time
        elapsed_str = f"{elapsed:.1f}с"

        logger.info(
            "карточка_завершена",
            step=f"id={listing.external_id}",
            total=elapsed_str,
        )

        return listing

    async def enrich_listings(self, listings: list[RawListing]) -> list[RawListing]:
        """Обогащает список объявлений данными календаря и цен.

        Последовательно обрабатывает каждое объявление.

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

        Каждая прокси запускает свой браузер, прогревает его на sutochno.ru,
        затем обрабатывает свою порцию карточек. Результаты объединяются.

        Args:
            settings: Настройки приложения.
            listings: Полный список карточек для обработки.
            proxies: Список рабочих прокси.

        Returns:
            Список обогащённых карточек (порядок может отличаться от входного).
        """
        from src.services.proxy_service import ProxyService

        # Распределяем карточки между прокси
        chunks = ProxyService.distribute_listings(listings, len(proxies))

        logger.info(
            "параллельная_обработка",
            total=len(listings),
            step=f"прокси={len(proxies)}",
        )

        # Запускаем воркеры параллельно
        tasks = [
            ListingService._worker(settings, chunk, proxy, worker_idx)
            for worker_idx, (chunk, proxy) in enumerate(zip(chunks, proxies), start=1)
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Собираем результаты
        all_enriched: list[RawListing] = []
        for worker_idx, result in enumerate(results, start=1):
            if isinstance(result, Exception):
                logger.warning(
                    "воркер_завершился_с_ошибкой",
                    error=str(result),
                    error_type=type(result).__name__,
                    step=f"воркер={worker_idx}",
                )
            elif isinstance(result, list):
                all_enriched.extend(result)

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
    ) -> list[RawListing]:
        """Воркер — обрабатывает порцию карточек через один прокси-браузер.

        Последовательность:
        1. Запускает браузер с прокси.
        2. Переходит на sutochno.ru для прогрева (15 секунд).
        3. Последовательно обрабатывает свои карточки.
        4. Останавливает браузер.

        Args:
            settings: Настройки приложения.
            listings: Порция карточек для этого воркера.
            proxy: Прокси для этого воркера.
            worker_idx: Номер воркера (для логов).

        Returns:
            Список обогащённых карточек.
        """
        if not listings:
            return []

        browser_service = BrowserService(settings=settings)

        try:
            # Шаг 1: Запускаем браузер с прокси
            await browser_service.start(proxy=proxy)

            logger.info(
                "воркер_запущен",
                step=f"воркер={worker_idx}",
                total=len(listings),
            )

            # Шаг 2: Прогрев — переходим на sutochno.ru и ждём 15 секунд
            await browser_service.navigate("https://sutochno.ru")
            await browser_service.scroll_page()
            await asyncio.sleep(15)

            logger.info(
                "воркер_прогрет",
                step=f"воркер={worker_idx}",
            )

            # Шаг 3: Создаём ListingService для этого браузера и обрабатываем карточки
            listing_service = ListingService(
                settings=settings,
                browser_service=browser_service,
            )

            total = len(listings)
            for idx, listing in enumerate(listings, start=1):
                logger.info(
                    "обработка_карточки",
                    current=idx,
                    total=total,
                    step=f"воркер={worker_idx}",
                )
                await listing_service.enrich_listing(listing)
                await browser_service.random_delay()

            return listings

        except Exception as e:
            logger.warning(
                "ошибка_воркера",
                error=str(e),
                error_type=type(e).__name__,
                step=f"воркер={worker_idx}",
            )
            return listings

        finally:
            # Шаг 4: Останавливаем браузер
            await browser_service.stop()
            logger.info(
                "воркер_остановлен",
                step=f"воркер={worker_idx}",
            )

    # ─────────────────────────────────────────────────────────────────────
    # Сбор цен по дням
    # ─────────────────────────────────────────────────────────────────────

    async def _extract_prices(self, calendar: list[int]) -> list[int]:
        """Собирает цены за сутки для каждого дня из 60-дневного диапазона.

        Для каждого свободного дня:
        1. Открывает датепикер.
        2. Сбрасывает даты (с проверкой, что датепикер остался открытым).
        3. Кликает на день N (заезд).
        4. Находит ближайший свободный день после N и кликает (выезд).
        5. Считывает цену со страницы.
        6. Если появилась ошибка «Минимальное количество суток — N»,
           повторяет с диапазоном checkin + N дней.
        7. Корректно вычисляет цену за ночь без двойного деления.

        Для занятых дней — цена = 0.

        Args:
            calendar: Список занятости (0 — свободен, 1 — занят).

        Returns:
            Список из 60 цен (int). 0 — если день занят.
        """
        if not calendar:
            return []

        today = date.today()
        prices: list[int] = []
        is_first_price_call = True
        free_days_count = sum(1 for d in calendar if d == 0)

        logger.debug(
            "начало_сбора_цен",
            step=f"свободных_дней={free_days_count}",
            total=len(calendar),
        )

        for day_idx in range(len(calendar)):
            current_date = today + timedelta(days=day_idx)

            # Если день занят — цена 0
            if calendar[day_idx] == 1:
                prices.append(0)
                continue

            # Находим ближайший свободный день для выезда (после текущего)
            checkout_offset = self._find_next_free_day(calendar, day_idx + 1)
            if checkout_offset is None:
                prices.append(0)
                logger.debug(
                    "нет_свободного_дня_для_выезда",
                    step=f"день={day_idx + 1}, дата={current_date.isoformat()}",
                )
                continue

            checkout_date = today + timedelta(days=checkout_offset)
            nights = (checkout_date - current_date).days

            logger.debug(
                "запрос_цены_для_дня",
                step=f"день={day_idx + 1}/{len(calendar)}",
                current=f"заезд={current_date.isoformat()}, выезд={checkout_date.isoformat()}, ночей={nights}",
            )

            # Получаем цену за выбранный диапазон
            price_per_night = await self._get_price_for_dates(
                current_date, checkout_date, is_first_call=is_first_price_call
            )
            is_first_price_call = False

            prices.append(price_per_night)

            logger.debug(
                "цена_дня_получена",
                step=f"день={day_idx + 1}",
                current=f"за_ночь={price_per_night}",
            )

        logger.debug(
            "сбор_цен_завершён",
            step=f"ненулевых={sum(1 for p in prices if p > 0)}",
            total=len(prices),
        )

        return prices

    @staticmethod
    def _find_next_free_day(calendar: list[int], start_idx: int) -> int | None:
        """Находит индекс ближайшего свободного дня начиная с start_idx.

        Поиск идёт до 61-го дня (индекс 60) включительно,
        чтобы для последнего (60-го) дня можно было найти выезд.

        Args:
            calendar: Список занятости.
            start_idx: Индекс, с которого начинать поиск.

        Returns:
            Индекс свободного дня или None, если не найден.
        """
        # Разрешаем выезд до 61-го дня (индекс 60)
        max_idx = min(len(calendar), 61)
        for idx in range(start_idx, max_idx):
            if idx >= len(calendar):
                return idx
            if calendar[idx] == 0:
                return idx
        # Если все дни до конца заняты, разрешаем выезд на день после календаря
        if start_idx <= 60:
            return min(start_idx, 60)
        return None

    async def _get_price_for_dates(
        self, checkin: date, checkout: date, *, is_first_call: bool = True
    ) -> int:
        """Получает цену за одну ночь для указанного диапазона дат через датепикер.

        Последовательность:
        1. Открывает датепикер.
        2. Сбрасывает даты и гарантирует, что датепикер остался открытым.
        3. Кликает дату заезда.
        4. Кликает дату выезда.
        5. Ждёт обновления цены.
        6. Проверяет ошибку минимального количества суток.
        7. Считывает цену и вычисляет цену за ночь.

        Args:
            checkin: Дата заезда.
            checkout: Дата выезда.
            is_first_call: Первый ли это вызов для данной карточки.

        Returns:
            Цена за одну ночь в рублях (int). 0 — если не удалось считать.
        """
        try:
            # Шаг 1: Открываем датепикер
            logger.debug(
                "шаг_1_открытие_датепикера",
                step=f"заезд={checkin.isoformat()}",
                current=f"skip_scroll={not is_first_call}",
            )
            opened = await self._open_datepicker(skip_scroll=not is_first_call)
            if not opened:
                logger.debug(
                    "датепикер_не_открылся_для_цены",
                    step=f"заезд={checkin.isoformat()}",
                )
                return 0

            logger.debug(
                "шаг_2_сброс_дат",
                step=f"заезд={checkin.isoformat()}",
            )

            # Шаг 2: Сбрасываем даты и гарантируем, что датепикер открыт
            await self._reset_dates_and_ensure_open(
                short_delay=not is_first_call
            )

            logger.debug(
                "шаг_3_клик_заезд",
                step=f"дата={checkin.isoformat()}",
            )

            # Шаг 3: Кликаем дату заезда
            clicked_checkin = await self._click_day_in_datepicker(checkin)
            if not clicked_checkin:
                logger.debug(
                    "не_удалось_кликнуть_заезд",
                    step=f"дата={checkin.isoformat()}",
                )
                await self._close_datepicker()
                return 0

            logger.debug(
                "заезд_кликнут_успешно",
                step=f"дата={checkin.isoformat()}",
            )

            await asyncio.sleep(0.3 if not is_first_call else 0.8)

            logger.debug(
                "шаг_4_клик_выезд",
                step=f"дата={checkout.isoformat()}",
            )

            # Шаг 4: Кликаем дату выезда
            clicked_checkout = await self._click_day_in_datepicker(checkout)
            if not clicked_checkout:
                logger.debug(
                    "не_удалось_кликнуть_выезд",
                    step=f"дата={checkout.isoformat()}",
                )
                await self._close_datepicker()
                return 0

            logger.debug(
                "выезд_кликнут_успешно",
                step=f"дата={checkout.isoformat()}",
            )

            # Шаг 5: Ждём закрытия датепикера и обновления цены
            wait_time = 1.5 if not is_first_call else 2.5
            logger.debug(
                "шаг_5_ожидание_обновления_цены",
                step=f"ожидание={wait_time}с",
            )
            await asyncio.sleep(wait_time)

            # Шаг 6: Проверяем ошибку минимального количества суток
            min_nights = await self._check_min_nights_error()
            if min_nights is not None:
                logger.debug(
                    "шаг_6_минимум_суток_требуется",
                    step=f"заезд={checkin.isoformat()}",
                    total=min_nights,
                )
                # _retry_with_min_nights возвращает уже цену за ночь
                price_per_night = await self._retry_with_min_nights(
                    checkin, min_nights
                )
                return price_per_night

            # Шаг 7: Считываем цену и вычисляем за ночь
            logger.debug(
                "шаг_7_чтение_цены",
                step=f"заезд={checkin.isoformat()}",
            )
            price_total = await self._read_price()

            nights = (checkout - checkin).days
            if price_total > 0 and nights > 0:
                price_per_night = round(price_total / nights)
            else:
                price_per_night = 0

            logger.debug(
                "цена_прочитана",
                step=f"заезд={checkin.isoformat()}",
                current=f"итого={price_total}, ночей={nights}, за_ночь={price_per_night}",
            )
            return price_per_night

        except Exception as e:
            logger.debug(
                "ошибка_получения_цены",
                error=str(e),
                error_type=type(e).__name__,
                step=f"заезд={checkin.isoformat()}",
            )
            return 0

    async def _check_min_nights_error(self) -> int | None:
        """Проверяет наличие ошибки «Минимальное количество суток — N».

        Ищет элемент с текстом ошибки и извлекает число минимальных суток.

        Returns:
            Число минимальных суток (int) если ошибка найдена, None — если ошибки нет.
        """
        page = self._browser.page

        try:
            error_el = await page.query_selector(_MIN_NIGHTS_ERROR_SELECTOR)
            if not error_el:
                return None

            error_text = await error_el.inner_text()
            if not error_text:
                return None

            # Извлекаем число из текста «Минимальное количество суток - 3.»
            digits = re.search(r"(\d+)", error_text)
            if not digits:
                return None

            min_nights = int(digits.group(1))
            if min_nights > 0:
                logger.debug(
                    "ошибка_минимум_суток",
                    step=f"текст='{error_text.strip()}'",
                    total=min_nights,
                )
                return min_nights

        except Exception:
            pass

        return None

    async def _retry_with_min_nights(self, checkin: date, min_nights: int) -> int:
        """Повторяет получение цены с учётом минимального количества суток.

        Открывает датепикер, сбрасывает даты, выбирает заезд = checkin,
        выезд = checkin + min_nights. Считывает общую цену и делит
        на min_nights для получения цены за одну ночь.

        Args:
            checkin: Дата заезда.
            min_nights: Минимальное количество суток.

        Returns:
            Цена за одну ночь (int). 0 — если не удалось считать.
        """
        checkout = checkin + timedelta(days=min_nights)

        logger.debug(
            "повтор_с_минимумом_суток",
            step=f"заезд={checkin.isoformat()}, выезд={checkout.isoformat()}, ночей={min_nights}",
        )

        try:
            opened = await self._open_datepicker(skip_scroll=True)
            if not opened:
                return 0

            await self._reset_dates_and_ensure_open(short_delay=True)

            clicked_checkin = await self._click_day_in_datepicker(checkin)
            if not clicked_checkin:
                await self._close_datepicker()
                return 0

            await asyncio.sleep(0.3)

            clicked_checkout = await self._click_day_in_datepicker(checkout)
            if not clicked_checkout:
                await self._close_datepicker()
                return 0

            await asyncio.sleep(1.5)

            price_total = await self._read_price()

            if price_total > 0:
                price_per_night = round(price_total / min_nights)
                logger.debug(
                    "цена_с_минимумом_суток_получена",
                    step=f"итого={price_total}, ночей={min_nights}, за_ночь={price_per_night}",
                )
                return price_per_night

        except Exception as e:
            logger.debug(
                "ошибка_повтора_с_минимумом_суток",
                error=str(e),
                error_type=type(e).__name__,
            )

        return 0

    async def _click_day_in_datepicker(self, target_date: date) -> bool:
        """Кликает на конкретный день в открытом датепикере.

        При необходимости листает месяцы вперёд или назад.
        Разрешает клик по граничным дням (``_disabled-left``,
        ``_disabled-right``), блокирует только полностью
        занятые (``_disabled-both``) и прошедшие (``_disabled``).

        Args:
            target_date: Дата, которую нужно выбрать.

        Returns:
            True если клик выполнен успешно, False — если день не найден.
        """
        page = self._browser.page

        # Навигируем к нужному месяцу (вперёд или назад)
        navigated = await self._navigate_to_month(target_date.year, target_date.month)
        if not navigated:
            logger.debug(
                "месяц_не_найден_в_датепикере",
                step=f"{target_date.year}-{target_date.month:02d}",
            )
            return False

        # Находим блок нужного месяца
        month_block = await self._find_month_block(target_date.year, target_date.month)
        if not month_block:
            logger.debug(
                "блок_месяца_не_найден_для_клика",
                step=f"{target_date.year}-{target_date.month:02d}",
            )
            return False

        # Находим ячейку нужного дня и кликаем
        day_cells = await month_block.query_selector_all("td.sc-base-datepicker-day")
        logger.debug(
            "поиск_дня_в_месяце",
            step=f"дата={target_date.isoformat()}, ячеек_найдено={len(day_cells)}",
        )

        for cell in day_cells:
            span = await cell.query_selector("span")
            if not span:
                continue
            day_text = await span.inner_text()
            day_text = day_text.strip()
            if not day_text.isdigit():
                continue
            if int(day_text) == target_date.day:
                # Проверяем, что день не полностью недоступен
                class_attr = await cell.get_attribute("class") or ""
                if _is_day_disabled(class_attr):
                    logger.debug(
                        "день_недоступен_пропускаем",
                        step=f"дата={target_date.isoformat()}, class='{class_attr}'",
                    )
                    return False

                logger.debug(
                    "кликаем_день",
                    step=f"дата={target_date.isoformat()}, class='{class_attr}'",
                )

                try:
                    await cell.click(timeout=3000)
                    return True
                except Exception as e:
                    logger.debug(
                        "обычный_клик_не_сработал_js_fallback",
                        step=f"дата={target_date.isoformat()}",
                        error=str(e),
                    )
                    # Fallback: JS-клик
                    await page.evaluate(
                        "(el) => el.click()",
                        cell,
                    )
                    return True

        logger.debug(
            "день_не_найден_в_ячейках",
            step=f"дата={target_date.isoformat()}, искали_день={target_date.day}",
        )
        return False

    async def _navigate_to_month(self, year: int, month: int) -> bool:
        """Навигирует датепикер к указанному месяцу (вперёд или назад).

        Определяет, в каком направлении листать, сравнивая целевой месяц
        с текущими видимыми месяцами в датепикере.

        Args:
            year: Целевой год.
            month: Целевой месяц (1-12).

        Returns:
            True если месяц стал видимым, False — если не удалось.
        """
        max_attempts = 12  # Максимум 12 листаний (год)

        for attempt in range(max_attempts):
            # Проверяем, виден ли уже нужный месяц
            if await self._is_month_visible(year, month):
                return True

            # Определяем направление листания
            direction = await self._get_navigation_direction(year, month)

            if direction == "forward":
                logger.debug(
                    "листаем_вперёд",
                    step=f"попытка={attempt + 1}, цель={year}-{month:02d}",
                )
                await self._click_next_month()
            elif direction == "backward":
                logger.debug(
                    "листаем_назад",
                    step=f"попытка={attempt + 1}, цель={year}-{month:02d}",
                )
                await self._click_prev_month()
            else:
                # Не удалось определить направление
                logger.debug(
                    "не_удалось_определить_направление",
                    step=f"цель={year}-{month:02d}",
                )
                return False

            await asyncio.sleep(0.5)

        return False

    async def _get_navigation_direction(self, target_year: int, target_month: int) -> str:
        """Определяет направление листания датепикера.

        Сравнивает целевой месяц с первым видимым месяцем в датепикере.

        Args:
            target_year: Целевой год.
            target_month: Целевой месяц.

        Returns:
            "forward", "backward" или "unknown".
        """
        page = self._browser.page
        titles = await page.query_selector_all(".sc-base-datepicker-month__title")

        if not titles:
            return "unknown"

        # Берём первый видимый месяц для сравнения
        first_title_text = await titles[0].inner_text()
        parsed = self._parse_month_title(first_title_text)

        if not parsed:
            return "unknown"

        visible_year, visible_month = parsed
        target_value = target_year * 12 + target_month
        visible_value = visible_year * 12 + visible_month

        if target_value < visible_value:
            return "backward"
        elif target_value > visible_value + 1:
            # +1 потому что обычно видны 2 месяца
            return "forward"
        else:
            # Целевой месяц должен быть виден (текущий или следующий)
            # но _is_month_visible вернул False — значит нужно листнуть вперёд
            return "forward"

    async def _click_prev_month(self) -> None:
        """Кликает кнопку «Назад» в датепикере для перехода к предыдущему месяцу."""
        page = self._browser.page

        prev_btn = await page.query_selector(".sc-base-datepicker-modal__prev")
        if not prev_btn:
            logger.debug("кнопка_назад_не_найдена")
            return

        # Проверяем что кнопка не скрыта (style="display: none;")
        is_hidden = await page.evaluate("""
            () => {
                const el = document.querySelector('.sc-base-datepicker-modal__prev');
                if (!el) return true;
                const style = window.getComputedStyle(el);
                return style.display === 'none';
            }
        """)

        if is_hidden:
            logger.debug("кнопка_назад_скрыта")
            return

        try:
            await prev_btn.click(timeout=5000)
            logger.debug("кнопка_назад_нажата")
        except Exception:
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-base-datepicker-modal__prev');
                    if (el) el.click();
                }
            """)
            logger.debug("кнопка_назад_нажата_js")

        await asyncio.sleep(0.5)

    async def _read_price(self) -> int:
        """Считывает цену из элемента на странице карточки.

        Пробует несколько CSS-селекторов в порядке приоритета.
        Ожидает, что текст содержит цифры (цена обновилась после выбора дат).
        Ретраит чтение до 5 раз с паузой, если текст пока пустой.

        Returns:
            Цена в рублях (int). 0 — если ни один элемент не найден.
        """
        page = self._browser.page

        for selector in _PRICE_SELECTORS:
            try:
                # Ждём появления элемента
                price_el = await page.wait_for_selector(
                    selector,
                    timeout=5000,
                )
                if not price_el:
                    logger.debug(
                        "селектор_не_найден",
                        step=f"selector='{selector}'",
                    )
                    continue

                # Ждём, пока текст цены содержит цифры
                for retry in range(5):
                    price_text = await price_el.inner_text()
                    cleaned = price_text.replace("\xa0", "").replace(" ", "")
                    digits = re.sub(r"[^\d]", "", cleaned)

                    logger.debug(
                        "чтение_текста_цены",
                        step=f"selector='{selector}', попытка={retry + 1}",
                        current=f"raw='{price_text}', cleaned='{cleaned}', digits='{digits}'",
                    )

                    if digits:
                        price = int(digits)
                        if price > 0:
                            logger.debug(
                                "цена_извлечена",
                                step=f"selector='{selector}'",
                                total=price,
                            )
                            return price

                    # Текст пока пустой или без цифр — ждём обновления
                    await asyncio.sleep(0.5)

                logger.debug(
                    "цена_не_появилась_после_ретраев",
                    step=f"selector='{selector}'",
                )

            except Exception as e:
                logger.debug(
                    "ошибка_при_чтении_цены",
                    step=f"selector='{selector}'",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                continue

        logger.debug("цена_не_найдена_ни_в_одном_селекторе")
        return 0

    # ─────────────────────────────────────────────────────────────────────
    # Извлечение календаря занятости
    # ─────────────────────────────────────────────────────────────────────

    async def _extract_calendar(self) -> list[int]:
        """Извлекает календарь занятости на 60 дней из датепикера.

        Последовательность:
        1. Прокрутка к блоку дат и клик на «Заезд» для открытия датепикера.
        2. Нажатие «Сбросить даты» с проверкой, что датепикер остался открытым.
        3. Считывание дней текущего и следующих месяцев.
        4. Листание месяцев кнопкой «Далее» при необходимости.

        Returns:
            Список из 60 элементов (0 — свободен, 1 — занят).
        """
        logger.debug("начало_сбора_календаря")

        # Шаг 1: Прокручиваем к блоку дат и открываем датепикер
        opened = await self._open_datepicker()
        if not opened:
            logger.warning("датепикер_не_открылся_при_сборе_календаря")
            return []

        logger.debug("датепикер_открыт_для_календаря")

        # Шаг 2: Сбрасываем даты и проверяем, что датепикер остался открытым
        await self._reset_dates_safe()

        logger.debug("даты_сброшены_для_календаря")

        # Шаг 3: Считываем календарь на 60 дней
        today = date.today()
        end_date = today + timedelta(days=59)
        calendar: list[int] = []

        # Определяем, какие месяцы нам нужны
        months_needed = self._get_months_range(today, end_date)

        logger.debug(
            "месяцы_для_сбора",
            step=f"всего={len(months_needed)}",
            current=str(months_needed),
        )

        for month_idx, (year, month) in enumerate(months_needed):
            # Листаем к нужному месяцу (первые два уже видны в датепикере)
            if month_idx >= 2:
                is_visible = await self._is_month_visible(year, month)
                if not is_visible:
                    logger.debug(
                        "листаем_к_месяцу_календарь",
                        step=f"{year}-{month:02d}",
                    )
                    await self._click_next_month()
                    await asyncio.sleep(1)

            # Считываем дни этого месяца
            month_days = await self._read_month_days(year, month)

            logger.debug(
                "дни_месяца_считаны",
                step=f"{year}-{month:02d}",
                total=len(month_days),
            )

            # Если блок месяца не найден — пробуем листнуть и повторить
            if not month_days and month_idx < len(months_needed):
                logger.debug(
                    "месяц_не_найден_пробуем_листнуть",
                    step=f"{year}-{month:02d}",
                )
                await self._click_next_month()
                await asyncio.sleep(1)
                month_days = await self._read_month_days(year, month)
                logger.debug(
                    "повторное_чтение_месяца",
                    step=f"{year}-{month:02d}",
                    total=len(month_days),
                )

            # Фильтруем: берём только дни в диапазоне [today, end_date]
            for day_num, is_occupied in month_days:
                current_date = date(year, month, day_num)
                if current_date < today:
                    continue
                if current_date > end_date:
                    break
                calendar.append(is_occupied)

            if len(calendar) >= 60:
                break

        # Обрезаем до 60 дней
        calendar = calendar[:60]

        # Закрываем датепикер
        await self._close_datepicker()

        # Проверяем, что собрали достаточно данных
        if len(calendar) < 60:
            logger.warning(
                "календарь_неполный",
                step=f"собрано={len(calendar)}",
                total=60,
            )

        logger.debug(
            "календарь_собран_итого",
            step=f"занятых={sum(calendar)}, свободных={len(calendar) - sum(calendar)}",
            total=len(calendar),
        )

        return calendar

    # ─────────────────────────────────────────────────────────────────────
    # Вспомогательные методы работы с датепикером
    # ─────────────────────────────────────────────────────────────────────

    async def _open_datepicker(self, *, skip_scroll: bool = False) -> bool:
        """Открывает датепикер кликом на блок «Заезд».

        Args:
            skip_scroll: Пропустить прокрутку к блоку дат (уже в позиции).

        Returns:
            True если датепикер открылся, False — если не удалось.
        """
        page = self._browser.page

        # Проверяем, может датепикер уже открыт
        if await self._is_datepicker_open():
            logger.debug("датепикер_уже_открыт")
            return True

        if not skip_scroll:
            # Прокручиваем к блоку дат
            logger.debug("прокрутка_к_блоку_дат")
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-detail-dates');
                    if (el) el.scrollIntoView({behavior: 'smooth', block: 'center'});
                }
            """)
            await asyncio.sleep(1)

        # Ищем блок «Заезд»
        checkin_block = await page.query_selector(".sc-detail-dates__item_in")
        if not checkin_block:
            logger.warning("блок_заезда_не_найден")
            return False

        # Пробуем обычный клик
        try:
            await checkin_block.click(timeout=5000)
            logger.debug("клик_по_блоку_заезда_выполнен")
        except Exception as e:
            logger.debug(
                "обычный_клик_не_сработал_пробуем_js",
                error=str(e),
            )
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-detail-dates__item_in');
                    if (el) el.click();
                }
            """)

        # Пауза для анимации
        await asyncio.sleep(0.5 if skip_scroll else 1.5)

        # Ждём появления датепикера
        try:
            await page.wait_for_selector(
                ".sc-base-datepicker-modal",
                timeout=5000,
            )
            # Дополнительно проверяем видимость
            if await self._is_datepicker_open():
                logger.debug("датепикер_открылся_успешно")
                return True
            # Элемент в DOM, но скрыт — ждём ещё
            await asyncio.sleep(0.5)
            is_open = await self._is_datepicker_open()
            logger.debug(
                "датепикер_после_доп_ожидания",
                step=f"открыт={is_open}",
            )
            return is_open
        except Exception:
            logger.warning("датепикер_не_открылся_таймаут")
            return False

    async def _reset_dates_safe(self) -> None:
        """Сбрасывает даты в датепикере с проверкой, что он остался открытым.

        После нажатия «Сбросить даты» датепикер может закрыться автоматически.
        В этом случае переоткрывает его.
        """
        page = self._browser.page

        # Нажимаем «Сбросить даты»
        reset_button = await page.query_selector(".sc-base-datepicker__reset")
        if reset_button:
            logger.debug("нажимаем_сбросить_даты")
            try:
                await reset_button.click(timeout=3000)
            except Exception:
                await page.evaluate("""
                    () => {
                        const el = document.querySelector('.sc-base-datepicker__reset');
                        if (el) el.click();
                    }
                """)

            await asyncio.sleep(1.0)
        else:
            logger.debug("кнопка_сбросить_даты_не_найдена")

        # Проверяем, остался ли датепикер открытым
        datepicker_still_open = await self._is_datepicker_open()
        logger.debug(
            "после_сброса_дат",
            step=f"датепикер_открыт={datepicker_still_open}",
        )

        if not datepicker_still_open:
            logger.debug("датепикер_закрылся_после_сброса_переоткрываем")

            reopened = await self._open_datepicker(skip_scroll=True)
            if not reopened:
                logger.warning("не_удалось_переоткрыть_датепикер_после_сброса")
                return

            await asyncio.sleep(1.0)

        # Ждём, пока блоки месяцев появятся в DOM
        try:
            await page.wait_for_selector(
                ".sc-base-datepicker-month",
                timeout=5000,
            )
            logger.debug("блоки_месяцев_найдены")
        except Exception:
            logger.warning("блоки_месяцев_не_появились_после_сброса")

        await asyncio.sleep(0.5)

    async def _reset_dates_and_ensure_open(
        self, *, short_delay: bool = False
    ) -> None:
        """Сбрасывает даты и гарантирует, что датепикер остаётся открытым.

        Используется в контексте сбора цен.

        Args:
            short_delay: Использовать сокращённые паузы (для повторных вызовов).
        """
        page = self._browser.page

        # Нажимаем «Сбросить даты»
        reset_button = await page.query_selector(".sc-base-datepicker__reset")
        if reset_button:
            logger.debug("сброс_дат_для_цены")
            try:
                await reset_button.click(timeout=3000)
            except Exception:
                await page.evaluate("""
                    () => {
                        const el = document.querySelector('.sc-base-datepicker__reset');
                        if (el) el.click();
                    }
                """)

            await asyncio.sleep(0.5 if short_delay else 1.0)
        else:
            logger.debug("кнопка_сброса_не_найдена_в_ценах")

        # Проверяем, что датепикер остался открытым
        is_open = await self._is_datepicker_open()
        if not is_open:
            logger.debug("датепикер_закрылся_после_сброса_в_ценах_переоткрываем")
            await self._open_datepicker(skip_scroll=True)
            await asyncio.sleep(0.5 if short_delay else 1.0)

        # Ждём появления блоков месяцев
        try:
            await page.wait_for_selector(
                ".sc-base-datepicker-month",
                timeout=3000,
            )
        except Exception:
            logger.debug("блоки_месяцев_не_найдены_после_сброса_в_ценах")

        await asyncio.sleep(0.3)

    async def _is_datepicker_open(self) -> bool:
        """Проверяет, открыт ли датепикер (виден в DOM и отображается).

        Returns:
            True если датепикер открыт и виден, False — если закрыт или не найден.
        """
        page = self._browser.page

        try:
            is_open = await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-base-datepicker-modal');
                    if (!el) return false;
                    const rect = el.getBoundingClientRect();
                    if (rect.width === 0 || rect.height === 0) return false;
                    const style = window.getComputedStyle(el);
                    return style.display !== 'none'
                        && style.visibility !== 'hidden'
                        && style.opacity !== '0';
                }
            """)
            return bool(is_open)
        except Exception:
            return False

    async def _close_datepicker(self) -> None:
        """Закрывает датепикер нажатием Escape или кликом вне его."""
        page = self._browser.page
        try:
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
            logger.debug("датепикер_закрыт_escape")
        except Exception:
            pass

    async def _is_month_visible(self, year: int, month: int) -> bool:
        """Проверяет, виден ли указанный месяц в датепикере.

        Args:
            year: Год.
            month: Номер месяца (1-12).

        Returns:
            True если месяц отображается в датепикере.
        """
        page = self._browser.page
        titles = await page.query_selector_all(".sc-base-datepicker-month__title")

        for title_el in titles:
            title_text = await title_el.inner_text()
            parsed = self._parse_month_title(title_text)
            if parsed and parsed == (year, month):
                return True

        return False

    async def _click_next_month(self) -> None:
        """Кликает кнопку «Далее» в датепикере для перехода к следующему месяцу."""
        page = self._browser.page

        next_btn = await page.query_selector(".sc-base-datepicker-modal__next")
        if not next_btn:
            logger.debug("кнопка_далее_не_найдена")
            return

        # Проверяем что кнопка не скрыта
        is_hidden = await page.evaluate("""
            () => {
                const el = document.querySelector('.sc-base-datepicker-modal__next');
                if (!el) return true;
                const style = window.getComputedStyle(el);
                return style.display === 'none';
            }
        """)

        if is_hidden:
            logger.debug("кнопка_далее_скрыта")
            return

        try:
            await next_btn.click(timeout=5000)
            logger.debug("кнопка_далее_нажата")
        except Exception:
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-base-datepicker-modal__next');
                    if (el) el.click();
                }
            """)
            logger.debug("кнопка_далее_нажата_js")

        await asyncio.sleep(0.5)

    async def _read_month_days(self, year: int, month: int) -> list[tuple[int, int]]:
        """Считывает статус всех дней указанного месяца из датепикера.

        Семантика CSS-классов:
        - ``_disabled-both`` — день занят полностью → статус 1.
        - ``_disabled`` (без суффикса) — прошедший день → статус 1.
        - ``_disabled-left`` — граничный, свободен → статус 0.
        - ``_disabled-right`` — граничный, свободен → статус 0.
        - Без disabled-классов — полностью свободен → статус 0.

        Args:
            year: Год.
            month: Номер месяца (1-12).

        Returns:
            Список кортежей (номер_дня, статус), где статус: 0=свободен, 1=занят.
        """
        days: list[tuple[int, int]] = []

        # Находим нужный блок месяца по заголовку
        month_block = await self._find_month_block(year, month)
        if not month_block:
            logger.debug(
                "блок_месяца_не_найден",
                step=f"{year}-{month:02d}",
            )
            return days

        # Находим все ячейки дней в этом месяце
        day_cells = await month_block.query_selector_all("td.sc-base-datepicker-day")

        for cell in day_cells:
            span = await cell.query_selector("span")
            if not span:
                continue

            day_text = await span.inner_text()
            day_text = day_text.strip()
            if not day_text.isdigit():
                continue

            day_num = int(day_text)

            # Определяем статус через централизованную функцию
            class_attr = await cell.get_attribute("class") or ""
            is_occupied = 1 if _is_day_disabled(class_attr) else 0

            days.append((day_num, is_occupied))

        return days

    async def _find_month_block(self, year: int, month: int) -> "any":  # type: ignore[name-defined]
        """Находит DOM-элемент блока указанного месяца в датепикере.

        Args:
            year: Год.
            month: Номер месяца (1-12).

        Returns:
            Элемент блока месяца или None.
        """
        page = self._browser.page
        month_blocks = await page.query_selector_all(".sc-base-datepicker-month")

        for block in month_blocks:
            title_el = await block.query_selector(".sc-base-datepicker-month__title")
            if not title_el:
                continue

            title_text = await title_el.inner_text()
            parsed = self._parse_month_title(title_text)
            if parsed and parsed == (year, month):
                return block

        return None

    @staticmethod
    def _parse_month_title(title: str) -> tuple[int, int] | None:
        """Парсит заголовок месяца вида «май 2026» или «июнь 2026».

        Args:
            title: Текст заголовка месяца.

        Returns:
            Кортеж (год, номер_месяца) или None, если не удалось распарсить.
        """
        title = title.strip().lower()
        parts = title.split()
        if len(parts) != 2:
            return None

        month_name = parts[0]
        year_str = parts[1]

        if not year_str.isdigit():
            return None

        year = int(year_str)

        # Ищем совпадение по началу названия месяца
        for prefix, month_num in _MONTH_MAP.items():
            if month_name.startswith(prefix):
                return (year, month_num)

        return None

    @staticmethod
    def _get_months_range(start: date, end: date) -> list[tuple[int, int]]:
        """Возвращает список пар (год, месяц) для покрытия диапазона дат.

        Args:
            start: Начальная дата.
            end: Конечная дата.

        Returns:
            Список кортежей (год, месяц) в хронологическом порядке.
        """
        months: list[tuple[int, int]] = []
        current = start.replace(day=1)

        while current <= end:
            months.append((current.year, current.month))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return months
