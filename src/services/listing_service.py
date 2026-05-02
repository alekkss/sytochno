"""Сервис парсинга карточки объявления — извлечение календаря занятости и цен."""

import asyncio
import re
import time
from datetime import date, timedelta

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
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
            await self._browser.navigate(listing.url)
            await self._browser.random_delay()

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
    # Сбор цен по дням
    # ─────────────────────────────────────────────────────────────────────

    async def _extract_prices(self, calendar: list[int]) -> list[int]:
        """Собирает цены за сутки для каждого дня из 60-дневного диапазона.

        Для каждого свободного дня:
        1. Открывает датепикер.
        2. Сбрасывает даты.
        3. Кликает на день N (заезд).
        4. Находит ближайший свободный день после N и кликает (выезд).
        5. Считывает цену со страницы.
        6. Если появилась ошибка «Минимальное количество суток — N»,
           повторяет с диапазоном checkin + N дней.
        7. Делит цену на количество ночей.

        Для занятых дней — цена = 0.
        Первая итерация использует полный цикл с прокруткой,
        последующие — ускоренный без прокрутки.

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

        for day_idx in range(len(calendar)):
            current_date = today + timedelta(days=day_idx)

            # Если день занят — цена 0
            if calendar[day_idx] == 1:
                prices.append(0)
                continue

            # Находим ближайший свободный день для выезда (после текущего)
            checkout_offset = self._find_next_free_day(calendar, day_idx + 1)
            if checkout_offset is None:
                # Нет свободного дня для выезда в пределах диапазона — пропускаем
                prices.append(0)
                logger.debug(
                    "нет_свободного_дня_для_выезда",
                    step=f"день={day_idx + 1}",
                )
                continue

            checkout_date = today + timedelta(days=checkout_offset)
            nights = (checkout_date - current_date).days

            # Получаем цену за выбранный диапазон
            price_total = await self._get_price_for_dates(
                current_date, checkout_date, is_first_call=is_first_price_call
            )
            is_first_price_call = False

            if price_total > 0 and nights > 0:
                price_per_night = round(price_total / nights)
            else:
                price_per_night = 0

            prices.append(price_per_night)

            logger.debug(
                "цена_дня",
                step=f"день={day_idx + 1}",
                current=f"{current_date} → {checkout_date}",
                total=price_per_night,
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
                # День за пределами собранного календаря — считаем свободным
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
        """Получает цену за указанный диапазон дат через датепикер.

        При первом вызове выполняет полный цикл с прокруткой и длинными паузами.
        При последующих вызовах пропускает прокрутку и использует сокращённые паузы.

        Если после выбора дат появляется ошибка «Минимальное количество суток — N»,
        повторяет попытку с увеличенным диапазоном (checkin + N дней) и делит
        итоговую цену на N.

        Args:
            checkin: Дата заезда.
            checkout: Дата выезда.
            is_first_call: Первый ли это вызов для данной карточки.

        Returns:
            Общая цена за период в рублях (int). 0 — если не удалось считать.
        """
        try:
            # Шаг 1: Открываем датепикер
            opened = await self._open_datepicker(skip_scroll=not is_first_call)
            if not opened:
                return 0

            # Шаг 2: Сбрасываем даты
            await self._reset_dates()
            await asyncio.sleep(0.3 if not is_first_call else 0.5)

            # Шаг 3: Кликаем дату заезда
            clicked_checkin = await self._click_day_in_datepicker(checkin)
            if not clicked_checkin:
                await self._close_datepicker()
                return 0

            await asyncio.sleep(0.3 if not is_first_call else 0.8)

            # Шаг 4: Кликаем дату выезда
            clicked_checkout = await self._click_day_in_datepicker(checkout)
            if not clicked_checkout:
                await self._close_datepicker()
                return 0

            # Шаг 5: Ждём закрытия датепикера и обновления цены
            await asyncio.sleep(1.0 if not is_first_call else 2.0)

            # Шаг 6: Проверяем ошибку минимального количества суток
            min_nights = await self._check_min_nights_error()
            if min_nights is not None:
                logger.debug(
                    "минимум_суток_требуется",
                    step=f"{checkin.isoformat()}",
                    total=min_nights,
                )
                # Повторяем с увеличенным диапазоном
                price = await self._retry_with_min_nights(checkin, min_nights)
                return price

            # Шаг 7: Считываем цену
            price = await self._read_price()
            return price

        except Exception as e:
            logger.debug(
                "ошибка_получения_цены",
                error=str(e),
                error_type=type(e).__name__,
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
                return min_nights

        except Exception:
            pass

        return None

    async def _retry_with_min_nights(self, checkin: date, min_nights: int) -> int:
        """Повторяет получение цены с учётом минимального количества суток.

        Открывает датепикер, сбрасывает даты, выбирает заезд = checkin,
        выезд = checkin + min_nights. Считывает цену и делит на min_nights.

        Args:
            checkin: Дата заезда.
            min_nights: Минимальное количество суток.

        Returns:
            Цена за одну ночь (int). 0 — если не удалось считать.
        """
        checkout = checkin + timedelta(days=min_nights)

        try:
            # Открываем датепикер (уже в правильной позиции)
            opened = await self._open_datepicker(skip_scroll=True)
            if not opened:
                return 0

            # Сбрасываем даты
            await self._reset_dates()
            await asyncio.sleep(0.3)

            # Кликаем дату заезда
            clicked_checkin = await self._click_day_in_datepicker(checkin)
            if not clicked_checkin:
                await self._close_datepicker()
                return 0

            await asyncio.sleep(0.3)

            # Кликаем дату выезда (checkin + min_nights)
            clicked_checkout = await self._click_day_in_datepicker(checkout)
            if not clicked_checkout:
                await self._close_datepicker()
                return 0

            # Ждём обновления цены
            await asyncio.sleep(1.0)

            # Считываем цену
            price_total = await self._read_price()

            if price_total > 0:
                price_per_night = round(price_total / min_nights)
                logger.debug(
                    "цена_с_минимумом_суток",
                    step=f"{checkin.isoformat()} → {checkout.isoformat()}",
                    total=price_per_night,
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

        При необходимости листает месяцы кнопкой «Далее».

        Args:
            target_date: Дата, которую нужно выбрать.

        Returns:
            True если клик выполнен успешно, False — если день не найден.
        """
        page = self._browser.page

        # Убедимся что нужный месяц виден, при необходимости листаем
        max_attempts = 6
        for _ in range(max_attempts):
            is_visible = await self._is_month_visible(target_date.year, target_date.month)
            if is_visible:
                break
            await self._click_next_month()
            await asyncio.sleep(0.5)
        else:
            logger.debug(
                "месяц_не_найден_в_датепикере",
                step=f"{target_date.year}-{target_date.month:02d}",
            )
            return False

        # Находим блок нужного месяца
        month_block = await self._find_month_block(target_date.year, target_date.month)
        if not month_block:
            return False

        # Находим ячейку нужного дня и кликаем
        day_cells = await month_block.query_selector_all("td.sc-base-datepicker-day")
        for cell in day_cells:
            span = await cell.query_selector("span")
            if not span:
                continue
            day_text = await span.inner_text()
            day_text = day_text.strip()
            if not day_text.isdigit():
                continue
            if int(day_text) == target_date.day:
                try:
                    await cell.click(timeout=3000)
                    return True
                except Exception:
                    # Fallback: JS-клик
                    await page.evaluate(
                        "(el) => el.click()",
                        cell,
                    )
                    return True

        logger.debug(
            "день_не_найден_в_календаре",
            step=f"{target_date.isoformat()}",
        )
        return False

    async def _read_price(self) -> int:
        """Считывает цену из элемента на странице карточки.

        Пробует несколько CSS-селекторов в порядке приоритета:
        1. .sc-detail-aside-price__cost — основной блок цены.
        2. .sc-detail-hotel-booking__price-sale — альтернативный блок (отели/гостиницы).

        Убирает неразрывные пробелы и символ валюты, извлекает число.

        Returns:
            Цена в рублях (int). 0 — если ни один элемент не найден или не распарсился.
        """
        page = self._browser.page

        for selector in _PRICE_SELECTORS:
            try:
                price_el = await page.wait_for_selector(
                    selector,
                    timeout=3000,
                )
                if not price_el:
                    continue

                price_text = await price_el.inner_text()

                # Убираем неразрывные пробелы, обычные пробелы, символ рубля и прочее
                cleaned = price_text.replace("\xa0", "").replace(" ", "")
                # Извлекаем только цифры
                digits = re.sub(r"[^\d]", "", cleaned)

                if not digits:
                    continue

                price = int(digits)
                if price > 0:
                    return price

            except Exception:
                continue

        logger.debug("цена_не_найдена_ни_в_одном_селекторе")
        return 0

    # ─────────────────────────────────────────────────────────────────────
    # Извлечение календаря занятости (существующий функционал)
    # ─────────────────────────────────────────────────────────────────────

    async def _extract_calendar(self) -> list[int]:
        """Извлекает календарь занятости на 60 дней из датепикера.

        Последовательность:
        1. Прокрутка к блоку дат и клик на «Заезд» для открытия датепикера.
        2. Нажатие «Сбросить даты».
        3. Считывание дней текущего и следующих месяцев.
        4. Листание месяцев кнопкой «Далее» при необходимости.

        Returns:
            Список из 60 элементов (0 — свободен, 1 — занят).
        """
        page = self._browser.page

        # Шаг 1: Прокручиваем к блоку дат и открываем датепикер
        opened = await self._open_datepicker()
        if not opened:
            return []

        # Шаг 2: Сбрасываем даты
        await self._reset_dates()

        # Шаг 3: Считываем календарь на 60 дней
        today = date.today()
        end_date = today + timedelta(days=59)
        calendar: list[int] = []

        # Определяем, какие месяцы нам нужны
        months_needed = self._get_months_range(today, end_date)

        for month_idx, (year, month) in enumerate(months_needed):
            # Листаем к нужному месяцу (первые два уже видны в датепикере)
            if month_idx >= 2:
                is_visible = await self._is_month_visible(year, month)
                if not is_visible:
                    await self._click_next_month()
                    await asyncio.sleep(1)

            # Считываем дни этого месяца
            month_days = await self._read_month_days(year, month)

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

        return calendar

    # ─────────────────────────────────────────────────────────────────────
    # Вспомогательные методы работы с датепикером
    # ─────────────────────────────────────────────────────────────────────

    async def _open_datepicker(self, *, skip_scroll: bool = False) -> bool:
        """Открывает датепикер кликом на блок «Заезд».

        При первом вызове прокручивает к элементу с полными паузами.
        При последующих (skip_scroll=True) — пропускает прокрутку
        и использует сокращённые паузы.

        Args:
            skip_scroll: Пропустить прокрутку к блоку дат (уже в позиции).

        Returns:
            True если датепикер открылся, False — если не удалось.
        """
        page = self._browser.page

        if not skip_scroll:
            # Прокручиваем к блоку дат
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
        except Exception:
            # Fallback: JavaScript-клик
            logger.debug("обычный_клик_не_сработал_пробуем_js")
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-detail-dates__item_in');
                    if (el) el.click();
                }
            """)

        # Сокращённая пауза при повторных вызовах
        await asyncio.sleep(0.5 if skip_scroll else 1.5)

        # Ждём появления датепикера
        try:
            await page.wait_for_selector(
                ".sc-base-datepicker-modal",
                timeout=5000,
            )
            return True
        except Exception:
            logger.warning("датепикер_не_открылся")
            return False

    async def _reset_dates(self) -> None:
        """Нажимает кнопку «Сбросить даты» в датепикере."""
        page = self._browser.page

        reset_button = await page.query_selector(".sc-base-datepicker__reset")
        if not reset_button:
            return

        try:
            await reset_button.click(timeout=3000)
        except Exception:
            # Fallback: JavaScript-клик
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-base-datepicker__reset');
                    if (el) el.click();
                }
            """)

        await asyncio.sleep(0.3)

    async def _close_datepicker(self) -> None:
        """Закрывает датепикер нажатием Escape или кликом вне его."""
        page = self._browser.page
        try:
            await page.keyboard.press("Escape")
            await asyncio.sleep(0.3)
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
            return

        # Проверяем что кнопка не скрыта (style="display: none;")
        is_hidden = await page.evaluate("""
            () => {
                const el = document.querySelector('.sc-base-datepicker-modal__next');
                if (!el) return true;
                const style = window.getComputedStyle(el);
                return style.display === 'none';
            }
        """)

        if is_hidden:
            return

        try:
            await next_btn.click(timeout=5000)
        except Exception:
            await page.evaluate("""
                () => {
                    const el = document.querySelector('.sc-base-datepicker-modal__next');
                    if (el) el.click();
                }
            """)

        await asyncio.sleep(0.5)

    async def _read_month_days(self, year: int, month: int) -> list[tuple[int, int]]:
        """Считывает статус всех дней указанного месяца из датепикера.

        Args:
            year: Год.
            month: Номер месяца (1-12).

        Returns:
            Список кортежей (номер_дня, статус), где статус: 0=свободен, 1=занят.
        """
        page = self._browser.page
        days: list[tuple[int, int]] = []

        # Находим нужный блок месяца по заголовку
        month_block = await self._find_month_block(year, month)
        if not month_block:
            return days

        # Находим все ячейки дней в этом месяце
        day_cells = await month_block.query_selector_all("td.sc-base-datepicker-day")

        for cell in day_cells:
            # Получаем номер дня
            span = await cell.query_selector("span")
            if not span:
                continue

            day_text = await span.inner_text()
            day_text = day_text.strip()
            if not day_text.isdigit():
                continue

            day_num = int(day_text)

            # Определяем статус: занят если есть класс _disabled
            class_attr = await cell.get_attribute("class") or ""
            is_occupied = 1 if "disabled" in class_attr else 0

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
            # Переходим к первому дню следующего месяца
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

        return months
