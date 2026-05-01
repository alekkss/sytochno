"""Сервис парсинга карточки объявления — извлечение календаря занятости."""

import asyncio
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


class ListingService:
    """Сервис парсинга карточки объявления на sutochno.ru.

    Заходит в каждое объявление, открывает календарь и считывает
    занятость на 60 дней (0 — свободен, 1 — занят).
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
        """Обогащает объявление данными календаря занятости.

        Переходит на страницу объявления, открывает датепикер,
        сбрасывает даты и считывает занятость на 60 дней.

        Args:
            listing: Объявление с базовыми данными из каталога.

        Returns:
            Объявление с заполненным calendar_60_days.
        """
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
                "карточка_обработана",
                step=f"id={listing.external_id}",
                total=len(calendar),
            )

        except Exception as e:
            logger.warning(
                "ошибка_парсинга_карточки",
                error=str(e),
                error_type=type(e).__name__,
                step=f"id={listing.external_id}",
            )

        return listing

    async def enrich_listings(self, listings: list[RawListing]) -> list[RawListing]:
        """Обогащает список объявлений данными календаря.

        Последовательно обрабатывает каждое объявление.

        Args:
            listings: Список объявлений из каталога.

        Returns:
            Список объявлений с заполненными calendar_60_days.
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

    async def _extract_calendar(self) -> list[int]:
        """Извлекает календарь занятости на 60 дней из датепикера.

        Последовательность:
        1. Клик на блок «Заезд» для открытия датепикера.
        2. Нажатие «Сбросить даты».
        3. Считывание дней текущего и следующих месяцев.
        4. Листание месяцев кнопкой «Далее» при необходимости.

        Returns:
            Список из 60 элементов (0 — свободен, 1 — занят).
        """
        page = self._browser.page

        # Шаг 1: Открываем датепикер кликом на «Заезд»
        checkin_block = await page.query_selector(".sc-detail-dates__item_in")
        if not checkin_block:
            logger.warning("блок_заезда_не_найден")
            return []

        await checkin_block.click()
        await asyncio.sleep(1.5)

        # Шаг 2: Ждём появления датепикера
        try:
            await page.wait_for_selector(
                ".sc-base-datepicker-modal",
                timeout=10000,
            )
        except Exception:
            logger.warning("датепикер_не_открылся")
            return []

        # Шаг 3: Сбрасываем даты
        reset_button = await page.query_selector(".sc-base-datepicker__reset")
        if reset_button:
            await reset_button.click()
            await asyncio.sleep(1)

        # Шаг 4: Считываем календарь на 60 дней
        today = date.today()
        end_date = today + timedelta(days=59)
        calendar: list[int] = []

        # Определяем, какие месяцы нам нужны
        months_needed = self._get_months_range(today, end_date)

        for month_idx, (year, month) in enumerate(months_needed):
            # Листаем к нужному месяцу (первый уже виден)
            if month_idx > 0:
                # Проверяем, виден ли нужный месяц
                is_visible = await self._is_month_visible(year, month)
                if not is_visible:
                    await self._click_next_month()
                    await asyncio.sleep(0.8)

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

        # Закрываем датепикер (клик вне его)
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.5)

        return calendar

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
        if next_btn:
            await next_btn.click()
            await asyncio.sleep(0.8)

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
