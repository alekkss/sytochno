"""Сервис парсинга каталога — обход страниц и извлечение данных объявлений."""

import re

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.services.browser_service import BrowserService

logger = get_logger("scraper")

# Базовый URL для формирования абсолютных ссылок
_BASE_URL = "https://sutochno.ru"


class ScraperService:
    """Сервис парсинга каталога sutochno.ru.

    Обходит страницы каталога, извлекает данные объявлений из карточек,
    обрабатывает пагинацию и возвращает список RawListing.
    """

    def __init__(self, settings: Settings, browser_service: BrowserService) -> None:
        """Инициализирует сервис парсинга.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
        """
        self._settings = settings
        self._browser = browser_service

    async def scrape_catalog(self) -> list[RawListing]:
        """Основной метод — обходит каталог и собирает все объявления.

        Обрабатывает пагинацию до MAX_PAGES или до последней страницы.

        Returns:
            Список объявлений со всех обработанных страниц.
        """
        all_listings: list[RawListing] = []
        current_page = 1
        max_pages = self._settings.max_pages or 999

        logger.info(
            "начало_парсинга_каталога",
            path=self._settings.sutochno_search_url,
        )

        # Переходим на первую страницу каталога
        await self._browser.navigate(self._settings.sutochno_search_url)

        # Ожидаем загрузку карточек
        await self._wait_for_cards()

        while current_page <= max_pages:
            logger.info(
                "парсинг_страницы",
                current=current_page,
                total=max_pages,
            )

            # Прокручиваем страницу для подгрузки контента
            await self._browser.scroll_page()
            await self._browser.random_delay()

            # Извлекаем объявления с текущей страницы
            page_listings = await self._parse_current_page()
            all_listings.extend(page_listings)

            logger.info(
                "страница_обработана",
                current=current_page,
                total=len(page_listings),
            )

            # Проверяем наличие следующей страницы
            if current_page >= max_pages:
                break

            has_next = await self._go_to_next_page()
            if not has_next:
                logger.info("последняя_страница_достигнута", current=current_page)
                break

            current_page += 1
            await self._wait_for_cards()

        logger.info(
            "парсинг_каталога_завершён",
            total=len(all_listings),
        )
        return all_listings

    async def _wait_for_cards(self) -> None:
        """Ожидает появления карточек объявлений на странице.

        Ждёт до 30 секунд появления хотя бы одной карточки.
        """
        page = self._browser.page
        try:
            await page.wait_for_selector(
                "[data-observe-id]",
                timeout=30000,
            )
        except Exception:
            logger.warning("карточки_не_найдены_на_странице")

    async def _parse_current_page(self) -> list[RawListing]:
        """Парсит все карточки объявлений на текущей странице.

        Returns:
            Список объявлений с текущей страницы.
        """
        page = self._browser.page
        listings: list[RawListing] = []

        # Находим все карточки по атрибуту data-observe-id
        cards = await page.query_selector_all(".card[data-observe-id]")

        if not cards:
            logger.warning("нет_карточек_на_странице")
            return listings

        for card in cards:
            try:
                listing = await self._parse_card(card, page)
                if listing is not None:
                    listings.append(listing)
            except Exception as e:
                logger.warning(
                    "ошибка_парсинга_карточки",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        return listings

    async def _parse_card(self, card: "any", page: Page) -> RawListing | None:  # type: ignore[name-defined]
        """Извлекает данные из одной карточки объявления.

        Args:
            card: Элемент карточки на странице.
            page: Страница Playwright.

        Returns:
            Объект RawListing или None, если не удалось извлечь обязательные данные.
        """
        # ID объявления
        external_id = await card.get_attribute("data-observe-id")
        if not external_id:
            return None

        # Название объявления
        title_el = await card.query_selector("h2.card-content__object-title")
        title = await title_el.inner_text() if title_el else None
        if not title:
            title_el = await card.query_selector(".card-content__object-title")
            title = await title_el.inner_text() if title_el else None
        if not title:
            return None

        # URL объявления
        link_el = await card.query_selector("a.card-content")
        href = await link_el.get_attribute("href") if link_el else None
        if not href:
            link_el = await card.query_selector("a.card__link")
            href = await link_el.get_attribute("href") if link_el else None
        if not href:
            return None

        url = href if href.startswith("http") else f"{_BASE_URL}{href}"

        # Цена за сутки
        price_per_night = await self._extract_price(card)

        # Рейтинг
        rating = await self._extract_rating(card)

        # Количество отзывов
        review_count = await self._extract_review_count(card)

        # Площадь
        area_m2 = await self._extract_area(card)

        # Количество гостей
        guests = await self._extract_guests(card)

        # Адрес
        address = await self._extract_address(card)

        # Метро
        metro_station = await self._extract_metro(card)

        # Быстрое бронирование
        has_instant_booking = await self._extract_instant_booking(card)

        return RawListing(
            external_id=external_id,
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

    async def _extract_price(self, card: "any") -> int | None:  # type: ignore[name-defined]
        """Извлекает цену за сутки из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Цена в рублях или None.
        """
        price_el = await card.query_selector(".price-total__number")
        if not price_el:
            return None

        price_text = await price_el.inner_text()
        # Убираем неразрывные пробелы, символ рубля и прочие нецифровые символы
        digits = re.sub(r"[^\d]", "", price_text)
        return int(digits) if digits else None

    async def _extract_rating(self, card: "any") -> float | None:  # type: ignore[name-defined]
        """Извлекает рейтинг объекта из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Рейтинг как float или None.
        """
        # Пробуем найти рейтинг в блоке отзывов (текстовое значение)
        rating_el = await card.query_selector(".rating-list__rating")
        if rating_el:
            rating_text = await rating_el.inner_text()
            # Формат: "9,1" — меняем запятую на точку
            rating_text = rating_text.replace(",", ".").strip()
            try:
                return float(rating_text)
            except ValueError:
                pass

        # Пробуем атрибут content у .rating-list
        rating_list_el = await card.query_selector(".rating-list[content]")
        if rating_list_el:
            content = await rating_list_el.get_attribute("content")
            if content:
                content = content.replace(",", ".").strip()
                try:
                    return float(content)
                except ValueError:
                    pass

        # Пробуем атрибут data-rating
        rating_data_el = await card.query_selector("[data-rating]")
        if rating_data_el:
            data_rating = await rating_data_el.get_attribute("data-rating")
            if data_rating:
                try:
                    return float(data_rating)
                except ValueError:
                    pass

        return None

    async def _extract_review_count(self, card: "any") -> int | None:  # type: ignore[name-defined]
        """Извлекает количество отзывов из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Количество отзывов или None.
        """
        # В блоке контента: "217 отзывов"
        review_el = await card.query_selector(".card-content .rating-list__count")
        if review_el:
            text = await review_el.inner_text()
            digits = re.sub(r"[^\d]", "", text)
            return int(digits) if digits else None

        # В карусели: просто число "217"
        review_carousel_el = await card.query_selector(
            ".carousel__owner-options .rating-list__count"
        )
        if review_carousel_el:
            text = await review_carousel_el.inner_text()
            digits = re.sub(r"[^\d]", "", text)
            return int(digits) if digits else None

        return None

    async def _extract_area(self, card: "any") -> int | None:  # type: ignore[name-defined]
        """Извлекает площадь объекта из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Площадь в м² или None.
        """
        # Из блока характеристик: "10 м²"
        facilities = await card.query_selector_all(".card-content__facility")
        for facility in facilities:
            text = await facility.inner_text()
            match = re.search(r"(\d+)\s*м", text)
            if match:
                return int(match.group(1))

        # Из блока карусели
        size_el = await card.query_selector(".carousel__size")
        if size_el:
            text = await size_el.inner_text()
            match = re.search(r"(\d+)", text)
            if match:
                return int(match.group(1))

        return None

    async def _extract_guests(self, card: "any") -> int | None:  # type: ignore[name-defined]
        """Извлекает количество гостей из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Количество гостей или None.
        """
        facilities = await card.query_selector_all(".card-content__facility")
        for facility in facilities:
            text = await facility.inner_text()
            match = re.search(r"(\d+)\s*гост", text)
            if match:
                return int(match.group(1))
        return None

    async def _extract_address(self, card: "any") -> str | None:  # type: ignore[name-defined]
        """Извлекает адрес объекта из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Строка адреса или None.
        """
        # Адрес находится в элементе с иконкой icon-app-point
        properties = await card.query_selector_all(".card-content__property")
        for prop in properties:
            icon = await prop.query_selector(".icon-app-point")
            if icon:
                text_el = await prop.query_selector(".card-content__property-text")
                if text_el:
                    return (await text_el.inner_text()).strip()
        return None

    async def _extract_metro(self, card: "any") -> str | None:  # type: ignore[name-defined]
        """Извлекает ближайшую станцию метро из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Станция метро с расстоянием или None.
        """
        # Метро находится в элементе с иконкой icon-app-navigator
        properties = await card.query_selector_all(".card-content__property")
        for prop in properties:
            icon = await prop.query_selector(".icon-app-navigator")
            if icon:
                text_el = await prop.query_selector(".card-content__property-text")
                if text_el:
                    return (await text_el.inner_text()).strip()
        return None

    async def _extract_instant_booking(self, card: "any") -> bool:  # type: ignore[name-defined]
        """Проверяет наличие быстрого бронирования.

        Args:
            card: Элемент карточки.

        Returns:
            True если есть быстрое бронирование.
        """
        lightning_el = await card.query_selector(".icon-app-lightning-2")
        return lightning_el is not None

    async def _go_to_next_page(self) -> bool:
        """Переходит на следующую страницу каталога.

        Ищет кнопку «Следующая» или ссылку пагинации и кликает по ней.

        Returns:
            True если переход выполнен, False если следующей страницы нет.
        """
        page = self._browser.page

        # Ищем кнопку "Следующая" или стрелку вправо в пагинации
        next_button = await page.query_selector(
            "button.pagination__arrow--right:not([disabled])"
        )
        if next_button:
            await next_button.click()
            await self._browser.random_delay()
            return True

        # Альтернативный селектор для пагинации
        next_link = await page.query_selector(
            "a.pagination__arrow--right"
        )
        if next_link:
            await next_link.click()
            await self._browser.random_delay()
            return True

        # Пробуем найти кнопку "Показать ещё" (если каталог подгружается кнопкой)
        show_more = await page.query_selector(
            "button:has-text('Показать ещё')"
        )
        if show_more:
            await show_more.click()
            await self._browser.random_delay()
            return True

        return False
