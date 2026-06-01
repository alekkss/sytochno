"""Сервис парсинга каталога — обход страниц и извлечение данных объявлений."""

import asyncio
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

    Обходит несколько URL поиска последовательно, извлекает данные объявлений
    из карточек, обрабатывает пагинацию и возвращает список уникальных RawListing.
    Дедупликация выполняется по external_id в процессе сбора.
    MAX_PAGES применяется суммарно ко всем ссылкам.
    """

    def __init__(self, settings: Settings, browser_service: BrowserService) -> None:
        """Инициализирует сервис парсинга.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером.
        """
        self._settings = settings
        self._browser = browser_service
        self._seen_ids: set[str] = set()
        self._duplicates_count: int = 0

    async def scrape_catalog(self) -> list[RawListing]:
        """Основной метод — обходит все URL поиска и собирает уникальные объявления.

        Последовательно обрабатывает каждый URL из settings.search_urls.
        Общий счётчик страниц применяется суммарно ко всем ссылкам:
        если MAX_PAGES=20 и на первой ссылке пройдено 20 страниц —
        парсер останавливается и не переходит к следующим ссылкам.

        Returns:
            Список уникальных объявлений со всех обработанных страниц и ссылок.
        """
        all_listings: list[RawListing] = []
        self._seen_ids.clear()
        self._duplicates_count = 0

        max_pages = self._settings.max_pages or 999
        total_pages_processed = 0

        logger.info(
            "начало_парсинга_каталога",
            urls_count=len(self._settings.search_urls),
            max_pages=max_pages,
        )

        for url_index, search_url in enumerate(self._settings.search_urls, start=1):
            # Проверяем, не исчерпан ли лимит страниц
            if total_pages_processed >= max_pages:
                logger.info(
                    "лимит_страниц_достигнут_пропуск_ссылки",
                    url_index=url_index,
                    total_pages_processed=total_pages_processed,
                    max_pages=max_pages,
                )
                break

            remaining_pages = max_pages - total_pages_processed

            logger.info(
                "начало_обхода_ссылки",
                url_index=url_index,
                urls_total=len(self._settings.search_urls),
                remaining_pages=remaining_pages,
                url=search_url[:80] + "..." if len(search_url) > 80 else search_url,
            )

            pages_from_url = await self._scrape_single_url(
                search_url=search_url,
                url_index=url_index,
                remaining_pages=remaining_pages,
                all_listings=all_listings,
            )

            total_pages_processed += pages_from_url

            logger.info(
                "ссылка_обработана",
                url_index=url_index,
                pages_from_url=pages_from_url,
                total_pages_processed=total_pages_processed,
                listings_so_far=len(all_listings),
            )

        logger.info(
            "парсинг_каталога_завершён",
            total=len(all_listings),
            total_pages=total_pages_processed,
            urls_processed=min(len(self._settings.search_urls), url_index if 'url_index' in dir() else 0),
        )

        if self._duplicates_count > 0:
            logger.info(
                "дубликаты_отброшены",
                total=self._duplicates_count,
            )

        return all_listings

    async def _scrape_single_url(
        self,
        search_url: str,
        url_index: int,
        remaining_pages: int,
        all_listings: list[RawListing],
    ) -> int:
        """Обходит один URL поиска с пагинацией.

        Args:
            search_url: URL страницы поиска.
            url_index: Порядковый номер ссылки (для логов).
            remaining_pages: Сколько страниц ещё можно обработать (общий лимит).
            all_listings: Общий список объявлений (мутируется — добавляются новые).

        Returns:
            Количество обработанных страниц для этой ссылки.
        """
        # Переходим на первую страницу каталога
        await self._browser.navigate(search_url)

        # Ожидаем загрузку карточек на первой странице
        cards_found = await self._wait_for_cards()
        if not cards_found:
            logger.warning(
                "страница_не_загрузилась",
                url_index=url_index,
                url=search_url[:80] + "..." if len(search_url) > 80 else search_url,
            )
            return 0

        pages_processed = 0

        while pages_processed < remaining_pages:
            current_page = pages_processed + 1

            logger.info(
                "парсинг_страницы",
                url_index=url_index,
                page=current_page,
                remaining=remaining_pages - pages_processed,
            )

            # Прокручиваем страницу для подгрузки контента
            await self._browser.scroll_page()
            await self._browser.random_delay()

            # Извлекаем объявления с текущей страницы
            page_listings = await self._parse_current_page()
            all_listings.extend(page_listings)

            pages_processed += 1

            logger.info(
                "страница_обработана",
                url_index=url_index,
                page=current_page,
                found=len(page_listings),
                total_so_far=len(all_listings),
            )

            # Проверяем, нужна ли следующая страница
            if pages_processed >= remaining_pages:
                break

            has_next = await self._go_to_next_page()
            if not has_next:
                logger.info(
                    "последняя_страница_достигнута",
                    url_index=url_index,
                    page=current_page,
                )
                break

        return pages_processed

    async def _wait_for_cards(self) -> bool:
        """Ожидает появления карточек объявлений на странице.

        Ждёт до 30 секунд появления хотя бы одной карточки.

        Returns:
            True если карточки появились, False если таймаут или ошибка.
        """
        page = self._browser.page
        try:
            await page.wait_for_selector(
                ".card[data-observe-id]",
                timeout=30000,
            )
            return True
        except Exception as e:
            logger.warning(
                "карточки_не_найдены_на_странице",
                error=str(e)[:200],
                error_type=type(e).__name__,
                path=page.url,
            )
            return False

    async def _parse_current_page(self) -> list[RawListing]:
        """Парсит все карточки объявлений на текущей странице.

        Пропускает карточки с уже встречавшимся external_id (дедупликация).

        Returns:
            Список уникальных объявлений с текущей страницы.
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
                # Предварительная проверка ID до полного парсинга (экономия времени)
                external_id = await card.get_attribute("data-observe-id")
                if not external_id:
                    continue

                if external_id in self._seen_ids:
                    self._duplicates_count += 1
                    continue

                listing = await self._parse_card(card, page)
                if listing is not None:
                    self._seen_ids.add(listing.external_id)
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

        Находит кнопку «Далее» среди всех li.navigation (их может быть две:
        «Назад» и «Далее»). Определяет нужную по тексту внутри
        span.pagination-arrow__text. После клика ожидает обновления контента.

        Returns:
            True если переход выполнен, False если кнопки «Далее» нет.
        """
        page = self._browser.page

        # Прокручиваем к пагинации
        await page.evaluate("""
            () => {
                const pagination = document.querySelector('.pagination-wrapper');
                if (pagination) pagination.scrollIntoView({behavior: 'smooth', block: 'center'});
            }
        """)
        await asyncio.sleep(1)

        # Ищем все li.navigation и находим тот, который содержит текст «Далее»
        next_link = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.navigation');
                for (const item of items) {
                    const text = item.querySelector('.pagination-arrow__text');
                    if (text && text.textContent.trim() === 'Далее') {
                        return true;
                    }
                }
                return false;
            }
        """)

        if not next_link:
            logger.debug("кнопка_далее_не_найдена")
            return False

        # Кликаем именно по кнопке «Далее» через JavaScript
        clicked = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.navigation');
                for (const item of items) {
                    const text = item.querySelector('.pagination-arrow__text');
                    if (text && text.textContent.trim() === 'Далее') {
                        const link = item.querySelector('a');
                        if (link) {
                            link.click();
                            return true;
                        }
                    }
                }
                return false;
            }
        """)

        if not clicked:
            logger.debug("клик_далее_не_выполнен")
            return False

        logger.debug("клик_далее_выполнен")

        # Ждём завершения сетевой активности (Vue делает XHR-запрос за новыми данными)
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass

        # Дополнительная пауза для рендеринга Vue-компонентов
        await asyncio.sleep(3)

        # Прокручиваем наверх
        await page.evaluate("window.scrollTo(0, 0)")
        await self._browser.random_delay()

        # Проверяем, что карточки есть на странице
        cards = await page.query_selector_all(".card[data-observe-id]")
        if not cards:
            logger.warning("карточки_не_загрузились_после_пагинации")
            return False

        logger.info("переход_на_следующую_страницу_выполнен")
        return True
