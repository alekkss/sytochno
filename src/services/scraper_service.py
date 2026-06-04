"""Сервис парсинга каталога — обход страниц и извлечение данных объявлений."""

import asyncio
import re

from playwright.async_api import Page

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import BrowserService

logger = get_logger("scraper")

# Базовый URL для формирования абсолютных ссылок
_BASE_URL = "https://sutochno.ru"


class _BidirectionalState:
    """Общее состояние для синхронизации двух браузеров при двунаправленном обходе.

    Хранит множество обработанных страниц и сигнал остановки.
    Оба браузера проверяют состояние после обработки каждой страницы:
    если номера встретились (forward_page >= backward_page) — оба останавливаются.
    """

    def __init__(self, total_pages: int) -> None:
        """Инициализирует состояние.

        Args:
            total_pages: Общее количество страниц в пагинации.
        """
        self.total_pages: int = total_pages
        self.forward_page: int = 0
        self.backward_page: int = total_pages + 1
        self.stop_event: asyncio.Event = asyncio.Event()
        self.lock: asyncio.Lock = asyncio.Lock()

    async def report_forward(self, page_num: int) -> bool:
        """Сообщает о завершении обработки страницы прямым браузером.

        Args:
            page_num: Номер обработанной страницы.

        Returns:
            True если нужно продолжать, False если пора остановиться.
        """
        async with self.lock:
            self.forward_page = page_num
            if self.forward_page >= self.backward_page - 1:
                self.stop_event.set()
                return False
            return not self.stop_event.is_set()

    async def report_backward(self, page_num: int) -> bool:
        """Сообщает о завершении обработки страницы обратным браузером.

        Args:
            page_num: Номер обработанной страницы.

        Returns:
            True если нужно продолжать, False если пора остановиться.
        """
        async with self.lock:
            self.backward_page = page_num
            if self.forward_page >= self.backward_page - 1:
                self.stop_event.set()
                return False
            return not self.stop_event.is_set()

    @property
    def should_stop(self) -> bool:
        """Проверяет, установлен ли сигнал остановки."""
        return self.stop_event.is_set()


class ScraperService:
    """Сервис парсинга каталога sutochno.ru.

    Обходит несколько URL поиска, для каждого запуская отдельный браузер.
    Извлекает данные объявлений из карточек, обрабатывает пагинацию
    и возвращает список уникальных RawListing.
    Дедупликация выполняется по external_id в процессе сбора.
    MAX_PAGES применяется суммарно ко всем ссылкам.

    Поддерживает двунаправленный обход пагинации: два браузера идут
    навстречу друг другу (первый — вперёд, второй — назад с последней
    страницы), что сокращает время обхода каталога вдвое.
    """

    def __init__(
        self,
        settings: Settings,
        browser_service: BrowserService,
        proxy: ProxyConfig | None = None,
    ) -> None:
        """Инициализирует сервис парсинга.

        Args:
            settings: Настройки приложения.
            browser_service: Сервис управления браузером (для прямого обхода).
            proxy: Прокси для второго (обратного) браузера. Если None —
                   второй браузер запускается без прокси.
        """
        self._settings = settings
        self._browser = browser_service
        self._proxy = proxy
        self._seen_ids: set[str] = set()
        self._duplicates_count: int = 0
        self._lock: asyncio.Lock = asyncio.Lock()

    async def scrape_catalog(self) -> list[RawListing]:
        """Основной метод — обходит все URL поиска и собирает уникальные объявления.

        Для каждой ссылки:
        1. Запускает прямой браузер на первую страницу.
        2. Определяет общее количество страниц из пагинации.
        3. Если страниц > 1 — запускает второй (обратный) браузер на последнюю страницу.
        4. Оба браузера работают параллельно навстречу друг другу.
        5. Когда встречаются — оба останавливаются.
        6. Закрывает оба браузера.

        Returns:
            Список уникальных объявлений со всех обработанных страниц и ссылок.
        """
        all_listings: list[RawListing] = []
        self._seen_ids.clear()
        self._duplicates_count = 0

        max_pages = self._settings.max_pages or 999
        total_pages_processed = 0
        urls_processed = 0

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

            pages_from_url = await self._scrape_single_url_bidirectional(
                search_url=search_url,
                url_index=url_index,
                remaining_pages=remaining_pages,
                all_listings=all_listings,
            )

            total_pages_processed += pages_from_url
            urls_processed += 1

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
            urls_processed=urls_processed,
        )

        if self._duplicates_count > 0:
            logger.info(
                "дубликаты_отброшены",
                total=self._duplicates_count,
            )

        return all_listings

    async def _scrape_single_url_bidirectional(
        self,
        search_url: str,
        url_index: int,
        remaining_pages: int,
        all_listings: list[RawListing],
    ) -> int:
        """Обходит один URL поиска двумя браузерами навстречу друг другу.

        Алгоритм:
        1. Запускает прямой браузер, переходит на первую страницу.
        2. Определяет общее количество страниц из пагинации.
        3. Если страниц > 1 — запускает обратный браузер на последнюю страницу.
        4. Оба работают параллельно через asyncio.gather.
        5. Синхронизация через _BidirectionalState.

        Args:
            search_url: URL страницы поиска.
            url_index: Порядковый номер ссылки (для логов).
            remaining_pages: Сколько страниц ещё можно обработать.
            all_listings: Общий список объявлений (мутируется).

        Returns:
            Количество обработанных страниц для этой ссылки.
        """
        # --- Запускаем прямой браузер ---
        await self._browser.start()

        try:
            # Переходим на первую страницу каталога
            await self._browser.navigate(search_url)

            # Ожидаем загрузку карточек
            cards_found = await self._wait_for_cards(self._browser.page)
            if not cards_found:
                logger.warning(
                    "страница_не_загрузилась",
                    url_index=url_index,
                    url=search_url[:80] + "..." if len(search_url) > 80 else search_url,
                )
                return 0

            # Определяем общее количество страниц из пагинации
            total_pages = await self._get_total_pages(self._browser.page)

            logger.info(
                "пагинация_определена",
                url_index=url_index,
                total_pages=total_pages,
            )

            # Ограничиваем общее количество страниц лимитом
            effective_pages = min(total_pages, remaining_pages)

            # Если всего одна страница — обрабатываем просто
            if total_pages == 1:
                await self._browser.scroll_page()
                await self._browser.random_delay()
                page_listings = await self._parse_current_page(self._browser.page)
                async with self._lock:
                    all_listings.extend(page_listings)
                logger.info(
                    "страница_обработана",
                    url_index=url_index,
                    page=1,
                    found=len(page_listings),
                )
                return 1

            # --- Двунаправленный обход ---
            state = _BidirectionalState(total_pages=effective_pages)

            # Запускаем обратный браузер
            backward_browser = BrowserService(settings=self._settings)
            await backward_browser.start(proxy=self._proxy)

            try:
                # Переходим обратным браузером на первую страницу
                await backward_browser.navigate(search_url)
                backward_cards_found = await self._wait_for_cards(backward_browser.page)

                if not backward_cards_found:
                    logger.warning(
                        "обратный_браузер_не_загрузился",
                        url_index=url_index,
                    )
                    # Fallback: обходим только прямым браузером
                    return await self._scrape_forward_only(
                        url_index=url_index,
                        remaining_pages=remaining_pages,
                        all_listings=all_listings,
                    )

                # Переходим на последнюю страницу
                last_page_reached = await self._go_to_page_number(
                    backward_browser.page, effective_pages
                )

                if not last_page_reached:
                    logger.warning(
                        "не_удалось_перейти_на_последнюю_страницу",
                        url_index=url_index,
                        target_page=effective_pages,
                    )
                    # Fallback: обходим только прямым браузером
                    return await self._scrape_forward_only(
                        url_index=url_index,
                        remaining_pages=remaining_pages,
                        all_listings=all_listings,
                    )

                logger.info(
                    "двунаправленный_обход_запущен",
                    url_index=url_index,
                    total_pages=effective_pages,
                    step="прямой=1, обратный=" + str(effective_pages),
                )

                # Запускаем оба обхода параллельно
                forward_task = asyncio.create_task(
                    self._run_forward(
                        browser=self._browser,
                        state=state,
                        url_index=url_index,
                        all_listings=all_listings,
                    )
                )
                backward_task = asyncio.create_task(
                    self._run_backward(
                        browser=backward_browser,
                        state=state,
                        url_index=url_index,
                        all_listings=all_listings,
                        start_page=effective_pages,
                    )
                )

                results = await asyncio.gather(
                    forward_task, backward_task, return_exceptions=True
                )

                # Подсчитываем общее количество обработанных страниц
                total_processed = 0
                for result in results:
                    if isinstance(result, Exception):
                        logger.warning(
                            "ошибка_в_браузере_обхода",
                            error=str(result),
                            error_type=type(result).__name__,
                            url_index=url_index,
                        )
                    elif isinstance(result, int):
                        total_processed += result

                logger.info(
                    "двунаправленный_обход_завершён",
                    url_index=url_index,
                    total_processed=total_processed,
                    listings_collected=len(all_listings),
                )

                return total_processed

            finally:
                await backward_browser.stop()
                logger.info(
                    "обратный_браузер_закрыт",
                    url_index=url_index,
                )
        finally:
            await self._browser.stop()
            logger.info(
                "браузер_закрыт_после_ссылки",
                url_index=url_index,
            )

    async def _run_forward(
        self,
        browser: BrowserService,
        state: _BidirectionalState,
        url_index: int,
        all_listings: list[RawListing],
    ) -> int:
        """Прямой обход — движется вперёд с первой страницы.

        Args:
            browser: Браузер для прямого обхода.
            state: Общее состояние синхронизации.
            url_index: Номер ссылки для логов.
            all_listings: Общий список объявлений.

        Returns:
            Количество обработанных страниц.
        """
        pages_processed = 0
        current_page = 1

        while not state.should_stop:
            logger.info(
                "прямой_парсинг_страницы",
                url_index=url_index,
                page=current_page,
            )

            # Прокручиваем и парсим
            await browser.scroll_page()
            await browser.random_delay()

            page_listings = await self._parse_current_page(browser.page)
            async with self._lock:
                all_listings.extend(page_listings)

            pages_processed += 1

            logger.info(
                "прямой_страница_обработана",
                url_index=url_index,
                page=current_page,
                found=len(page_listings),
            )

            # Сообщаем о прогрессе и проверяем, нужно ли продолжать
            should_continue = await state.report_forward(current_page)
            if not should_continue:
                logger.info(
                    "прямой_браузер_остановлен_встреча",
                    url_index=url_index,
                    page=current_page,
                )
                break

            # Переходим на следующую страницу
            has_next = await self._go_to_next_page(browser.page)
            if not has_next:
                logger.info(
                    "прямой_последняя_страница",
                    url_index=url_index,
                    page=current_page,
                )
                break

            current_page += 1

        return pages_processed

    async def _run_backward(
        self,
        browser: BrowserService,
        state: _BidirectionalState,
        url_index: int,
        all_listings: list[RawListing],
        start_page: int,
    ) -> int:
        """Обратный обход — движется назад с последней страницы.

        Args:
            browser: Браузер для обратного обхода.
            state: Общее состояние синхронизации.
            url_index: Номер ссылки для логов.
            all_listings: Общий список объявлений.
            start_page: Номер последней страницы (откуда начинать).

        Returns:
            Количество обработанных страниц.
        """
        pages_processed = 0
        current_page = start_page

        while not state.should_stop:
            logger.info(
                "обратный_парсинг_страницы",
                url_index=url_index,
                page=current_page,
            )

            # Прокручиваем и парсим
            await browser.scroll_page()
            await browser.random_delay()

            page_listings = await self._parse_current_page(browser.page)
            async with self._lock:
                all_listings.extend(page_listings)

            pages_processed += 1

            logger.info(
                "обратный_страница_обработана",
                url_index=url_index,
                page=current_page,
                found=len(page_listings),
            )

            # Сообщаем о прогрессе и проверяем, нужно ли продолжать
            should_continue = await state.report_backward(current_page)
            if not should_continue:
                logger.info(
                    "обратный_браузер_остановлен_встреча",
                    url_index=url_index,
                    page=current_page,
                )
                break

            # Переходим на предыдущую страницу
            has_prev = await self._go_to_prev_page(browser.page)
            if not has_prev:
                logger.info(
                    "обратный_первая_страница_достигнута",
                    url_index=url_index,
                    page=current_page,
                )
                break

            current_page -= 1

        return pages_processed

    async def _scrape_forward_only(
        self,
        url_index: int,
        remaining_pages: int,
        all_listings: list[RawListing],
    ) -> int:
        """Обходит каталог только прямым браузером (fallback).

        Используется когда обратный браузер не удалось запустить.

        Args:
            url_index: Номер ссылки для логов.
            remaining_pages: Лимит страниц.
            all_listings: Общий список объявлений.

        Returns:
            Количество обработанных страниц.
        """
        pages_processed = 0

        while pages_processed < remaining_pages:
            current_page = pages_processed + 1

            logger.info(
                "парсинг_страницы",
                url_index=url_index,
                page=current_page,
                remaining=remaining_pages - pages_processed,
            )

            await self._browser.scroll_page()
            await self._browser.random_delay()

            page_listings = await self._parse_current_page(self._browser.page)
            async with self._lock:
                all_listings.extend(page_listings)

            pages_processed += 1

            logger.info(
                "страница_обработана",
                url_index=url_index,
                page=current_page,
                found=len(page_listings),
                total_so_far=len(all_listings),
            )

            if pages_processed >= remaining_pages:
                break

            has_next = await self._go_to_next_page(self._browser.page)
            if not has_next:
                logger.info(
                    "последняя_страница_достигнута",
                    url_index=url_index,
                    page=current_page,
                )
                break

        return pages_processed

    async def _get_total_pages(self, page: Page) -> int:
        """Определяет общее количество страниц из пагинации.

        Парсит элементы пагинации и находит максимальный номер страницы.
        Ищет последний li.page-item с числовым значением перед кнопкой «Далее».

        Args:
            page: Страница Playwright.

        Returns:
            Общее количество страниц (минимум 1).
        """
        total = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('.pagination li.page-item a');
                let maxPage = 1;
                for (const item of items) {
                    const text = item.textContent.trim();
                    const num = parseInt(text, 10);
                    if (!isNaN(num) && num > maxPage) {
                        maxPage = num;
                    }
                }
                return maxPage;
            }
        """)

        return max(1, total)

    async def _go_to_page_number(self, page: Page, target_page: int) -> bool:
        """Переходит на указанный номер страницы через клик по элементу пагинации.

        Если номер страницы виден в пагинации — кликает по нему.
        Если нет (скрыт за «...») — кликает по последнему видимому номеру,
        затем повторяет попытку найти нужный номер.

        Args:
            page: Страница Playwright.
            target_page: Номер целевой страницы.

        Returns:
            True если удалось перейти на целевую страницу.
        """
        max_attempts = 10

        for attempt in range(max_attempts):
            # Пробуем кликнуть по номеру целевой страницы
            clicked = await page.evaluate("""
                (targetPage) => {
                    const items = document.querySelectorAll('.pagination li.page-item a');
                    for (const item of items) {
                        const text = item.textContent.trim();
                        const num = parseInt(text, 10);
                        if (num === targetPage) {
                            item.click();
                            return true;
                        }
                    }
                    return false;
                }
            """, target_page)

            if clicked:
                # Ждём загрузку страницы
                try:
                    await page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:
                    pass
                await asyncio.sleep(3)

                # Проверяем что карточки загрузились
                cards_found = await self._wait_for_cards(page)
                if cards_found:
                    # Проверяем, что текущая страница — целевая
                    current = await self._get_active_page_number(page)
                    if current == target_page:
                        logger.info(
                            "переход_на_страницу_выполнен",
                            target_page=target_page,
                        )
                        return True

                logger.debug(
                    "попытка_перехода_на_страницу",
                    attempt=attempt + 1,
                    target_page=target_page,
                )
                continue

            # Номер не виден — кликаем по максимальному видимому номеру,
            # чтобы сдвинуть пагинацию ближе к целевой странице
            shifted = await page.evaluate("""
                (targetPage) => {
                    const items = document.querySelectorAll('.pagination li.page-item a');
                    let maxVisible = 0;
                    let maxElement = null;
                    for (const item of items) {
                        const text = item.textContent.trim();
                        const num = parseInt(text, 10);
                        if (!isNaN(num) && num > maxVisible && num < targetPage) {
                            maxVisible = num;
                            maxElement = item;
                        }
                    }
                    if (maxElement) {
                        maxElement.click();
                        return maxVisible;
                    }
                    return 0;
                }
            """, target_page)

            if not shifted:
                logger.warning(
                    "не_удалось_сдвинуть_пагинацию",
                    target_page=target_page,
                    attempt=attempt + 1,
                )
                return False

            # Ждём загрузку после сдвига
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            await asyncio.sleep(3)

            logger.debug(
                "пагинация_сдвинута",
                shifted_to=shifted,
                target_page=target_page,
                attempt=attempt + 1,
            )

        logger.warning(
            "не_удалось_перейти_на_страницу_лимит_попыток",
            target_page=target_page,
            max_attempts=max_attempts,
        )
        return False

    async def _get_active_page_number(self, page: Page) -> int:
        """Определяет номер текущей активной страницы в пагинации.

        Args:
            page: Страница Playwright.

        Returns:
            Номер активной страницы или 0, если не определён.
        """
        result = await page.evaluate("""
            () => {
                const active = document.querySelector('.pagination li.page-item.active a');
                if (active) {
                    const num = parseInt(active.textContent.trim(), 10);
                    return isNaN(num) ? 0 : num;
                }
                return 0;
            }
        """)
        return result

    async def _wait_for_cards(self, page: Page) -> bool:
        """Ожидает появления карточек объявлений на странице.

        Args:
            page: Страница Playwright.

        Returns:
            True если карточки появились, False если таймаут.
        """
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

    async def _parse_current_page(self, page: Page) -> list[RawListing]:
        """Парсит все карточки объявлений на текущей странице.

        Потокобезопасная дедупликация через asyncio.Lock.

        Args:
            page: Страница Playwright.

        Returns:
            Список уникальных объявлений с текущей страницы.
        """
        listings: list[RawListing] = []

        # Находим все карточки по атрибуту data-observe-id
        cards = await page.query_selector_all(".card[data-observe-id]")

        if not cards:
            logger.warning("нет_карточек_на_странице")
            return listings

        for card in cards:
            try:
                # Предварительная проверка ID до полного парсинга
                external_id = await card.get_attribute("data-observe-id")
                if not external_id:
                    continue

                async with self._lock:
                    if external_id in self._seen_ids:
                        self._duplicates_count += 1
                        continue
                    # Резервируем ID сразу, чтобы второй браузер не взял его
                    self._seen_ids.add(external_id)

                listing = await self._parse_card(card, page)
                if listing is not None:
                    listings.append(listing)
                else:
                    # Если парсинг не удался — убираем из seen
                    async with self._lock:
                        self._seen_ids.discard(external_id)
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
        digits = re.sub(r"[^\d]", "", price_text)
        return int(digits) if digits else None

    async def _extract_rating(self, card: "any") -> float | None:  # type: ignore[name-defined]
        """Извлекает рейтинг объекта из карточки.

        Args:
            card: Элемент карточки.

        Returns:
            Рейтинг как float или None.
        """
        rating_el = await card.query_selector(".rating-list__rating")
        if rating_el:
            rating_text = await rating_el.inner_text()
            rating_text = rating_text.replace(",", ".").strip()
            try:
                return float(rating_text)
            except ValueError:
                pass

        rating_list_el = await card.query_selector(".rating-list[content]")
        if rating_list_el:
            content = await rating_list_el.get_attribute("content")
            if content:
                content = content.replace(",", ".").strip()
                try:
                    return float(content)
                except ValueError:
                    pass

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
        review_el = await card.query_selector(".card-content .rating-list__count")
        if review_el:
            text = await review_el.inner_text()
            digits = re.sub(r"[^\d]", "", text)
            return int(digits) if digits else None

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
        facilities = await card.query_selector_all(".card-content__facility")
        for facility in facilities:
            text = await facility.inner_text()
            match = re.search(r"(\d+)\s*м", text)
            if match:
                return int(match.group(1))

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

    async def _go_to_next_page(self, page: Page) -> bool:
        """Переходит на следующую страницу каталога.

        Находит кнопку «Далее» среди элементов li.navigation
        и выполняет клик.

        Args:
            page: Страница Playwright.

        Returns:
            True если переход выполнен, False если кнопки нет.
        """
        # Прокручиваем к пагинации
        await page.evaluate("""
            () => {
                const pagination = document.querySelector('.pagination-wrapper');
                if (pagination) pagination.scrollIntoView({behavior: 'smooth', block: 'center'});
            }
        """)
        await asyncio.sleep(1)

        # Ищем кнопку «Далее»
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

        # Кликаем по кнопке «Далее»
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

        # Ждём загрузку
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass

        await asyncio.sleep(3)

        # Прокручиваем наверх
        await page.evaluate("window.scrollTo(0, 0)")

        # Проверяем карточки
        cards = await page.query_selector_all(".card[data-observe-id]")
        if not cards:
            logger.warning("карточки_не_загрузились_после_пагинации")
            return False

        logger.debug("переход_на_следующую_страницу_выполнен")
        return True

    async def _go_to_prev_page(self, page: Page) -> bool:
        """Переходит на предыдущую страницу каталога.

        Находит кнопку «Назад» среди элементов li.navigation
        и выполняет клик.

        Args:
            page: Страница Playwright.

        Returns:
            True если переход выполнен, False если кнопки нет.
        """
        # Прокручиваем к пагинации
        await page.evaluate("""
            () => {
                const pagination = document.querySelector('.pagination-wrapper');
                if (pagination) pagination.scrollIntoView({behavior: 'smooth', block: 'center'});
            }
        """)
        await asyncio.sleep(1)

        # Ищем кнопку «Назад»
        prev_link = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.navigation');
                for (const item of items) {
                    const text = item.querySelector('.pagination-arrow__text');
                    if (text && text.textContent.trim() === 'Назад') {
                        return true;
                    }
                }
                return false;
            }
        """)

        if not prev_link:
            logger.debug("кнопка_назад_не_найдена")
            return False

        # Кликаем по кнопке «Назад»
        clicked = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.navigation');
                for (const item of items) {
                    const text = item.querySelector('.pagination-arrow__text');
                    if (text && text.textContent.trim() === 'Назад') {
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
            logger.debug("клик_назад_не_выполнен")
            return False

        logger.debug("клик_назад_выполнен")

        # Ждём загрузку
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass

        await asyncio.sleep(3)

        # Прокручиваем наверх
        await page.evaluate("window.scrollTo(0, 0)")

        # Проверяем карточки
        cards = await page.query_selector_all(".card[data-observe-id]")
        if not cards:
            logger.warning("карточки_не_загрузились_после_пагинации_назад")
            return False

        logger.debug("переход_на_предыдущую_страницу_выполнен")
        return True
