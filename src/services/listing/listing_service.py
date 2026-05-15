"""Сервис обогащения карточек — оркестрация загрузки, токена, стратегии."""

import asyncio
from datetime import date

from playwright.async_api import BrowserContext, Page

from src.config.logger import get_logger
from src.services.browser_service import BrowserService
from src.services.listing.api_client import ApiClient
from src.services.listing.constants import (
    DAYS_COUNT,
    DEFAULT_GUESTS,
    LISTING_URL_TEMPLATE,
    MAX_TABS,
    TAB_DELAY_SECONDS,
)
from src.services.listing.hybrid_strategy import HybridStrategy
from src.services.listing.page_loader import PageLoader
from src.services.listing.price_parser import PriceParser
from src.services.listing.token_manager import TokenManager

logger = get_logger("listing_service")


class ListingService:
    """Сервис обогащения карточек объявлений данными о ценах и занятости.

    Оркестрирует:
    - Загрузку страницы карточки (PageLoader)
    - Перехват и валидацию токена (TokenManager)
    - Гибридную стратегию получения данных (HybridStrategy)
    - Параллельную обработку через вкладки (enrich_listings_tabbed)
    """

    def __init__(self, browser_service: BrowserService, guests: int = DEFAULT_GUESTS) -> None:
        """Инициализирует сервис.

        Args:
            browser_service: Сервис управления браузером.
            guests: Количество гостей для запросов цен.
        """
        self._browser = browser_service
        self._guests = guests

        self._page_loader = PageLoader()
        self._price_parser = PriceParser()
        self._api_client = ApiClient(price_parser=self._price_parser)
        self._token_manager = TokenManager(
            page_loader=self._page_loader,
            browser_service=self._browser,
        )
        self._strategy = HybridStrategy(
            api_client=self._api_client,
            token_manager=self._token_manager,
            guests=self._guests,
        )

    async def enrich_listing(
        self, page: Page, listing: dict
    ) -> dict:
        """Обогащает одну карточку данными о ценах и занятости.

        Загружает страницу карточки, перехватывает токен, запускает
        гибридную стратегию. Результат записывается в listing dict.

        Args:
            page: Вкладка браузера.
            listing: Словарь карточки с полем 'id'.

        Returns:
            Обогащённый словарь карточки с полями:
            - prices_60: список из 60 цен
            - calendar_60: список из 60 значений занятости (0/1)
            - price_date: дата начала периода
            - enriched: True/False
        """
        object_id = str(listing.get("id", ""))
        url = LISTING_URL_TEMPLATE.format(object_id=object_id)

        logger.info("обогащение_начало", step=f"id={object_id}", path=url)

        try:
            # ── Загрузка страницы и перехват токена ──
            loaded, token = await self._page_loader.goto_and_capture_token(page, url)

            if not loaded:
                logger.warning("страница_не_загружена", step=f"id={object_id}")
                listing["prices_60"] = [0] * DAYS_COUNT
                listing["calendar_60"] = [0] * DAYS_COUNT
                listing["price_date"] = date.today().isoformat()
                listing["enriched"] = False
                return listing

            await self._browser.random_delay()

            if not token:
                logger.warning(
                    "токен_не_перехвачен_пробуем_перезагрузку",
                    step=f"id={object_id}",
                )
                token = await self._token_manager.reload_and_get_token(
                    page, url, object_id
                )

            if not token:
                logger.warning("нет_токена", step=f"id={object_id}")
                listing["prices_60"] = [0] * DAYS_COUNT
                listing["calendar_60"] = [0] * DAYS_COUNT
                listing["price_date"] = date.today().isoformat()
                listing["enriched"] = False
                return listing

            # ── Гибридная стратегия ──
            calendar_60, prices_60 = await self._strategy.fetch_calendar_and_prices(
                page, object_id, token, url
            )

            listing["prices_60"] = prices_60
            listing["calendar_60"] = calendar_60
            listing["price_date"] = date.today().isoformat()
            listing["enriched"] = True

            free_days = sum(1 for c in calendar_60 if c == 0)
            priced_days = sum(1 for p in prices_60 if p > 0)

            logger.info(
                "обогащение_завершено",
                step=f"id={object_id}",
                total=f"свободных={free_days}, с_ценой={priced_days}",
            )

        except Exception as e:
            logger.error(
                "обогащение_ошибка",
                step=f"id={object_id}",
                error=str(e)[:300],
                error_type=type(e).__name__,
            )
            listing["prices_60"] = [0] * DAYS_COUNT
            listing["calendar_60"] = [0] * DAYS_COUNT
            listing["price_date"] = date.today().isoformat()
            listing["enriched"] = False

        return listing

    async def enrich_listings(
        self, context: BrowserContext, listings: list[dict]
    ) -> list[dict]:
        """Обогащает список карточек последовательно (одна вкладка).

        Args:
            context: Контекст браузера.
            listings: Список карточек.

        Returns:
            Список обогащённых карточек.
        """
        if not listings:
            return listings

        page = await context.new_page()

        try:
            for listing in listings:
                await self.enrich_listing(page, listing)
                await self._browser.random_delay()
        finally:
            await page.close()

        return listings

    async def enrich_listings_tabbed(
        self, context: BrowserContext, listings: list[dict],
        max_tabs: int = MAX_TABS
    ) -> list[dict]:
        """Обогащает список карточек параллельно через несколько вкладок.

        Разбивает список на чанки по max_tabs, обрабатывает каждый чанк
        параллельно (asyncio.gather). Вкладки создаются и закрываются
        для каждого чанка.

        Args:
            context: Контекст браузера.
            listings: Список карточек.
            max_tabs: Максимальное количество параллельных вкладок.

        Returns:
            Список обогащённых карточек.
        """
        if not listings:
            return listings

        total = len(listings)
        effective_tabs = min(max_tabs, total)

        logger.info(
            "обогащение_пакетное_начало",
            step=f"всего={total}, вкладок={effective_tabs}",
        )

        # Разбиваем на чанки
        chunks: list[list[dict]] = []
        for i in range(0, total, effective_tabs):
            chunks.append(listings[i : i + effective_tabs])

        processed = 0

        for chunk_idx, chunk in enumerate(chunks):
            pages: list[Page] = []

            try:
                # Создаём вкладки для чанка
                for _ in chunk:
                    page = await context.new_page()
                    pages.append(page)
                    await asyncio.sleep(TAB_DELAY_SECONDS)

                # Параллельная обработка
                tasks = [
                    self.enrich_listing(pages[i], chunk[i])
                    for i in range(len(chunk))
                ]
                await asyncio.gather(*tasks, return_exceptions=True)

                processed += len(chunk)

                logger.info(
                    "чанк_завершён",
                    step=f"чанк={chunk_idx + 1}/{len(chunks)}, "
                         f"обработано={processed}/{total}",
                )

            except Exception as e:
                logger.error(
                    "чанк_ошибка",
                    step=f"чанк={chunk_idx + 1}",
                    error=str(e)[:200],
                )
            finally:
                # Закрываем вкладки
                for page in pages:
                    try:
                        await page.close()
                    except Exception:
                        pass

            # Пауза между чанками
            if chunk_idx < len(chunks) - 1:
                await self._browser.random_delay()

        enriched_count = sum(1 for l in listings if l.get("enriched"))
        logger.info(
            "обогащение_пакетное_завершено",
            step=f"обогащено={enriched_count}/{total}",
        )

        return listings