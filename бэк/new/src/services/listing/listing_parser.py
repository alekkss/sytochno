"""Оркестратор парсинга карточки объявления."""

import asyncio
from datetime import date

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.browser_service import BrowserService
from src.services.listing.api_client import ApiClient
from src.services.listing.constants import (
    DAYS_COUNT,
    DEFAULT_GUESTS,
    MAX_TOKEN_RETRIES,
    SUTOCHNO_BASE_URL,
)
from src.services.listing.page_loader import PageLoader
from src.services.listing.price_parser import PriceParser
from src.services.listing.token_manager import TokenManager

logger = get_logger("listing_parser")


class ListingParser:
    """Оркестратор парсинга одной карточки объявления."""

    def __init__(self, browser_service: BrowserService, guests: int = DEFAULT_GUESTS) -> None:
        """Инициализирует парсер карточки.

        Args:
            browser_service: Сервис управления браузером.
            guests: Количество гостей для запросов цен.
        """
        self._browser = browser_service
        self._guests = guests
        self._page_loader = PageLoader()
        self._token_manager = TokenManager(self._page_loader, browser_service, guests)
        self._api_client = ApiClient(browser_service, guests)
        self._price_parser = PriceParser()

    async def parse_listing(self, page: Page, object_id: str) -> dict | None:
        """Парсит одну карточку объявления — цены, занятость, метаданные.

        Порядок:
        1. Загрузка страницы + перехват токена.
        2. Валидация токена (при неудаче — перезагрузка).
        3. Сбор цен и занятости через API.
        4. Извлечение метаданных со страницы.
        5. Формирование итогового результата.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления на sutochno.ru.

        Returns:
            Словарь с данными карточки или None при критической ошибке.
        """
        url = f"{SUTOCHNO_BASE_URL}/{object_id}"
        today = date.today()

        logger.info("начало_парсинга", step=f"id={object_id}, url={url}")

        # 1. Загрузка страницы и перехват токена
        token = await self._obtain_valid_token(page, url, object_id)

        if not token:
            logger.warning("токен_не_получен", step=f"id={object_id}")
            return self._build_fallback_result(object_id, page, today)

        # 2. Сбор цен и занятости через API
        api_data = await self._api_client.fetch_prices_and_availability(
            page=page,
            object_id=object_id,
            token=token,
            today=today,
        )

        if not api_data:
            logger.warning("api_данные_не_получены", step=f"id={object_id}")
            return self._build_fallback_result(object_id, page, today)

        # 3. Извлечение метаданных со страницы
        metadata = await self._extract_metadata(page, object_id)

        # 4. Формирование результата
        result = self._build_result(
            object_id=object_id,
            api_data=api_data,
            metadata=metadata,
            today=today,
        )

        prices_filled = sum(1 for p in result["prices"] if p > 0)
        available_days = sum(1 for a in result["availability"] if a)

        logger.info(
            "парсинг_завершён",
            step=f"id={object_id}, цен={prices_filled}/{DAYS_COUNT}, "
                 f"доступно={available_days}/{DAYS_COUNT}, "
                 f"min_nights={result.get('min_nights', '?')}",
        )

        return result

    async def _obtain_valid_token(self, page: Page, url: str, object_id: str) -> str | None:
        """Получает и валидирует токен с повторными попытками.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления.

        Returns:
            Валидный токен или None.
        """
        for attempt in range(1, MAX_TOKEN_RETRIES + 1):
            logger.debug(
                "получение_токена",
                step=f"id={object_id}, попытка={attempt}/{MAX_TOKEN_RETRIES}",
            )

            loaded, token = await self._token_manager.goto_and_capture_token(page, url)

            if not loaded:
                logger.warning(
                    "страница_не_загружена",
                    step=f"id={object_id}, попытка={attempt}",
                )
                if attempt < MAX_TOKEN_RETRIES:
                    await asyncio.sleep(2)
                continue

            if not token:
                logger.debug(
                    "токен_не_перехвачен",
                    step=f"id={object_id}, попытка={attempt}",
                )
                if attempt < MAX_TOKEN_RETRIES:
                    token = await self._token_manager.reload_and_get_token(
                        page, url, object_id
                    )
                    if token:
                        is_valid = await self._token_manager.validate_token(
                            page, object_id, token
                        )
                        if is_valid:
                            return token
                continue

            # Токен перехвачен — валидируем
            is_valid = await self._token_manager.validate_token(page, object_id, token)

            if is_valid:
                return token

            # Токен невалиден — пробуем перезагрузку
            if attempt < MAX_TOKEN_RETRIES:
                token = await self._token_manager.reload_and_get_token(
                    page, url, object_id
                )
                if token:
                    is_valid = await self._token_manager.validate_token(
                        page, object_id, token
                    )
                    if is_valid:
                        return token

        return None

    async def _extract_metadata(self, page: Page, object_id: str) -> dict:
        """Извлекает метаданные карточки со страницы.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.

        Returns:
            Словарь с метаданными (title, address, rating, reviews_count, image_url).
        """
        try:
            metadata = await page.evaluate(
                """
                () => {
                    const result = {
                        title: '',
                        address: '',
                        rating: null,
                        reviews_count: 0,
                        image_url: ''
                    };

                    // Заголовок
                    const titleEl = document.querySelector('h1')
                        || document.querySelector('[data-testid="object-title"]')
                        || document.querySelector('.object-title');
                    if (titleEl) result.title = titleEl.textContent.trim();

                    // Адрес
                    const addressEl = document.querySelector('[data-testid="object-address"]')
                        || document.querySelector('.address')
                        || document.querySelector('[class*="address"]');
                    if (addressEl) result.address = addressEl.textContent.trim();

                    // Рейтинг
                    const ratingEl = document.querySelector('[data-testid="object-rating"]')
                        || document.querySelector('[class*="rating"]');
                    if (ratingEl) {
                        const ratingText = ratingEl.textContent.trim();
                        const ratingNum = parseFloat(ratingText);
                        if (!isNaN(ratingNum) && ratingNum > 0 && ratingNum <= 10) {
                            result.rating = ratingNum;
                        }
                    }

                    // Количество отзывов
                    const reviewsEl = document.querySelector('[data-testid="reviews-count"]')
                        || document.querySelector('[class*="reviews"]');
                    if (reviewsEl) {
                        const match = reviewsEl.textContent.match(/(\d+)/);
                        if (match) result.reviews_count = parseInt(match[1]);
                    }

                    // Главное фото
                    const imgEl = document.querySelector('[data-testid="gallery-image"] img')
                        || document.querySelector('.gallery img')
                        || document.querySelector('[class*="gallery"] img')
                        || document.querySelector('meta[property="og:image"]');
                    if (imgEl) {
                        result.image_url = imgEl.src || imgEl.content || '';
                    }

                    return result;
                }
                """
            )
            return metadata

        except Exception as e:
            logger.debug(
                "metadata_ошибка",
                error=str(e)[:200],
                step=f"id={object_id}",
            )
            return {
                "title": "",
                "address": "",
                "rating": None,
                "reviews_count": 0,
                "image_url": "",
            }

    def _build_result(
        self,
        object_id: str,
        api_data: dict,
        metadata: dict,
        today: date,
    ) -> dict:
        """Формирует итоговый результат парсинга.

        Args:
            object_id: ID объявления.
            api_data: Данные из API (prices, availability, min_nights).
            metadata: Метаданные со страницы.
            today: Дата начала.

        Returns:
            Итоговый словарь с данными карточки.
        """
        return {
            "object_id": object_id,
            "url": f"{SUTOCHNO_BASE_URL}/{object_id}",
            "title": metadata.get("title", ""),
            "address": metadata.get("address", ""),
            "rating": metadata.get("rating"),
            "reviews_count": metadata.get("reviews_count", 0),
            "image_url": metadata.get("image_url", ""),
            "prices": api_data["prices"],
            "availability": api_data["availability"],
            "min_nights": api_data["min_nights"],
            "date_start": today.isoformat(),
            "days_count": DAYS_COUNT,
            "guests": self._guests,
            "parsed_at": today.isoformat(),
        }

    async def _build_fallback_result(self, object_id: str, page: Page, today: date) -> dict:
        """Формирует результат-заглушку при невозможности получить данные API.

        Извлекает хотя бы метаданные со страницы.

        Args:
            object_id: ID объявления.
            page: Вкладка браузера.
            today: Дата начала.

        Returns:
            Словарь с пустыми ценами и метаданными.
        """
        metadata = await self._extract_metadata(page, object_id)

        return {
            "object_id": object_id,
            "url": f"{SUTOCHNO_BASE_URL}/{object_id}",
            "title": metadata.get("title", ""),
            "address": metadata.get("address", ""),
            "rating": metadata.get("rating"),
            "reviews_count": metadata.get("reviews_count", 0),
            "image_url": metadata.get("image_url", ""),
            "prices": [0] * DAYS_COUNT,
            "availability": [False] * DAYS_COUNT,
            "min_nights": 0,
            "date_start": today.isoformat(),
            "days_count": DAYS_COUNT,
            "guests": self._guests,
            "parsed_at": today.isoformat(),
            "error": "token_or_api_unavailable",
        }