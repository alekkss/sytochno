"""Управление токеном API — валидация и перезагрузка."""

import asyncio
from datetime import date, timedelta

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.browser_service import BrowserService
from src.services.listing.constants import (
    API_PRICES_URL,
    DEFAULT_GUESTS,
    RELOAD_WAIT_SECONDS,
)
from src.services.listing.page_loader import PageLoader

logger = get_logger("token_manager")


class TokenManager:
    """Валидация токена и получение нового через перезагрузку страницы."""

    def __init__(self, page_loader: PageLoader, browser_service: BrowserService) -> None:
        """Инициализирует менеджер токенов.

        Args:
            page_loader: Загрузчик страниц.
            browser_service: Сервис браузера (для random_delay).
        """
        self._page_loader = page_loader
        self._browser = browser_service

    async def validate_token(
        self, page: Page, object_id: str, token: str, guests: int = DEFAULT_GUESTS
    ) -> bool:
        """Проверяет работоспособность токена одним тестовым запросом.

        Отправляет запрос на 2 ночи для дня через 3 дня от сегодня.
        Если получен ответ с success=true (на уровне data.objects) — токен валиден.
        Ошибка min_nights в ответе — тоже означает валидный токен.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен для проверки.
            guests: Количество гостей.

        Returns:
            True если токен работает, False — если невалиден.
        """
        today = date.today()
        test_date = today + timedelta(days=3)
        end_date = test_date + timedelta(days=2)

        result = await page.evaluate(
            """
            async ({apiUrl, objectId, dateBegin, dateEnd, token, guests}) => {
                try {
                    const resp = await fetch(apiUrl, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Accept': 'application/json',
                            'token': token,
                            'platform': 'js',
                            'api-version': '1.13'
                        },
                        body: JSON.stringify({
                            objects: [parseInt(objectId)],
                            rooms_cnt: {},
                            guests: guests,
                            date_begin: dateBegin,
                            date_end: dateEnd,
                            currency_id: 1,
                            is_pets: 0,
                            documents: 0,
                            target: 0,
                            ages: [],
                            no_time: 1
                        })
                    });

                    if (!resp.ok) return {valid: false, reason: 'http_' + resp.status};

                    const data = await resp.json();
                    if (!data.success) return {valid: false, reason: 'api_false'};
                    if (!data.data || !data.data.objects || !data.data.objects[0]) {
                        return {valid: false, reason: 'no_objects'};
                    }

                    // Объект может вернуть ошибку min_nights — это ОК, токен валиден
                    return {valid: true};

                } catch (e) {
                    return {valid: false, reason: 'exception_' + e.message};
                }
            }
            """,
            {
                "apiUrl": API_PRICES_URL,
                "objectId": object_id,
                "dateBegin": f"{test_date.isoformat()} 14:00:00",
                "dateEnd": f"{end_date.isoformat()} 11:00:00",
                "token": token,
                "guests": guests,
            },
        )

        is_valid = result.get("valid", False)

        if not is_valid:
            logger.warning(
                "токен_невалиден",
                step=f"id={object_id}, причина={result.get('reason', '?')}",
            )
        else:
            logger.debug("токен_валиден", step=f"id={object_id}")

        return is_valid

    async def reload_and_get_token(
        self, page: Page, url: str, object_id: str
    ) -> str | None:
        """Перезагружает страницу и получает новый токен.

        Ждёт RELOAD_WAIT_SECONDS перед перезагрузкой, затем загружает
        страницу заново с перехватом токена.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов).

        Returns:
            Новый токен или None.
        """
        await asyncio.sleep(RELOAD_WAIT_SECONDS)

        loaded, new_token = await self._page_loader.goto_and_capture_token(page, url)

        if not loaded:
            logger.warning(
                "перезагрузка_не_удалась",
                step=f"id={object_id}",
            )
            return None

        if not new_token:
            logger.warning(
                "токен_не_получен_после_перезагрузки",
                step=f"id={object_id}",
            )
            return None

        await self._browser.random_delay()

        logger.debug("новый_токен_получен", step=f"id={object_id}")

        return new_token