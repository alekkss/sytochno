"""Загрузка страницы карточки и перехват токена API."""

import asyncio

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.constants import (
    GOTO_RETRY_DELAY,
    MAX_GOTO_RETRIES,
    NETWORKIDLE_SOFT_TIMEOUT_MS,
    PAGE_READY_SELECTORS,
    PAGE_READY_TIMEOUT_MS,
)

logger = get_logger("page_loader")


class PageLoader:
    """Загрузка страницы карточки с retry и перехватом токена."""

    async def goto_and_capture_token(self, page: Page, url: str) -> tuple[bool, str | None]:
        """Загружает страницу карточки и перехватывает токен API.

        Устанавливает обработчик на request-события, загружает страницу,
        снимает обработчик. Токен извлекается из заголовка 'token' первого
        запроса к sutochno.ru/api/json.

        Args:
            page: Вкладка браузера.
            url: URL карточки.

        Returns:
            Кортеж (страница_загружена, токен_или_None).
        """
        captured_token: list[str] = []

        def on_request(request: "any") -> None:  # type: ignore[name-defined]
            """Синхронный обработчик — перехватывает токен из заголовков."""
            if captured_token:
                return
            req_url = request.url
            if "sutochno.ru/api/json" in req_url:
                token_header = request.headers.get("token")
                if token_header:
                    captured_token.append(token_header)

        page.on("request", on_request)

        try:
            loaded = await self.goto_with_retry(page, url)
        finally:
            page.remove_listener("request", on_request)

        token = captured_token[0] if captured_token else None

        if token:
            logger.debug(
                "токен_перехвачен",
                step=f"длина={len(token)}, источник=request_header",
            )
        else:
            logger.debug("токен_не_перехвачен_при_загрузке")

        return loaded, token

    async def goto_with_retry(self, page: Page, url: str) -> bool:
        """Загружает страницу карточки с повторными попытками.

        При сетевых ошибках (таймаут, сброс соединения, проблемы прокси)
        повторяет попытку с паузой. Ожидает domcontentloaded, затем
        пытается дождаться networkidle (мягкий таймаут), затем проверяет
        наличие ключевых элементов.

        Args:
            page: Вкладка браузера.
            url: URL карточки.

        Returns:
            True если страница загружена, False — если все попытки исчерпаны.
        """
        for attempt in range(1, MAX_GOTO_RETRIES + 1):
            try:
                logger.debug(
                    "goto_попытка",
                    step=f"попытка={attempt}/{MAX_GOTO_RETRIES}",
                    path=url,
                )

                await page.goto(url, wait_until="domcontentloaded", timeout=30000)

                try:
                    await page.wait_for_load_state(
                        "networkidle", timeout=NETWORKIDLE_SOFT_TIMEOUT_MS
                    )
                except Exception:
                    logger.debug(
                        "networkidle_не_достигнут_продолжаем",
                        step=f"попытка={attempt}",
                    )

                page_ready = await self.wait_for_page_ready(page)
                if page_ready:
                    logger.debug("страница_готова", step=f"попытка={attempt}")
                    return True

                logger.debug(
                    "элементы_не_найдены_но_продолжаем",
                    step=f"попытка={attempt}",
                )
                return True

            except Exception as e:
                error_msg = str(e)
                is_network_error = any(
                    err in error_msg
                    for err in [
                        "ERR_TIMED_OUT",
                        "ERR_CONNECTION_RESET",
                        "ERR_CONNECTION_CLOSED",
                        "ERR_CONNECTION_REFUSED",
                        "ERR_PROXY_CONNECTION_FAILED",
                        "ERR_TUNNEL_CONNECTION_FAILED",
                        "NS_ERROR_NET_RESET",
                        "Timeout",
                    ]
                )

                if is_network_error and attempt < MAX_GOTO_RETRIES:
                    logger.warning(
                        "сетевая_ошибка_повтор",
                        error=error_msg[:200],
                        step=f"попытка={attempt}/{MAX_GOTO_RETRIES}",
                    )
                    await asyncio.sleep(GOTO_RETRY_DELAY)
                    continue

                logger.warning(
                    "goto_не_удался",
                    error=error_msg[:200],
                    error_type=type(e).__name__,
                    step=f"попытка={attempt}/{MAX_GOTO_RETRIES}",
                )
                return False

        return False

    async def wait_for_page_ready(self, page: Page) -> bool:
        """Ожидает появления ключевых элементов на странице карточки.

        Проверяет селекторы последовательно. Достаточно одного совпадения.

        Args:
            page: Вкладка браузера.

        Returns:
            True если хотя бы один ключевой элемент найден.
        """
        for selector in PAGE_READY_SELECTORS:
            try:
                await page.wait_for_selector(selector, timeout=PAGE_READY_TIMEOUT_MS)
                return True
            except Exception:
                continue
        return False