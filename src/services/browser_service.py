"""Сервис управления браузером — Playwright + stealth-настройки."""

import asyncio
import random

from playwright.async_api import (
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from src.config.logger import get_logger
from src.config.settings import Settings

logger = get_logger("browser")


class BrowserService:
    """Сервис для управления браузером Playwright.

    Обеспечивает:
    - Stealth-настройки для обхода детекции бота.
    - Полную загрузку страницы без блокировки ресурсов.
    - Случайные паузы между действиями.
    - Навигацию с обработкой таймаутов.
    """

    def __init__(self, settings: Settings) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
        """
        self._settings = settings
        self._playwright: Playwright | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    @property
    def page(self) -> Page:
        """Возвращает активную страницу браузера.

        Returns:
            Экземпляр Page.

        Raises:
            RuntimeError: Если браузер не запущен.
        """
        if self._page is None:
            raise RuntimeError(
                "Браузер не запущен. Вызовите start() перед использованием."
            )
        return self._page

    async def start(self) -> None:
        """Запускает браузер с настройками stealth без блокировки ресурсов."""
        logger.info(
            "запуск_браузера",
            step="start",
        )

        self._playwright = await async_playwright().start()

        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir="",
            headless=self._settings.headless_mode,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
            ignore_default_args=["--enable-automation"],
        )

        # Скрываем признаки автоматизации
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        self._page = self._context.pages[0] if self._context.pages else await self._context.new_page()

        # Устанавливаем таймаут навигации
        self._page.set_default_navigation_timeout(self._settings.navigation_timeout)

        logger.info(
            "браузер_запущен",
            step="start",
        )

    async def stop(self) -> None:
        """Останавливает браузер и освобождает ресурсы."""
        if self._context is not None:
            await self._context.close()
            self._context = None
            self._page = None

        if self._playwright is not None:
            await self._playwright.stop()
            self._playwright = None

        logger.info("браузер_остановлен")

    async def navigate(self, url: str) -> None:
        """Переходит по URL с ожиданием загрузки DOM.

        Args:
            url: Целевой URL.

        Raises:
            RuntimeError: Если браузер не запущен.
        """
        page = self.page
        logger.debug("навигация", path=url)

        await page.goto(url, wait_until="domcontentloaded")
        await self.random_delay()

    async def random_delay(self) -> None:
        """Выполняет случайную паузу между действиями.

        Диапазон задержки определяется настройками MIN_DELAY_MS и MAX_DELAY_MS.
        Имитирует поведение реального пользователя.
        """
        delay_ms = random.randint(
            self._settings.min_delay_ms,
            self._settings.max_delay_ms,
        )
        delay_seconds = delay_ms / 1000.0
        await asyncio.sleep(delay_seconds)

    async def scroll_page(self) -> None:
        """Плавно прокручивает страницу вниз для имитации поведения пользователя.

        Прокручивает порциями с небольшими паузами между ними.
        """
        page = self.page
        viewport_height = page.viewport_size["height"] if page.viewport_size else 1080

        # Получаем высоту страницы
        page_height = await page.evaluate("document.body.scrollHeight")
        current_position = 0

        while current_position < page_height:
            scroll_step = random.randint(
                int(viewport_height * 0.3),
                int(viewport_height * 0.7),
            )
            current_position += scroll_step
            await page.evaluate(f"window.scrollTo(0, {current_position})")
            await asyncio.sleep(random.uniform(0.3, 0.8))

    async def get_page_content(self) -> str:
        """Возвращает HTML-содержимое текущей страницы.

        Returns:
            HTML-строка.
        """
        return await self.page.content()
