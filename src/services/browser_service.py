"""Сервис управления браузером — Playwright + stealth-настройки."""

import asyncio
import random

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    async_playwright,
)

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.proxy import ProxyConfig

logger = get_logger("browser")

# Таймаут ожидания завершения Playwright (секунды)
_PLAYWRIGHT_STOP_TIMEOUT: float = 10.0


class BrowserService:
    """Сервис для управления браузером Playwright.

    Обеспечивает:
    - Stealth-настройки для обхода детекции бота.
    - Полную загрузку страницы без блокировки ресурсов.
    - Случайные паузы между действиями.
    - Навигацию с обработкой таймаутов.
    - Запуск через прокси-сервер.
    - Создание дополнительных вкладок для параллельной обработки карточек.
    """

    def __init__(self, settings: Settings) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
        """
        self._settings = settings
        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._proxy: ProxyConfig | None = None

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

    @property
    def context(self) -> BrowserContext:
        """Возвращает контекст браузера.

        Returns:
            Экземпляр BrowserContext.

        Raises:
            RuntimeError: Если браузер не запущен.
        """
        if self._context is None:
            raise RuntimeError(
                "Браузер не запущен. Вызовите start() перед использованием."
            )
        return self._context

    async def start(self, proxy: ProxyConfig | None = None) -> None:
        """Запускает браузер с настройками stealth.

        Если передана прокси — браузер использует её для всех соединений.
        Без прокси — запускает обычный браузер без прокси.

        Args:
            proxy: Конфигурация прокси (опционально).
        """
        self._proxy = proxy
        proxy_label = str(proxy) if proxy else "без прокси"

        logger.info(
            "запуск_браузера",
            step=proxy_label,
        )

        self._playwright = await async_playwright().start()

        if proxy:
            await self._start_with_proxy(proxy)
        else:
            await self._start_without_proxy()

        # Устанавливаем таймаут навигации
        self._page.set_default_navigation_timeout(self._settings.navigation_timeout)

        logger.info(
            "браузер_запущен",
            step=proxy_label,
        )

    async def _start_without_proxy(self) -> None:
        """Запускает браузер без прокси через launch + new_context.

        Использует обычный launch() вместо launch_persistent_context(),
        чтобы гарантировать корректное завершение процесса Chromium
        через browser.close().
        """
        self._browser = await self._playwright.chromium.launch(
            headless=self._settings.headless_mode,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
            ignore_default_args=["--enable-automation"],
        )

        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # Скрываем признаки автоматизации
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        self._page = await self._context.new_page()

    async def _start_with_proxy(self, proxy: ProxyConfig) -> None:
        """Запускает браузер с прокси через обычный launch + context.

        Args:
            proxy: Конфигурация прокси.
        """
        self._browser = await self._playwright.chromium.launch(
            headless=self._settings.headless_mode,
            proxy={
                "server": proxy.server_url,
                "username": proxy.username,
                "password": proxy.password,
            },
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        self._context = await self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # Скрываем признаки автоматизации
        await self._context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        self._page = await self._context.new_page()

    async def create_page(self) -> Page:
        """Создаёт новую вкладку (page) в существующем контексте браузера.

        Используется для параллельной обработки карточек — каждая вкладка
        работает со своим объявлением независимо, разделяя один сетевой канал.

        Новая вкладка наследует все stealth-настройки контекста (user-agent,
        скрытие webdriver, locale). Таймаут навигации устанавливается
        из настроек приложения.

        Returns:
            Новый экземпляр Page.

        Raises:
            RuntimeError: Если браузер не запущен.
        """
        context = self.context

        new_page = await context.new_page()
        new_page.set_default_navigation_timeout(self._settings.navigation_timeout)

        logger.debug(
            "вкладка_создана",
            step=f"всего_вкладок={len(context.pages)}",
        )

        return new_page

    async def close_page(self, page: Page) -> None:
        """Закрывает указанную вкладку и освобождает её ресурсы.

        Не закрывает основную страницу (self._page) — только дополнительные.
        Если передана основная страница, закрытие пропускается с предупреждением.

        Args:
            page: Вкладка для закрытия.
        """
        if page is self._page:
            logger.warning("попытка_закрыть_основную_страницу_пропущена")
            return

        try:
            if not page.is_closed():
                await page.close()
                logger.debug("вкладка_закрыта")
        except Exception as e:
            logger.debug(
                "ошибка_при_закрытии_вкладки",
                error=str(e),
                error_type=type(e).__name__,
            )

    async def stop(self) -> None:
        """Останавливает браузер и освобождает все ресурсы.

        Последовательно закрывает контекст, браузер и Playwright.
        Каждый шаг обёрнут в try/except — ошибка на одном шаге
        не блокирует выполнение остальных. На playwright.stop()
        установлен таймаут, чтобы избежать бесконечного зависания
        при незавершённых процессах Chromium.
        """
        # Шаг 1: Закрываем контекст браузера
        if self._context is not None:
            try:
                await self._context.close()
            except Exception as e:
                logger.debug(
                    "ошибка_при_закрытии_контекста",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            finally:
                self._context = None
                self._page = None

        # Шаг 2: Закрываем браузер (убивает процесс Chromium)
        if self._browser is not None:
            try:
                await self._browser.close()
            except Exception as e:
                logger.debug(
                    "ошибка_при_закрытии_браузера",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            finally:
                self._browser = None

        # Шаг 3: Останавливаем Playwright с таймаутом
        if self._playwright is not None:
            try:
                await asyncio.wait_for(
                    self._playwright.stop(),
                    timeout=_PLAYWRIGHT_STOP_TIMEOUT,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "playwright_stop_таймаут",
                    step=f"превышен_лимит={_PLAYWRIGHT_STOP_TIMEOUT}с",
                )
            except Exception as e:
                logger.debug(
                    "ошибка_при_остановке_playwright",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            finally:
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
