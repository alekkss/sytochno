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

# Пауза между шагами закрытия, чтобы Node.js-драйвер
# успел обработать pending-события до разрыва pipe (секунды).
# Решает проблему EPIPE в Node.js v24+, где unhandled write
# на закрытый pipe стал fatal error.
_CLOSE_DRAIN_DELAY: float = 0.5

# Таймаут на полную прокрутку страницы (секунды).
# Если Chromium захлебнулся по памяти/CPU — evaluate() может
# зависнуть навсегда. Таймаут гарантирует, что scroll_page
# завершится и не заблокирует весь pipeline.
_SCROLL_TIMEOUT_SECONDS: float = 30.0

# Общие аргументы запуска Chromium — stealth + экономия памяти.
# Используются и с прокси, и без прокси (единый набор, без дублирования).
_BROWSER_ARGS: list[str] = [
    # ── Stealth: обход детекции автоматизации ──
    "--disable-blink-features=AutomationControlled",

    # ── Стабильность: предотвращение краша в контейнерах/серверах ──
    "--disable-dev-shm-usage",
    "--no-sandbox",

    # ── Экономия памяти: ограничение ресурсов Chromium ──
    # Ограничиваем JS-хип каждого renderer-процесса до 512 МБ.
    # Для sutochno.ru (не тяжёлое SPA) достаточно с запасом.
    # Без этого флага Chromium может потреблять до 4 ГБ на renderer.
    "--js-flags=--max-old-space-size=512",
    # Ограничиваем количество renderer-процессов на один браузер.
    # По умолчанию Chromium создаёт отдельный процесс для каждой вкладки.
    # С 5 вкладками на воркер и 20 воркерами — это 100 процессов.
    # Лимит 4 заставляет вкладки разделять renderer-процессы.
    "--renderer-process-limit=4",
    # Запрещаем фоновую активность неактивных вкладок.
    # Без этих флагов Chromium продолжает выполнять JS-таймеры,
    # requestAnimationFrame и сетевые запросы во вкладках,
    # которые не находятся в фокусе — впустую расходуя CPU и RAM.
    "--disable-renderer-backgrounding",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    # Отключаем GPU-ускорение — на серверах нет GPU,
    # а software-рендеринг расходует дополнительную память.
    "--disable-gpu",
]

# Общий скрипт stealth-инъекции — скрывает признаки автоматизации.
_STEALTH_INIT_SCRIPT: str = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
    window.chrome = {runtime: {}};
"""

# Общие параметры контекста браузера.
_CONTEXT_OPTIONS: dict = {
    "viewport": {"width": 1920, "height": 1080},
    "locale": "ru-RU",
    "timezone_id": "Europe/Moscow",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


class BrowserService:
    """Сервис для управления браузером Playwright.

    Обеспечивает:
    - Stealth-настройки для обхода детекции бота.
    - Полную загрузку страницы без блокировки ресурсов.
    - Случайные паузы между действиями.
    - Навигацию с обработкой таймаутов.
    - Запуск через прокси-сервер.
    - Создание дополнительных вкладок для параллельной обработки карточек.
    - Оптимизацию потребления памяти через аргументы Chromium.
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

        launch_kwargs: dict = {
            "headless": self._settings.headless_mode,
            "args": _BROWSER_ARGS,
            "ignore_default_args": ["--enable-automation"],
        }

        if proxy:
            launch_kwargs["proxy"] = {
                "server": proxy.server_url,
                "username": proxy.username,
                "password": proxy.password,
            }

        self._browser = await self._playwright.chromium.launch(**launch_kwargs)

        self._context = await self._browser.new_context(**_CONTEXT_OPTIONS)

        # Скрываем признаки автоматизации
        await self._context.add_init_script(_STEALTH_INIT_SCRIPT)

        self._page = await self._context.new_page()

        # Устанавливаем таймаут навигации
        self._page.set_default_navigation_timeout(self._settings.navigation_timeout)

        logger.info(
            "браузер_запущен",
            step=proxy_label,
        )

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

    async def close_all_pages(self) -> None:
        """Закрывает все дополнительные вкладки, оставляя только основную.

        Используется для освобождения памяти между пачками карточек
        без полного перезапуска браузера. Каждая вкладка Chromium
        потребляет ~50–150 МБ — при 5 вкладках на 20 воркеров это до 15 ГБ.

        Основная страница (self._page) не закрывается — она нужна
        для поддержания сессии и контекста браузера.
        """
        if self._context is None:
            return

        pages_to_close = [
            p for p in self._context.pages
            if p is not self._page and not p.is_closed()
        ]

        if not pages_to_close:
            return

        closed_count = 0
        for p in pages_to_close:
            try:
                await p.close()
                closed_count += 1
            except Exception as e:
                logger.debug(
                    "ошибка_при_закрытии_вкладки",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        if closed_count > 0:
            logger.debug(
                "вкладки_очищены",
                step=f"закрыто={closed_count}",
            )

    async def stop(self) -> None:
        """Останавливает браузер и освобождает все ресурсы.

        Последовательность закрытия спроектирована для предотвращения
        ошибки EPIPE в Node.js v24+. Между каждым шагом выдерживается
        пауза (_CLOSE_DRAIN_DELAY), чтобы Node.js-драйвер Playwright
        успел обработать pending dispose/event-сообщения до разрыва pipe.

        Порядок закрытия:
        1. Закрытие всех дополнительных страниц (вкладок).
        2. Пауза — Node.js обрабатывает page dispose-события.
        3. Закрытие контекста браузера.
        4. Пауза — Node.js обрабатывает context dispose-события.
        5. Закрытие браузера (убивает процесс Chromium).
        6. Пауза — Node.js завершает финализацию процесса.
        7. Остановка Playwright (закрывает pipe к Node.js-драйверу).
        """
        # Шаг 1: Закрываем все дополнительные страницы
        if self._context is not None:
            try:
                pages_to_close = [
                    p for p in self._context.pages
                    if p is not self._page and not p.is_closed()
                ]
                for p in pages_to_close:
                    try:
                        await p.close()
                    except Exception:
                        pass
            except Exception as e:
                logger.debug(
                    "ошибка_при_закрытии_вкладок",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        # Пауза: даём Node.js обработать page dispose-события
        await asyncio.sleep(_CLOSE_DRAIN_DELAY)

        # Шаг 2: Закрываем контекст браузера
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

        # Пауза: даём Node.js обработать context dispose-события
        await asyncio.sleep(_CLOSE_DRAIN_DELAY)

        # Шаг 3: Закрываем браузер (убивает процесс Chromium)
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

        # Пауза: даём Node.js завершить финализацию перед разрывом pipe
        await asyncio.sleep(_CLOSE_DRAIN_DELAY)

        # Шаг 4: Останавливаем Playwright с таймаутом
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
                # EPIPE или другие ошибки при разрыве соединения —
                # не критичны, браузер уже закрыт, ресурсы освобождены.
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

        Вся прокрутка выполняется в одном вызове page.evaluate —
        JS-цикл внутри Chromium прокручивает страницу порциями с паузами.
        Это минимизирует обращения через pipe к Chromium (1 вместо 20+),
        что критично при двух параллельных браузерах в одном event loop.

        Защищена общим таймаутом: если Chromium захлебнулся по памяти/CPU
        и evaluate не возвращает ответ — прокрутка прерывается штатно.
        Парсинг продолжается с тем, что успело загрузиться.
        """
        page = self.page

        try:
            await asyncio.wait_for(
                page.evaluate("""
                    async () => {
                        const viewportHeight = window.innerHeight || 1080;
                        const maxSteps = 20;
                        let currentPosition = 0;
                        const pageHeight = document.body.scrollHeight;

                        for (let step = 0; step < maxSteps; step++) {
                            if (currentPosition >= pageHeight) break;

                            const scrollStep = Math.floor(
                                viewportHeight * (0.3 + Math.random() * 0.4)
                            );
                            currentPosition += scrollStep;
                            window.scrollTo(0, currentPosition);

                            await new Promise(r => setTimeout(r, 300 + Math.random() * 500));
                        }
                    }
                """),
                timeout=_SCROLL_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "прокрутка_прервана_по_таймауту",
                step=f"лимит={_SCROLL_TIMEOUT_SECONDS}с",
            )
        except Exception as e:
            logger.warning(
                "ошибка_при_прокрутке",
                error=str(e),
                error_type=type(e).__name__,
            )

    async def get_page_content(self) -> str:
        """Возвращает HTML-содержимое текущей страницы.

        Returns:
            HTML-строка.
        """
        return await self.page.content()
