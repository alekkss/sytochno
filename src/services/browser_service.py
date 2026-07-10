"""Сервис управления браузером — Playwright + stealth-настройки."""

import asyncio
import logging
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

# Пауза между шагами закрытия (страницы → контекст → браузер),
# чтобы Node.js-драйвер успел обработать dispose-события (секунды).
_CLOSE_DRAIN_DELAY: float = 0.5

# Увеличенная пауза перед playwright.stop() — критический шаг,
# где закрывается pipe к Node.js-драйверу. Если в этот момент
# Node.js ещё обрабатывает dispose-события от browser.close() —
# запись в закрытый pipe даёт EPIPE (fatal в Node.js v24+).
# 2 секунды — достаточно для одного браузера с несколькими вкладками.
# При параллельной остановке 80 браузеров (через _stop_browsers)
# каждый процесс Node.js независим, поэтому 2 сек хватает.
_CLOSE_DRAIN_BEFORE_STOP: float = 2.0

# Таймаут на навигацию about:blank при закрытии страницы (мс).
# Перед закрытием контекста навигируем все страницы на about:blank —
# это отменяет все pending-операции (goto, wait_for_selector, route)
# и гарантирует, что Node.js-драйвер не попытается отправить
# ответ через pipe после его закрытия.
_BLANK_NAVIGATION_TIMEOUT_MS: int = 3000

# Таймаут на полную прокрутку страницы (секунды).
# Если Chromium захлебнулся по памяти/CPU — evaluate() может
# зависнуть навсегда. Таймаут гарантирует, что scroll_page
# завершится и не заблокирует весь pipeline.
_SCROLL_TIMEOUT_SECONDS: float = 30.0

# Таймаут на один вызов page.evaluate внутри прокрутки (секунды).
# Если один шаг завис — прекращаем прокрутку, а не ждём бесконечно.
_SCROLL_STEP_TIMEOUT_SECONDS: float = 10.0

# Максимальное количество шагов прокрутки.
# Защита от infinite scroll — сайт может подгружать контент
# при прокрутке, увеличивая высоту страницы бесконечно.
# 20 шагов × ~500px = ~10000px — достаточно для любой страницы каталога.
_MAX_SCROLL_STEPS: int = 20

# Общие аргументы запуска Chromium — stealth + экономия памяти.
# Используются и с прокси, и без прокси (единый набор, без дублирования).
_BROWSER_ARGS: list[str] = [
    # ── Stealth: обход детекции автоматизации ──
    "--disable-blink-features=AutomationControlled",

    # ── Стабильность: предотвращение краша в контейнерах/серверах ──
    "--disable-dev-shm-usage",
    "--no-sandbox",

    # ── Экономия памяти: ограничение ресурсов Chromium ──
    "--single-process",
    "--js-flags=--max-old-space-size=256",
    "--disable-renderer-backgrounding",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-gpu",
    "--disable-features=PaintHolding,ImageDecodeService",
    "--blink-settings=imagesEnabled=false",
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

# ── Маркеры для идентификации Playwright-исключений ──

_PLAYWRIGHT_MODULE_MARKERS: tuple[str, ...] = (
    "playwright.",
    "playwright._impl.",
)

_PLAYWRIGHT_ERROR_MARKERS: tuple[str, ...] = (
    "net::ERR_",
    "Timeout",
    "frame was detached",
    "Target closed",
    "Target crashed",
    "Navigation failed",
    "Protocol error",
    "Connection closed",
    "Browser closed",
    "browser has been closed",
    "context has been closed",
    "page has been closed",
)


def _is_playwright_exception(exception: BaseException | None) -> bool:
    """Определяет, является ли исключение внутренним Playwright-исключением.

    Проверяет три критерия (любого достаточно):
    1. asyncio.InvalidStateError — гонка при закрытии браузера.
    2. Модуль класса исключения начинается с 'playwright.' —
       TimeoutError и Error из playwright._impl._errors.
    3. Текст ошибки содержит маркеры Playwright-проблем.

    Args:
        exception: Исключение из контекста asyncio.

    Returns:
        True если исключение из внутренних задач Playwright и безвредно.
    """
    if exception is None:
        return False

    if isinstance(exception, asyncio.InvalidStateError):
        return True

    exc_module = getattr(type(exception), "__module__", "") or ""
    if any(exc_module.startswith(marker) for marker in _PLAYWRIGHT_MODULE_MARKERS):
        return True

    error_text = str(exception)
    if any(marker in error_text for marker in _PLAYWRIGHT_ERROR_MARKERS):
        return True

    return False


def _make_playwright_exception_handler(
    default_handler: "asyncio.events._ExceptionHandler | None",
) -> "callable":
    """Создаёт обработчик необработанных исключений asyncio-задач.

    Перехватывает и подавляет безвредные исключения из внутренних задач
    Playwright (InvalidStateError, TimeoutError, net::ERR_*, EPIPE).

    Args:
        default_handler: Предыдущий обработчик исключений (может быть None).

    Returns:
        Функция-обработчик для loop.set_exception_handler().
    """

    def handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exception = context.get("exception")

        if _is_playwright_exception(exception):
            exc_type = type(exception).__name__ if exception else "unknown"
            exc_msg = str(exception)[:150] if exception else ""

            future = context.get("future")
            source_name = ""
            if future is not None:
                source_name = getattr(future, "get_name", lambda: "")()
                if not source_name:
                    coro = getattr(future, "get_coro", lambda: None)()
                    if coro is not None:
                        source_name = getattr(coro, "__qualname__", "")

            logger.debug(
                "playwright_осиротевшее_исключение_подавлено",
                step=f"тип={exc_type}, источник={source_name or 'unknown'}",
                error=exc_msg,
            )
            return

        if default_handler is not None:
            default_handler(loop, context)
        else:
            logging.getLogger("asyncio").error(
                "Необработанное исключение в asyncio-задаче",
                exc_info=exception,
                extra={"asyncio_context": context},
            )

    return handler


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
    - Отключение загрузки изображений для экономии RAM.
    - Подавление безвредных осиротевших исключений из внутренних задач Playwright.
    - Корректное завершение без EPIPE в Node.js v24+.
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
        self._original_exception_handler: "asyncio.events._ExceptionHandler | None" = (
            None
        )

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

        Устанавливает обработчик необработанных исключений asyncio-задач.

        Args:
            proxy: Конфигурация прокси (опционально).
        """
        self._proxy = proxy
        proxy_label = str(proxy) if proxy else "без прокси"

        logger.info(
            "запуск_браузера",
            step=proxy_label,
        )

        loop = asyncio.get_running_loop()
        self._original_exception_handler = loop.get_exception_handler()
        loop.set_exception_handler(
            _make_playwright_exception_handler(self._original_exception_handler)
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

    async def is_alive(self) -> bool:
        """Проверяет, жив ли renderer процесс страницы.

        Returns:
            True если renderer отвечает, False если мёртв или недоступен.
        """
        if self._page is None:
            return False

        try:
            await asyncio.wait_for(
                self._page.evaluate("1 + 1"),
                timeout=5.0,
            )
            return True
        except Exception:
            return False

    async def create_page(self) -> Page:
        """Создаёт новую вкладку (page) в существующем контексте браузера.

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
        """Закрывает все дополнительные вкладки, оставляя только основную."""
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

    async def _navigate_all_to_blank(self) -> None:
        """Навигирует все открытые страницы на about:blank.

        Это отменяет все pending-операции Playwright (goto, wait_for_selector,
        route handlers) на каждой странице. Без этого при закрытии контекста
        Node.js-драйвер пытается отправить ответы/ошибки по pending-операциям
        через pipe — если pipe уже закрыт, получается EPIPE (fatal в Node.js v24+).

        Навигация на about:blank — лёгкая операция (локальная, без сети),
        которая заставляет Playwright отменить все pending-запросы и route
        handlers страницы. После этого context.close() и browser.close()
        не генерируют dispose-событий, требующих записи в pipe.

        Ошибки навигации игнорируются — страница может быть уже закрыта
        или renderer мёртв. Главное — попытка отмены pending-операций.
        """
        if self._context is None:
            return

        try:
            pages = self._context.pages
        except Exception:
            return

        for p in pages:
            try:
                if not p.is_closed():
                    await p.goto(
                        "about:blank",
                        timeout=_BLANK_NAVIGATION_TIMEOUT_MS,
                        wait_until="commit",
                    )
            except Exception:
                # Страница закрыта, renderer мёртв, таймаут — не критично.
                # Главное — мы попытались отменить pending-операции.
                pass

    async def stop(self) -> None:
        """Останавливает браузер и освобождает все ресурсы.

        Последовательность закрытия спроектирована для предотвращения
        ошибки EPIPE в Node.js v24+:

        1. Навигация всех страниц на about:blank — отменяет все pending-
           операции (goto, wait_for_selector, route handlers). Это ключевой
           шаг: именно pending-операции, пытающиеся отправить ответ через
           pipe после его закрытия, вызывают EPIPE.
        2. Закрытие всех дополнительных страниц (вкладок).
        3. Пауза — Node.js обрабатывает page dispose-события.
        4. Закрытие контекста браузера.
        5. Пауза — Node.js обрабатывает context dispose-события.
        6. Закрытие браузера (убивает процесс Chromium).
        7. Увеличенная пауза — Node.js завершает финализацию процесса.
        8. Остановка Playwright (закрывает pipe к Node.js-драйверу).
        9. Восстановление оригинального обработчика исключений asyncio.
        """
        # Шаг 1: Навигируем все страницы на about:blank.
        # Это отменяет pending goto, wait_for_selector, route handlers —
        # после этого закрытие контекста/браузера не генерирует событий,
        # требующих записи в pipe.
        await self._navigate_all_to_blank()

        # Шаг 2: Закрываем все дополнительные страницы
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

        # Шаг 3: Закрываем контекст браузера
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

        # Шаг 4: Закрываем браузер (убивает процесс Chromium)
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

        # Увеличенная пауза перед playwright.stop() — критический момент.
        # browser.close() убивает процесс Chromium, и Node.js-драйвер
        # обрабатывает серию событий (disconnected, closed). Если начать
        # playwright.stop() (закрытие pipe) до завершения обработки —
        # запись в закрытый pipe даёт EPIPE.
        await asyncio.sleep(_CLOSE_DRAIN_BEFORE_STOP)

        # Шаг 5: Останавливаем Playwright с таймаутом
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

        # Шаг 6: Восстанавливаем оригинальный обработчик исключений asyncio
        try:
            loop = asyncio.get_running_loop()
            loop.set_exception_handler(self._original_exception_handler)
            self._original_exception_handler = None
        except RuntimeError:
            pass

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
        """Выполняет случайную паузу между действиями."""
        delay_ms = random.randint(
            self._settings.min_delay_ms,
            self._settings.max_delay_ms,
        )
        delay_seconds = delay_ms / 1000.0
        await asyncio.sleep(delay_seconds)

    async def scroll_page(self) -> None:
        """Плавно прокручивает страницу вниз для имитации поведения пользователя."""
        try:
            await asyncio.wait_for(
                self._scroll_page_inner(),
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

    async def _scroll_page_inner(self) -> None:
        """Внутренняя реализация прокрутки страницы."""
        page = self.page
        viewport_height = page.viewport_size["height"] if page.viewport_size else 1080

        try:
            page_height = await asyncio.wait_for(
                page.evaluate("document.body.scrollHeight"),
                timeout=_SCROLL_STEP_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            logger.warning("не_удалось_получить_высоту_страницы")
            return

        current_position = 0
        steps = 0

        while current_position < page_height and steps < _MAX_SCROLL_STEPS:
            scroll_step = random.randint(
                int(viewport_height * 0.3),
                int(viewport_height * 0.7),
            )
            current_position += scroll_step
            steps += 1

            try:
                await asyncio.wait_for(
                    page.evaluate(f"window.scrollTo(0, {current_position})"),
                    timeout=_SCROLL_STEP_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "шаг_прокрутки_завис",
                    step=f"позиция={current_position}, шаг={steps}",
                )
                break
            except Exception as e:
                logger.debug(
                    "ошибка_шага_прокрутки",
                    error=str(e),
                    error_type=type(e).__name__,
                    step=f"позиция={current_position}, шаг={steps}",
                )
                break

            await asyncio.sleep(random.uniform(0.3, 0.8))

    async def get_page_content(self) -> str:
        """Возвращает HTML-содержимое текущей страницы.

        Returns:
            HTML-строка.
        """
        return await self.page.content()
