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
    # Один процесс вместо отдельных renderer'ов на каждую вкладку.
    # Предотвращает ситуацию, когда OOM-killer убивает отдельный renderer
    # (Target crashed), оставляя главный процесс живым и неработоспособным.
    # С --single-process Chromium управляет памятью в рамках одного процесса,
    # и при нехватке RAM завершается целиком (что обрабатывается retry-логикой).
    "--single-process",
    # Ограничиваем JS-хип до 256 МБ.
    # Для парсинга каталога sutochno.ru (не тяжёлое SPA) достаточно с запасом.
    # С --single-process это единый лимит на весь JS-контекст.
    "--js-flags=--max-old-space-size=256",
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
    # Отключаем PaintHolding (буферизация рендеринга до полной загрузки —
    # расходует RAM на хранение промежуточных фреймов) и ImageDecodeService
    # (декодирование изображений в отдельных потоках — расходует RAM).
    "--disable-features=PaintHolding,ImageDecodeService",
    # Полностью отключаем загрузку изображений.
    # Для парсинга каталога изображения не нужны — данные извлекаются из текста.
    # Каждая страница содержит ~50 карточек с фото — это основной потребитель
    # памяти renderer'а. Отключение экономит ~200-400 МБ на страницу.
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

    async def is_alive(self) -> bool:
        """Проверяет, жив ли renderer процесс страницы.

        Отправляет минимальный evaluate-запрос с таймаутом.
        Если renderer мёртв (Target crashed) или завис — возвращает False.

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

        Прокручивает порциями с небольшими паузами между ними.
        Защищена тройным таймаутом:
        1. Общий таймаут на всю прокрутку (_SCROLL_TIMEOUT_SECONDS).
        2. Таймаут на каждый шаг evaluate (_SCROLL_STEP_TIMEOUT_SECONDS).
        3. Лимит шагов (_MAX_SCROLL_STEPS) — защита от infinite scroll.

        Если любой таймаут сработал — прокрутка прекращается штатно
        с предупреждением в логах. Парсинг продолжается с тем,
        что успело загрузиться — карточки уже в DOM после domcontentloaded.
        """
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
        """Внутренняя реализация прокрутки страницы.

        Вынесена из scroll_page для обёртки в asyncio.wait_for.
        Каждый вызов page.evaluate обёрнут в отдельный таймаут —
        если Chromium завис на одном шаге, цикл прерывается
        без блокировки всего процесса.
        """
        page = self.page
        viewport_height = page.viewport_size["height"] if page.viewport_size else 1080

        # Получаем высоту страницы с таймаутом
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
