"""Сервис управления прокси — загрузка, проверка и распределение."""

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig
from src.services.browser_service import _STEALTH_INIT_SCRIPT

logger = get_logger("proxy")

# Максимальное количество одновременных проверок прокси.
# Каждая проверка запускает свой Chromium (~500 МБ RAM).
# На сервере с 8 ГБ RAM безопасно проверять 3 одновременно.
_MAX_CONCURRENT_CHECKS: int = 3

# Таймаут навигации при проверке прокси (мс).
# Увеличен по сравнению со стандартным — прокси могут быть медленными.
_CHECK_NAVIGATION_TIMEOUT_MS: int = 60000

# Время ожидания после загрузки страницы (секунды).
# Достаточно убедиться, что контент подгрузился.
_CHECK_SETTLE_DELAY: float = 5.0

# Уменьшенный viewport для проверки прокси — Full HD не нужен,
# экономим ~30% памяти на рендеринг страницы при каждой проверке.
_CHECK_VIEWPORT: dict[str, int] = {"width": 1280, "height": 720}

# Аргументы запуска Chromium для проверки прокси.
# Минимальный набор для стабильности + экономия памяти.
_CHECK_BROWSER_ARGS: list[str] = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
    "--js-flags=--max-old-space-size=256",
    "--renderer-process-limit=1",
    "--disable-renderer-backgrounding",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
]


class ProxyService:
    """Сервис для работы с прокси-серверами.

    Обеспечивает:
    - Загрузку списка прокси из текстового файла.
    - Проверку каждой прокси на работоспособность.
    - Распределение карточек между рабочими прокси.
    - Проверку и замену прокси при сбоях соединения.
    """

    def __init__(self, settings: Settings) -> None:
        """Инициализирует сервис.

        Args:
            settings: Настройки приложения.
        """
        self._settings = settings
        self._working_proxies: list[ProxyConfig] = []

    @property
    def working_proxies(self) -> list[ProxyConfig]:
        """Возвращает список рабочих прокси после проверки.

        Returns:
            Список проверенных рабочих прокси.
        """
        return self._working_proxies

    @property
    def has_working_proxies(self) -> bool:
        """Проверяет, есть ли рабочие прокси.

        Returns:
            True если есть хотя бы одна рабочая прокси.
        """
        return len(self._working_proxies) > 0

    def load_proxies(self) -> list[ProxyConfig]:
        """Загружает список прокси из файла.

        Читает файл построчно, пропускает пустые строки и комментарии.

        Returns:
            Список загруженных прокси.

        Raises:
            RuntimeError: Если файл пуст или не содержит валидных прокси.
        """
        proxies_path = Path(self._settings.proxies_path)

        if not proxies_path.exists():
            raise RuntimeError(
                f"Файл прокси не найден: {self._settings.proxies_path}"
            )

        proxies: list[ProxyConfig] = []
        lines = proxies_path.read_text(encoding="utf-8").splitlines()

        for line_num, line in enumerate(lines, start=1):
            line = line.strip()
            # Пропускаем пустые строки и комментарии
            if not line or line.startswith("#"):
                continue

            try:
                proxy = ProxyConfig.from_string(line)
                proxies.append(proxy)
            except ValueError as e:
                logger.warning(
                    "ошибка_парсинга_прокси",
                    error=str(e),
                    step=f"строка={line_num}",
                )

        if not proxies:
            raise RuntimeError(
                f"Файл прокси не содержит валидных записей: {self._settings.proxies_path}"
            )

        logger.info(
            "прокси_загружены",
            total=len(proxies),
            path=self._settings.proxies_path,
        )
        return proxies

    async def check_proxies(self, proxies: list[ProxyConfig]) -> list[ProxyConfig]:
        """Проверяет работоспособность каждой прокси.

        Проверка выполняется пакетами — не более _MAX_CONCURRENT_CHECKS
        одновременно, чтобы не перегружать RAM/CPU сервера
        (каждая проверка запускает отдельный процесс Chromium).

        Args:
            proxies: Список прокси для проверки.

        Returns:
            Список рабочих прокси.
        """
        logger.info(
            "начало_проверки_прокси",
            total=len(proxies),
            step=f"параллельно={_MAX_CONCURRENT_CHECKS}",
        )

        semaphore = asyncio.Semaphore(_MAX_CONCURRENT_CHECKS)

        async def _check_with_limit(proxy: ProxyConfig) -> tuple[ProxyConfig, bool]:
            """Проверяет прокси с ограничением параллельности."""
            async with semaphore:
                result = await self.check_single_proxy(proxy)
                return proxy, result

        tasks = [_check_with_limit(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        working: list[ProxyConfig] = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(
                    "прокси_ошибка_проверки",
                    error=str(result),
                    error_type=type(result).__name__,
                )
            elif isinstance(result, tuple):
                proxy, is_working = result
                if is_working:
                    working.append(proxy)
                    logger.info(
                        "прокси_работает",
                        step=str(proxy),
                    )
                else:
                    logger.warning(
                        "прокси_недоступна",
                        step=str(proxy),
                    )

        self._working_proxies = working

        logger.info(
            "проверка_прокси_завершена",
            total=len(working),
            step=f"из {len(proxies)}",
        )
        return working

    async def check_single_proxy(self, proxy: ProxyConfig) -> bool:
        """Проверяет одну прокси на работоспособность.

        Открывает браузер с прокси, переходит на sutochno.ru,
        прокручивает страницу и ждёт стабилизации контента.

        Каждый ресурс (context, browser, playwright) закрывается
        в отдельном try/except внутри finally — это гарантирует
        освобождение памяти даже при исключениях на любом шаге.
        Один незакрытый Chromium = ~500 МБ утечки.

        Args:
            proxy: Прокси для проверки.

        Returns:
            True если прокси работает, False — если нет.
        """
        playwright = None
        browser = None
        context = None

        try:
            playwright = await async_playwright().start()

            browser = await playwright.chromium.launch(
                headless=self._settings.headless_mode,
                proxy={
                    "server": proxy.server_url,
                    "username": proxy.username,
                    "password": proxy.password,
                },
                args=_CHECK_BROWSER_ARGS,
            )

            context = await browser.new_context(
                viewport=_CHECK_VIEWPORT,
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
            )

            # Скрываем признаки автоматизации
            await context.add_init_script(_STEALTH_INIT_SCRIPT)

            page = await context.new_page()
            page.set_default_navigation_timeout(_CHECK_NAVIGATION_TIMEOUT_MS)

            # Переходим на главную страницу sutochno.ru
            await page.goto("https://sutochno.ru", wait_until="domcontentloaded")

            # Прокручиваем страницу
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            await asyncio.sleep(2)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            # Ждём стабилизации контента
            await asyncio.sleep(_CHECK_SETTLE_DELAY)

            # Проверяем что страница загрузилась (есть контент)
            content = await page.content()
            if len(content) < 1000:
                return False

            return True

        except Exception as e:
            logger.debug(
                "ошибка_проверки_прокси",
                error=str(e),
                error_type=type(e).__name__,
                step=str(proxy),
            )
            return False

        finally:
            # Каждый ресурс закрывается отдельно — если context.close()
            # бросит исключение, browser и playwright всё равно закроются.
            # Без этого: один сбой в close() = утечка процесса Chromium (~500 МБ).
            if context is not None:
                try:
                    await context.close()
                except Exception:
                    pass

            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass

            if playwright is not None:
                try:
                    await playwright.stop()
                except Exception:
                    pass

    async def get_replacement_proxy(
        self,
        current_proxy: ProxyConfig | None,
        in_use_proxies: list[ProxyConfig] | None = None,
    ) -> ProxyConfig | None:
        """Ищет рабочую замену для текущей прокси из пула.

        Перебирает рабочие прокси, исключая текущую и занятые другими
        воркерами. Для каждого кандидата выполняет проверку через
        check_single_proxy. Возвращает первую прошедшую проверку.

        Args:
            current_proxy: Текущая прокси, которая не прошла проверку
                (исключается из кандидатов).
            in_use_proxies: Список прокси, занятых другими воркерами
                (исключаются из кандидатов).

        Returns:
            Рабочая прокси для замены или None если замена не найдена.
        """
        if in_use_proxies is None:
            in_use_proxies = []

        # Собираем множество прокси, которые нельзя использовать
        excluded: set[str] = set()
        if current_proxy is not None:
            excluded.add(current_proxy.server_url)
        for proxy in in_use_proxies:
            excluded.add(proxy.server_url)

        # Ищем кандидатов среди рабочих прокси
        candidates = [
            p for p in self._working_proxies
            if p.server_url not in excluded
        ]

        if not candidates:
            logger.warning(
                "нет_кандидатов_для_замены",
                step=f"рабочих={len(self._working_proxies)}, "
                     f"исключено={len(excluded)}",
            )
            return None

        logger.info(
            "поиск_замены_прокси",
            step=f"кандидатов={len(candidates)}",
        )

        # Проверяем кандидатов последовательно — возвращаем первую рабочую
        for candidate in candidates:
            is_working = await self.check_single_proxy(candidate)
            if is_working:
                logger.info(
                    "замена_прокси_найдена",
                    step=str(candidate),
                )
                return candidate
            else:
                logger.warning(
                    "кандидат_не_прошёл_проверку",
                    step=str(candidate),
                )
                # Убираем из списка рабочих — она больше не работает
                if candidate in self._working_proxies:
                    self._working_proxies.remove(candidate)

        logger.warning(
            "замена_прокси_не_найдена",
            step=f"проверено={len(candidates)}, ни одна не работает",
        )
        return None

    @staticmethod
    def distribute_listings(
        listings: list[RawListing], proxy_count: int
    ) -> list[list[RawListing]]:
        """Распределяет карточки поровну между прокси.

        Если карточек не делится поровну, остаток распределяется
        по одной на первые прокси.

        Args:
            listings: Общий список карточек.
            proxy_count: Количество рабочих прокси.

        Returns:
            Список списков — порция карточек для каждой прокси.
        """
        if proxy_count <= 0:
            return [listings]

        chunks: list[list[RawListing]] = [[] for _ in range(proxy_count)]

        for idx, listing in enumerate(listings):
            chunk_idx = idx % proxy_count
            chunks[chunk_idx].append(listing)

        return chunks
