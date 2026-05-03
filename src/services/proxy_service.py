"""Сервис управления прокси — загрузка, проверка и распределение."""

import asyncio
from pathlib import Path

from playwright.async_api import async_playwright

from src.config.logger import get_logger
from src.config.settings import Settings
from src.models.listing import RawListing
from src.models.proxy import ProxyConfig

logger = get_logger("proxy")


class ProxyService:
    """Сервис для работы с прокси-серверами.

    Обеспечивает:
    - Загрузку списка прокси из текстового файла.
    - Проверку каждой прокси на работоспособность.
    - Распределение карточек между рабочими прокси.
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

        Для каждой прокси открывает отдельный браузер, переходит на sutochno.ru,
        прокручивает страницу и ждёт 15 секунд. Если загрузка успешна — прокси рабочая.

        Args:
            proxies: Список прокси для проверки.

        Returns:
            Список рабочих прокси.
        """
        logger.info(
            "начало_проверки_прокси",
            total=len(proxies),
        )

        # Проверяем все прокси параллельно
        tasks = [self._check_single_proxy(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        working: list[ProxyConfig] = []
        for proxy, result in zip(proxies, results):
            if isinstance(result, Exception):
                logger.warning(
                    "прокси_недоступна",
                    error=str(result),
                    step=str(proxy),
                )
            elif result is True:
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

    async def _check_single_proxy(self, proxy: ProxyConfig) -> bool:
        """Проверяет одну прокси на работоспособность.

        Открывает браузер с прокси, переходит на sutochno.ru,
        прокручивает страницу и ждёт 15 секунд.

        Args:
            proxy: Прокси для проверки.

        Returns:
            True если прокси работает, False — если нет.
        """
        playwright = None
        browser = None

        try:
            playwright = await async_playwright().start()

            browser = await playwright.chromium.launch(
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

            context = await browser.new_context(
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
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
                window.chrome = {runtime: {}};
            """)

            page = await context.new_page()
            page.set_default_navigation_timeout(30000)

            # Переходим на главную страницу sutochno.ru
            await page.goto("https://sutochno.ru", wait_until="domcontentloaded")

            # Прокручиваем страницу
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            await asyncio.sleep(2)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            # Ждём 15 секунд для полной загрузки
            await asyncio.sleep(15)

            # Проверяем что страница загрузилась (есть контент)
            content = await page.content()
            if len(content) < 1000:
                return False

            await context.close()
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
            if browser:
                await browser.close()
            if playwright:
                await playwright.stop()

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
