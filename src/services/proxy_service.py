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
# Используется при первоначальной проверке всего пула (check_proxies) —
# там точность важнее скорости, прокси проверяются один раз при старте.
_CHECK_NAVIGATION_TIMEOUT_MS: int = 60000

# Время ожидания после загрузки страницы (секунды).
# Достаточно убедиться, что контент подгрузился.
_CHECK_SETTLE_DELAY: float = 5.0

# ── Быстрая проверка (для поиска замены во время восстановления) ──
# get_replacement_proxy() и recovery-логика в enrich_strategies.py
# перебирают кандидатов ПОСЛЕДОВАТЕЛЬНО — один неотвечающий кандидат
# при полном таймауте (60с + 5с устаканивания) стоит ~65 секунд.
# Если подряd попадаются несколько мёртвых кандидатов, прогрев/восстановление
# воркера растягивается на 200-300+ секунд (подтверждено логами прогона).
# Для этого сценария точность менее важна, чем скорость: ложно отбракованная
# медленная-но-живая прокси просто не будет использована в этом раунде,
# тогда как реально мёртвая прокси не должна стоить минуты на отбраковку.
_FAST_CHECK_NAVIGATION_TIMEOUT_MS: int = 12000
_FAST_CHECK_SETTLE_DELAY: float = 2.0

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

        # ── Централизованный реестр занятых прокси ──
        # Раньше "занятость" прокси каждый воркер вычислял локально
        # (in_use_proxies из статичного снимка all_proxies) — из-за этого
        # несколько воркеров могли ОДНОВРЕМЕННО решить, что один и тот же
        # прокси свободен, и захватить его параллельно. Один прокси получал
        # в разы больше соединений, чем допустимо, и сам начинал отваливаться,
        # что запускало каскад повторных замен.
        #
        # self._lock гарантирует, что резервирование кандидата (добавление
        # его server_url в self._claimed_urls) — атомарная операция без
        # сетевых вызовов внутри критической секции. Сама проверка прокси
        # (check_single_proxy, секунды) выполняется уже ВНЕ блокировки —
        # иначе все проверки прокси сериализовались бы в один поток.
        self._lock: asyncio.Lock = asyncio.Lock()
        self._claimed_urls: set[str] = set()

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

    async def check_single_proxy(
        self, proxy: ProxyConfig, fast: bool = False
    ) -> bool:
        """Проверяет одну прокси на работоспособность.

        Открывает браузер с прокси, переходит на sutochno.ru,
        прокручивает страницу и ждёт стабилизации контента.

        Каждый ресурс (context, browser, playwright) закрывается
        в отдельном try/except внутри finally — это гарантирует
        освобождение памяти даже при исключениях на любом шаге.
        Один незакрытый Chromium = ~500 МБ утечки.

        Args:
            proxy: Прокси для проверки.
            fast: Если True — используется сокращённый таймаут навигации
                (_FAST_CHECK_NAVIGATION_TIMEOUT_MS) и минимальная пауза
                устаканивания вместо полной (_CHECK_NAVIGATION_TIMEOUT_MS).
                Предназначено для сценария поиска замены прокси во время
                восстановления воркера (get_replacement_proxy и recovery-
                логика), где кандидаты перебираются последовательно и
                полный таймаут на каждого мёртвого кандидата (~65 секунд)
                растягивает восстановление на минуты. Обычная (нефастовая)
                проверка остаётся для первоначальной проверки всего пула
                прокси при старте — там точность важнее скорости.

        Returns:
            True если прокси работает, False — если нет.
        """
        navigation_timeout_ms = (
            _FAST_CHECK_NAVIGATION_TIMEOUT_MS if fast else _CHECK_NAVIGATION_TIMEOUT_MS
        )
        settle_delay = _FAST_CHECK_SETTLE_DELAY if fast else _CHECK_SETTLE_DELAY

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
            page.set_default_navigation_timeout(navigation_timeout_ms)

            # Переходим на главную страницу sutochno.ru
            await page.goto("https://sutochno.ru", wait_until="domcontentloaded")

            # Прокручиваем страницу. В быстром режиме — без промежуточной
            # паузы между шагами скролла, только финальное устаканивание.
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            if not fast:
                await asyncio.sleep(2)
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            # Ждём стабилизации контента
            await asyncio.sleep(settle_delay)

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

    async def claim_proxy(self, proxy: ProxyConfig) -> None:
        """Резервирует прокси как используемую.

        Вызывается при начальном статичном распределении прокси по
        воркерам — ДО старта конкурентной обработки. Это гарантирует,
        что последующий поиск замены (get_replacement_proxy), вызываемый
        параллельно из разных воркеров, не выберет прокси, которая уже
        занята с самого начала прогона.

        Args:
            proxy: Прокси для резервирования.
        """
        async with self._lock:
            self._claimed_urls.add(proxy.server_url)

    async def release_proxy(self, proxy: ProxyConfig | None) -> None:
        """Снимает резерв с прокси — она снова доступна для замены.

        Args:
            proxy: Прокси для освобождения. None игнорируется (удобно
                вызывать без дополнительной проверки на стороне вызывающего).
        """
        if proxy is None:
            return
        async with self._lock:
            self._claimed_urls.discard(proxy.server_url)

    async def mark_dead(self, proxy: ProxyConfig) -> None:
        """Удаляет прокси из пула рабочих и снимает с неё резерв.

        Вызывается, когда прокси провалила проверку — она не должна
        больше предлагаться как кандидат ни на начальное распределение,
        ни на замену.

        Args:
            proxy: Прокси, провалившая проверку.
        """
        async with self._lock:
            self._claimed_urls.discard(proxy.server_url)
            if proxy in self._working_proxies:
                self._working_proxies.remove(proxy)

    async def _claim_next_candidate(
        self, exclude: set[str]
    ) -> ProxyConfig | None:
        """Атомарно выбирает и резервирует первый свободный рабочий прокси.

        Резервирование (добавление в self._claimed_urls) происходит
        внутри блокировки без каких-либо сетевых вызовов — это единственное
        место, где решается, какому воркеру достанется кандидат. Именно
        атомарность этого шага устраняет гонку за один и тот же прокси.

        Args:
            exclude: Server_url прокси, которые нельзя выбирать
                (текущая неработающая прокси и т.п.).

        Returns:
            Зарезервированный ProxyConfig или None, если свободных
            кандидатов не осталось.
        """
        async with self._lock:
            for candidate in self._working_proxies:
                if candidate.server_url in exclude:
                    continue
                if candidate.server_url in self._claimed_urls:
                    continue
                self._claimed_urls.add(candidate.server_url)
                return candidate
        return None

    async def get_replacement_proxy(
        self,
        current_proxy: ProxyConfig | None,
        in_use_proxies: list[ProxyConfig] | None = None,
    ) -> ProxyConfig | None:
        """Атомарно резервирует и проверяет рабочую замену для прокси.

        В отличие от предыдущей реализации, занятость прокси отслеживается
        централизованно (self._claimed_urls) под блокировкой, а не
        вычисляется каждым воркером локально из статичного снимка списка
        прокси всех воркеров. Это устраняет гонку: раньше несколько
        воркеров могли одновременно посчитать один и тот же прокси
        свободным и захватить его параллельно.

        Текущая прокси (current_proxy) считается уже провалившей проверку
        на момент вызова этого метода — она немедленно удаляется из пула
        рабочих (mark_dead), а не просто освобождается.

        Args:
            current_proxy: Текущая прокси, которая не прошла проверку.
            in_use_proxies: Устаревший параметр, оставлен только для
                обратной совместимости старых вызовов. Дополнительно
                исключает перечисленные прокси из кандидатов. Обычно
                передавать не нужно — занятость теперь отслеживается
                внутри сервиса через claim_proxy/release_proxy.

        Returns:
            Зарезервированная рабочая прокси или None, если рабочих
            кандидатов не осталось.
        """
        if current_proxy is not None:
            await self.mark_dead(current_proxy)

        legacy_exclude = {p.server_url for p in (in_use_proxies or [])}

        while True:
            candidate = await self._claim_next_candidate(exclude=legacy_exclude)

            if candidate is None:
                logger.warning(
                    "нет_кандидатов_для_замены",
                    step=f"рабочих={len(self._working_proxies)}",
                )
                return None

            logger.info("поиск_замены_прокси", step=f"кандидат={candidate}")

            is_working = await self.check_single_proxy(candidate, fast=True)

            if is_working:
                logger.info("замена_прокси_найдена", step=str(candidate))
                return candidate

            logger.warning("кандидат_не_прошёл_проверку", step=str(candidate))
            await self.mark_dead(candidate)

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
