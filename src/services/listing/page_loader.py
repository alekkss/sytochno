"""Загрузка страницы карточки и перехват токена API."""

import asyncio
import re
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.constants import (
    GOTO_RETRY_DELAY,
    MAX_GOTO_RETRIES,
    NETWORKIDLE_SOFT_TIMEOUT_MS,
    PAGE_READY_SELECTORS,
    PAGE_READY_TIMEOUT_MS,
)

if TYPE_CHECKING:
    from src.services.listing.concurrency_controller import ConcurrencyController
    from src.services.listing.connection_monitor import ConnectionMonitor

logger = get_logger("page_loader")

# Сетевые ошибки, при которых навигация считается провалом нагрузки.
# Эти ошибки репортятся в ConcurrencyController для адаптации лимита.
_NETWORK_ERROR_MARKERS: tuple[str, ...] = (
    "ERR_EMPTY_RESPONSE",
    "ERR_SOCKET_NOT_CONNECTED",
    "ERR_TIMED_OUT",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_CONNECTION_REFUSED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "NS_ERROR_NET_RESET",
    "Timeout",
)

# Шаблон альтернативного URL карточки через фронтенд-роутер sutochno.ru.
# Используется как fallback при сетевых ошибках на прямом URL.
_FALLBACK_URL_TEMPLATE: str = "https://sutochno.ru/front/searchapp/detail/{object_id}"

# Паттерн для извлечения числового ID из URL карточки.
# Примеры: https://sutochno.ru/1519545, https://spb.sutochno.ru/1257263
_LISTING_ID_PATTERN: re.Pattern[str] = re.compile(r"/(\d{4,})$")


def _build_fallback_url(original_url: str) -> str | None:
    """Формирует альтернативный URL карточки для fallback при сетевых ошибках.

    Извлекает числовой ID объявления из оригинального URL и подставляет
    его в шаблон /front/searchapp/detail/{id}.

    Args:
        original_url: Оригинальный URL карточки
            (например, https://sutochno.ru/1519545).

    Returns:
        Альтернативный URL или None, если ID не удалось извлечь.
    """
    match = _LISTING_ID_PATTERN.search(original_url)
    if not match:
        return None
    object_id = match.group(1)
    return _FALLBACK_URL_TEMPLATE.format(object_id=object_id)


class PageLoader:
    """Загрузка страницы карточки с retry и перехватом токена через route interception.

    При наличии ConnectionMonitor репортит результаты загрузки —
    это позволяет централизованно детектировать массовые сбои
    соединения/прокси и инициировать перезапуск браузера.

    При наличии ConcurrencyController репортит успехи и провалы —
    это позволяет глобально адаптировать параллелизм по обратной связи.
    """

    def __init__(
        self,
        monitor: "ConnectionMonitor | None" = None,
        concurrency_controller: "ConcurrencyController | None" = None,
    ) -> None:
        """Инициализирует загрузчик страниц.

        Args:
            monitor: Монитор здоровья соединения (опциональный).
                Если передан — результаты загрузки репортятся в монитор.
            concurrency_controller: Глобальный контроллер параллелизма
                (опциональный). Если передан — успехи и провалы навигации
                репортятся для адаптации лимита.
        """
        self._monitor = monitor
        self._controller = concurrency_controller

    @property
    def monitor(self) -> "ConnectionMonitor | None":
        """Возвращает текущий монитор соединения.

        Returns:
            Экземпляр ConnectionMonitor или None.
        """
        return self._monitor

    @monitor.setter
    def monitor(self, value: "ConnectionMonitor | None") -> None:
        """Устанавливает монитор соединения.

        Args:
            value: Новый монитор или None для отключения.
        """
        self._monitor = value

    @property
    def concurrency_controller(self) -> "ConcurrencyController | None":
        """Возвращает текущий контроллер параллелизма.

        Returns:
            Экземпляр ConcurrencyController или None.
        """
        return self._controller

    @concurrency_controller.setter
    def concurrency_controller(self, value: "ConcurrencyController | None") -> None:
        """Устанавливает контроллер параллелизма.

        Args:
            value: Новый контроллер или None для отключения.
        """
        self._controller = value

    async def goto_and_capture_token(
        self, page: Page, url: str, object_id: str = ""
    ) -> tuple[bool, str | None]:
        """Загружает страницу карточки и перехватывает токен API.

        Перехват выполняется через page.route — надёжнее page.on('request'),
        так как гарантирует перехват даже для запросов из iframe,
        service workers или асинхронных init-скриптов.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов монитора).

        Returns:
            Кортеж (страница_загружена, токен_или_None).
        """
        # Проверяем — не требуется ли уже перезапуск браузера
        if self._monitor and self._monitor.should_skip():
            logger.debug(
                "загрузка_пропущена_перезапуск_требуется",
                step=f"id={object_id}",
            )
            return False, None

        captured_token: list[str] = []

        async def _route_handler(route: "any") -> None:  # type: ignore[name-defined]
            req = route.request
            if "sutochno.ru/api/json" in req.url:
                token = req.headers.get("token") or req.headers.get("Token")
                if token and not captured_token:
                    captured_token.append(token)
            # При навигации (page.goto) Playwright автоматически отменяет
            # pending-запросы текущей страницы. Если route.continue_()
            # вызывается для уже отменённого запроса — выбрасывается
            # "Route is already handled!". Это не ошибка логики —
            # безопасно игнорируем, чтобы не ронять page.evaluate().
            try:
                await route.continue_()
            except Exception as e:
                if "Route is already handled" in str(e):
                    logger.debug(
                        "route_уже_обработан_пропущен",
                        step=f"id={object_id}",
                    )
                else:
                    raise

        await page.route("**/api/json/**", _route_handler)

        try:
            loaded = await self.goto_with_retry(page, url, object_id)
            await asyncio.sleep(1.0)
        finally:
            await page.unroute("**/api/json/**")

        token = captured_token[0] if captured_token else None

        if token:
            logger.debug(
                "токен_перехвачен",
                step=f"длина={len(token)}, источник=route_interception",
            )
        else:
            logger.warning("токен_не_перехвачен_при_загрузке")

        return loaded, token

    async def goto_with_retry(self, page: Page, url: str, object_id: str = "") -> bool:
        """Загружает страницу карточки с повторными попытками.

        При сетевых ошибках (таймаут, сброс соединения, проблемы прокси)
        повторяет попытку с паузой. Ожидает domcontentloaded, затем
        пытается дождаться networkidle (мягкий таймаут), затем проверяет
        наличие ключевых элементов.

        При первой сетевой ошибке пробует альтернативный URL
        (/front/searchapp/detail/{id}) — один раз. Если он тоже
        не сработал — продолжает retry на оригинальном URL.

        Репортит результаты в два компонента:
        - ConnectionMonitor: для детектирования массовых локальных сбоев
          (2 подряд → перезапуск браузера).
        - ConcurrencyController: для глобальной адаптации параллелизма
          (>30% ошибок → снижение лимита + cooldown).

        После полного провала (все попытки исчерпаны) репортит сбой
        в оба компонента. При успехе — сбрасывает счётчик монитора
        и репортит успех в контроллер.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов монитора).

        Returns:
            True если страница загружена, False — если все попытки исчерпаны.
        """
        # Быстрая проверка перед началом retry-цикла
        if self._monitor and self._monitor.should_skip():
            logger.debug(
                "goto_пропущен_перезапуск_требуется",
                step=f"id={object_id}",
            )
            return False

        # Флаг: альтернативный URL уже был опробован
        fallback_attempted: bool = False

        for attempt in range(1, MAX_GOTO_RETRIES + 1):
            # Проверяем перед каждой попыткой — другая вкладка могла
            # зафиксировать критическое количество сбоев
            if self._monitor and self._monitor.should_skip():
                logger.debug(
                    "goto_прерван_перезапуск_требуется",
                    step=f"id={object_id}, попытка={attempt}",
                )
                return False

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
                    # Успех — репортим в монитор и контроллер
                    if self._monitor:
                        await self._monitor.report_success(object_id)
                    if self._controller:
                        self._controller.report_success()
                    return True

                # Ключевые элементы не найдены — возможна CAPTCHA, редирект
                # или изменение вёрстки. Проверяем URL: если нас увели
                # со страницы карточки — это явный признак блокировки.
                current_url = page.url
                if "sutochno.ru" not in current_url:
                    logger.warning(
                        "редирект_за_пределы_сайта",
                        path=current_url,
                        step=f"попытка={attempt}",
                    )
                    # Редирект — это признак блокировки, репортим провал
                    if self._controller:
                        self._controller.report_failure()
                    if attempt < MAX_GOTO_RETRIES:
                        await asyncio.sleep(GOTO_RETRY_DELAY)
                        continue
                    # Все попытки исчерпаны — редирект на каждой
                    if self._monitor:
                        await self._monitor.report_failure(object_id)
                    return False

                # URL в порядке, но элементы не найдены — вёрстка могла
                # измениться. Продолжаем: токен может быть уже перехвачен.
                logger.warning(
                    "элементы_карточки_не_найдены",
                    path=current_url,
                    step=f"попытка={attempt}",
                )
                # Считаем частичным успехом — страница загрузилась
                if self._monitor:
                    await self._monitor.report_success(object_id)
                if self._controller:
                    self._controller.report_success()
                return True

            except Exception as e:
                error_msg = str(e)
                is_network_error = any(
                    err in error_msg for err in _NETWORK_ERROR_MARKERS
                )

                if is_network_error:
                    # Сетевая ошибка — репортим в контроллер для адаптации.
                    # Репортим КАЖДУЮ попытку, а не только финальный провал —
                    # это даёт контроллеру более быструю обратную связь.
                    if self._controller:
                        self._controller.report_failure()

                    # --- Fallback на альтернативный URL (одна попытка) ---
                    # При первой сетевой ошибке пробуем /front/searchapp/detail/{id}.
                    # Если он тоже не сработал — продолжаем retry на оригинальном.
                    if not fallback_attempted:
                        fallback_attempted = True
                        fallback_url = _build_fallback_url(url)

                        if fallback_url:
                            logger.info(
                                "fallback_альтернативный_url",
                                path=fallback_url,
                                step=f"id={object_id}, после_попытки={attempt}",
                            )
                            try:
                                await page.goto(
                                    fallback_url,
                                    wait_until="domcontentloaded",
                                    timeout=30000,
                                )

                                try:
                                    await page.wait_for_load_state(
                                        "networkidle",
                                        timeout=NETWORKIDLE_SOFT_TIMEOUT_MS,
                                    )
                                except Exception:
                                    pass

                                page_ready = await self.wait_for_page_ready(page)
                                if page_ready:
                                    logger.info(
                                        "fallback_успех",
                                        path=fallback_url,
                                        step=f"id={object_id}",
                                    )
                                    if self._monitor:
                                        await self._monitor.report_success(object_id)
                                    if self._controller:
                                        self._controller.report_success()
                                    return True

                                # Элементы не найдены, но URL на sutochno.ru
                                current_url = page.url
                                if "sutochno.ru" in current_url:
                                    logger.warning(
                                        "fallback_элементы_не_найдены",
                                        path=current_url,
                                        step=f"id={object_id}",
                                    )
                                    if self._monitor:
                                        await self._monitor.report_success(object_id)
                                    if self._controller:
                                        self._controller.report_success()
                                    return True

                                logger.warning(
                                    "fallback_редирект",
                                    path=current_url,
                                    step=f"id={object_id}",
                                )

                            except Exception as fallback_err:
                                fallback_error_msg = str(fallback_err)
                                logger.warning(
                                    "fallback_не_удался",
                                    error=fallback_error_msg[:200],
                                    step=f"id={object_id}",
                                )

                    # --- Конец fallback-логики ---

                    if attempt < MAX_GOTO_RETRIES:
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
                # Все попытки исчерпаны — репортим сбой в монитор
                if self._monitor:
                    await self._monitor.report_failure(object_id)
                # Если последняя попытка не была сетевой ошибкой —
                # репортим и в контроллер (неизвестная ошибка тоже сигнал)
                if self._controller and not is_network_error:
                    self._controller.report_failure()
                return False

        # Сюда попадаем если цикл завершился без return (теоретически не должно)
        if self._monitor:
            await self._monitor.report_failure(object_id)
        if self._controller:
            self._controller.report_failure()
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
