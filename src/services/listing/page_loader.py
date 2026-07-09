"""Загрузка страницы карточки и перехват токена API."""

import asyncio
from typing import TYPE_CHECKING

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.constants import (
    GOTO_RETRY_DELAY,
    MAX_GOTO_RETRIES,
)

if TYPE_CHECKING:
    from src.services.listing.concurrency_controller import ConcurrencyController
    from src.services.listing.connection_monitor import ConnectionMonitor

logger = get_logger("page_loader")

# Сетевые ошибки, при которых навигация считается провалом нагрузки.
# Эти ошибки репортятся в ConcurrencyController для адаптации лимита.
_NETWORK_ERROR_MARKERS: tuple[str, ...] = (
    "ERR_EMPTY_RESPONSE",
    "ERR_TIMED_OUT",
    "ERR_CONNECTION_RESET",
    "ERR_CONNECTION_CLOSED",
    "ERR_CONNECTION_REFUSED",
    "ERR_PROXY_CONNECTION_FAILED",
    "ERR_TUNNEL_CONNECTION_FAILED",
    "NS_ERROR_NET_RESET",
    "Timeout",
)

# Количество подряд идущих сетевых ошибок, после которого retry
# прерывается досрочно. При бане IP третья попытка с того же адреса
# гарантированно бесполезна — экономим 5+ секунд на каждой карточке.
_CONSECUTIVE_NETWORK_ERRORS_LIMIT: int = 2

# Максимальное время ожидания токена после domcontentloaded (секунды).
# Фронтенд sutochno.ru отправляет API-запрос (с токеном в заголовке)
# практически сразу при загрузке страницы — обычно токен перехватывается
# ещё ДО domcontentloaded. Но в редких случаях (медленная прокси,
# тяжёлый JS-бандл) запрос может уйти чуть позже.
# 3 секунды — достаточный запас для перехвата без бесполезного ожидания
# networkidle (10 сек) и CSS-селекторов (до 45 сек).
_TOKEN_WAIT_AFTER_DOM_SECONDS: float = 3.0

# Интервал проверки наличия токена внутри цикла ожидания (секунды).
# Мелкий шаг позволяет выйти из ожидания практически мгновенно
# после перехвата, не дожидаясь полных _TOKEN_WAIT_AFTER_DOM_SECONDS.
_TOKEN_POLL_INTERVAL_SECONDS: float = 0.2


class PageLoader:
    """Загрузка страницы карточки с retry и перехватом токена через route interception.

    Оптимизация: после domcontentloaded не ожидает networkidle и
    появления CSS-селекторов. Вместо этого ждёт только перехвата токена
    (до _TOKEN_WAIT_AFTER_DOM_SECONDS). Токен — единственное, что нужно
    от страницы; все данные потом получаются через fetch() в контексте
    браузера, а не из DOM.

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
    ) -> tuple[bool, str | None, bool]:
        """Загружает страницу карточки и перехватывает токен API.

        Перехват выполняется через page.route — надёжнее page.on('request'),
        так как гарантирует перехват даже для запросов из iframe,
        service workers или асинхронных init-скриптов.

        Оптимизация: после domcontentloaded не ожидает networkidle
        (10 сек) и появления CSS-селекторов (до 45 сек). Вместо этого
        ждёт только перехвата токена — обычно токен появляется ещё до
        domcontentloaded, поэтому дополнительное ожидание минимально
        (0–3 секунды). Это экономит 10–25 секунд на каждой карточке.

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов монитора).

        Returns:
            Кортеж из трёх элементов:
            - loaded (bool): True если страница загружена (навигация
              завершилась без сетевой ошибки и без редиректа за пределы сайта).
            - token (str | None): Перехваченный сессионный токен API или None.
            - elements_not_found (bool): Всегда False (сохранён для обратной
              совместимости с listing_service — CSS-селекторы больше не
              проверяются, так как данные получаются через API, а не DOM).
        """
        # Проверяем — не требуется ли уже перезапуск браузера
        if self._monitor and self._monitor.should_skip():
            logger.debug(
                "загрузка_пропущена_перезапуск_требуется",
                step=f"id={object_id}",
            )
            return False, None, False

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
            loaded = await self.goto_with_retry(
                page, url, object_id, captured_token
            )

            # Если страница загрузилась, но токен ещё не перехвачен —
            # даём короткое время на перехват. Фронтенд мог отправить
            # API-запрос с небольшой задержкой после domcontentloaded.
            if loaded and not captured_token:
                await self._wait_for_token(captured_token, object_id)

        finally:
            # unroute безопасен даже если page уже закрыта —
            # Playwright просто проигнорирует вызов
            try:
                await page.unroute("**/api/json/**")
            except Exception:
                pass

        token = captured_token[0] if captured_token else None

        if token:
            logger.debug(
                "токен_перехвачен",
                step=f"длина={len(token)}, источник=route_interception",
            )
        else:
            logger.warning("токен_не_перехвачен_при_загрузке")

        # elements_not_found=False всегда — CSS-селекторы больше не проверяются
        return loaded, token, False

    async def goto_with_retry(
        self,
        page: Page,
        url: str,
        object_id: str = "",
        captured_token: list[str] | None = None,
    ) -> bool:
        """Загружает страницу карточки с повторными попытками.

        При сетевых ошибках (таймаут, сброс соединения, проблемы прокси)
        повторяет попытку с паузой. Ожидает только domcontentloaded —
        networkidle и CSS-селекторы больше не проверяются.

        Раннее прерывание: если две подряд попытки завершились сетевой
        ошибкой (любой из _NETWORK_ERROR_MARKERS) — retry прекращается
        досрочно без третьей попытки. При бане IP третья попытка с того
        же адреса гарантированно бесполезна — экономим 5+ секунд.

        Если токен перехвачен (captured_token не пуст) — domcontentloaded
        считается достаточным для успеха, даже без проверки DOM-элементов.

        Репортит результаты в два компонента:
        - ConnectionMonitor: для детектирования массовых локальных сбоев
          (2 подряд → перезапуск браузера).
        - ConcurrencyController: для глобальной адаптации параллелизма
          (>30% ошибок → снижение лимита + cooldown).

        Args:
            page: Вкладка браузера.
            url: URL карточки.
            object_id: ID объявления (для логов монитора).
            captured_token: Разделяемый список для перехваченного токена.
                Если не None — используется для раннего определения успеха.

        Returns:
            True если страница загружена (domcontentloaded без редиректа),
            False — если все попытки исчерпаны.
        """
        # Быстрая проверка перед началом retry-цикла
        if self._monitor and self._monitor.should_skip():
            logger.debug(
                "goto_пропущен_перезапуск_требуется",
                step=f"id={object_id}",
            )
            return False

        # Счётчик подряд идущих сетевых ошибок. Сбрасывается при любом
        # успехе (страница загрузилась). При достижении лимита — досрочный
        # выход: с забаненного IP следующая попытка гарантированно провалится.
        consecutive_network_errors: int = 0

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

                # Страница загрузилась — сбрасываем счётчик сетевых ошибок
                consecutive_network_errors = 0

                # Проверяем URL: если нас увели со страницы карточки —
                # это явный признак блокировки (CAPTCHA, антибот).
                current_url = page.url
                if "sutochno.ru" not in current_url:
                    logger.warning(
                        "редирект_за_пределы_сайта",
                        path=current_url,
                        step=f"попытка={attempt}",
                    )
                    if self._controller:
                        self._controller.report_failure()
                    if attempt < MAX_GOTO_RETRIES:
                        await asyncio.sleep(GOTO_RETRY_DELAY)
                        continue
                    # Все попытки исчерпаны — редирект на каждой
                    if self._monitor:
                        await self._monitor.report_failure(object_id)
                    return False

                # ── Успех: domcontentloaded + URL в порядке ──
                # Токен уже мог быть перехвачен route_handler'ом во время
                # навигации — если да, логируем это как бонус.
                token_status = "да" if (captured_token and captured_token) else "нет"
                logger.debug(
                    "страница_загружена",
                    step=f"попытка={attempt}, токен_уже_перехвачен={token_status}",
                )

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
                    consecutive_network_errors += 1

                    # Сетевая ошибка — репортим в контроллер для адаптации.
                    if self._controller:
                        self._controller.report_failure()

                    # ── Раннее прерывание при подряд идущих сетевых ошибках ──
                    if consecutive_network_errors >= _CONSECUTIVE_NETWORK_ERRORS_LIMIT:
                        logger.warning(
                            "досрочное_прерывание_сетевых_ошибок",
                            error=error_msg[:200],
                            step=f"id={object_id}, "
                                 f"попытка={attempt}/{MAX_GOTO_RETRIES}, "
                                 f"подряд_сетевых={consecutive_network_errors}",
                        )
                        if self._monitor:
                            await self._monitor.report_failure(object_id)
                        return False

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
                if self._controller and not is_network_error:
                    self._controller.report_failure()
                return False

        # Сюда попадаем если цикл завершился без return (теоретически не должно)
        if self._monitor:
            await self._monitor.report_failure(object_id)
        if self._controller:
            self._controller.report_failure()
        return False

    @staticmethod
    async def _wait_for_token(
        captured_token: list[str],
        object_id: str,
    ) -> None:
        """Ожидает перехвата токена с коротким поллингом.

        Вызывается если после domcontentloaded токен ещё не перехвачен.
        Ждёт до _TOKEN_WAIT_AFTER_DOM_SECONDS, проверяя каждые
        _TOKEN_POLL_INTERVAL_SECONDS. Выходит досрочно как только
        токен появился.

        Args:
            captured_token: Разделяемый список — route_handler добавляет
                токен сюда при перехвате.
            object_id: ID объявления (для логов).
        """
        elapsed = 0.0
        while elapsed < _TOKEN_WAIT_AFTER_DOM_SECONDS:
            if captured_token:
                logger.debug(
                    "токен_перехвачен_после_ожидания",
                    step=f"id={object_id}, ожидание={elapsed:.1f}с",
                )
                return
            await asyncio.sleep(_TOKEN_POLL_INTERVAL_SECONDS)
            elapsed += _TOKEN_POLL_INTERVAL_SECONDS

        logger.debug(
            "токен_не_перехвачен_за_лимит",
            step=f"id={object_id}, лимит={_TOKEN_WAIT_AFTER_DOM_SECONDS}с",
        )
