"""Сервис загрузки занятых периодов объекта через orders/getOrdersByObject.

Источник истины для разделения «продан» и «техблок»:
эндпоинт возвращает calendars[] — список занятых периодов (брони и
блокировки владельца). Занятые ночи интервала = [date_begin; date_end),
дата выезда (время 12:00) свободна, заезд — 15:00.

Скользящее окно getPricesAndAvailabilities не способно отличить «продан»
от «свободен, но заблокирован ограничением минимального срока»: busy —
свойство всего окна, а не дня заезда. Данный сервис запрашивает точный
список занятых ночей, по которому batch-обогащение переклассифицирует
календарь: дата из calendars[] → 1 (продан), прочие busy-дни → 2 (техблок).

Режимы (EMPLOYMENT_CALENDAR_SCOPE):
  all  — запросы для всех переданных ID;
  list — только ID из EMPLOYMENT_CALENDAR_IDS (первый этап внедрения);
  off  — фича выключена, календарь определяется только окном (откат).

Обработка ошибок — тихая деградация: неудачный запрос объекта не валит
прогон, календарь объекта остаётся как определило скользящее окно.
При 403 (протухший токен) и «Execution context was destroyed» выполняется
восстановление контекста через колбэк recover_context (переиспользуется
метод BatchEnrichmentService._recover_page_context).
"""

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from typing import Any, Protocol

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.constants import DAYS_COUNT

logger = get_logger("employment_calendar")

# ── Константы ──────────────────────────────────────────────

# URL эндпоинта занятых периодов объекта (брони и блокировки владельца)
API_ORDERS_URL: str = "https://sutochno.ru/api/json/orders/getOrdersByObject"

# Версия API для заголовка запроса
_ORDERS_API_VERSION: str = "1.8"

# Таймаут одного fetch-запроса внутри браузера (секунды)
_FETCH_TIMEOUT_SECONDS: int = 30

# Таймаут page.evaluate для одного запроса (секунды)
_EVALUATE_TIMEOUT: float = 60.0

# Максимальное количество попыток на один объект (включая recovery)
_MAX_ATTEMPTS: int = 3

# Пауза между попытками одного объекта после сбоя (секунды)
_RETRY_PAUSE: float = 1.0

# Пауза после восстановления контекста перед повтором (секунды)
_RECOVERY_PAUSE: float = 3.0

# Маркеры ошибки уничтоженного контекста в ответе/исключении fetch
_CONTEXT_DESTROYED_MARKERS: tuple[str, ...] = (
    "execution context was destroyed",
    "context was destroyed",
    "most likely because of a navigation",
)

# Допустимые значения scope
_SCOPE_ALL: str = "all"
_SCOPE_LIST: str = "list"
_SCOPE_OFF: str = "off"

# Формат дат в calendars[] эндпоинта ("2026-10-11 15:00:00")
_INTERVAL_DT_FORMAT: str = "%Y-%m-%d %H:%M:%S"

# Колбэк восстановления контекста: перезагрузка страницы и перехват
# нового токена. Мутирует переданный контекст при успехе, возвращает True.
# Структурный контракт соответствует _PageContext из batch_enrichment_service.
RecoverFn = Callable[[Any], Awaitable[bool]]


class PageContextLike(Protocol):
    """Структурный контракт контекста страницы для GET-запросов.

    Соответствует приватному _PageContext из batch_enrichment_service —
    сервис зависит от структуры (DIP), а не от конкретного класса.
    Контекст мутируемый: token обновляется после восстановления.

    Attributes:
        page: Страница Playwright с живой сессией.
        token: Сессионный токен API.
        search_url: URL страницы поиска (для перезагрузки при сбое).
    """

    page: Page
    token: str
    search_url: str


class EmploymentCalendarService:
    """Загружает занятые ночи объектов через orders/getOrdersByObject.

    Один GET-запрос на объект (эндпоинт не пакетный), пауза между
    запросами из настроек. Возвращает словарь {object_id: множество
    занятых дат} только для объектов с успешным ответом — ошибки
    логируются и не прерывают прогон.

    Вызов выполняется в существующем контексте страницы (прокси-воркер
    или браузер каталога) — отдельный браузер не запускается.
    """

    def __init__(
        self,
        scope: str = _SCOPE_ALL,
        allowed_ids: frozenset[int] = frozenset(),
        pause_seconds: float = 0.3,
    ) -> None:
        """Инициализирует сервис.

        Args:
            scope: Режим работы — all / list / off.
            allowed_ids: Разрешённые ID для режима list.
            pause_seconds: Пауза между GET-запросами (секунды).
        """
        self._scope = scope.strip().lower()
        self._allowed_ids = allowed_ids
        self._pause_seconds = pause_seconds

    @property
    def enabled(self) -> bool:
        """Включена ли фича (scope != off)."""
        return self._scope != _SCOPE_OFF

    def filter_ids(self, object_ids: list[int]) -> list[int]:
        """Фильтрует список ID согласно режиму работы.

        Args:
            object_ids: Входные ID объектов.

        Returns:
            ID, для которых нужно запросить занятые периоды.
        """
        if self._scope == _SCOPE_OFF:
            return []
        if self._scope == _SCOPE_LIST:
            filtered = [i for i in object_ids if i in self._allowed_ids]
            skipped = len(object_ids) - len(filtered)
            if skipped > 0:
                logger.debug(
                    "employment_режим_list_пропущены",
                    step=f"передано={len(object_ids)}, "
                         f"прошло_фильтр={len(filtered)}",
                )
            return filtered
        return list(object_ids)

    async def load_busy_nights(
        self,
        ctx: PageContextLike,
        object_ids: list[int],
        today: date,
        recover_context: RecoverFn | None = None,
    ) -> dict[int, set[date]]:
        """Загружает множества занятых ночей для списка объектов.

        Занятая ночь — дата, входящая в интервал [date_begin; date_end)
        какого-либо элемента calendars[] и лежащая в пределах горизонта
        [today; today + 60). Интервалы за пределами горизонта
        игнорируются.

        Args:
            ctx: Контекст страницы (тот же, что у batch-обогащения).
            object_ids: ID объектов (обычно busy-ID после bulk-фазы).
            today: Дата начала календаря прогона — та же, которой
                строились окна скользящего этапа (единообразие).
            recover_context: Колбэк восстановления контекста при
                403/уничтожении контекста. Если None — recovery
                не выполняется, только повторы.

        Returns:
            Словарь {object_id: {занятые даты}}. Только успешные ответы;
            при scope=off или пустом списке после фильтра — пустой словарь.
        """
        if not self.enabled:
            return {}

        target_ids = self.filter_ids(object_ids)
        if not target_ids:
            logger.debug(
                "employment_календари_пропущены_нет_id",
                step=f"передано={len(object_ids)}",
            )
            return {}

        horizon_start = today
        horizon_end = today + timedelta(days=DAYS_COUNT)

        start_time = time.perf_counter()
        result: dict[int, set[date]] = {}
        errors = 0

        logger.info(
            "employment_календари_начало",
            step=f"объектов={len(target_ids)}, режим={self._scope}, "
                 f"пауза={self._pause_seconds}с",
        )

        for index, object_id in enumerate(target_ids, start=1):
            nights = await self._load_for_object(
                ctx=ctx,
                object_id=object_id,
                horizon_start=horizon_start,
                horizon_end=horizon_end,
                recover_context=recover_context,
            )

            if nights is None:
                errors += 1
            elif nights:
                result[object_id] = nights

            if index < len(target_ids):
                await asyncio.sleep(self._pause_seconds)

            if index % 200 == 0 or index == len(target_ids):
                logger.info(
                    "employment_календари_прогресс",
                    step=f"обработано={index}/{len(target_ids)}, "
                         f"ошибок={errors}",
                )

        elapsed = time.perf_counter() - start_time
        total_nights = sum(len(nights) for nights in result.values())

        logger.info(
            "employment_календари_завершено",
            step=f"успешно={len(result)}, ошибок={errors}, "
                 f"занятых_ночей={total_nights}, время={elapsed:.1f}с",
        )
        return result

    async def _load_for_object(
        self,
        ctx: PageContextLike,
        object_id: int,
        horizon_start: date,
        horizon_end: date,
        recover_context: RecoverFn | None,
    ) -> set[date] | None:
        """Загружает занятые ночи одного объекта с повторами.

        При 403 (протухший токен) или уничтожении контекста вызывает
        recover_context и повторяет запрос. До _MAX_ATTEMPTS попыток.

        Args:
            ctx: Контекст страницы (мутируется при recovery).
            object_id: ID объекта.
            horizon_start: Начало горизонта календаря (включительно).
            horizon_end: Конец горизонта (исключительно).
            recover_context: Колбэк восстановления контекста.

        Returns:
            Множество занятых дат или None при исчерпании попыток.
        """
        last_error = ""

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            outcome = await self._fetch_orders(ctx, object_id)

            if outcome.get("success"):
                intervals = outcome.get("intervals")
                if not isinstance(intervals, list):
                    intervals = []
                return self._parse_intervals(
                    intervals, horizon_start, horizon_end, object_id,
                )

            last_error = str(outcome.get("error", "unknown"))
            status = outcome.get("status")

            need_recovery = (
                self._is_context_error(last_error)
                or status == 403
                or "http_403" in last_error
            )

            if (
                need_recovery
                and recover_context is not None
                and attempt < _MAX_ATTEMPTS
            ):
                logger.warning(
                    "employment_календарь_восстановление_контекста",
                    step=f"id={object_id}, "
                         f"попытка={attempt}/{_MAX_ATTEMPTS}, "
                         f"ошибка={last_error[:100]}",
                )
                await asyncio.sleep(_RECOVERY_PAUSE)

                if await recover_context(ctx):
                    continue

                logger.warning(
                    "employment_календарь_восстановление_не_удалось",
                    step=f"id={object_id}",
                )
                break

            if attempt < _MAX_ATTEMPTS:
                await asyncio.sleep(_RETRY_PAUSE)

        logger.warning(
            "employment_календарь_не_получен",
            step=f"id={object_id}, ошибка={last_error[:150]}",
        )
        return None

    async def _fetch_orders(
        self,
        ctx: PageContextLike,
        object_id: int,
    ) -> dict:
        """Выполняет один GET-запрос orders/getOrdersByObject.

        Args:
            ctx: Контекст страницы.
            object_id: ID объекта.

        Returns:
            Словарь: {success: True, intervals: [...]} при успехе или
            {success: False, error: str, status?: int} при ошибке.
        """
        try:
            raw = await asyncio.wait_for(
                ctx.page.evaluate(
                    """
                    async ({apiUrl, objectId, token, apiVersion,
                            fetchTimeout}) => {
                        try {
                            const controller = new AbortController();
                            const tid = setTimeout(
                                () => controller.abort(), fetchTimeout * 1000
                            );

                            const resp = await fetch(
                                apiUrl + '?object_id=' + objectId,
                                {
                                    method: 'GET',
                                    headers: {
                                        'Accept': 'application/json',
                                        'token': token,
                                        'platform': 'js',
                                        'api-version': apiVersion
                                    },
                                    credentials: 'include',
                                    signal: controller.signal
                                }
                            );

                            clearTimeout(tid);

                            if (!resp.ok) {
                                return {
                                    success: false,
                                    error: 'http_' + resp.status,
                                    status: resp.status
                                };
                            }

                            const data = await resp.json();

                            if (!data.success) {
                                return {
                                    success: false,
                                    error: 'api_false',
                                    status: resp.status
                                };
                            }

                            const calendars =
                                (data.data && data.data.calendars) || [];
                            const intervals = calendars.map(c => ({
                                date_begin: c.date_begin,
                                date_end: c.date_end,
                                type: c.type
                            }));
                            return {success: true, intervals: intervals};

                        } catch (e) {
                            if (e.name === 'AbortError') {
                                return {success: false, error: 'fetch_timeout'};
                            }
                            return {success: false, error: e.message};
                        }
                    }
                    """,
                    {
                        "apiUrl": API_ORDERS_URL,
                        "objectId": object_id,
                        "token": ctx.token,
                        "apiVersion": _ORDERS_API_VERSION,
                        "fetchTimeout": _FETCH_TIMEOUT_SECONDS,
                    },
                ),
                timeout=_EVALUATE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            return {"success": False, "error": "evaluate_timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}

        if not isinstance(raw, dict):
            return {"success": False, "error": "unexpected_response"}
        return raw

    @staticmethod
    def _parse_intervals(
        intervals: list[dict],
        horizon_start: date,
        horizon_end: date,
        object_id: int,
    ) -> set[date]:
        """Преобразует intervals из ответа API в множество занятых дат.

        Занятые ночи интервала = [date_begin.date(); date_end.date()) —
        дата выезда свободна. Даты за пределами [horizon_start;
        horizon_end) отбрасываются (усечение до 60-дневного горизонта).

        Args:
            intervals: Список {date_begin, date_end, type} из ответа.
            horizon_start: Начало горизонта (включительно).
            horizon_end: Конец горизонта (исключительно).
            object_id: ID объекта (для логов).

        Returns:
            Множество занятых дат в пределах горизонта.
        """
        nights: set[date] = set()

        for raw in intervals:
            begin_raw = str(raw.get("date_begin", "") or "")
            end_raw = str(raw.get("date_end", "") or "")

            if not begin_raw or not end_raw:
                continue

            try:
                begin_day = datetime.strptime(
                    begin_raw, _INTERVAL_DT_FORMAT,
                ).date()
                end_day = datetime.strptime(
                    end_raw, _INTERVAL_DT_FORMAT,
                ).date()
            except ValueError:
                logger.warning(
                    "employment_интервал_не_разобран",
                    step=f"id={object_id}, begin={begin_raw}, end={end_raw}",
                )
                continue

            if end_day <= begin_day:
                logger.warning(
                    "employment_интервал_некорректен",
                    step=f"id={object_id}, begin={begin_day}, end={end_day}",
                )
                continue

            night = begin_day
            while night < end_day:
                if horizon_start <= night < horizon_end:
                    nights.add(night)
                night += timedelta(days=1)

        return nights

    @staticmethod
    def _is_context_error(error_text: str) -> bool:
        """Проверяет, связана ли ошибка с уничтожением контекста.

        Args:
            error_text: Текст ошибки из ответа _fetch_orders.

        Returns:
            True если ошибка навигации/уничтожения контекста.
        """
        lowered = error_text.lower()
        return any(marker in lowered for marker in _CONTEXT_DESTROYED_MARKERS)
