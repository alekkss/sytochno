"""Гибридная стратегия получения календаря и цен."""

import re

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.api_client import ApiClient
from src.services.listing.constants import (
    DAYS_COUNT,
    DEFAULT_GUESTS,
    ERROR_THRESHOLD,
    MIN_NIGHTS_ERROR_KEYWORDS,
    MIN_NIGHTS_VARIANTS,
)
from src.services.listing.token_manager import TokenManager

logger = get_logger("hybrid_strategy")


class HybridStrategy:
    """Гибридная стратегия извлечения календаря занятости и цен.

    Алгоритм:
    1. Валидация токена (тестовый запрос).
    2. Bulk-запрос на 60 ночей → цены из detail[].
       Приоритет: type="season_price" → type=1 (базовая цена).
       Если busy="unbusy" → все дни свободны, готово за 1 запрос.
    3. Если bulk вернул busy="busy" → скользящее окно для занятости.
    4. Если bulk вернул api_false → токен протух или аномалия.
       Перезагрузка страницы + повтор с новым токеном.
    5. При массовых ошибках в скользящем окне (>30 из 60) →
       перезагрузка страницы + повтор (НЕ нормализация как "свободен").
    """

    def __init__(
        self,
        api_client: ApiClient,
        token_manager: TokenManager,
        guests: int = DEFAULT_GUESTS,
    ) -> None:
        """Инициализирует стратегию.

        Args:
            api_client: Клиент API.
            token_manager: Менеджер токенов.
            guests: Количество гостей для запросов.
        """
        self._api = api_client
        self._token_manager = token_manager
        self._guests = guests

    async def fetch_calendar_and_prices(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает календарь и цены гибридной стратегией.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки.

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        # ── Валидация токена ──
        token_valid = await self._token_manager.validate_token(
            page, object_id, current_token, guests=self._guests
        )

        if not token_valid:
            logger.info(
                "токен_невалиден_перезагрузка",
                step=f"id={object_id}",
            )
            new_token = await self._token_manager.reload_and_get_token(
                page, url, object_id
            )
            if not new_token:
                logger.warning(
                    "не_удалось_получить_валидный_токен",
                    step=f"id={object_id}",
                )
                return [0] * DAYS_COUNT, [0] * DAYS_COUNT
            current_token = new_token

        # ── Шаг 1: Bulk-запрос на 60 ночей → цены ──
        busy_status, prices_60, bulk_success = await self._api.fetch_bulk_prices(
            page, object_id, current_token, guests=self._guests
        )

        if not bulk_success:
            logger.info(
                "bulk_не_удался_пробуем_перезагрузку",
                step=f"id={object_id}",
            )

            new_token = await self._token_manager.reload_and_get_token(
                page, url, object_id
            )
            if new_token:
                current_token = new_token
                busy_status, prices_60, bulk_success = await self._api.fetch_bulk_prices(
                    page, object_id, current_token, guests=self._guests
                )

            if not bulk_success:
                logger.info(
                    "bulk_окончательно_не_удался_скользящее_окно",
                    step=f"id={object_id}",
                )
                return await self._full_sliding_window(
                    page, object_id, current_token, url
                )

        # ── Шаг 2: Определение занятости ──
        if busy_status == "unbusy":
            calendar_60 = [0] * DAYS_COUNT
            logger.info(
                "все_дни_свободны_bulk",
                step=f"id={object_id}, цен={sum(1 for p in prices_60 if p > 0)}/60",
            )
            return calendar_60, prices_60

        # busy="busy" — нужно определить какие дни заняты
        calendar_60 = await self._determine_availability(
            page, object_id, current_token, url
        )

        # Объединяем: обнуляем цены для занятых дней
        final_prices: list[int] = []
        for i in range(DAYS_COUNT):
            if calendar_60[i] == 1:
                final_prices.append(0)
            else:
                final_prices.append(prices_60[i])

        free_days = sum(1 for c in calendar_60 if c == 0)
        busy_days = sum(1 for c in calendar_60 if c == 1)

        logger.info(
            "гибридная_стратегия_завершена",
            step=f"id={object_id}",
            total=f"свободных={free_days}, занятых={busy_days}, "
                  f"цен={sum(1 for p in final_prices if p > 0)}",
        )

        return calendar_60, final_prices

    async def _determine_availability(
        self, page: Page, object_id: str, token: str, url: str
    ) -> list[int]:
        """Определяет занятость каждого дня с адаптацией min_nights и retry.

        Перебирает варианты min_nights по возрастанию. Если ошибок много
        и detected min_nights указывает на более высокое значение —
        переходит к следующему варианту. Если detected <= текущего nights,
        но ошибок всё ещё много — тоже переходит к следующему варианту
        (API может занижать реальный min_nights в тексте ошибки).

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.

        Returns:
            Список из 60 значений: 0=свободен, 1=занят.
        """
        current_token = token
        best_calendar: list[int] = [-1] * DAYS_COUNT
        best_error_days: int = DAYS_COUNT
        reloaded_for_nights: int | None = None

        for nights in MIN_NIGHTS_VARIANTS:
            calendar, errors_details = await self._api.fetch_availability(
                page, object_id, current_token, nights=nights, guests=self._guests
            )

            error_days = sum(1 for c in calendar if c == -1)

            # Сохраняем лучший результат
            if error_days < best_error_days:
                best_calendar = calendar
                best_error_days = error_days

            if error_days == 0:
                return calendar

            if error_days <= 5:
                return [0 if c == -1 else c for c in calendar]

            # Много ошибок — проверяем причину
            detected = self._detect_min_nights(errors_details)

            if detected is not None and detected > nights:
                logger.info(
                    "адаптация_min_nights",
                    step=f"id={object_id}, текущий={nights}, нужен={detected}",
                )
                continue

            # detected <= nights ИЛИ detected is None, но ошибок много.
            # Стратегия: пробуем следующий вариант nights.
            # Перезагрузку делаем только один раз за весь цикл.
            if error_days >= ERROR_THRESHOLD and reloaded_for_nights is None:
                logger.info(
                    "много_ошибок_пробуем_перезагрузку",
                    step=f"id={object_id}, ночей={nights}, ошибок={error_days}",
                )
                new_token = await self._token_manager.reload_and_get_token(
                    page, url, object_id
                )
                if new_token:
                    current_token = new_token
                    reloaded_for_nights = nights

                    # Повторяем текущий nights с новым токеном
                    calendar_retry, _ = await self._api.fetch_availability(
                        page, object_id, current_token, nights=nights, guests=self._guests
                    )
                    error_days_retry = sum(1 for c in calendar_retry if c == -1)

                    if error_days_retry < best_error_days:
                        best_calendar = calendar_retry
                        best_error_days = error_days_retry

                    if best_error_days == 0:
                        return best_calendar
                    if best_error_days <= 5:
                        return [0 if c == -1 else c for c in best_calendar]

            logger.debug(
                "переход_к_следующему_nights",
                step=f"id={object_id}, текущий={nights}, ошибок={error_days}, "
                     f"detected={detected}",
            )
            continue

        # Все варианты перебраны — нормализуем лучший результат
        normalized = [0 if c == -1 else c for c in best_calendar]

        if best_error_days > 10:
            logger.warning(
                "занятость_с_ошибками",
                step=f"id={object_id}, ошибок_нормализовано={best_error_days}",
            )

        return normalized

    async def _full_sliding_window(
        self, page: Page, object_id: str, token: str, url: str
    ) -> tuple[list[int], list[int]]:
        """Получает и цены, и занятость через скользящее окно (fallback).

        Используется когда bulk-запрос полностью не работает.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.

        Returns:
            Кортеж (calendar_60_days, prices_60_days).
        """
        current_token = token

        for nights in MIN_NIGHTS_VARIANTS:
            logger.info(
                "скользящее_окно_полное",
                step=f"id={object_id}, ночей={nights}",
            )

            calendar, errors_details = await self._api.fetch_availability(
                page, object_id, current_token, nights=nights, guests=self._guests
            )

            error_days = sum(1 for c in calendar if c == -1)

            if error_days == 0:
                # Данные получены — пробуем bulk для цен
                busy_status, prices_60, bulk_ok = await self._api.fetch_bulk_prices(
                    page, object_id, current_token, guests=self._guests
                )
                if bulk_ok and sum(1 for p in prices_60 if p > 0) > 0:
                    final_prices = [
                        0 if calendar[i] == 1 else prices_60[i]
                        for i in range(DAYS_COUNT)
                    ]
                    return calendar, final_prices

                return await self._api.sliding_window_with_prices(
                    page, object_id, current_token, nights=nights, guests=self._guests
                )

            if error_days < ERROR_THRESHOLD:
                # Частичный успех — нормализуем и пробуем bulk для цен
                calendar_norm = [0 if c == -1 else c for c in calendar]
                _, prices_60, bulk_ok = await self._api.fetch_bulk_prices(
                    page, object_id, current_token, guests=self._guests
                )
                if bulk_ok:
                    final_prices = [
                        0 if calendar_norm[i] == 1 else prices_60[i]
                        for i in range(DAYS_COUNT)
                    ]
                    return calendar_norm, final_prices

                return await self._api.sliding_window_with_prices(
                    page, object_id, current_token, nights=nights, guests=self._guests
                )

            # Много ошибок — проверяем min_nights и продолжаем цикл
            detected = self._detect_min_nights(errors_details)

            if detected is not None and detected > nights:
                logger.info(
                    "скользящее_окно_адаптация",
                    step=f"id={object_id}, текущий={nights}, нужен={detected}",
                )
                continue

            # detected <= nights или None — пробуем следующий вариант
            if nights < MIN_NIGHTS_VARIANTS[-1]:
                logger.debug(
                    "скользящее_окно_следующий_вариант",
                    step=f"id={object_id}, текущий={nights}, ошибок={error_days}",
                )
                continue

            # Последний вариант — перезагрузка как крайняя мера
            new_token = await self._token_manager.reload_and_get_token(
                page, url, object_id
            )
            if new_token:
                current_token = new_token
                continue
            break

        # Полный провал
        logger.warning(
            "полный_провал_нет_данных",
            step=f"id={object_id}",
        )
        return [0] * DAYS_COUNT, [0] * DAYS_COUNT

    def _detect_min_nights(self, errors_details: list[dict[str, str | int]]) -> int | None:
        """Определяет min_nights из текстов ошибок API.

        Args:
            errors_details: Список ошибок.

        Returns:
            Значение min_nights или None.
        """
        if not errors_details:
            return None

        for error_info in errors_details[:3]:
            error_body = str(error_info.get("error_body", "")).lower()
            error_code = str(error_info.get("error", "")).lower()
            errors_list = str(error_info.get("errors", "")).lower()
            combined_text = f"{error_body} {error_code} {errors_list}"

            is_min_nights_error = any(
                keyword in combined_text
                for keyword in MIN_NIGHTS_ERROR_KEYWORDS
            )

            if is_min_nights_error:
                numbers = re.findall(r"(\d+)", combined_text)
                for num_str in numbers:
                    num = int(num_str)
                    if 2 <= num <= 30:
                        logger.info("min_nights_обнаружен", step=f"min_nights={num}")
                        return num
                return 2

        if len(errors_details) >= 55:
            unique_errors = set(str(e.get("error", "")) for e in errors_details)
            if len(unique_errors) <= 2:
                return 2

        return None