"""Гибридная стратегия получения календаря и цен."""

import re
from datetime import date

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

# Порог min_nights, выше которого карточка считается необогащаемой.
# Если API возвращает ошибку с числом >= этого значения — карточку
# бессмысленно повторять, потому что наше окно анализа = 60 дней.
_MIN_NIGHTS_SKIP_THRESHOLD: int = 60


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
    6. При 100% ошибках (60 из 60), не связанных с min_nights →
       немедленное прерывание: токен протух или IP заблокирован.
       Карточка уходит на retry верхнего уровня с новым токеном.

    Дополнительно детектирует фатальные ошибки:
    - min_nights >= 60 — объект с долгосрочной арендой (необогащаем).
    - no_objects — объявление удалено или заблокировано.
    В этих случаях третий элемент возврата содержит причину.

    Дата начала календаря (today) фиксируется один раз в начале
    fetch_calendar_and_prices и передаётся во все методы api_client —
    это гарантирует согласованность данных при прогонах, пересекающих полночь.
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
    ) -> tuple[list[int], list[int], str | None]:
        """Получает календарь и цены гибридной стратегией.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            url: URL карточки.

        Returns:
            Кортеж (calendar_60_days, prices_60_days, skip_reason).
            skip_reason: None если данные получены или ошибка временная.
                "min_nights_exceeded" — min_nights объекта >= 60 дней.
                "object_not_found" — объявление удалено/заблокировано.
        """
        # Фиксируем дату один раз на весь прогон карточки.
        # Все методы api_client используют эту же дату — календари
        # не разъедутся даже если прогон пересечёт полночь.
        today = date.today()

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
                return [0] * DAYS_COUNT, [0] * DAYS_COUNT, None

            current_token = new_token

        # ── Шаг 1: Bulk-запрос на 60 ночей → цены ──
        bulk_result = await self._api.fetch_bulk_prices(
            page, object_id, current_token, guests=self._guests, today=today
        )
        busy_status, prices_60, bulk_success = bulk_result

        if not bulk_success:
            # Проверяем: не является ли ошибка фатальной (удалённое объявление
            # или слишком большой min_nights). Для этого делаем повторный
            # bulk-запрос после перезагрузки токена и анализируем результат.
            skip_reason = await self._check_fatal_bulk_error(
                page, object_id, current_token, url, today
            )

            if skip_reason is not None:
                return [0] * DAYS_COUNT, [0] * DAYS_COUNT, skip_reason

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
                    page, object_id, current_token, guests=self._guests, today=today
                )

            if not bulk_success:
                logger.info(
                    "bulk_окончательно_не_удался_скользящее_окно",
                    step=f"id={object_id}",
                )
                calendar, prices, sw_reason = await self._full_sliding_window(
                    page, object_id, current_token, url, today=today
                )
                return calendar, prices, sw_reason

        # ── Шаг 2: Определение занятости ──
        if busy_status == "unbusy":
            calendar_60 = [0] * DAYS_COUNT
            logger.info(
                "все_дни_свободны_bulk",
                step=f"id={object_id}, цен={sum(1 for p in prices_60 if p > 0)}/60",
            )
            return calendar_60, prices_60, None

        # busy="busy" — нужно определить какие дни заняты
        calendar_60, avail_reason = await self._determine_availability(
            page, object_id, current_token, url, today=today
        )

        if avail_reason is not None:
            return [0] * DAYS_COUNT, [0] * DAYS_COUNT, avail_reason

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

        return calendar_60, final_prices, None

    async def _check_fatal_bulk_error(
        self,
        page: Page,
        object_id: str,
        token: str,
        url: str,
        today: date,
    ) -> str | None:
        """Проверяет, является ли ошибка bulk-запроса фатальной.

        Выполняет дополнительный диагностический bulk-запрос и анализирует
        текст ошибки. Фатальные ошибки — те, при которых повторные попытки
        бессмысленны:
        - "no_objects" → объявление удалено/заблокировано.
        - min_nights >= 60 → объект для долгосрочной аренды.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Текущий токен.
            url: URL карточки.
            today: Дата начала календаря.

        Returns:
            Причина пропуска или None если ошибка не фатальная.
        """
        # Для диагностики используем evaluate с прямым доступом к raw-ответу API.
        # Стандартный fetch_bulk_prices не возвращает текст ошибки —
        # поэтому выполняем отдельный диагностический запрос.
        from datetime import timedelta

        from src.services.listing.constants import API_PRICES_URL

        start_date = today
        end_date = start_date + timedelta(days=DAYS_COUNT)
        date_begin = f"{start_date.isoformat()} 14:00:00"
        date_end = f"{end_date.isoformat()} 11:00:00"

        try:
            result = await page.evaluate(
                """
                async ({apiUrl, objectId, dateBegin, dateEnd, token, guests}) => {
                    try {
                        const resp = await fetch(apiUrl, {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'token': token,
                                'platform': 'js',
                                'api-version': '1.13'
                            },
                            body: JSON.stringify({
                                objects: [parseInt(objectId)],
                                rooms_cnt: {},
                                guests: guests,
                                date_begin: dateBegin,
                                date_end: dateEnd,
                                currency_id: 1,
                                is_pets: 0,
                                documents: 0,
                                target: 0,
                                ages: [],
                                no_time: 1
                            })
                        });

                        if (!resp.ok) return {error_type: 'http', status: resp.status};

                        const data = await resp.json();

                        if (!data.success) return {error_type: 'api_false'};

                        if (!data.data || !data.data.objects || !data.data.objects[0]) {
                            return {error_type: 'no_objects'};
                        }

                        const obj = data.data.objects[0];
                        if (!obj.success) {
                            return {
                                error_type: 'obj_error',
                                errors: obj.errors || [],
                                error_text: JSON.stringify(obj.errors || [])
                            };
                        }

                        return {error_type: null};

                    } catch (e) {
                        return {error_type: 'exception', message: e.message};
                    }
                }
                """,
                {
                    "apiUrl": API_PRICES_URL,
                    "objectId": object_id,
                    "dateBegin": date_begin,
                    "dateEnd": date_end,
                    "token": token,
                    "guests": self._guests,
                },
            )
        except Exception as e:
            logger.debug(
                "диагностика_ошибка",
                step=f"id={object_id}, error={e}",
            )
            return None

        error_type = result.get("error_type")

        if error_type is None:
            # Запрос успешен — ошибка была временной
            return None

        # ── Объявление удалено/заблокировано ──
        if error_type == "no_objects":
            logger.info(
                "фатальная_ошибка_объект_не_найден",
                step=f"id={object_id}",
            )
            return "object_not_found"

        # ── Проверяем min_nights в тексте ошибки ──
        if error_type == "obj_error":
            error_text = result.get("error_text", "").lower()
            min_nights_value = self._extract_min_nights_from_error(error_text)

            if min_nights_value is not None and min_nights_value >= _MIN_NIGHTS_SKIP_THRESHOLD:
                logger.info(
                    "фатальная_ошибка_min_nights_превышает_окно",
                    step=f"id={object_id}, min_nights={min_nights_value}, "
                         f"порог={_MIN_NIGHTS_SKIP_THRESHOLD}",
                )
                return "min_nights_exceeded"

        return None

    def _extract_min_nights_from_error(self, error_text: str) -> int | None:
        """Извлекает значение min_nights из текста ошибки API.

        Ищет числа в тексте ошибки, которая содержит ключевые слова
        о минимальном сроке проживания.

        Args:
            error_text: Текст ошибки (в нижнем регистре).

        Returns:
            Значение min_nights или None если не найдено.
        """
        if not error_text:
            return None

        # Проверяем, содержит ли текст ключевые слова о min_nights
        is_min_nights_error = any(
            keyword in error_text
            for keyword in MIN_NIGHTS_ERROR_KEYWORDS
        )

        if not is_min_nights_error:
            return None

        # Извлекаем все числа из текста
        numbers = re.findall(r"(\d+)", error_text)
        for num_str in numbers:
            num = int(num_str)
            # Ищем число в разумном диапазоне для min_nights (от 2 до 999)
            if 2 <= num <= 999:
                return num

        return None

    async def _determine_availability(
        self,
        page: Page,
        object_id: str,
        token: str,
        url: str,
        today: date | None = None,
    ) -> tuple[list[int], str | None]:
        """Определяет занятость каждого дня с адаптацией min_nights и retry.

        При 100% ошибках (все 60 дней), не связанных с min_nights,
        цикл немедленно прерывается — дальнейший перебор вариантов
        бесполезен, так как причина в протухшем токене или блокировке IP.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.
            today: Дата начала календаря.

        Returns:
            Кортеж (calendar_60_days, skip_reason).
            skip_reason: None если данные получены или ошибка временная.
                "min_nights_exceeded" — все варианты nights исчерпаны и
                обнаружен min_nights >= 60.
        """
        current_token = token
        best_calendar: list[int] = [-1] * DAYS_COUNT
        best_error_days: int = DAYS_COUNT
        reloaded_for_nights: int | None = None

        for nights in MIN_NIGHTS_VARIANTS:
            calendar, errors_details = await self._api.fetch_availability(
                page, object_id, current_token,
                nights=nights, guests=self._guests, today=today,
            )

            error_days = sum(1 for c in calendar if c == -1)

            if error_days < best_error_days:
                best_calendar = calendar
                best_error_days = error_days

            if error_days == 0:
                return calendar, None

            if error_days <= 5:
                return [0 if c == -1 else c for c in calendar], None

            detected = self._detect_min_nights(errors_details)

            # ── Проверка фатального min_nights ──
            # Если детектирован min_nights >= порога — карточка необогащаема
            if detected is not None and detected >= _MIN_NIGHTS_SKIP_THRESHOLD:
                logger.info(
                    "фатальная_ошибка_min_nights_в_окне",
                    step=f"id={object_id}, min_nights={detected}, "
                         f"порог={_MIN_NIGHTS_SKIP_THRESHOLD}",
                )
                return [0] * DAYS_COUNT, "min_nights_exceeded"

            # ── Раннее прерывание при 100% ошибках ──
            # Если ВСЕ 60 дней вернули ошибку и причина НЕ в min_nights —
            # токен протух или IP заблокирован. Перебирать остальные варианты
            # nights бессмысленно: каждый вариант делает 60 запросов впустую.
            # Прерываем цикл — карточка уйдёт на retry верхнего уровня.
            if error_days == DAYS_COUNT and detected is None:
                # Даём один шанс перезагрузить токен, если ещё не пробовали
                if reloaded_for_nights is None:
                    logger.info(
                        "полный_провал_окна_перезагрузка_токена",
                        step=f"id={object_id}, ночей={nights}, ошибок={error_days}",
                    )
                    new_token = await self._token_manager.reload_and_get_token(
                        page, url, object_id
                    )
                    if new_token:
                        current_token = new_token
                        reloaded_for_nights = nights

                        calendar_retry, _ = await self._api.fetch_availability(
                            page, object_id, current_token,
                            nights=nights, guests=self._guests, today=today,
                        )
                        error_days_retry = sum(
                            1 for c in calendar_retry if c == -1
                        )

                        if error_days_retry < best_error_days:
                            best_calendar = calendar_retry
                            best_error_days = error_days_retry

                        if best_error_days == 0:
                            return best_calendar, None
                        if best_error_days <= 5:
                            return [
                                0 if c == -1 else c for c in best_calendar
                            ], None

                        # После перезагрузки всё ещё 100% ошибок — выходим
                        if error_days_retry == DAYS_COUNT:
                            logger.warning(
                                "блокировка_или_протухший_токен",
                                step=f"id={object_id}, ночей={nights}, "
                                     f"ошибок_после_перезагрузки={error_days_retry}",
                            )
                            break
                    else:
                        # Не удалось получить новый токен — выходим
                        logger.warning(
                            "блокировка_или_протухший_токен",
                            step=f"id={object_id}, ночей={nights}, "
                                 f"ошибок={error_days}, новый_токен=нет",
                        )
                        break
                else:
                    # Уже перезагружали — повторная перезагрузка не поможет
                    logger.warning(
                        "блокировка_или_протухший_токен",
                        step=f"id={object_id}, ночей={nights}, "
                             f"ошибок={error_days}, перезагрузка_была={reloaded_for_nights}",
                    )
                    break

            if detected is not None and detected > nights:
                logger.info(
                    "адаптация_min_nights",
                    step=f"id={object_id}, текущий={nights}, нужен={detected}",
                )
                continue

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

                    calendar_retry, _ = await self._api.fetch_availability(
                        page, object_id, current_token,
                        nights=nights, guests=self._guests, today=today,
                    )
                    error_days_retry = sum(1 for c in calendar_retry if c == -1)

                    if error_days_retry < best_error_days:
                        best_calendar = calendar_retry
                        best_error_days = error_days_retry

                    if best_error_days == 0:
                        return best_calendar, None
                    if best_error_days <= 5:
                        return [0 if c == -1 else c for c in best_calendar], None

            logger.debug(
                "переход_к_следующему_nights",
                step=f"id={object_id}, текущий={nights}, ошибок={error_days}, "
                     f"detected={detected}",
            )
            continue

        normalized = [0 if c == -1 else c for c in best_calendar]

        if best_error_days > 10:
            logger.warning(
                "занятость_с_ошибками",
                step=f"id={object_id}, ошибок_нормализовано={best_error_days}",
            )

        return normalized, None

    async def _full_sliding_window(
        self,
        page: Page,
        object_id: str,
        token: str,
        url: str,
        today: date | None = None,
    ) -> tuple[list[int], list[int], str | None]:
        """Получает и цены, и занятость через скользящее окно (fallback).

        При 100% ошибках (все 60 дней), не связанных с min_nights,
        цикл немедленно прерывается — дальнейший перебор вариантов
        бесполезен, так как причина в протухшем токене или блокировке IP.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            url: URL карточки.
            today: Дата начала календаря.

        Returns:
            Кортеж (calendar_60_days, prices_60_days, skip_reason).
        """
        current_token = token
        reloaded_in_full_sw: bool = False

        for nights in MIN_NIGHTS_VARIANTS:
            logger.info(
                "скользящее_окно_полное",
                step=f"id={object_id}, ночей={nights}",
            )

            calendar, errors_details = await self._api.fetch_availability(
                page, object_id, current_token,
                nights=nights, guests=self._guests, today=today,
            )

            error_days = sum(1 for c in calendar if c == -1)

            if error_days == 0:
                busy_status, prices_60, bulk_ok = await self._api.fetch_bulk_prices(
                    page, object_id, current_token, guests=self._guests, today=today
                )
                if bulk_ok and sum(1 for p in prices_60 if p > 0) > 0:
                    final_prices = [
                        0 if calendar[i] == 1 else prices_60[i]
                        for i in range(DAYS_COUNT)
                    ]
                    return calendar, final_prices, None

                cal, prices = await self._api.sliding_window_with_prices(
                    page, object_id, current_token,
                    nights=nights, guests=self._guests, today=today,
                )
                return cal, prices, None

            if error_days < ERROR_THRESHOLD:
                calendar_norm = [0 if c == -1 else c for c in calendar]
                _, prices_60, bulk_ok = await self._api.fetch_bulk_prices(
                    page, object_id, current_token, guests=self._guests, today=today
                )
                if bulk_ok:
                    final_prices = [
                        0 if calendar_norm[i] == 1 else prices_60[i]
                        for i in range(DAYS_COUNT)
                    ]
                    return calendar_norm, final_prices, None

                cal, prices = await self._api.sliding_window_with_prices(
                    page, object_id, current_token,
                    nights=nights, guests=self._guests, today=today,
                )
                return cal, prices, None

            detected = self._detect_min_nights(errors_details)

            # ── Проверка фатального min_nights ──
            if detected is not None and detected >= _MIN_NIGHTS_SKIP_THRESHOLD:
                logger.info(
                    "фатальная_ошибка_min_nights_в_скользящем_окне",
                    step=f"id={object_id}, min_nights={detected}, "
                         f"порог={_MIN_NIGHTS_SKIP_THRESHOLD}",
                )
                return [0] * DAYS_COUNT, [0] * DAYS_COUNT, "min_nights_exceeded"

            # ── Раннее прерывание при 100% ошибках ──
            # Аналогично _determine_availability: если ВСЕ 60 дней — ошибки
            # и причина НЕ в min_nights — прекращаем перебор немедленно.
            if error_days == DAYS_COUNT and detected is None:
                if not reloaded_in_full_sw:
                    logger.info(
                        "скользящее_окно_полный_провал_перезагрузка",
                        step=f"id={object_id}, ночей={nights}, ошибок={error_days}",
                    )
                    new_token = await self._token_manager.reload_and_get_token(
                        page, url, object_id
                    )
                    if new_token:
                        current_token = new_token
                        reloaded_in_full_sw = True

                        calendar_retry, _ = await self._api.fetch_availability(
                            page, object_id, current_token,
                            nights=nights, guests=self._guests, today=today,
                        )
                        error_days_retry = sum(
                            1 for c in calendar_retry if c == -1
                        )

                        if error_days_retry == 0:
                            _, prices_60, bulk_ok = (
                                await self._api.fetch_bulk_prices(
                                    page, object_id, current_token,
                                    guests=self._guests, today=today,
                                )
                            )
                            if bulk_ok and sum(1 for p in prices_60 if p > 0) > 0:
                                final_prices = [
                                    0 if calendar_retry[i] == 1
                                    else prices_60[i]
                                    for i in range(DAYS_COUNT)
                                ]
                                return calendar_retry, final_prices, None

                            cal, prices = await self._api.sliding_window_with_prices(
                                page, object_id, current_token,
                                nights=nights, guests=self._guests, today=today,
                            )
                            return cal, prices, None

                        if error_days_retry < ERROR_THRESHOLD:
                            calendar_norm = [
                                0 if c == -1 else c for c in calendar_retry
                            ]
                            _, prices_60, bulk_ok = (
                                await self._api.fetch_bulk_prices(
                                    page, object_id, current_token,
                                    guests=self._guests, today=today,
                                )
                            )
                            if bulk_ok:
                                final_prices = [
                                    0 if calendar_norm[i] == 1
                                    else prices_60[i]
                                    for i in range(DAYS_COUNT)
                                ]
                                return calendar_norm, final_prices, None

                            cal, prices = await self._api.sliding_window_with_prices(
                                page, object_id, current_token,
                                nights=nights, guests=self._guests, today=today,
                            )
                            return cal, prices, None

                        # После перезагрузки всё ещё 100% ошибок — выходим
                        if error_days_retry == DAYS_COUNT:
                            logger.warning(
                                "скользящее_окно_блокировка_или_протухший_токен",
                                step=f"id={object_id}, ночей={nights}, "
                                     f"ошибок_после_перезагрузки={error_days_retry}",
                            )
                            break
                    else:
                        logger.warning(
                            "скользящее_окно_блокировка_или_протухший_токен",
                            step=f"id={object_id}, ночей={nights}, "
                                 f"ошибок={error_days}, новый_токен=нет",
                        )
                        break
                else:
                    # Уже перезагружали — выходим
                    logger.warning(
                        "скользящее_окно_блокировка_или_протухший_токен",
                        step=f"id={object_id}, ночей={nights}, "
                             f"ошибок={error_days}, перезагрузка_была=да",
                    )
                    break

            if detected is not None and detected > nights:
                logger.info(
                    "скользящее_окно_адаптация",
                    step=f"id={object_id}, текущий={nights}, нужен={detected}",
                )
                continue

            if nights < MIN_NIGHTS_VARIANTS[-1]:
                logger.debug(
                    "скользящее_окно_следующий_вариант",
                    step=f"id={object_id}, текущий={nights}, ошибок={error_days}",
                )
                continue

            new_token = await self._token_manager.reload_and_get_token(
                page, url, object_id
            )
            if new_token:
                current_token = new_token
                continue
            break

        logger.warning(
            "полный_провал_нет_данных",
            step=f"id={object_id}",
        )
        return [0] * DAYS_COUNT, [0] * DAYS_COUNT, None

    def _detect_min_nights(
        self, errors_details: list[dict[str, str | int]]
    ) -> int | None:
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
                    if 2 <= num <= 999:
                        logger.info("min_nights_обнаружен", step=f"min_nights={num}")
                        return num
                return 2

        if len(errors_details) >= 55:
            unique_errors = set(str(e.get("error", "")) for e in errors_details)
            if len(unique_errors) <= 2:
                return 2

        return None
