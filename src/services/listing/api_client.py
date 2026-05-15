"""Клиент API sutochno.ru — bulk-запросы и скользящее окно."""

from datetime import date, timedelta

from playwright.async_api import Page

from src.config.logger import get_logger
from src.services.listing.constants import (
    API_BATCH_DELAY,
    API_BATCH_SIZE,
    API_PRICES_URL,
    DAYS_COUNT,
    DEFAULT_GUESTS,
)
from src.services.listing.price_parser import PriceParser

logger = get_logger("api_client")


class ApiClient:
    """Клиент для работы с API цен и занятости sutochno.ru.

    Предоставляет:
    - fetch_bulk_prices: один запрос на 60 ночей → все цены из detail[].
    - fetch_availability: скользящее окно → занятость каждого дня.
    - sliding_window_with_prices: скользящее окно → и занятость, и цены.
    """

    def __init__(self, price_parser: PriceParser | None = None) -> None:
        """Инициализирует клиент API.

        Args:
            price_parser: Парсер цен. Если None — создаётся новый.
        """
        self._price_parser = price_parser or PriceParser()

    async def fetch_bulk_prices(
        self, page: Page, object_id: str, token: str, guests: int = DEFAULT_GUESTS
    ) -> tuple[str | None, list[int], bool]:
        """Получает все цены одним запросом на 60 ночей.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            guests: Количество гостей.

        Returns:
            Кортеж (busy_status, prices_60_days, success).
            busy_status: "busy", "unbusy" или None при ошибке.
            prices_60_days: массив из 60 цен.
            success: True если запрос успешен.
        """
        today = date.today()
        end_date = today + timedelta(days=DAYS_COUNT)

        date_begin = f"{today.isoformat()} 14:00:00"
        date_end = f"{end_date.isoformat()} 11:00:00"

        logger.debug(
            "запрос_цен_bulk",
            step=f"id={object_id}, период={today}→{end_date}",
        )

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

                    if (!resp.ok) {
                        return {success: false, error: 'http_' + resp.status};
                    }

                    const data = await resp.json();

                    if (!data.success) {
                        return {success: false, error: 'api_false'};
                    }

                    if (!data.data || !data.data.objects || !data.data.objects[0]) {
                        return {success: false, error: 'no_objects'};
                    }

                    const obj = data.data.objects[0];

                    if (!obj.success) {
                        return {
                            success: false,
                            error: 'obj_error',
                            errors: obj.errors || []
                        };
                    }

                    const objData = obj.data;
                    return {
                        success: true,
                        busy: objData.busy,
                        detail: objData.detail || [],
                        rooms_available: objData.rooms_available
                    };

                } catch (e) {
                    return {success: false, error: 'exception_' + e.message};
                }
            }
            """,
            {
                "apiUrl": API_PRICES_URL,
                "objectId": object_id,
                "dateBegin": date_begin,
                "dateEnd": date_end,
                "token": token,
                "guests": guests,
            },
        )

        if not result.get("success"):
            error = result.get("error", "unknown")
            logger.debug(
                "bulk_запрос_ошибка",
                step=f"id={object_id}, ошибка={error}, errors={result.get('errors', [])}",
            )
            return None, [0] * DAYS_COUNT, False

        busy_status = result.get("busy")
        detail = result.get("detail", [])

        # Используем PriceParser для разворачивания цен
        prices_60 = self._price_parser.extract_prices_from_detail(detail)

        prices_filled = sum(1 for p in prices_60 if p > 0)
        logger.debug(
            "bulk_цены_получены",
            step=f"id={object_id}, busy={busy_status}, цен={prices_filled}/60",
        )

        return busy_status, prices_60, True
    
    async def fetch_availability(
        self, page: Page, object_id: str, token: str,
        nights: int = 2, guests: int = DEFAULT_GUESTS
    ) -> tuple[list[int], list[dict[str, str | int]]]:
        """Определяет занятость каждого дня через скользящее окно.

        Отправляет пакетные запросы (по API_BATCH_SIZE) для каждого дня
        с окном в `nights` ночей. Возвращает календарь и детали ошибок.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Сессионный токен API.
            nights: Количество ночей в окне.
            guests: Количество гостей.

        Returns:
            Кортеж (calendar_60_days, errors_details).
            calendar: 0=свободен, 1=занят, -1=ошибка.
        """
        today = date.today()

        days_data = []
        for i in range(DAYS_COUNT):
            day = today + timedelta(days=i)
            end_day = day + timedelta(days=nights)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end_day.isoformat()} 11:00:00",
            })

        logger.debug(
            "запрос_занятости",
            step=f"id={object_id}, ночей={nights}, пакет={API_BATCH_SIZE}",
        )

        result = await page.evaluate(
            """
            async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
                const results = [];
                const batches = [];

                for (let i = 0; i < daysData.length; i += batchSize) {
                    batches.push(daysData.slice(i, i + batchSize));
                }

                for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                    const batch = batches[batchIdx];

                    const promises = batch.map(async (dayInfo) => {
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
                                    date_begin: dayInfo.date_begin,
                                    date_end: dayInfo.date_end,
                                    currency_id: 1,
                                    is_pets: 0,
                                    documents: 0,
                                    target: 0,
                                    ages: [],
                                    no_time: 1
                                })
                            });

                            if (!resp.ok) {
                                return {status: 'error', error: 'http_' + resp.status};
                            }

                            const data = await resp.json();

                            if (!data.success) {
                                return {status: 'error', error: 'api_false'};
                            }

                            if (!data.data || !data.data.objects || !data.data.objects[0]) {
                                return {status: 'error', error: 'no_data'};
                            }

                            const obj = data.data.objects[0];

                            if (!obj.success) {
                                return {
                                    status: 'obj_error',
                                    errors: obj.errors || [],
                                    error_body: JSON.stringify(obj.errors || []).substring(0, 300)
                                };
                            }

                            return {
                                status: 'ok',
                                busy: obj.data.busy === 'busy',
                                price: (() => {
                                    const detail = obj.data.detail || [];
                                    let seasonPrice = 0;
                                    let basePrice = 0;
                                    for (const d of detail) {
                                        if (d.type === 'season_price' && d.cost && !seasonPrice) {
                                            seasonPrice = Math.round(d.cost);
                                        }
                                        if (d.type === 1 && d.cost && !basePrice) {
                                            basePrice = Math.round(d.cost);
                                        }
                                    }
                                    return seasonPrice || basePrice;
                                })()
                            };

                        } catch (e) {
                            return {status: 'error', error: 'exception_' + e.message};
                        }
                    });

                    const batchResults = await Promise.all(promises);
                    results.push(...batchResults);

                    if (batchIdx < batches.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, batchDelay * 1000));
                    }
                }

                return results;
            }
            """,
            {
                "objectId": object_id,
                "token": token,
                "guests": guests,
                "daysData": days_data,
                "batchSize": API_BATCH_SIZE,
                "batchDelay": API_BATCH_DELAY,
                "apiUrl": API_PRICES_URL,
            },
        )

        calendar: list[int] = []
        errors_details: list[dict[str, str | int]] = []

        for day_idx, day_result in enumerate(result):
            status = day_result.get("status", "error")

            if status == "ok":
                if day_result.get("busy", False):
                    calendar.append(1)
                else:
                    calendar.append(0)
            elif status == "obj_error":
                errors_details.append({
                    "day": day_idx,
                    "error": "obj_error",
                    "errors": str(day_result.get("errors", [])),
                    "error_body": day_result.get("error_body", ""),
                })
                calendar.append(-1)
            else:
                errors_details.append({
                    "day": day_idx,
                    "error": day_result.get("error", "unknown"),
                })
                calendar.append(-1)

        return calendar, errors_details

    async def sliding_window_with_prices(
        self, page: Page, object_id: str, token: str,
        nights: int = 2, guests: int = DEFAULT_GUESTS
    ) -> tuple[list[int], list[int]]:
        """Скользящее окно с извлечением цен из каждого ответа.

        Используется как последний fallback — получает и занятость, и цены.

        Args:
            page: Вкладка браузера.
            object_id: ID объявления.
            token: Токен API.
            nights: Количество ночей в окне.
            guests: Количество гостей.

        Returns:
            Кортеж (calendar, prices).
        """
        today = date.today()

        days_data = []
        for i in range(DAYS_COUNT):
            day = today + timedelta(days=i)
            end_day = day + timedelta(days=nights)
            days_data.append({
                "date_begin": f"{day.isoformat()} 14:00:00",
                "date_end": f"{end_day.isoformat()} 11:00:00",
            })

        result = await page.evaluate(
            """
            async ({objectId, token, guests, daysData, batchSize, batchDelay, apiUrl}) => {
                const results = [];
                const batches = [];

                for (let i = 0; i < daysData.length; i += batchSize) {
                    batches.push(daysData.slice(i, i + batchSize));
                }

                for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
                    const batch = batches[batchIdx];

                    const promises = batch.map(async (dayInfo) => {
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
                                    date_begin: dayInfo.date_begin,
                                    date_end: dayInfo.date_end,
                                    currency_id: 1,
                                    is_pets: 0,
                                    documents: 0,
                                    target: 0,
                                    ages: [],
                                    no_time: 1
                                })
                            });

                            if (!resp.ok) return {status: 'error'};

                            const data = await resp.json();

                            if (!data.success || !data.data || !data.data.objects || !data.data.objects[0]) {
                                return {status: 'error'};
                            }

                            const obj = data.data.objects[0];
                            if (!obj.success) return {status: 'obj_error'};

                            const objData = obj.data;
                            let price = 0;
                            if (objData.detail) {
                                let seasonPrice = 0;
                                let basePrice = 0;
                                for (const d of objData.detail) {
                                    if (d.type === 'season_price' && d.cost && !seasonPrice) {
                                        seasonPrice = Math.round(d.cost);
                                    }
                                    if (d.type === 1 && d.cost && !basePrice) {
                                        basePrice = Math.round(d.cost);
                                    }
                                }
                                price = seasonPrice || basePrice;
                            }

                            return {
                                status: 'ok',
                                busy: objData.busy === 'busy',
                                price: price
                            };

                        } catch (e) {
                            return {status: 'error'};
                        }
                    });

                    const batchResults = await Promise.all(promises);
                    results.push(...batchResults);

                    if (batchIdx < batches.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, batchDelay * 1000));
                    }
                }

                return results;
            }
            """,
            {
                "objectId": object_id,
                "token": token,
                "guests": guests,
                "daysData": days_data,
                "batchSize": API_BATCH_SIZE,
                "batchDelay": API_BATCH_DELAY,
                "apiUrl": API_PRICES_URL,
            },
        )

        calendar: list[int] = []
        prices: list[int] = []

        for day_result in result:
            if day_result.get("status") == "ok":
                if day_result.get("busy", False):
                    calendar.append(1)
                    prices.append(0)
                else:
                    calendar.append(0)
                    prices.append(day_result.get("price", 0))
            else:
                # Ошибка → свободен (лучше не терять данные)
                calendar.append(0)
                prices.append(0)

        return calendar, prices