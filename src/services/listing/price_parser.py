"""Парсинг цен из detail[] ответа API — разворачивание season_price."""

from datetime import date, timedelta

from src.config.logger import get_logger
from src.services.listing.constants import (
    BASE_PRICE_TYPE_INT,
    DAYS_COUNT,
    SEASON_PRICE_TYPE,
)

logger = get_logger("price_parser")


class PriceParser:
    """Извлечение и разворачивание цен из массива detail[] API-ответа.

    Обрабатывает два формата ценовых записей:

    1. type="season_price" — сезонные цены с диапазонами дат
       (date_begin, date_end заполнены). Каждая запись покрывает
       конкретный период. Разворачиваются в дневные цены.

    2. type=1 (числовой) — единая базовая цена за сутки.
       Поля date_begin/date_end = null. Применяется ко всем дням,
       не покрытым записями season_price (fallback).

    Записи type="interval" (скидки за длительность), "dop_persons"
    (доплата за гостей), "sale" (акции) игнорируются — это не базовая
    цена за сутки.
    """

    def extract_prices_from_detail(self, detail: list[dict]) -> list[int]:
        """Извлекает массив цен на 60 дней из detail[].

        Приоритет: season_price (с датами) → type=1 (базовая цена).

        Args:
            detail: Массив detail[] из ответа API (bulk-запрос на 60 ночей).

        Returns:
            Список из DAYS_COUNT цен (0 если цена не определена).
        """
        today = date.today()

        # ── Извлекаем базовую цену из type=1 (fallback) ──
        base_price: int = 0
        for det in detail:
            if det.get("type") == BASE_PRICE_TYPE_INT and det.get("cost"):
                base_price = int(det["cost"])
                break

        # ── Разворачиваем season_price в дневные цены ──
        daily_prices: dict[str, int] = {}

        for det in detail:
            if det.get("type") != SEASON_PRICE_TYPE:
                continue

            d_begin = det.get("date_begin")
            d_end = det.get("date_end")
            cost = det.get("cost", 0)

            if not d_begin or not d_end or not cost:
                continue

            d_begin_str = str(d_begin)[:10]
            d_end_str = str(d_end)[:10]

            try:
                period_start = date.fromisoformat(d_begin_str)
                period_end = date.fromisoformat(d_end_str)
                current = period_start
                while current <= period_end:
                    daily_prices[current.isoformat()] = int(cost)
                    current += timedelta(days=1)
            except (ValueError, TypeError):
                continue

        # ── Формируем массив цен на 60 дней ──
        # Приоритет: season_price (с датами) → type=1 (базовая цена)
        prices_60: list[int] = []
        for i in range(DAYS_COUNT):
            day = today + timedelta(days=i)
            day_key = day.isoformat()
            price = daily_prices.get(day_key, base_price)
            prices_60.append(price)

        prices_filled = sum(1 for p in prices_60 if p > 0)

        logger.debug(
            "цены_извлечены",
            step=f"season_price={len(daily_prices)}, base_price={base_price}, "
                 f"заполнено={prices_filled}/{DAYS_COUNT}",
        )

        return prices_60

    def extract_single_day_price(self, detail: list[dict]) -> int:
        """Извлекает цену за одну ночь из detail[] (скользящее окно).

        Используется при обработке ответа на запрос одного дня.
        Приоритет: season_price → type=1.

        Args:
            detail: Массив detail[] из ответа API (запрос на 1 день).

        Returns:
            Цена за ночь (0 если не определена).
        """
        season_price: int = 0
        base_price: int = 0

        for d in detail:
            if d.get("type") == SEASON_PRICE_TYPE and d.get("cost") and not season_price:
                season_price = int(round(d["cost"]))
            if d.get("type") == BASE_PRICE_TYPE_INT and d.get("cost") and not base_price:
                base_price = int(round(d["cost"]))

        return season_price or base_price