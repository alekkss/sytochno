"""Доменная модель снимка объявления в момент парсинга."""

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class DayPrice:
    """Цена за конкретный день."""

    date: date
    price: float


@dataclass
class ListingSnapshot:
    """Снимок состояния объявления в момент парсинга.

    Атрибуты:
        listing_external_id: Внешний ID объявления (с sutochno.ru).
        snapshot_dt: Дата и время снятия снимка.
        calendar: Строка из 60 символов '0'/'1' — занятость по дням.
                  Индекс 0 = сегодня, индекс 59 = сегодня+59 дней.
        prices: Список цен по дням (параллельно с calendar).
        snapshot_id: Внутренний ID снимка (None до сохранения в БД).
    """

    listing_external_id: str
    snapshot_dt: datetime
    calendar: str  # ровно 60 символов '0'/'1'
    prices: list[DayPrice] = field(default_factory=list)
    snapshot_id: int | None = None

    def calendar_as_list(self) -> list[int]:
        """Возвращает календарь как список целых чисел (0 или 1).

        Returns:
            Список из 60 элементов: 0 — свободен, 1 — занят.
        """
        return [int(ch) for ch in self.calendar]

    def price_for_date(self, target: date) -> float | None:
        """Возвращает цену за указанную дату или None, если не найдена.

        Args:
            target: Дата, для которой нужна цена.

        Returns:
            Цена в рублях или None.
        """
        for dp in self.prices:
            if dp.date == target:
                return dp.price
        return None