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
        calendar: Строка из 60 символов '0'/'1'/'2' — статус дня:
                  '0' — свободен и бронируем; '1' — продан (дата входит
                  в занятый период из orders/getOrdersByObject);
                  '2' — техблок (не продан, но забронировать нельзя —
                  ограничение минимального срока или блок соседними
                  бронями). Индекс 0 = сегодня, индекс 59 = сегодня+59
                  дней. Схема хранения не менялась: TEXT-маска принимает
                  любой символ, история снимков совместима без миграций.
        prices: Список цен по дням (параллельно с calendar). У проданных
                дней ('1') цена 0. У дней техблока ('2') цена
                СОХРАНЯЕТСЯ — она валидна и используется при отмене
                (1→2) в comparison_service и как цена продажи в
                аналитике RentPulse.
        snapshot_id: Внутренний ID снимка (None до сохранения в БД).
    """

    listing_external_id: str
    snapshot_dt: datetime
    calendar: str  # ровно 60 символов '0'/'1'/'2'
    prices: list[DayPrice] = field(default_factory=list)
    snapshot_id: int | None = None

    def calendar_as_list(self) -> list[int]:
        """Возвращает календарь как список целых чисел (0, 1 или 2).

        Returns:
            Список из 60 элементов: 0 — свободен и бронируем,
            1 — продан, 2 — техблок (не продан, но забронировать нельзя).
        """
        return [int(ch) for ch in self.calendar]

    def price_for_date(self, target: date) -> float | None:
        """Возвращает цену за указанную дату или None, если не найдена.

        Для проданных дней цена равна 0 (обнуляется при сборе).
        Для дней техблока и свободных дней — актуальная цена из
        prices_60_days прогона, снявшего снимок.

        Args:
            target: Дата, для которой нужна цена.

        Returns:
            Цена в рублях или None.
        """
        for dp in self.prices:
            if dp.date == target:
                return dp.price
        return None
