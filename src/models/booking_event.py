"""Доменные модели событий бронирования и отмены."""

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


class EventType(Enum):
    """Тип события, зафиксированного при сравнении снимков."""

    BOOKING = "бронь"
    CANCELLATION = "отмена"


@dataclass
class BookingEvent:
    """Событие бронирования: блок дней, где 0 → 1.

    Атрибуты:
        listing_external_id: Внешний ID объявления.
        listing_title: Название объявления (для отчёта).
        event_type: Тип события — всегда EventType.BOOKING.
        snapshot_dt: Дата и время Снимка №2 (момент фиксации брони).
        checkin_date: Дата заезда (первый день блока).
        checkout_date: Дата выезда (день после последнего дня блока).
        nights: Количество ночей в брони.
        depth_days: Глубина бронирования — разница между датой сделки
                    и датой заезда в днях.
        price_per_night: Средняя цена за ночь по дням блока (руб.).
        total_price: Итоговая стоимость брони (price_per_night × nights).
    """

    listing_external_id: str
    listing_title: str
    event_type: EventType
    snapshot_dt: datetime
    checkin_date: date
    checkout_date: date
    nights: int
    depth_days: int
    price_per_night: float
    total_price: float


@dataclass
class CancellationEvent:
    """Событие отмены бронирования: блок дней, где 1 → 0.

    Атрибуты:
        listing_external_id: Внешний ID объявления.
        listing_title: Название объявления (для отчёта).
        event_type: Тип события — всегда EventType.CANCELLATION.
        snapshot_dt: Дата и время Снимка №2 (момент фиксации отмены).
        checkin_date: Дата заезда отменённой брони (первый день блока).
        checkout_date: Дата выезда отменённой брони.
        nights: Количество ночей в отменённой брони.
        depth_days: Глубина — разница между датой фиксации и датой заезда.
        price_per_night: Средняя цена за ночь по дням блока (руб.).
        total_price: Итоговая стоимость отменённой брони.
    """

    listing_external_id: str
    listing_title: str
    event_type: EventType
    snapshot_dt: datetime
    checkin_date: date
    checkout_date: date
    nights: int
    depth_days: int
    price_per_night: float
    total_price: float


# Псевдоним типа для удобства аннотаций во всех модулях
AnyEvent = BookingEvent | CancellationEvent