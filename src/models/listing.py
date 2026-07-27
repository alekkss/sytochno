"""Модель объявления посуточной аренды с sutochno.ru."""

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RawListing:
    """Данные объявления, извлечённые с sutochno.ru.

    Attributes:
        external_id: Уникальный идентификатор объявления на sutochno.ru.
        title: Название объявления (заголовок карточки).
        price_per_night: Цена за сутки в рублях.
        rating: Рейтинг объекта (например, 9.1).
        review_count: Количество отзывов.
        area_m2: Площадь объекта в квадратных метрах.
        guests: Количество гостей.
        address: Полный адрес объекта.
        metro_station: Ближайшая станция метро с расстоянием.
        has_instant_booking: Наличие быстрого бронирования.
        calendar_60_days: Массив занятости на 60 дней (0 — свободен, 1 — занят).
        prices_60_days: Массив цен за сутки на 60 дней (0 — день занят).
        url: Прямая ссылка на объявление.
        snapshot_date: Дата и время сбора данных.
        lat: Широта объекта (координата на карте).
        lng: Долгота объекта (координата на карте).
        rooms: Количество комнат (0 — студия, 1, 2, 3, 4+).
        property_type: Тип жилья из API («Квартира», «Комната», «Дом» и т.д.).
        enrichment_skip_reason: Причина невозможности обогащения (None = можно обогатить).
            Заполняется при обнаружении фатальных ошибок:
            - "min_nights_exceeded" — min_nights объекта превышает окно анализа (60 дней).
            - "object_not_found" — объявление удалено или заблокировано на сайте.
            Карточки с заполненным полем мгновенно исключаются из повторного обогащения.
        price_per_sqm: Средняя стоимость за м² в сутки (руб./м²/сут.).
            Рассчитывается после очистки выбросов как средняя цена по свободным дням ÷ площадь.
            None — если площадь не указана или нет свободных дней с ценами.
    """

    external_id: str
    title: str
    url: str
    price_per_night: int | None = None
    rating: float | None = None
    review_count: int | None = None
    area_m2: int | None = None
    guests: int | None = None
    address: str | None = None
    metro_station: str | None = None
    has_instant_booking: bool = False
    calendar_60_days: list[int] = field(default_factory=list)
    prices_60_days: list[int] = field(default_factory=list)
    snapshot_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    lat: float | None = None
    lng: float | None = None
    rooms: int | None = None
    property_type: str | None = None
    enrichment_skip_reason: str | None = None
    price_per_sqm: float | None = None

    @property
    def occupancy_percent(self) -> float:
        """Вычисляет процент занятости из календаря на 60 дней.

        Returns:
            Процент занятых дней (0.0–100.0). Если календарь пуст — 0.0.
        """
        if not self.calendar_60_days:
            return 0.0
        occupied = sum(self.calendar_60_days)
        return round((occupied / len(self.calendar_60_days)) * 100, 1)

    @property
    def average_price(self) -> int:
        """Вычисляет среднюю цену за сутки по свободным дням.

        Returns:
            Средняя цена (целое число). Если нет свободных дней — 0.
        """
        if not self.prices_60_days:
            return 0
        non_zero = [p for p in self.prices_60_days if p > 0]
        if not non_zero:
            return 0
        return round(sum(non_zero) / len(non_zero))

    def calculate_price_per_sqm(self) -> None:
        """Рассчитывает и сохраняет среднюю стоимость за м² в сутки.

        Формула: средняя цена по свободным дням ÷ площадь.
        Если площадь не указана (None или 0) или нет свободных дней — устанавливает None.
        Вызывается после очистки выбросов в DataCleanerService.
        """
        if not self.area_m2 or self.area_m2 <= 0:
            self.price_per_sqm = None
            return

        avg_price = self.average_price
        if avg_price <= 0:
            self.price_per_sqm = None
            return

        self.price_per_sqm = round(avg_price / self.area_m2, 2)

    def __post_init__(self) -> None:
        """Валидация обязательных полей после инициализации.

        Raises:
            ValueError: Если external_id, title или url пустые.
        """
        if not self.external_id or not self.external_id.strip():
            raise ValueError("external_id не может быть пустым.")
        if not self.title or not self.title.strip():
            raise ValueError("title не может быть пустым.")
        if not self.url or not self.url.strip():
            raise ValueError("url не может быть пустым.")

        # Нормализация
        self.external_id = self.external_id.strip()
        self.title = self.title.strip()
        self.url = self.url.strip()
