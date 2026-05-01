"""Модель объявления посуточной аренды с sutochno.ru."""

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class RawListing:
    """Данные объявления, извлечённые со страницы каталога sutochno.ru.

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
        url: Прямая ссылка на объявление.
        snapshot_date: Дата и время сбора данных.
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
    snapshot_date: datetime = field(default_factory=datetime.now)

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
