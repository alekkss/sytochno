"""Абстрактный базовый репозиторий для хранения событий сравнения снимков."""

from abc import ABC, abstractmethod

from src.models.booking_event import AnyEvent


class BaseComparisonEventsRepository(ABC):
    """Абстрактный репозиторий событий бронирования и отмен.

    Определяет контракт для любого хранилища событий сравнения снимков.
    Конкретные реализации (PostgreSQL и др.) наследуют этот класс.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Инициализирует хранилище (проверяет доступность таблицы, соединение).

        Вызывается один раз при старте приложения.
        """

    @abstractmethod
    def bulk_insert(self, events: list[AnyEvent]) -> int:
        """Сохраняет пачку событий с защитой от дублирования.

        Дубликаты (одно и то же событие по external_id, event_type,
        checkin_date, deal_dt) игнорируются на уровне БД.

        Args:
            events: Список событий бронирования и отмен.

        Returns:
            Количество реально вставленных строк (без дубликатов).
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с хранилищем и освобождает ресурсы."""
