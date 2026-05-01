"""Абстрактный базовый репозиторий для хранения объявлений."""

from abc import ABC, abstractmethod

from src.models.listing import RawListing


class BaseListingRepository(ABC):
    """Абстрактный репозиторий объявлений.

    Определяет контракт для любого хранилища данных.
    Конкретные реализации (SQLite, PostgreSQL и др.) наследуют этот класс.
    """

    @abstractmethod
    def initialize(self) -> None:
        """Инициализирует хранилище (создаёт таблицы, индексы и т.д.).

        Вызывается один раз при старте приложения.
        """

    @abstractmethod
    def upsert(self, listing: RawListing) -> None:
        """Сохраняет или обновляет объявление по external_id.

        Если объявление с таким external_id уже существует — обновляет его данные.
        Если не существует — создаёт новую запись.

        Args:
            listing: Объявление для сохранения.
        """

    @abstractmethod
    def upsert_many(self, listings: list[RawListing]) -> int:
        """Сохраняет или обновляет несколько объявлений за одну операцию.

        Args:
            listings: Список объявлений для сохранения.

        Returns:
            Количество успешно сохранённых/обновлённых записей.
        """

    @abstractmethod
    def get_all(self) -> list[RawListing]:
        """Возвращает все объявления из хранилища.

        Returns:
            Список всех сохранённых объявлений.
        """

    @abstractmethod
    def get_by_external_id(self, external_id: str) -> RawListing | None:
        """Возвращает объявление по его внешнему идентификатору.

        Args:
            external_id: Идентификатор объявления на sutochno.ru.

        Returns:
            Объявление или None, если не найдено.
        """

    @abstractmethod
    def count(self) -> int:
        """Возвращает общее количество объявлений в хранилище.

        Returns:
            Количество записей.
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с хранилищем и освобождает ресурсы."""
