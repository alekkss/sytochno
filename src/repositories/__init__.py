"""Подпакет репозиториев — абстракция над хранилищем данных."""

from src.repositories.base import BaseListingRepository
from src.repositories.base_comparison_events_repository import (
    BaseComparisonEventsRepository,
)
from src.repositories.postgres_comparison_events_repository import (
    PostgreSQLComparisonEventsRepository,
)
from src.repositories.sqlite_repository import SQLiteListingRepository

__all__ = [
    "BaseComparisonEventsRepository",
    "BaseListingRepository",
    "PostgreSQLComparisonEventsRepository",
    "SQLiteListingRepository",
]
