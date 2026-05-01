"""Подпакет репозиториев — абстракция над хранилищем данных."""

from src.repositories.base import BaseListingRepository
from src.repositories.sqlite_repository import SQLiteListingRepository

__all__ = [
    "BaseListingRepository",
    "SQLiteListingRepository",
]
