"""Подпакет конфигурации — настройки приложения и логирование."""

from src.config.logger import get_logger
from src.config.settings import Settings

__all__ = [
    "Settings",
    "get_logger",
]
