"""Подпакет моделей — доменные объекты приложения."""

from src.models.listing import RawListing
from src.models.proxy import ProxyConfig

__all__ = [
    "ProxyConfig",
    "RawListing",
]
