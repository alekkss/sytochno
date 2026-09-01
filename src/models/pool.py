"""Модель записи пула объявлений.

Пул — список ID объявлений, с которыми работает этап batch-обогащения.
Записи появляются из двух источников: первичный импорт из Excel-отчёта
и плановая синхронизация с каталогом sutochno.ru (этап 1 по расписанию).

external_id хранится как строка — в том же типе, что и в таблице
listings и во всём конвейере парсера (RawListing.external_id: str).
"""

from dataclasses import dataclass
from datetime import datetime

# Источник записи: первичный импорт из Excel-отчёта
POOL_SOURCE_EXCEL: str = "excel_import"

# Источник записи: плановая синхронизация каталога (этап 1)
POOL_SOURCE_CATALOG: str = "catalog_sync"


@dataclass
class PoolEntry:
    """Запись пула: один ID объявления и метаданные его появления.

    Атрибуты:
        external_id: ID объявления на sutochno.ru (строка из цифр,
            как в RawListing.external_id).
        added_at: Дата и время добавления ID в пул (UTC).
        last_seen_at: Дата и время последнего обнаружения ID в каталоге
            при плановой синхронизации (UTC). Пока удаление из пула
            отключено — поле заполняется на будущее.
        source: Источник записи: excel_import или catalog_sync.
    """

    external_id: str
    added_at: datetime | None = None
    last_seen_at: datetime | None = None
    source: str = POOL_SOURCE_CATALOG

    def __post_init__(self) -> None:
        """Проверяет корректность ID объявления.

        ID в проекте — строка из цифр (sutochno.ru использует числовые ID,
        но конвейер работает с ними как со строками для единообразия
        с таблицей listings).

        Raises:
            ValueError: если ID пуст, не является строкой или содержит
                нецифровые символы.
        """
        if not isinstance(self.external_id, str):
            raise ValueError(
                f"Некорректный ID объявления: {self.external_id!r}. "
                "Ожидалась строка."
            )

        stripped = self.external_id.strip()

        if not stripped or not stripped.isdigit():
            raise ValueError(
                f"Некорректный ID объявления: {self.external_id!r}. "
                "Ожидалась непустая строка из цифр."
            )
