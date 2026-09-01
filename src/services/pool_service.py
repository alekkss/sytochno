"""Сервис пула ID объявлений — синхронизация и выдача для обогащения.

Отделяет бизнес-логику работы с пулом от персистентности:

- sync_from_catalog() — плановая синхронизация: находит ID каталога,
  которых ещё нет в пуле, добавляет их (source=catalog_sync) и
  обновляет метку last_seen_at для всех собранных ID. Удаление
  не выполняется — пул только растёт.
- add_ids() — добавление произвольного набора ID с указанием источника
  (используется импортёром из Excel, source=excel_import).
- get_enrichment_ids() — выдача всех ID пула этапу batch-обогащения.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from src.config.logger import get_logger
from src.models.pool import POOL_SOURCE_CATALOG, PoolEntry
from src.repositories.base_pool_repository import BasePoolRepository

logger = get_logger("pool_service")


@dataclass
class PoolAddResult:
    """Результат операции добавления ID в пул.

    Attributes:
        requested: Сколько ID передано для добавления.
        added: Сколько реально вставлено новых записей.
        skipped_existing: Сколько ID уже были в пуле (пропущены).
        skipped_invalid: Сколько ID отброшено как некорректные
            (пустые или нецифровые).
    """

    requested: int = 0
    added: int = 0
    skipped_existing: int = 0
    skipped_invalid: int = 0


@dataclass
class PoolSyncResult:
    """Результат полной синхронизации пула с каталогом.

    Attributes:
        collected: Сколько уникальных ID собрал каталог.
        new_ids: Сколько добавлено новых записей в пул.
        updated_seen: Сколько меток last_seen_at обновлено.
        pool_total: Общий размер пула после синхронизации.
    """

    collected: int = 0
    new_ids: int = 0
    updated_seen: int = 0
    pool_total: int = 0


class PoolService:
    """Сервис бизнес-логики пула ID объявлений.

    Зависит только от абстракции BasePoolRepository (Dependency Inversion):
    конкретная реализация (SQLite/PostgreSQL) подставляется через фабрику.
    """

    def __init__(self, pool_repository: BasePoolRepository) -> None:
        """Инициализирует сервис.

        Args:
            pool_repository: Репозиторий пула (инициализированный).
        """
        self._repository = pool_repository

    def sync_from_catalog(
        self,
        external_ids: set[str],
        now: datetime | None = None,
    ) -> PoolSyncResult:
        """Синхронизирует пул с каталогом: добавляет новые ID, обновляет метки.

        Последовательность:
        1. Очищает вход от некорректных ID.
        2. Через exists_ids находит ID, отсутствующие в пуле.
        3. Добавляет их как PoolEntry с source=catalog_sync,
           added_at = last_seen_at = now.
        4. Обновляет last_seen_at для всех собранных ID (включая новые —
           единая метка на всю синхронизацию).
        5. Возвращает статистику.

        Метод не удаляет записи — пул только накапливается (требование).

        Args:
            external_ids: Набор ID, собранных каталогом (этап 1).
            now: Метка времени синхронизации (UTC). None = текущий момент.

        Returns:
            Статистика синхронизации.
        """
        sync_time = now or datetime.now(timezone.utc)

        collected, valid_ids = self._split_valid(external_ids)

        existing = self._repository.exists_ids(valid_ids)
        new_ids = valid_ids - existing

        added = 0
        if new_ids:
            entries = [
                PoolEntry(
                    external_id=external_id,
                    added_at=sync_time,
                    last_seen_at=sync_time,
                    source=POOL_SOURCE_CATALOG,
                )
                for external_id in new_ids
            ]
            added = self._repository.add_entries(entries)

        updated_seen = self._repository.update_last_seen(valid_ids, sync_time)
        pool_total = self._repository.count()

        logger.info(
            "пул_синхронизирован_с_каталогом",
            collected=collected,
            new_ids=added,
            skipped_existing=len(existing),
            updated_seen=updated_seen,
            pool_total=pool_total,
        )

        return PoolSyncResult(
            collected=collected,
            new_ids=added,
            updated_seen=updated_seen,
            pool_total=pool_total,
        )

    def add_ids(
        self,
        external_ids: set[str],
        source: str,
        now: datetime | None = None,
    ) -> PoolAddResult:
        """Добавляет набор ID в пул с указанием источника.

        Идемпотентен: существующие ID пропускаются, их метаданные
        (added_at, source) не перезаписываются. Метка last_seen_at
        не трогается — она имеет смысл только для каталога.

        Используется импортёром из Excel (source=excel_import).

        Args:
            external_ids: Набор ID для добавления.
            source: Источник записей (константы POOL_SOURCE_*).
            now: Метка времени добавления (UTC). None = текущий момент.

        Returns:
            Статистика добавления.
        """
        add_time = now or datetime.now(timezone.utc)

        requested, valid_ids = self._split_valid(external_ids)

        existing = self._repository.exists_ids(valid_ids)
        new_ids = valid_ids - existing

        added = 0
        if new_ids:
            entries = [
                PoolEntry(
                    external_id=external_id,
                    added_at=add_time,
                    last_seen_at=None,
                    source=source,
                )
                for external_id in new_ids
            ]
            added = self._repository.add_entries(entries)

        logger.info(
            "id_добавлены_в_пул",
            source=source,
            requested=requested,
            added=added,
            skipped_existing=len(existing),
            pool_total=self._repository.count(),
        )

        return PoolAddResult(
            requested=requested,
            added=added,
            skipped_existing=len(existing),
            skipped_invalid=requested - len(valid_ids),
        )

    def get_enrichment_ids(self) -> list[str]:
        """Возвращает все ID пула для этапа batch-обогащения.

        Логирует размер пула; пустой пул логируется отдельно —
        вызывающий код использует это как признак пропуска прогона.

        Returns:
            Список всех ID пула (может быть пустым).
        """
        ids = self._repository.get_all_ids()

        if not ids:
            logger.warning(
                "пул_пуст_прогон_пропущен",
                step="пул не содержит ни одного ID — выполните импорт "
                     "или дождитесь синхронизации каталога",
            )
        else:
            logger.info(
                "id_получены_из_пула",
                total=len(ids),
            )

        return ids

    def count(self) -> int:
        """Возвращает текущий размер пула.

        Returns:
            Количество записей в пуле.
        """
        return self._repository.count()

    @staticmethod
    def _split_valid(external_ids: set[str]) -> tuple[int, set[str]]:
        """Отделяет корректные ID от некорректных.

        ID считается корректным, если это непустая строка из цифр —
        то же правило, что валидация PoolEntry. Отбраковка здесь, а не
        исключением из PoolEntry, гарантирует: одна мусорная строка
        (например, из Excel) не сорвёт всю синхронизацию.

        Args:
            external_ids: Входной набор ID.

        Returns:
            Кортеж (исходное количество, множество корректных ID).
        """
        valid = {
            external_id
            for external_id in external_ids
            if isinstance(external_id, str)
            and external_id.strip().isdigit()
        }

        invalid_count = len(external_ids) - len(valid)

        if invalid_count > 0:
            logger.warning(
                "некорректные_id_отброшены",
                invalid=invalid_count,
                valid=len(valid),
                step="ожидалась непустая строка из цифр",
            )

        return len(external_ids), valid
