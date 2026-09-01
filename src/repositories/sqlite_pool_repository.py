"""SQLite-реализация репозитория пула ID объявлений."""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from src.config.logger import get_logger
from src.models.pool import PoolEntry
from src.repositories.base_pool_repository import BasePoolRepository

logger = get_logger("repository.pool")

# Максимальный размер батча для IN-запросов и executemany.
# SQLite ограничивает количество параметров в запросе (по умолчанию 999).
_BATCH_SIZE: int = 500


class SQLitePoolRepository(BasePoolRepository):
    """Репозиторий пула ID с хранением в SQLite.

    Использует ту же базу данных, что и таблица listings (db_path из настроек).
    Таблица listing_pool создаётся автоматически при initialize().
    """

    def __init__(self, db_path: str) -> None:
        """Инициализирует репозиторий.

        Args:
            db_path: Путь к файлу базы данных SQLite (общий с listings).
        """
        self._db_path = db_path
        self._connection: sqlite3.Connection | None = None

    def _get_connection(self) -> sqlite3.Connection:
        """Возвращает активное соединение с базой данных.

        Returns:
            Соединение SQLite.

        Raises:
            RuntimeError: Если соединение не установлено (не вызван initialize).
        """
        if self._connection is None:
            raise RuntimeError(
                "Соединение с базой данных не установлено. "
                "Вызовите initialize() перед использованием."
            )
        return self._connection

    def initialize(self) -> None:
        """Создаёт директорию, файл БД и таблицу пула.

        Вызывается один раз при старте приложения. Использует общий
        файл базы с таблицей listings — отдельная база не создаётся.
        """
        db_file = Path(self._db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(str(db_file))
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")

        self._create_table()
        logger.info("пул_инициализирован", path=self._db_path)

    def _create_table(self) -> None:
        """Создаёт таблицу listing_pool, если она не существует."""
        conn = self._get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS listing_pool (
                external_id TEXT PRIMARY KEY,
                added_at TEXT NOT NULL,
                last_seen_at TEXT,
                source TEXT NOT NULL
            )
        """)
        conn.commit()

    def add_entries(self, entries: list[PoolEntry]) -> int:
        """Добавляет записи в пул, пропуская уже существующие ID.

        Идемпотентность обеспечивается INSERT OR IGNORE: повторная вставка
        существующего external_id игнорируется, исходные метаданные записи
        (added_at, source) не перезаписываются.

        Args:
            entries: Записи для добавления.

        Returns:
            Количество реально вставленных записей.
        """
        if not entries:
            return 0

        conn = self._get_connection()
        now = datetime.now(timezone.utc)

        rows = [
            (
                entry.external_id,
                (entry.added_at or now).isoformat(),
                entry.last_seen_at.isoformat() if entry.last_seen_at else None,
                entry.source,
            )
            for entry in entries
        ]

        # total_changes — надёжный способ посчитать реальные вставки:
        # строки, пропущенные INSERT OR IGNORE, в разницу не попадают
        changes_before = conn.total_changes

        conn.executemany(
            """
            INSERT OR IGNORE INTO listing_pool (
                external_id, added_at, last_seen_at, source
            ) VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()

        inserted = conn.total_changes - changes_before

        if inserted > 0:
            logger.info(
                "записи_добавлены_в_пул",
                inserted=inserted,
                requested=len(rows),
            )

        return inserted

    def exists_ids(self, external_ids: set[str]) -> set[str]:
        """Возвращает подмножество ID, которые уже есть в пуле.

        Проверка выполняется батчами по _BATCH_SIZE — SQLite ограничивает
        количество параметров в одном IN-запросе.

        Args:
            external_ids: Набор ID для проверки.

        Returns:
            Подмножество external_ids, присутствующее в пуле.
        """
        if not external_ids:
            return set()

        conn = self._get_connection()

        found: set[str] = set()
        ids_list = list(external_ids)

        for i in range(0, len(ids_list), _BATCH_SIZE):
            batch = ids_list[i: i + _BATCH_SIZE]
            placeholders = ",".join("?" * len(batch))
            cursor = conn.execute(
                f"SELECT external_id FROM listing_pool "
                f"WHERE external_id IN ({placeholders})",
                batch,
            )
            found.update(row["external_id"] for row in cursor.fetchall())

        return found

    def get_all_ids(self) -> list[str]:
        """Возвращает все ID из пула.

        Returns:
            Список всех ID пула, отсортированный лексикографически
            (стабильный порядок между прогонами).
        """
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT external_id FROM listing_pool ORDER BY external_id"
        )
        return [row["external_id"] for row in cursor.fetchall()]

    def update_last_seen(self, external_ids: set[str], now: datetime) -> int:
        """Обновляет метку last_seen_at для перечисленных ID.

        ID, отсутствующие в пуле, молча пропускаются (UPDATE не затронет
        несуществующие строки).

        Args:
            external_ids: Набор ID, замеченных в каталоге.
            now: Метка времени синхронизации (UTC).

        Returns:
            Количество обновлённых записей.
        """
        if not external_ids:
            return 0

        conn = self._get_connection()
        now_iso = now.isoformat()

        updated = 0
        ids_list = list(external_ids)

        for i in range(0, len(ids_list), _BATCH_SIZE):
            batch = ids_list[i: i + _BATCH_SIZE]
            cursor = conn.executemany(
                "UPDATE listing_pool SET last_seen_at = ? WHERE external_id = ?",
                [(now_iso, external_id) for external_id in batch],
            )
            updated += cursor.rowcount

        conn.commit()

        if updated > 0:
            logger.info(
                "метки_видимости_обновлены",
                updated=updated,
                requested=len(external_ids),
            )

        return updated

    def count(self) -> int:
        """Возвращает общее количество записей в пуле.

        Returns:
            Количество записей.
        """
        conn = self._get_connection()
        cursor = conn.execute("SELECT COUNT(*) FROM listing_pool")
        result = cursor.fetchone()
        return int(result[0])

    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("соединение_пула_закрыто")
