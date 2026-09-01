"""PostgreSQL-реализация репозитория пула ID объявлений."""

from datetime import datetime, timezone

from src.config.logger import get_logger
from src.models.pool import PoolEntry
from src.repositories.base_pool_repository import BasePoolRepository

logger = get_logger("repository.pool.pg")

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError as e:
    raise ImportError(
        "Для работы с PostgreSQL необходимо установить psycopg: "
        "pip install 'psycopg[binary]' psycopg_pool"
    ) from e


class PostgreSQLPoolRepository(BasePoolRepository):
    """Репозиторий пула ID с хранением в PostgreSQL.

    Использует пул соединений (psycopg_pool.ConnectionPool) — тот же
    подход, что и PostgreSQLListingRepository. Таблица listing_pool
    создаётся автоматически при initialize().
    """

    def __init__(
        self,
        dsn: str,
        min_pool_size: int = 1,
        max_pool_size: int = 5,
    ) -> None:
        """Инициализирует репозиторий.

        Args:
            dsn: Строка подключения PostgreSQL.
            min_pool_size: Минимальное количество соединений в пуле.
            max_pool_size: Максимальное количество соединений в пуле.
        """
        self._dsn = dsn
        self._min_pool_size = min_pool_size
        self._max_pool_size = max_pool_size
        self._pool: ConnectionPool | None = None

    def _get_pool(self) -> ConnectionPool:
        """Возвращает активный пул соединений.

        Returns:
            Пул соединений PostgreSQL.

        Raises:
            RuntimeError: Если пул не создан (не вызван initialize).
        """
        if self._pool is None:
            raise RuntimeError(
                "Пул соединений PostgreSQL не создан. "
                "Вызовите initialize() перед использованием."
            )
        return self._pool

    def initialize(self) -> None:
        """Создаёт пул соединений и таблицу пула.

        Вызывается один раз при старте приложения.
        """
        self._pool = ConnectionPool(
            conninfo=self._dsn,
            min_size=self._min_pool_size,
            max_size=self._max_pool_size,
            kwargs={"row_factory": dict_row},
        )

        self._create_table()
        logger.info(
            "postgresql_пул_репозиторий_инициализирован",
            dsn=self._dsn.split("@")[-1],  # Без пароля — только host:port/db
        )

    def _create_table(self) -> None:
        """Создаёт таблицу listing_pool, если она не существует."""
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS listing_pool (
                        external_id TEXT PRIMARY KEY,
                        added_at TIMESTAMPTZ NOT NULL,
                        last_seen_at TIMESTAMPTZ,
                        source TEXT NOT NULL
                    )
                """)
            conn.commit()

    def add_entries(self, entries: list[PoolEntry]) -> int:
        """Добавляет записи в пул, пропуская уже существующие ID.

        Чтобы вернуть точное количество реально вставленных записей,
        метод сначала фильтрует уже существующие ID через exists_ids,
        а затем вставляет только отсутствующие. ON CONFLICT DO NOTHING
        оставлен как страховка от повторной вставки (метод идемпотентен
        и безопасен при повторных запусках без предварительной фильтрации).

        Args:
            entries: Записи для добавления.

        Returns:
            Количество реально вставленных записей.
        """
        if not entries:
            return 0

        now = datetime.now(timezone.utc)

        existing = self.exists_ids({e.external_id for e in entries})
        new_entries = [e for e in entries if e.external_id not in existing]

        if not new_entries:
            return 0

        rows = [
            (
                entry.external_id,
                entry.added_at or now,
                entry.last_seen_at,
                entry.source,
            )
            for entry in new_entries
        ]

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO listing_pool (
                        external_id, added_at, last_seen_at, source
                    ) VALUES (%s, %s, %s, %s)
                    ON CONFLICT (external_id) DO NOTHING
                    """,
                    rows,
                )
            conn.commit()

        logger.info(
            "записи_добавлены_в_пул",
            inserted=len(new_entries),
            requested=len(entries),
            skipped_existing=len(existing),
        )

        return len(new_entries)

    def exists_ids(self, external_ids: set[str]) -> set[str]:
        """Возвращает подмножество ID, которые уже есть в пуле.

        Использует оператор ANY(массив) — PostgreSQL не ограничивает
        размер массива, батчинг не требуется.

        Args:
            external_ids: Набор ID для проверки.

        Returns:
            Подмножество external_ids, присутствующее в пуле.
        """
        if not external_ids:
            return set()

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT external_id FROM listing_pool
                    WHERE external_id = ANY(%(ids)s)
                    """,
                    {"ids": list(external_ids)},
                )
                rows = cur.fetchall()

        return {row["external_id"] for row in rows}

    def get_all_ids(self) -> list[str]:
        """Возвращает все ID из пула.

        Returns:
            Список всех ID пула, отсортированный лексикографически
            (стабильный порядок между прогонами).
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT external_id FROM listing_pool "
                    "ORDER BY external_id"
                )
                rows = cur.fetchall()

        return [row["external_id"] for row in rows]

    def update_last_seen(self, external_ids: set[str], now: datetime) -> int:
        """Обновляет метку last_seen_at для перечисленных ID.

        Выполняется одним UPDATE-запросом с ANY(массив) — rowcount
        такого запроса надёжен и возвращает точное число обновлённых
        строк. ID, отсутствующие в пуле, не затрагиваются.

        Args:
            external_ids: Набор ID, замеченных в каталоге.
            now: Метка времени синхронизации (UTC).

        Returns:
            Количество обновлённых записей.
        """
        if not external_ids:
            return 0

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE listing_pool
                    SET last_seen_at = %(now)s
                    WHERE external_id = ANY(%(ids)s)
                    """,
                    {"now": now, "ids": list(external_ids)},
                )
                updated = int(cur.rowcount or 0)
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
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM listing_pool")
                row = cur.fetchone()

        return int(row["cnt"]) if row else 0

    def close(self) -> None:
        """Закрывает пул соединений с базой данных."""
        if self._pool is not None:
            self._pool.close()
            self._pool = None
            logger.info("postgresql_пул_репозиторий_закрыт")
