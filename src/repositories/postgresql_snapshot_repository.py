"""PostgreSQL-реализация репозитория снимков объявлений."""

from datetime import date, datetime, timezone

from src.config.logger import get_logger
from src.models.snapshot import DayPrice, ListingSnapshot
from src.repositories.snapshot_repository import BaseSnapshotRepository

logger = get_logger("repository.pg.snapshot")

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError as e:
    raise ImportError(
        "Для работы с PostgreSQL необходимо установить psycopg: "
        "pip install 'psycopg[binary]' psycopg_pool"
    ) from e


class PostgreSQLSnapshotRepository(BaseSnapshotRepository):
    """Репозиторий снимков с хранением в PostgreSQL.

    Таблицы:
        listing_snapshots — основные данные снимка (external_id, дата, календарь).
        snapshot_prices   — цены по дням, привязанные к снимку (FK → listing_snapshots.id).

    Использует пул соединений для эффективного управления подключениями.

    Args:
        dsn: Строка подключения PostgreSQL.
        min_pool_size: Минимальное количество соединений в пуле.
        max_pool_size: Максимальное количество соединений в пуле.
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
                "Пул соединений PostgreSQL (снимки) не создан. "
                "Вызовите initialize() перед использованием."
            )
        return self._pool

    def initialize(self) -> None:
        """Создаёт пул соединений, таблицы и индексы для снимков."""
        self._pool = ConnectionPool(
            conninfo=self._dsn,
            min_size=self._min_pool_size,
            max_size=self._max_pool_size,
            kwargs={"row_factory": dict_row},
        )

        self._create_tables()
        logger.info("postgresql_снимки_инициализированы")

    def _create_tables(self) -> None:
        """Создаёт таблицы listing_snapshots и snapshot_prices."""
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS listing_snapshots (
                        id BIGSERIAL PRIMARY KEY,
                        external_id TEXT NOT NULL,
                        snapshot_dt TIMESTAMPTZ NOT NULL,
                        calendar TEXT NOT NULL
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_snapshots_external_id_dt
                    ON listing_snapshots (external_id, snapshot_dt DESC)
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS snapshot_prices (
                        id BIGSERIAL PRIMARY KEY,
                        snapshot_id BIGINT NOT NULL
                            REFERENCES listing_snapshots(id) ON DELETE CASCADE,
                        price_date DATE NOT NULL,
                        price DOUBLE PRECISION NOT NULL
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_snapshot_prices_snapshot_id
                    ON snapshot_prices (snapshot_id)
                """)

            conn.commit()

    def save(self, snapshot: ListingSnapshot) -> int:
        """Сохраняет снимок и его цены в БД.

        Args:
            snapshot: Снимок объявления.

        Returns:
            Присвоенный внутренний ID снимка.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO listing_snapshots (external_id, snapshot_dt, calendar)
                    VALUES (%(external_id)s, %(snapshot_dt)s, %(calendar)s)
                    RETURNING id
                    """,
                    {
                        "external_id": snapshot.listing_external_id,
                        "snapshot_dt": snapshot.snapshot_dt,
                        "calendar": snapshot.calendar,
                    },
                )
                row = cur.fetchone()
                snapshot_id: int = row["id"]

                if snapshot.prices:
                    self._insert_prices(cur, snapshot_id, snapshot.prices)

            conn.commit()

        logger.debug(
            "снимок_сохранён",
            external_id=snapshot.listing_external_id,
            snapshot_id=snapshot_id,
        )
        return snapshot_id

    def save_batch(self, snapshots: list[ListingSnapshot]) -> int:
        """Сохраняет партию снимков одной транзакцией.

        Все INSERT выполняются в рамках одного COMMIT —
        это значительно быстрее поштучного сохранения.

        Args:
            snapshots: Список снимков для сохранения.

        Returns:
            Количество успешно сохранённых снимков.
        """
        if not snapshots:
            return 0

        pool = self._get_pool()
        saved_count = 0

        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    for snapshot in snapshots:
                        cur.execute(
                            """
                            INSERT INTO listing_snapshots
                                (external_id, snapshot_dt, calendar)
                            VALUES (%(external_id)s, %(snapshot_dt)s, %(calendar)s)
                            RETURNING id
                            """,
                            {
                                "external_id": snapshot.listing_external_id,
                                "snapshot_dt": snapshot.snapshot_dt,
                                "calendar": snapshot.calendar,
                            },
                        )
                        row = cur.fetchone()
                        snapshot_id: int = row["id"]
                        snapshot.snapshot_id = snapshot_id

                        if snapshot.prices:
                            self._insert_prices(cur, snapshot_id, snapshot.prices)

                        saved_count += 1

                conn.commit()

            logger.info(
                "батч_снимков_сохранён",
                total=saved_count,
            )

        except Exception as e:
            logger.error(
                "ошибка_батчевого_сохранения_снимков",
                error=str(e),
                error_type=type(e).__name__,
                total=len(snapshots),
            )
            raise

        return saved_count

    def get_last_two(self, listing_external_id: str) -> list[ListingSnapshot]:
        """Возвращает два последних снимка для объявления (от старого к новому).

        Args:
            listing_external_id: Внешний ID объявления.

        Returns:
            Список из 0, 1 или 2 снимков.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Получаем два последних снимка
                cur.execute(
                    """
                    SELECT id, external_id, snapshot_dt, calendar
                    FROM listing_snapshots
                    WHERE external_id = %(external_id)s
                    ORDER BY snapshot_dt DESC
                    LIMIT 2
                    """,
                    {"external_id": listing_external_id},
                )
                snapshot_rows = cur.fetchall()

                if not snapshot_rows:
                    return []

                # Загружаем цены для найденных снимков
                snapshot_ids = [row["id"] for row in snapshot_rows]
                cur.execute(
                    """
                    SELECT snapshot_id, price_date, price
                    FROM snapshot_prices
                    WHERE snapshot_id = ANY(%(ids)s)
                    ORDER BY snapshot_id, price_date
                    """,
                    {"ids": snapshot_ids},
                )
                price_rows = cur.fetchall()

        # Группируем цены по snapshot_id
        prices_by_snapshot: dict[int, list[DayPrice]] = {}
        for row in price_rows:
            s_id = row["snapshot_id"]
            if s_id not in prices_by_snapshot:
                prices_by_snapshot[s_id] = []
            prices_by_snapshot[s_id].append(
                DayPrice(
                    date=row["price_date"] if isinstance(row["price_date"], date)
                    else date.fromisoformat(str(row["price_date"])),
                    price=row["price"],
                )
            )

        # Собираем снимки (разворачиваем: старый → новый)
        snapshots: list[ListingSnapshot] = []
        for row in reversed(snapshot_rows):
            s_id = row["id"]
            snapshot_dt = row["snapshot_dt"]
            if snapshot_dt is not None and snapshot_dt.tzinfo is None:
                snapshot_dt = snapshot_dt.replace(tzinfo=timezone.utc)

            snapshots.append(
                ListingSnapshot(
                    snapshot_id=s_id,
                    listing_external_id=row["external_id"],
                    snapshot_dt=snapshot_dt,
                    calendar=row["calendar"],
                    prices=prices_by_snapshot.get(s_id, []),
                )
            )

        return snapshots

    def get_last_two_batch(
        self, external_ids: list[str]
    ) -> dict[str, list[ListingSnapshot]]:
        """Возвращает два последних снимка для каждого ID из списка.

        Использует оконную функцию ROW_NUMBER() и ANY(array) для
        эффективной выборки — ровно 2 SQL-запроса независимо от
        количества объявлений (без временных таблиц).

        Args:
            external_ids: Список внешних ID объявлений.

        Returns:
            Словарь {external_id: [старый_снимок, новый_снимок]}.
        """
        if not external_ids:
            return {}

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # ── Шаг 1: Два последних снимка для каждого external_id ──
                cur.execute(
                    """
                    SELECT id, external_id, snapshot_dt, calendar
                    FROM (
                        SELECT
                            id,
                            external_id,
                            snapshot_dt,
                            calendar,
                            ROW_NUMBER() OVER (
                                PARTITION BY external_id
                                ORDER BY snapshot_dt DESC
                            ) AS rn
                        FROM listing_snapshots
                        WHERE external_id = ANY(%(ids)s)
                    ) sub
                    WHERE rn <= 2
                    """,
                    {"ids": external_ids},
                )
                snapshot_rows = cur.fetchall()

                if not snapshot_rows:
                    return {}

                # ── Шаг 2: Загрузка цен для всех найденных снимков ──
                snapshot_ids = [row["id"] for row in snapshot_rows]

                cur.execute(
                    """
                    SELECT snapshot_id, price_date, price
                    FROM snapshot_prices
                    WHERE snapshot_id = ANY(%(ids)s)
                    ORDER BY snapshot_id, price_date
                    """,
                    {"ids": snapshot_ids},
                )
                price_rows = cur.fetchall()

        # Группируем цены по snapshot_id
        prices_by_snapshot: dict[int, list[DayPrice]] = {}
        for row in price_rows:
            s_id = row["snapshot_id"]
            if s_id not in prices_by_snapshot:
                prices_by_snapshot[s_id] = []
            prices_by_snapshot[s_id].append(
                DayPrice(
                    date=row["price_date"] if isinstance(row["price_date"], date)
                    else date.fromisoformat(str(row["price_date"])),
                    price=row["price"],
                )
            )

        # Группируем снимки по external_id
        grouped: dict[str, list[dict]] = {}
        for row in snapshot_rows:
            ext_id = row["external_id"]
            if ext_id not in grouped:
                grouped[ext_id] = []
            grouped[ext_id].append(row)

        # ── Шаг 3: Собираем результат ──
        result: dict[str, list[ListingSnapshot]] = {}

        for ext_id, rows_list in grouped.items():
            # Сортируем: старый → новый
            rows_sorted = sorted(rows_list, key=lambda r: r["snapshot_dt"])

            snapshots: list[ListingSnapshot] = []
            for row_data in rows_sorted:
                s_id = row_data["id"]
                snapshot_dt = row_data["snapshot_dt"]
                if snapshot_dt is not None and snapshot_dt.tzinfo is None:
                    snapshot_dt = snapshot_dt.replace(tzinfo=timezone.utc)

                snapshots.append(
                    ListingSnapshot(
                        snapshot_id=s_id,
                        listing_external_id=row_data["external_id"],
                        snapshot_dt=snapshot_dt,
                        calendar=row_data["calendar"],
                        prices=prices_by_snapshot.get(s_id, []),
                    )
                )

            result[ext_id] = snapshots

        logger.info(
            "батч_снимков_загружен",
            total_ids=len(external_ids),
            total_snapshots=len(snapshot_rows),
            total_prices=len(price_rows),
        )

        return result

    def close(self) -> None:
        """Закрывает пул соединений с БД."""
        if self._pool is not None:
            self._pool.close()
            self._pool = None
            logger.info("postgresql_пул_снимков_закрыт")

    @staticmethod
    def _insert_prices(
        cur: "psycopg.Cursor",
        snapshot_id: int,
        prices: list[DayPrice],
    ) -> None:
        """Вставляет цены по дням для снимка.

        Использует executemany для эффективной вставки всех цен одной командой.

        Args:
            cur: Активный курсор.
            snapshot_id: ID снимка в БД.
            prices: Список цен по дням.
        """
        cur.executemany(
            """
            INSERT INTO snapshot_prices (snapshot_id, price_date, price)
            VALUES (%(snapshot_id)s, %(price_date)s, %(price)s)
            """,
            [
                {
                    "snapshot_id": snapshot_id,
                    "price_date": dp.date,
                    "price": dp.price,
                }
                for dp in prices
            ],
        )
