"""Абстракция и SQLite-реализация репозитория снимков объявлений."""

import sqlite3
from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path

from src.config.logger import get_logger
from src.models.snapshot import DayPrice, ListingSnapshot

logger = get_logger("repository.snapshot")


class BaseSnapshotRepository(ABC):
    """Абстрактный интерфейс репозитория снимков.

    Определяет контракт для любого хранилища снимков.
    SQLite-реализация может быть заменена на PostgreSQL
    без изменения сервисов (LSP).
    """

    @abstractmethod
    def initialize(self) -> None:
        """Создаёт необходимые таблицы, если они не существуют."""

    @abstractmethod
    def save(self, snapshot: ListingSnapshot) -> int:
        """Сохраняет снимок и возвращает его внутренний ID.

        Args:
            snapshot: Снимок объявления для сохранения.

        Returns:
            Присвоенный внутренний ID снимка.
        """

    @abstractmethod
    def save_batch(self, snapshots: list[ListingSnapshot]) -> int:
        """Сохраняет партию снимков одной транзакцией.

        Все снимки и их цены вставляются за один COMMIT —
        это в сотни раз быстрее, чем поштучное сохранение.

        Args:
            snapshots: Список снимков для сохранения.

        Returns:
            Количество успешно сохранённых снимков.
        """

    @abstractmethod
    def get_last_two(self, listing_external_id: str) -> list[ListingSnapshot]:
        """Возвращает два последних снимка для объявления.

        Снимки отсортированы от старого к новому:
        [снимок_1 (старый), снимок_2 (новый)].
        Если снимков меньше двух — возвращает столько, сколько есть.

        Args:
            listing_external_id: Внешний ID объявления.

        Returns:
            Список из 0, 1 или 2 снимков.
        """

    @abstractmethod
    def get_last_two_batch(
        self, external_ids: list[str]
    ) -> dict[str, list[ListingSnapshot]]:
        """Возвращает два последних снимка для каждого объявления из списка.

        Батчевая версия get_last_two — выполняет минимум SQL-запросов
        для всей партии, вместо N×3 запросов поштучно.

        Args:
            external_ids: Список внешних ID объявлений.

        Returns:
            Словарь {external_id: [старый_снимок, новый_снимок]}.
            Если для ID меньше двух снимков — список будет короче.
            Если снимков нет — ключ отсутствует в словаре.
        """

    @abstractmethod
    def close(self) -> None:
        """Закрывает соединение с хранилищем."""


class SQLiteSnapshotRepository(BaseSnapshotRepository):
    """SQLite-реализация репозитория снимков.

    Таблицы:
        listing_snapshots — основные данные снимка.
        snapshot_prices   — цены по дням, привязанные к снимку.
    """

    def __init__(self, db_path: str) -> None:
        """Инициализирует репозиторий.

        Args:
            db_path: Путь к файлу базы данных SQLite.
        """
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def _get_conn(self) -> sqlite3.Connection:
        """Возвращает активное соединение с БД.

        Returns:
            Активное соединение с БД.

        Raises:
            RuntimeError: Если соединение не установлено (не вызван initialize).
        """
        if self._conn is None:
            raise RuntimeError(
                "Соединение с БД снимков не установлено. Вызовите initialize() перед использованием."
            )
        return self._conn

    def initialize(self) -> None:
        """Создаёт директорию, открывает соединение и создаёт таблицы снимков."""
        db_file = Path(self._db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

        self._conn = sqlite3.connect(str(db_file))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")

        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS listing_snapshots (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id      TEXT    NOT NULL,
                snapshot_dt      TEXT    NOT NULL,
                calendar         TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshots_external_id
                ON listing_snapshots (external_id, snapshot_dt DESC);

            CREATE TABLE IF NOT EXISTS snapshot_prices (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL REFERENCES listing_snapshots(id),
                price_date  TEXT    NOT NULL,
                price       REAL    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_snapshot_prices_snapshot_id
                ON snapshot_prices (snapshot_id);
        """)
        self._conn.commit()
        logger.info("таблицы_снимков_инициализированы")

    def save(self, snapshot: ListingSnapshot) -> int:
        """Сохраняет снимок и его цены в БД.

        Args:
            snapshot: Снимок объявления.

        Returns:
            Присвоенный внутренний ID снимка.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO listing_snapshots (external_id, snapshot_dt, calendar)
            VALUES (?, ?, ?)
            """,
            (
                snapshot.listing_external_id,
                snapshot.snapshot_dt.isoformat(),
                snapshot.calendar,
            ),
        )
        snapshot_id = cursor.lastrowid

        if snapshot.prices:
            cursor.executemany(
                """
                INSERT INTO snapshot_prices (snapshot_id, price_date, price)
                VALUES (?, ?, ?)
                """,
                [
                    (snapshot_id, dp.date.isoformat(), dp.price)
                    for dp in snapshot.prices
                ],
            )

        conn.commit()

        logger.debug(
            "снимок_сохранён",
            external_id=snapshot.listing_external_id,
            snapshot_id=snapshot_id,
        )
        return snapshot_id  # type: ignore[return-value]

    def save_batch(self, snapshots: list[ListingSnapshot]) -> int:
        """Сохраняет партию снимков одной транзакцией.

        Все INSERT выполняются в рамках одного BEGIN/COMMIT —
        это исключает fsync после каждого снимка и ускоряет
        сохранение в 100-500 раз при больших объёмах.

        Args:
            snapshots: Список снимков для сохранения.

        Returns:
            Количество успешно сохранённых снимков.
        """
        if not snapshots:
            return 0

        conn = self._get_conn()
        cursor = conn.cursor()
        saved_count = 0

        try:
            cursor.execute("BEGIN")

            for snapshot in snapshots:
                cursor.execute(
                    """
                    INSERT INTO listing_snapshots (external_id, snapshot_dt, calendar)
                    VALUES (?, ?, ?)
                    """,
                    (
                        snapshot.listing_external_id,
                        snapshot.snapshot_dt.isoformat(),
                        snapshot.calendar,
                    ),
                )
                snapshot_id = cursor.lastrowid
                snapshot.snapshot_id = snapshot_id

                if snapshot.prices:
                    cursor.executemany(
                        """
                        INSERT INTO snapshot_prices (snapshot_id, price_date, price)
                        VALUES (?, ?, ?)
                        """,
                        [
                            (snapshot_id, dp.date.isoformat(), dp.price)
                            for dp in snapshot.prices
                        ],
                    )

                saved_count += 1

            conn.commit()

            logger.info(
                "батч_снимков_сохранён",
                total=saved_count,
            )

        except Exception as e:
            conn.rollback()
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
        conn = self._get_conn()

        rows = conn.execute(
            """
            SELECT id, external_id, snapshot_dt, calendar
            FROM listing_snapshots
            WHERE external_id = ?
            ORDER BY snapshot_dt DESC
            LIMIT 2
            """,
            (listing_external_id,),
        ).fetchall()

        if not rows:
            return []

        snapshots: list[ListingSnapshot] = []
        for row in reversed(rows):  # разворачиваем: старый → новый
            prices = self._load_prices(row["id"])
            snapshots.append(
                ListingSnapshot(
                    snapshot_id=row["id"],
                    listing_external_id=row["external_id"],
                    snapshot_dt=datetime.fromisoformat(row["snapshot_dt"]),
                    calendar=row["calendar"],
                    prices=prices,
                )
            )

        return snapshots

    def get_last_two_batch(
        self, external_ids: list[str]
    ) -> dict[str, list[ListingSnapshot]]:
        """Возвращает два последних снимка для каждого ID из списка.

        Использует оконную функцию ROW_NUMBER() для выборки максимум
        двух последних снимков на каждый external_id за один запрос.
        Цены загружаются одним запросом для всех найденных снимков.

        Вместо N×3 SQL-запросов (при поштучном get_last_two) выполняет
        ровно 2 запроса независимо от количества объявлений.

        Args:
            external_ids: Список внешних ID объявлений.

        Returns:
            Словарь {external_id: [старый_снимок, новый_снимок]}.
        """
        if not external_ids:
            return {}

        conn = self._get_conn()

        # ── Шаг 1: Загрузка двух последних снимков для всех ID ──
        # SQLite поддерживает оконные функции с версии 3.25 (Python 3.8+).
        # ROW_NUMBER() OVER (PARTITION BY external_id ORDER BY snapshot_dt DESC)
        # нумерует снимки внутри каждого external_id — берём только rn <= 2.
        #
        # Для передачи большого количества ID используем временную таблицу,
        # чтобы избежать лимита на количество параметров в SQL (SQLITE_MAX_VARIABLE_NUMBER = 999).
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _batch_ids (external_id TEXT PRIMARY KEY)
        """)
        cursor.execute("DELETE FROM _batch_ids")
        cursor.executemany(
            "INSERT OR IGNORE INTO _batch_ids (external_id) VALUES (?)",
            [(ext_id,) for ext_id in external_ids],
        )

        snapshot_rows = cursor.execute("""
            SELECT id, external_id, snapshot_dt, calendar
            FROM (
                SELECT
                    ls.id,
                    ls.external_id,
                    ls.snapshot_dt,
                    ls.calendar,
                    ROW_NUMBER() OVER (
                        PARTITION BY ls.external_id
                        ORDER BY ls.snapshot_dt DESC
                    ) AS rn
                FROM listing_snapshots ls
                INNER JOIN _batch_ids b ON ls.external_id = b.external_id
            )
            WHERE rn <= 2
        """).fetchall()

        if not snapshot_rows:
            cursor.execute("DELETE FROM _batch_ids")
            return {}

        # Собираем snapshot_id → данные и группируем по external_id
        snapshot_ids: list[int] = []
        snapshots_by_id: dict[int, dict] = {}
        grouped: dict[str, list[dict]] = {}

        for row in snapshot_rows:
            s_id = row["id"]
            ext_id = row["external_id"]
            snapshot_ids.append(s_id)

            row_data = {
                "id": s_id,
                "external_id": ext_id,
                "snapshot_dt": row["snapshot_dt"],
                "calendar": row["calendar"],
            }
            snapshots_by_id[s_id] = row_data

            if ext_id not in grouped:
                grouped[ext_id] = []
            grouped[ext_id].append(row_data)

        # ── Шаг 2: Загрузка всех цен для найденных снимков одним запросом ──
        # Используем временную таблицу для snapshot_id (тот же приём).
        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _batch_snapshot_ids (snapshot_id INTEGER PRIMARY KEY)
        """)
        cursor.execute("DELETE FROM _batch_snapshot_ids")
        cursor.executemany(
            "INSERT OR IGNORE INTO _batch_snapshot_ids (snapshot_id) VALUES (?)",
            [(s_id,) for s_id in snapshot_ids],
        )

        price_rows = cursor.execute("""
            SELECT sp.snapshot_id, sp.price_date, sp.price
            FROM snapshot_prices sp
            INNER JOIN _batch_snapshot_ids bs ON sp.snapshot_id = bs.snapshot_id
            ORDER BY sp.snapshot_id, sp.price_date
        """).fetchall()

        # Группируем цены по snapshot_id
        prices_by_snapshot: dict[int, list[DayPrice]] = {}
        for row in price_rows:
            s_id = row["snapshot_id"]
            if s_id not in prices_by_snapshot:
                prices_by_snapshot[s_id] = []
            prices_by_snapshot[s_id].append(
                DayPrice(
                    date=date.fromisoformat(row["price_date"]),
                    price=row["price"],
                )
            )

        # ── Шаг 3: Собираем результат ──
        result: dict[str, list[ListingSnapshot]] = {}

        for ext_id, rows_list in grouped.items():
            # Сортируем: старый → новый (rows пришли DESC, разворачиваем)
            rows_sorted = sorted(rows_list, key=lambda r: r["snapshot_dt"])

            snapshots: list[ListingSnapshot] = []
            for row_data in rows_sorted:
                s_id = row_data["id"]
                prices = prices_by_snapshot.get(s_id, [])

                snapshots.append(
                    ListingSnapshot(
                        snapshot_id=s_id,
                        listing_external_id=row_data["external_id"],
                        snapshot_dt=datetime.fromisoformat(row_data["snapshot_dt"]),
                        calendar=row_data["calendar"],
                        prices=prices,
                    )
                )

            result[ext_id] = snapshots

        # Очищаем временные таблицы
        cursor.execute("DELETE FROM _batch_ids")
        cursor.execute("DELETE FROM _batch_snapshot_ids")

        logger.info(
            "батч_снимков_загружен",
            total_ids=len(external_ids),
            total_snapshots=len(snapshot_rows),
            total_prices=len(price_rows),
        )

        return result

    def _load_prices(self, snapshot_id: int) -> list[DayPrice]:
        """Загружает цены по дням для снимка.

        Args:
            snapshot_id: Внутренний ID снимка.

        Returns:
            Список цен по дням.
        """
        conn = self._get_conn()
        rows = conn.execute(
            """
            SELECT price_date, price
            FROM snapshot_prices
            WHERE snapshot_id = ?
            ORDER BY price_date
            """,
            (snapshot_id,),
        ).fetchall()

        return [
            DayPrice(
                date=date.fromisoformat(row["price_date"]),
                price=row["price"],
            )
            for row in rows
        ]

    def close(self) -> None:
        """Закрывает соединение с БД."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            logger.info("соединение_снимков_закрыто")
