"""Абстракция и SQLite-реализация репозитория снимков объявлений."""

import json
import sqlite3
from abc import ABC, abstractmethod
from datetime import date, datetime

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
        """Возвращает активное соединение, создавая его при необходимости.

        Returns:
            Активное соединение с БД.
        """
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def initialize(self) -> None:
        """Создаёт таблицы снимков, если они не существуют."""
        conn = self._get_conn()
        conn.executescript("""
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
        """)
        conn.commit()
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

        logger.info(
            "снимок_сохранён",
            external_id=snapshot.listing_external_id,
            snapshot_id=snapshot_id,
        )
        return snapshot_id  # type: ignore[return-value]

    def get_last_two(self, listing_external_id: str) -> list[ListingSnapshot]:
        """Возвращает два последних снимка для объявления (от старого к новому).

        Args:
            listing_external_id: Внешний ID объявления.

        Returns:
            Список из 0, 1 или 2 снимков.
        """
        conn = self._get_conn()

        # Берём два последних снимка по дате
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