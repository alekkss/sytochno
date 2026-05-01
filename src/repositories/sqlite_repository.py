"""SQLite-реализация репозитория объявлений."""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from src.config.logger import get_logger
from src.models.listing import RawListing
from src.repositories.base import BaseListingRepository

logger = get_logger("repository")


class SQLiteListingRepository(BaseListingRepository):
    """Репозиторий объявлений с хранением в SQLite.

    Создаёт файл базы данных и директорию автоматически при инициализации.
    Поддерживает upsert-семантику: повторный парсинг обновляет существующие записи.
    """

    def __init__(self, db_path: str) -> None:
        """Инициализирует репозиторий.

        Args:
            db_path: Путь к файлу базы данных SQLite.
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
                "Соединение с базой данных не установлено. Вызовите initialize() перед использованием."
            )
        return self._connection

    def initialize(self) -> None:
        """Создаёт директорию, файл БД и таблицу объявлений.

        Вызывается один раз при старте приложения.
        """
        db_file = Path(self._db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

        self._connection = sqlite3.connect(str(db_file))
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA busy_timeout=5000")

        self._create_table()
        self._migrate()
        logger.info("база_данных_инициализирована", path=self._db_path)

    def _create_table(self) -> None:
        """Создаёт таблицу listings, если она не существует."""
        conn = self._get_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                external_id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                price_per_night INTEGER,
                rating REAL,
                review_count INTEGER,
                area_m2 INTEGER,
                guests INTEGER,
                address TEXT,
                metro_station TEXT,
                has_instant_booking INTEGER NOT NULL DEFAULT 0,
                calendar_60_days TEXT NOT NULL DEFAULT '[]',
                snapshot_date TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_listings_external_id
            ON listings (external_id)
        """)
        conn.commit()

    def _migrate(self) -> None:
        """Миграция: добавляет столбец calendar_60_days, если он отсутствует.

        Обеспечивает обратную совместимость с базами, созданными ранее.
        """
        conn = self._get_connection()
        cursor = conn.execute("PRAGMA table_info(listings)")
        columns = {row["name"] for row in cursor.fetchall()}

        if "calendar_60_days" not in columns:
            conn.execute(
                "ALTER TABLE listings ADD COLUMN calendar_60_days TEXT NOT NULL DEFAULT '[]'"
            )
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_calendar_60_days")

    def upsert(self, listing: RawListing) -> None:
        """Сохраняет или обновляет объявление по external_id.

        Args:
            listing: Объявление для сохранения.
        """
        conn = self._get_connection()
        conn.execute(
            """
            INSERT INTO listings (
                external_id, title, url, price_per_night, rating,
                review_count, area_m2, guests, address, metro_station,
                has_instant_booking, calendar_60_days, snapshot_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_id) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                price_per_night = excluded.price_per_night,
                rating = excluded.rating,
                review_count = excluded.review_count,
                area_m2 = excluded.area_m2,
                guests = excluded.guests,
                address = excluded.address,
                metro_station = excluded.metro_station,
                has_instant_booking = excluded.has_instant_booking,
                calendar_60_days = excluded.calendar_60_days,
                snapshot_date = excluded.snapshot_date
            """,
            self._listing_to_row(listing),
        )
        conn.commit()

    def upsert_many(self, listings: list[RawListing]) -> int:
        """Сохраняет или обновляет несколько объявлений за одну транзакцию.

        Args:
            listings: Список объявлений для сохранения.

        Returns:
            Количество успешно обработанных записей.
        """
        if not listings:
            return 0

        conn = self._get_connection()
        rows = [self._listing_to_row(listing) for listing in listings]

        conn.executemany(
            """
            INSERT INTO listings (
                external_id, title, url, price_per_night, rating,
                review_count, area_m2, guests, address, metro_station,
                has_instant_booking, calendar_60_days, snapshot_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(external_id) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                price_per_night = excluded.price_per_night,
                rating = excluded.rating,
                review_count = excluded.review_count,
                area_m2 = excluded.area_m2,
                guests = excluded.guests,
                address = excluded.address,
                metro_station = excluded.metro_station,
                has_instant_booking = excluded.has_instant_booking,
                calendar_60_days = excluded.calendar_60_days,
                snapshot_date = excluded.snapshot_date
            """,
            rows,
        )
        conn.commit()

        saved_count = len(rows)
        logger.info("объявления_сохранены", total=saved_count)
        return saved_count

    def get_all(self) -> list[RawListing]:
        """Возвращает все объявления из базы данных.

        Returns:
            Список всех сохранённых объявлений.
        """
        conn = self._get_connection()
        cursor = conn.execute("SELECT * FROM listings ORDER BY id")
        rows = cursor.fetchall()
        return [self._row_to_listing(row) for row in rows]

    def get_by_external_id(self, external_id: str) -> RawListing | None:
        """Возвращает объявление по внешнему идентификатору.

        Args:
            external_id: Идентификатор объявления на sutochno.ru.

        Returns:
            Объявление или None, если не найдено.
        """
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT * FROM listings WHERE external_id = ?",
            (external_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._row_to_listing(row)

    def count(self) -> int:
        """Возвращает общее количество объявлений в базе.

        Returns:
            Количество записей.
        """
        conn = self._get_connection()
        cursor = conn.execute("SELECT COUNT(*) FROM listings")
        result = cursor.fetchone()
        return int(result[0])

    def close(self) -> None:
        """Закрывает соединение с базой данных."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("соединение_с_бд_закрыто")

    @staticmethod
    def _listing_to_row(listing: RawListing) -> tuple[
        str, str, str, int | None, float | None,
        int | None, int | None, int | None, str | None, str | None,
        int, str, str,
    ]:
        """Преобразует объект RawListing в кортеж для SQL-запроса.

        Args:
            listing: Объявление.

        Returns:
            Кортеж значений в порядке столбцов таблицы.
        """
        return (
            listing.external_id,
            listing.title,
            listing.url,
            listing.price_per_night,
            listing.rating,
            listing.review_count,
            listing.area_m2,
            listing.guests,
            listing.address,
            listing.metro_station,
            1 if listing.has_instant_booking else 0,
            json.dumps(listing.calendar_60_days),
            listing.snapshot_date.isoformat(),
        )

    @staticmethod
    def _row_to_listing(row: sqlite3.Row) -> RawListing:
        """Преобразует строку из БД в объект RawListing.

        Args:
            row: Строка результата SQL-запроса.

        Returns:
            Экземпляр RawListing.
        """
        # Десериализация calendar_60_days из JSON-строки
        calendar_raw = row["calendar_60_days"]
        calendar: list[int] = json.loads(calendar_raw) if calendar_raw else []

        return RawListing(
            external_id=row["external_id"],
            title=row["title"],
            url=row["url"],
            price_per_night=row["price_per_night"],
            rating=row["rating"],
            review_count=row["review_count"],
            area_m2=row["area_m2"],
            guests=row["guests"],
            address=row["address"],
            metro_station=row["metro_station"],
            has_instant_booking=bool(row["has_instant_booking"]),
            calendar_60_days=calendar,
            snapshot_date=datetime.fromisoformat(row["snapshot_date"]),
        )
