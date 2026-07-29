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
                prices_60_days TEXT NOT NULL DEFAULT '[]',
                snapshot_date TEXT NOT NULL,
                lat REAL,
                lng REAL,
                rooms INTEGER,
                property_type TEXT
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_listings_external_id
            ON listings (external_id)
        """)
        conn.commit()

    def _migrate(self) -> None:
        """Миграция: добавляет отсутствующие столбцы для обратной совместимости.

        Проверяет наличие столбцов и добавляет их при отсутствии.
        Безопасно для повторного вызова — ALTER TABLE IF NOT EXISTS
        эмулируется через проверку PRAGMA table_info.
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

        if "prices_60_days" not in columns:
            conn.execute(
                "ALTER TABLE listings ADD COLUMN prices_60_days TEXT NOT NULL DEFAULT '[]'"
            )
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_prices_60_days")

        if "lat" not in columns:
            conn.execute("ALTER TABLE listings ADD COLUMN lat REAL")
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_lat")

        if "lng" not in columns:
            conn.execute("ALTER TABLE listings ADD COLUMN lng REAL")
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_lng")

        if "rooms" not in columns:
            conn.execute("ALTER TABLE listings ADD COLUMN rooms INTEGER")
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_rooms")

        if "property_type" not in columns:
            conn.execute("ALTER TABLE listings ADD COLUMN property_type TEXT")
            conn.commit()
            logger.info("миграция_выполнена", step="добавлен_столбец_property_type")

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
                has_instant_booking, calendar_60_days, prices_60_days, snapshot_date,
                lat, lng, rooms, property_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                prices_60_days = excluded.prices_60_days,
                snapshot_date = excluded.snapshot_date,
                lat = excluded.lat,
                lng = excluded.lng,
                rooms = excluded.rooms,
                property_type = excluded.property_type
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
                has_instant_booking, calendar_60_days, prices_60_days, snapshot_date,
                lat, lng, rooms, property_type
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                prices_60_days = excluded.prices_60_days,
                snapshot_date = excluded.snapshot_date,
                lat = excluded.lat,
                lng = excluded.lng,
                rooms = excluded.rooms,
                property_type = excluded.property_type
            """,
            rows,
        )
        conn.commit()

        saved_count = len(rows)
        logger.info("объявления_сохранены", total=saved_count)
        return saved_count

    def delete_not_in_ids(self, active_external_ids: set[str]) -> int:
        """Удаляет из БД объявления, отсутствующие в переданном наборе ID.

        Используется для очистки удалённых с сайта объявлений: если объект
        не появился в каталоге текущего прогона — значит он удалён с сайта
        и должен быть удалён из БД.

        Безопасность: вызывается только после успешного сбора каталога
        с достаточным количеством объявлений (проверка на стороне вызывающего).

        Args:
            active_external_ids: Набор external_id, собранных в текущем прогоне.

        Returns:
            Количество удалённых записей.
        """
        if not active_external_ids:
            return 0

        conn = self._get_connection()

        # Получаем все external_id из БД
        cursor = conn.execute("SELECT external_id FROM listings")
        all_db_ids = {row["external_id"] for row in cursor.fetchall()}

        # Определяем ID для удаления
        ids_to_delete = all_db_ids - active_external_ids

        if not ids_to_delete:
            return 0

        # Удаляем пачками (SQLite ограничивает количество параметров)
        deleted_count = 0
        batch_size = 500

        ids_list = list(ids_to_delete)
        for i in range(0, len(ids_list), batch_size):
            batch = ids_list[i: i + batch_size]
            placeholders = ",".join("?" * len(batch))
            conn.execute(
                f"DELETE FROM listings WHERE external_id IN ({placeholders})",
                batch,
            )
            deleted_count += len(batch)

        conn.commit()

        if deleted_count > 0:
            logger.info(
                "удалены_неактивные_объявления",
                deleted=deleted_count,
                active=len(active_external_ids),
                was_in_db=len(all_db_ids),
            )

        return deleted_count

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

    def get_empty_listings(self) -> list[RawListing]:
        """Возвращает объявления с пустыми данными календаря и цен.

        Карточка считается «пустой», если:
        - calendar_60_days = '[]' или содержит только нули;
        - И prices_60_days = '[]' или содержит только нули.

        SQL-фильтрация выполняется по текстовому представлению JSON:
        - '[]' — пустой массив (данные не собирались).
        - Массив из нулей — данные не были получены (sentinel сбоя).

        Дополнительная проверка на уровне Python отсеивает пограничные случаи.

        Returns:
            Список объявлений без данных о занятости и ценах.
        """
        conn = self._get_connection()

        cursor = conn.execute("""
            SELECT * FROM listings
            WHERE (
                calendar_60_days = '[]'
                OR calendar_60_days = '""'
                OR calendar_60_days IS NULL
                OR calendar_60_days NOT LIKE '%1%'
            )
            AND (
                prices_60_days = '[]'
                OR prices_60_days = '""'
                OR prices_60_days IS NULL
                OR prices_60_days NOT LIKE '%1%'
                AND prices_60_days NOT LIKE '%2%'
                AND prices_60_days NOT LIKE '%3%'
                AND prices_60_days NOT LIKE '%4%'
                AND prices_60_days NOT LIKE '%5%'
                AND prices_60_days NOT LIKE '%6%'
                AND prices_60_days NOT LIKE '%7%'
                AND prices_60_days NOT LIKE '%8%'
                AND prices_60_days NOT LIKE '%9%'
            )
            ORDER BY id
        """)
        rows = cursor.fetchall()

        result: list[RawListing] = []
        for row in rows:
            listing = self._row_to_listing(row)
            if self._is_listing_empty(listing):
                result.append(listing)

        logger.info(
            "пустые_карточки_найдены",
            total=len(result),
            step="get_empty_listings",
        )
        return result

    @staticmethod
    def _is_listing_empty(listing: RawListing) -> bool:
        """Проверяет, являются ли данные карточки пустыми.

        Карточка пустая, если одновременно:
        - Календарь пуст или содержит только нули (нет ни одного занятого дня).
        - Цены пусты или содержат только нули (нет ни одной цены).

        Args:
            listing: Объявление для проверки.

        Returns:
            True если карточка не содержит полезных данных.
        """
        calendar_empty = (
            not listing.calendar_60_days
            or all(c == 0 for c in listing.calendar_60_days)
        )
        prices_empty = (
            not listing.prices_60_days
            or all(p == 0 for p in listing.prices_60_days)
        )
        return calendar_empty and prices_empty

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
        int, str, str, str,
        float | None, float | None, int | None, str | None,
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
            json.dumps(listing.prices_60_days),
            listing.snapshot_date.isoformat(),
            listing.lat,
            listing.lng,
            listing.rooms,
            listing.property_type,
        )

    @staticmethod
    def _row_to_listing(row: sqlite3.Row) -> RawListing:
        """Преобразует строку из БД в объект RawListing.

        Args:
            row: Строка результата SQL-запроса.

        Returns:
            Экземпляр RawListing.
        """
        calendar_raw = row["calendar_60_days"]
        calendar: list[int] = json.loads(calendar_raw) if calendar_raw else []

        prices_raw = row["prices_60_days"]
        prices: list[int] = json.loads(prices_raw) if prices_raw else []

        row_keys = row.keys()
        lat = row["lat"] if "lat" in row_keys else None
        lng = row["lng"] if "lng" in row_keys else None
        rooms = row["rooms"] if "rooms" in row_keys else None
        property_type = row["property_type"] if "property_type" in row_keys else None

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
            prices_60_days=prices,
            snapshot_date=datetime.fromisoformat(row["snapshot_date"]),
            lat=lat,
            lng=lng,
            rooms=rooms,
            property_type=property_type,
        )
