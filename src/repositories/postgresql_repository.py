"""PostgreSQL-реализация репозитория объявлений."""

import json
from datetime import datetime, timezone

from src.config.logger import get_logger
from src.models.listing import RawListing
from src.repositories.base import BaseListingRepository

logger = get_logger("repository.pg")

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError as e:
    raise ImportError(
        "Для работы с PostgreSQL необходимо установить psycopg: "
        "pip install 'psycopg[binary]' psycopg_pool"
    ) from e


class PostgreSQLListingRepository(BaseListingRepository):
    """Репозиторий объявлений с хранением в PostgreSQL.

    Использует пул соединений (psycopg_pool.ConnectionPool) для эффективного
    управления подключениями. Таблицы и индексы создаются при инициализации.

    Args:
        dsn: Строка подключения PostgreSQL (postgresql://user:pass@host:port/db).
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
                "Пул соединений PostgreSQL не создан. "
                "Вызовите initialize() перед использованием."
            )
        return self._pool

    def initialize(self) -> None:
        """Создаёт пул соединений, таблицу и индексы.

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
            "postgresql_репозиторий_инициализирован",
            dsn=self._dsn.split("@")[-1],  # Логируем только host:port/db (без пароля)
        )

    def _create_table(self) -> None:
        """Создаёт таблицу listings и индексы, если они не существуют."""
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS listings (
                        id BIGSERIAL PRIMARY KEY,
                        external_id TEXT NOT NULL UNIQUE,
                        title TEXT NOT NULL,
                        url TEXT NOT NULL,
                        price_per_night INTEGER,
                        rating DOUBLE PRECISION,
                        review_count INTEGER,
                        area_m2 INTEGER,
                        guests INTEGER,
                        address TEXT,
                        metro_station TEXT,
                        has_instant_booking BOOLEAN NOT NULL DEFAULT FALSE,
                        calendar_60_days JSONB NOT NULL DEFAULT '[]'::jsonb,
                        prices_60_days JSONB NOT NULL DEFAULT '[]'::jsonb,
                        snapshot_date TIMESTAMPTZ NOT NULL,
                        lat DOUBLE PRECISION,
                        lng DOUBLE PRECISION,
                        rooms INTEGER,
                        property_type TEXT
                    )
                """)

                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_external_id
                    ON listings (external_id)
                """)

                # Индекс для гео-запросов (поиск конкурентов в радиусе)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_coordinates
                    ON listings (lat, lng)
                    WHERE lat IS NOT NULL AND lng IS NOT NULL
                """)

                # Индекс для фильтрации по категории жилья
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_property_type
                    ON listings (property_type)
                    WHERE property_type IS NOT NULL
                """)

                # Индекс для фильтрации по количеству комнат
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_listings_rooms
                    ON listings (rooms)
                    WHERE rooms IS NOT NULL
                """)

            conn.commit()

    def upsert(self, listing: RawListing) -> None:
        """Сохраняет или обновляет объявление по external_id.

        Args:
            listing: Объявление для сохранения.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO listings (
                        external_id, title, url, price_per_night, rating,
                        review_count, area_m2, guests, address, metro_station,
                        has_instant_booking, calendar_60_days, prices_60_days,
                        snapshot_date, lat, lng, rooms, property_type
                    ) VALUES (
                        %(external_id)s, %(title)s, %(url)s, %(price_per_night)s,
                        %(rating)s, %(review_count)s, %(area_m2)s, %(guests)s,
                        %(address)s, %(metro_station)s, %(has_instant_booking)s,
                        %(calendar_60_days)s, %(prices_60_days)s, %(snapshot_date)s,
                        %(lat)s, %(lng)s, %(rooms)s, %(property_type)s
                    )
                    ON CONFLICT (external_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        price_per_night = EXCLUDED.price_per_night,
                        rating = EXCLUDED.rating,
                        review_count = EXCLUDED.review_count,
                        area_m2 = EXCLUDED.area_m2,
                        guests = EXCLUDED.guests,
                        address = EXCLUDED.address,
                        metro_station = EXCLUDED.metro_station,
                        has_instant_booking = EXCLUDED.has_instant_booking,
                        calendar_60_days = EXCLUDED.calendar_60_days,
                        prices_60_days = EXCLUDED.prices_60_days,
                        snapshot_date = EXCLUDED.snapshot_date,
                        lat = EXCLUDED.lat,
                        lng = EXCLUDED.lng,
                        rooms = EXCLUDED.rooms,
                        property_type = EXCLUDED.property_type
                    """,
                    self._listing_to_params(listing),
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

        pool = self._get_pool()
        params_list = [self._listing_to_params(listing) for listing in listings]

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO listings (
                        external_id, title, url, price_per_night, rating,
                        review_count, area_m2, guests, address, metro_station,
                        has_instant_booking, calendar_60_days, prices_60_days,
                        snapshot_date, lat, lng, rooms, property_type
                    ) VALUES (
                        %(external_id)s, %(title)s, %(url)s, %(price_per_night)s,
                        %(rating)s, %(review_count)s, %(area_m2)s, %(guests)s,
                        %(address)s, %(metro_station)s, %(has_instant_booking)s,
                        %(calendar_60_days)s, %(prices_60_days)s, %(snapshot_date)s,
                        %(lat)s, %(lng)s, %(rooms)s, %(property_type)s
                    )
                    ON CONFLICT (external_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        url = EXCLUDED.url,
                        price_per_night = EXCLUDED.price_per_night,
                        rating = EXCLUDED.rating,
                        review_count = EXCLUDED.review_count,
                        area_m2 = EXCLUDED.area_m2,
                        guests = EXCLUDED.guests,
                        address = EXCLUDED.address,
                        metro_station = EXCLUDED.metro_station,
                        has_instant_booking = EXCLUDED.has_instant_booking,
                        calendar_60_days = EXCLUDED.calendar_60_days,
                        prices_60_days = EXCLUDED.prices_60_days,
                        snapshot_date = EXCLUDED.snapshot_date,
                        lat = EXCLUDED.lat,
                        lng = EXCLUDED.lng,
                        rooms = EXCLUDED.rooms,
                        property_type = EXCLUDED.property_type
                    """,
                    params_list,
                )
            conn.commit()

        saved_count = len(params_list)
        logger.info("объявления_сохранены", total=saved_count)
        return saved_count

    def delete_not_in_ids(self, active_external_ids: set[str]) -> int:
        """Удаляет из БД объявления, отсутствующие в переданном наборе ID.

        Args:
            active_external_ids: Набор external_id, собранных в текущем прогоне.

        Returns:
            Количество удалённых записей.
        """
        if not active_external_ids:
            return 0

        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                # Получаем все external_id из БД
                cur.execute("SELECT external_id FROM listings")
                all_db_ids = {row["external_id"] for row in cur.fetchall()}

                # Определяем ID для удаления
                ids_to_delete = all_db_ids - active_external_ids

                if not ids_to_delete:
                    return 0

                # PostgreSQL поддерживает ANY(array) — нет лимита на параметры
                cur.execute(
                    "DELETE FROM listings WHERE external_id = ANY(%(ids)s)",
                    {"ids": list(ids_to_delete)},
                )
                deleted_count = cur.rowcount

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
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM listings ORDER BY id")
                rows = cur.fetchall()

        return [self._row_to_listing(row) for row in rows]

    def get_by_external_id(self, external_id: str) -> RawListing | None:
        """Возвращает объявление по внешнему идентификатору.

        Args:
            external_id: Идентификатор объявления на sutochno.ru.

        Returns:
            Объявление или None, если не найдено.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT * FROM listings WHERE external_id = %(external_id)s",
                    {"external_id": external_id},
                )
                row = cur.fetchone()

        if row is None:
            return None
        return self._row_to_listing(row)

    def get_empty_listings(self) -> list[RawListing]:
        """Возвращает объявления с пустыми данными календаря и цен.

        Использует JSONB-операторы PostgreSQL для эффективной фильтрации:
        - calendar_60_days не содержит значение 1 (нет занятых дней).
        - prices_60_days не содержит ненулевых цен.

        Returns:
            Список объявлений без данных о занятости и ценах.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM listings
                    WHERE (
                        calendar_60_days = '[]'::jsonb
                        OR NOT calendar_60_days @> '[1]'::jsonb
                    )
                    AND (
                        prices_60_days = '[]'::jsonb
                        OR NOT EXISTS (
                            SELECT 1 FROM jsonb_array_elements(prices_60_days) AS elem
                            WHERE elem::text::integer > 0
                        )
                    )
                    ORDER BY id
                """)
                rows = cur.fetchall()

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
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS cnt FROM listings")
                row = cur.fetchone()

        return int(row["cnt"]) if row else 0

    def close(self) -> None:
        """Закрывает пул соединений с базой данных."""
        if self._pool is not None:
            self._pool.close()
            self._pool = None
            logger.info("postgresql_пул_закрыт")

    @staticmethod
    def _listing_to_params(listing: RawListing) -> dict:
        """Преобразует объект RawListing в словарь параметров для SQL-запроса.

        Args:
            listing: Объявление.

        Returns:
            Словарь параметров для именованных плейсхолдеров.
        """
        # psycopg3 автоматически сериализует list/dict в JSON для JSONB-столбцов
        # через адаптер Jsonb, но для надёжности передаём явно как Json
        from psycopg.types.json import Jsonb

        return {
            "external_id": listing.external_id,
            "title": listing.title,
            "url": listing.url,
            "price_per_night": listing.price_per_night,
            "rating": listing.rating,
            "review_count": listing.review_count,
            "area_m2": listing.area_m2,
            "guests": listing.guests,
            "address": listing.address,
            "metro_station": listing.metro_station,
            "has_instant_booking": listing.has_instant_booking,
            "calendar_60_days": Jsonb(listing.calendar_60_days),
            "prices_60_days": Jsonb(listing.prices_60_days),
            "snapshot_date": listing.snapshot_date,
            "lat": listing.lat,
            "lng": listing.lng,
            "rooms": listing.rooms,
            "property_type": listing.property_type,
        }

    @staticmethod
    def _row_to_listing(row: dict) -> RawListing:
        """Преобразует строку из БД в объект RawListing.

        Args:
            row: Словарь с данными строки (dict_row).

        Returns:
            Экземпляр RawListing.
        """
        # JSONB автоматически десериализуется psycopg3 в list/dict
        calendar = row["calendar_60_days"] if row["calendar_60_days"] else []
        prices = row["prices_60_days"] if row["prices_60_days"] else []

        # snapshot_date приходит как datetime с tzinfo (TIMESTAMPTZ)
        snapshot_date = row["snapshot_date"]
        if snapshot_date is not None and snapshot_date.tzinfo is None:
            snapshot_date = snapshot_date.replace(tzinfo=timezone.utc)

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
            snapshot_date=snapshot_date,
            lat=row.get("lat"),
            lng=row.get("lng"),
            rooms=row.get("rooms"),
            property_type=row.get("property_type"),
        )
