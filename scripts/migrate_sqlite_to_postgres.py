"""Скрипт однократной миграции данных из SQLite в PostgreSQL.

Переносит все данные из существующей SQLite-базы парсера
в PostgreSQL. Безопасен для повторного запуска — целевые
таблицы очищаются перед вставкой.

Использование:
    python -m scripts.migrate_sqlite_to_postgres

Требования:
    - Заполненные переменные PG_HOST, PG_PORT, PG_NAME, PG_USER, PG_PASSWORD в .env
    - Существующая SQLite-база (по умолчанию: data/sutochno_listings.db)
    - Установленный psycopg: pip install 'psycopg[binary]' psycopg_pool
"""

import sqlite3
import sys
import time
from pathlib import Path

# Добавляем корень проекта в sys.path для импорта src
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import Settings  # noqa: E402


def _connect_sqlite(db_path: str) -> sqlite3.Connection:
    """Открывает соединение с SQLite-базой.

    Args:
        db_path: Путь к файлу базы данных.

    Returns:
        Соединение с SQLite.

    Raises:
        SystemExit: Если файл не существует.
    """
    path = Path(db_path)
    if not path.exists():
        print(f"[ОШИБКА] Файл SQLite не найден: {db_path}")  # noqa: T201
        sys.exit(1)

    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def _get_table_count(sqlite_conn: sqlite3.Connection, table: str) -> int:
    """Возвращает количество записей в таблице SQLite.

    Args:
        sqlite_conn: Соединение с SQLite.
        table: Имя таблицы.

    Returns:
        Количество записей или 0 если таблица не существует.
    """
    try:
        cursor = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
        return cursor.fetchone()[0]
    except sqlite3.OperationalError:
        return 0


def _migrate_listings(sqlite_conn: sqlite3.Connection, pg_conn: "psycopg.Connection") -> int:
    """Переносит таблицу listings из SQLite в PostgreSQL.

    Args:
        sqlite_conn: Соединение с SQLite (источник).
        pg_conn: Соединение с PostgreSQL (цель).

    Returns:
        Количество перенесённых записей.
    """
    import json

    from psycopg.types.json import Jsonb

    cursor = sqlite_conn.execute("SELECT * FROM listings ORDER BY id")
    rows = cursor.fetchall()

    if not rows:
        print("  [!] Таблица listings пуста — пропускаем.")  # noqa: T201
        return 0

    # Очищаем целевую таблицу
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE listings RESTART IDENTITY CASCADE")

    # Определяем доступные столбцы в SQLite
    sample_keys = rows[0].keys()

    batch_size = 500
    total = len(rows)
    migrated = 0

    with pg_conn.cursor() as cur:
        for i in range(0, total, batch_size):
            batch = rows[i: i + batch_size]

            for row in batch:
                # Парсинг JSON-полей из TEXT (SQLite) в list (для JSONB)
                calendar_raw = row["calendar_60_days"] if "calendar_60_days" in sample_keys else "[]"
                calendar = json.loads(calendar_raw) if calendar_raw else []

                prices_raw = row["prices_60_days"] if "prices_60_days" in sample_keys else "[]"
                prices = json.loads(prices_raw) if prices_raw else []

                # Координаты и новые поля (могут отсутствовать в старых базах)
                lat = row["lat"] if "lat" in sample_keys else None
                lng = row["lng"] if "lng" in sample_keys else None
                rooms = row["rooms"] if "rooms" in sample_keys else None
                property_type = row["property_type"] if "property_type" in sample_keys else None

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
                        %(calendar_60_days)s, %(prices_60_days)s,
                        %(snapshot_date)s::timestamptz,
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
                    {
                        "external_id": row["external_id"],
                        "title": row["title"],
                        "url": row["url"],
                        "price_per_night": row["price_per_night"],
                        "rating": row["rating"],
                        "review_count": row["review_count"],
                        "area_m2": row["area_m2"],
                        "guests": row["guests"],
                        "address": row["address"],
                        "metro_station": row["metro_station"],
                        "has_instant_booking": bool(row["has_instant_booking"]),
                        "calendar_60_days": Jsonb(calendar),
                        "prices_60_days": Jsonb(prices),
                        "snapshot_date": row["snapshot_date"],
                        "lat": lat,
                        "lng": lng,
                        "rooms": rooms,
                        "property_type": property_type,
                    },
                )

            migrated += len(batch)
            print(f"  listings: {migrated}/{total} ({migrated * 100 // total}%)", end="\r")  # noqa: T201

    pg_conn.commit()
    print(f"  listings: {migrated}/{total} — готово.        ")  # noqa: T201
    return migrated


def _migrate_snapshots(sqlite_conn: sqlite3.Connection, pg_conn: "psycopg.Connection") -> tuple[int, int]:
    """Переносит таблицы listing_snapshots и snapshot_prices из SQLite в PostgreSQL.

    Args:
        sqlite_conn: Соединение с SQLite (источник).
        pg_conn: Соединение с PostgreSQL (цель).

    Returns:
        Кортеж (количество снимков, количество записей цен).
    """
    # Проверяем существование таблицы listing_snapshots в SQLite
    check = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='listing_snapshots'"
    ).fetchone()

    if not check:
        print("  [!] Таблица listing_snapshots не найдена в SQLite — пропускаем.")  # noqa: T201
        return 0, 0

    # Очищаем целевые таблицы (CASCADE удалит snapshot_prices)
    with pg_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE listing_snapshots RESTART IDENTITY CASCADE")

    # ── Миграция listing_snapshots ──
    snapshot_rows = sqlite_conn.execute(
        "SELECT id, external_id, snapshot_dt, calendar FROM listing_snapshots ORDER BY id"
    ).fetchall()

    if not snapshot_rows:
        print("  [!] Таблица listing_snapshots пуста — пропускаем.")  # noqa: T201
        return 0, 0

    total_snapshots = len(snapshot_rows)
    # Маппинг старых ID → новые ID (для FK в snapshot_prices)
    id_mapping: dict[int, int] = {}

    batch_size = 1000
    migrated_snapshots = 0

    with pg_conn.cursor() as cur:
        for i in range(0, total_snapshots, batch_size):
            batch = snapshot_rows[i: i + batch_size]

            for row in batch:
                old_id = row["id"]

                cur.execute(
                    """
                    INSERT INTO listing_snapshots (external_id, snapshot_dt, calendar)
                    VALUES (%(external_id)s, %(snapshot_dt)s::timestamptz, %(calendar)s)
                    RETURNING id
                    """,
                    {
                        "external_id": row["external_id"],
                        "snapshot_dt": row["snapshot_dt"],
                        "calendar": row["calendar"],
                    },
                )
                new_row = cur.fetchone()
                new_id: int = new_row["id"]
                id_mapping[old_id] = new_id

            migrated_snapshots += len(batch)
            print(  # noqa: T201
                f"  listing_snapshots: {migrated_snapshots}/{total_snapshots} "
                f"({migrated_snapshots * 100 // total_snapshots}%)",
                end="\r",
            )

    pg_conn.commit()
    print(  # noqa: T201
        f"  listing_snapshots: {migrated_snapshots}/{total_snapshots} — готово.        "
    )

    # ── Миграция snapshot_prices ──
    check_prices = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='snapshot_prices'"
    ).fetchone()

    if not check_prices:
        print("  [!] Таблица snapshot_prices не найдена в SQLite — пропускаем.")  # noqa: T201
        return migrated_snapshots, 0

    price_rows = sqlite_conn.execute(
        "SELECT snapshot_id, price_date, price FROM snapshot_prices ORDER BY id"
    ).fetchall()

    if not price_rows:
        print("  [!] Таблица snapshot_prices пуста — пропускаем.")  # noqa: T201
        return migrated_snapshots, 0

    total_prices = len(price_rows)
    migrated_prices = 0
    skipped_prices = 0

    with pg_conn.cursor() as cur:
        for i in range(0, total_prices, batch_size):
            batch = price_rows[i: i + batch_size]

            for row in batch:
                old_snapshot_id = row["snapshot_id"]
                new_snapshot_id = id_mapping.get(old_snapshot_id)

                if new_snapshot_id is None:
                    # Снимок не был перенесён (осиротевшая запись) — пропускаем
                    skipped_prices += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO snapshot_prices (snapshot_id, price_date, price)
                    VALUES (%(snapshot_id)s, %(price_date)s::date, %(price)s)
                    """,
                    {
                        "snapshot_id": new_snapshot_id,
                        "price_date": row["price_date"],
                        "price": row["price"],
                    },
                )

            migrated_prices += len(batch) - skipped_prices
            print(  # noqa: T201
                f"  snapshot_prices: {migrated_prices}/{total_prices} "
                f"({migrated_prices * 100 // total_prices}%)",
                end="\r",
            )

    pg_conn.commit()

    if skipped_prices > 0:
        print(  # noqa: T201
            f"  snapshot_prices: {migrated_prices} перенесено, "
            f"{skipped_prices} пропущено (осиротевшие) — готово.        "
        )
    else:
        print(  # noqa: T201
            f"  snapshot_prices: {migrated_prices}/{total_prices} — готово.        "
        )

    return migrated_snapshots, migrated_prices


def main() -> None:
    """Основная функция миграции — запускает перенос данных."""
    print("=" * 60)  # noqa: T201
    print("  Миграция данных: SQLite → PostgreSQL")  # noqa: T201
    print("=" * 60)  # noqa: T201

    # ── Загрузка настроек ──
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"\n[ОШИБКА] Не удалось загрузить настройки: {e}")  # noqa: T201
        sys.exit(1)

    if settings.db_type != "postgresql":
        print(  # noqa: T201
            "\n[ОШИБКА] Для миграции необходимо установить DB_TYPE=postgresql в .env.\n"
            "  Текущее значение: DB_TYPE=" + settings.db_type
        )
        sys.exit(1)

    sqlite_path = settings.db_path
    pg_dsn = settings.pg_dsn

    print(f"\n  Источник (SQLite): {sqlite_path}")  # noqa: T201
    print(f"  Цель (PostgreSQL): {pg_dsn.split('@')[-1]}")  # noqa: T201

    # ── Подключение к SQLite ──
    sqlite_conn = _connect_sqlite(sqlite_path)

    # Статистика источника
    listings_count = _get_table_count(sqlite_conn, "listings")
    snapshots_count = _get_table_count(sqlite_conn, "listing_snapshots")
    prices_count = _get_table_count(sqlite_conn, "snapshot_prices")

    print(f"\n  Данные в SQLite:")  # noqa: T201
    print(f"    listings:          {listings_count:,}")  # noqa: T201
    print(f"    listing_snapshots: {snapshots_count:,}")  # noqa: T201
    print(f"    snapshot_prices:   {prices_count:,}")  # noqa: T201

    if listings_count == 0 and snapshots_count == 0:
        print("\n[!] SQLite-база пуста — нечего мигрировать.")  # noqa: T201
        sqlite_conn.close()
        sys.exit(0)

    # ── Подключение к PostgreSQL ──
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:
        print(  # noqa: T201
            "\n[ОШИБКА] Библиотека psycopg не установлена.\n"
            "  Установите: pip install 'psycopg[binary]' psycopg_pool"
        )
        sys.exit(1)

    print("\n  Подключение к PostgreSQL...")  # noqa: T201

    try:
        pg_conn = psycopg.connect(pg_dsn, row_factory=dict_row)
    except Exception as e:
        print(f"\n[ОШИБКА] Не удалось подключиться к PostgreSQL: {e}")  # noqa: T201
        sqlite_conn.close()
        sys.exit(1)

    print("  Подключение установлено.")  # noqa: T201

    # ── Создание таблиц в PostgreSQL (через репозитории) ──
    print("\n  Создание таблиц в PostgreSQL...")  # noqa: T201

    from src.repositories.postgresql_repository import PostgreSQLListingRepository
    from src.repositories.postgresql_snapshot_repository import PostgreSQLSnapshotRepository

    listing_repo = PostgreSQLListingRepository(dsn=pg_dsn)
    listing_repo.initialize()

    snapshot_repo = PostgreSQLSnapshotRepository(dsn=pg_dsn)
    snapshot_repo.initialize()

    # Закрываем пулы репозиториев — дальше работаем через прямое соединение
    listing_repo.close()
    snapshot_repo.close()

    print("  Таблицы готовы.")  # noqa: T201

    # ── Подтверждение ──
    print(  # noqa: T201
        f"\n  ⚠️  ВНИМАНИЕ: целевые таблицы в PostgreSQL будут ОЧИЩЕНЫ перед вставкой."
    )
    answer = input("  Продолжить миграцию? (yes/no): ").strip().lower()

    if answer not in ("yes", "y", "да"):
        print("\n  Миграция отменена.")  # noqa: T201
        sqlite_conn.close()
        pg_conn.close()
        sys.exit(0)

    # ── Миграция ──
    print("\n  Начинаю перенос данных...\n")  # noqa: T201
    start_time = time.perf_counter()

    # 1. Listings
    migrated_listings = _migrate_listings(sqlite_conn, pg_conn)

    # 2. Snapshots + Prices
    migrated_snapshots, migrated_prices = _migrate_snapshots(sqlite_conn, pg_conn)

    elapsed = time.perf_counter() - start_time

    # ── Результат ──
    print(f"\n{'=' * 60}")  # noqa: T201
    print(f"  Миграция завершена за {elapsed:.1f} секунд.")  # noqa: T201
    print(f"\n  Перенесено:")  # noqa: T201
    print(f"    listings:          {migrated_listings:,}")  # noqa: T201
    print(f"    listing_snapshots: {migrated_snapshots:,}")  # noqa: T201
    print(f"    snapshot_prices:   {migrated_prices:,}")  # noqa: T201
    print(f"\n  Теперь парсер будет писать в PostgreSQL (DB_TYPE=postgresql).")  # noqa: T201
    print(f"  SQLite-файл можно сохранить как бэкап или удалить.")  # noqa: T201
    print(f"{'=' * 60}")  # noqa: T201

    # ── Закрытие соединений ──
    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
