"""Потоковая миграция snapshot_prices из SQLite в PostgreSQL.

Использует COPY FROM STDIN и staging-таблицу, чтобы уложиться в
константный объём RAM (~200 МБ) при переносе 147 млн строк.

Предполагается, что listings и listing_snapshots уже в PostgreSQL,
а таблица snapshot_prices пуста.
"""

import sqlite3
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config.settings import Settings  # noqa: E402

import psycopg  # noqa: E402

SQLITE_BATCH = 50_000       # сколько строк за раз читаем из SQLite
COPY_REPORT_EVERY = 500_000  # как часто выводим прогресс


def stream_prices(sqlite_conn: sqlite3.Connection):
    """Итератор по ценам с JOIN'ом на снимки — без загрузки в память."""
    cur = sqlite_conn.cursor()
    cur.arraysize = SQLITE_BATCH
    cur.execute(
        """
        SELECT ls.external_id, ls.snapshot_dt, sp.price_date, sp.price
        FROM snapshot_prices sp
        JOIN listing_snapshots ls ON ls.id = sp.snapshot_id
        ORDER BY sp.id
        """
    )
    while True:
        rows = cur.fetchmany(SQLITE_BATCH)
        if not rows:
            break
        for row in rows:
            yield row


def main() -> None:
    settings = Settings.load()
    if settings.db_type != "postgresql":
        print("[ОШИБКА] DB_TYPE должен быть postgresql в .env")
        sys.exit(1)

    sqlite_path = settings.db_path
    pg_dsn = settings.pg_dsn

    print("=" * 60)
    print("  Миграция snapshot_prices: SQLite → PostgreSQL (streaming)")
    print("=" * 60)
    print(f"  Источник: {sqlite_path}")
    print(f"  Цель:     {pg_dsn.split('@')[-1]}")

    if not Path(sqlite_path).exists():
        print(f"[ОШИБКА] SQLite не найден: {sqlite_path}")
        sys.exit(1)

    sqlite_conn = sqlite3.connect(sqlite_path)

    # Оценка объёма
    total_estimate = sqlite_conn.execute(
        "SELECT COUNT(*) FROM snapshot_prices"
    ).fetchone()[0]
    print(f"  Строк к переносу: {total_estimate:,}")

    answer = input("\n  Продолжить? (yes/no): ").strip().lower()
    if answer not in ("yes", "y", "да"):
        print("  Отменено.")
        return

    pg_conn = psycopg.connect(pg_dsn, autocommit=False)

    try:
        with pg_conn.cursor() as cur:
            # Проверяем, что цель пуста
            cur.execute("SELECT COUNT(*) FROM snapshot_prices")
            existing = cur.fetchone()[0]
            if existing > 0:
                print(f"[ОШИБКА] snapshot_prices не пуста: {existing:,} строк")
                print("  Очистите её вручную (TRUNCATE snapshot_prices) и запустите снова.")
                sys.exit(1)

            print("\n[1/5] Создаю staging-таблицу...")
            cur.execute("DROP TABLE IF EXISTS snapshot_prices_staging")
            cur.execute(
                """
                CREATE UNLOGGED TABLE snapshot_prices_staging (
                    external_id  TEXT        NOT NULL,
                    snapshot_dt  TIMESTAMPTZ NOT NULL,
                    price_date   DATE        NOT NULL,
                    price        DOUBLE PRECISION NOT NULL
                )
                """
            )
            pg_conn.commit()

            # Разгоняем сессию под массовую загрузку
            cur.execute("SET synchronous_commit = OFF")
            cur.execute("SET work_mem = '512MB'")
            cur.execute("SET maintenance_work_mem = '1GB'")

            print("[2/5] Стримю цены в staging через COPY...")
            t0 = time.perf_counter()
            copied = 0
            last_report = 0

            copy_sql = (
                "COPY snapshot_prices_staging "
                "(external_id, snapshot_dt, price_date, price) FROM STDIN"
            )
            with cur.copy(copy_sql) as copy:
                for row in stream_prices(sqlite_conn):
                    copy.write_row(row)
                    copied += 1
                    if copied - last_report >= COPY_REPORT_EVERY:
                        elapsed = time.perf_counter() - t0
                        rate = copied / elapsed if elapsed else 0
                        pct = copied * 100 / total_estimate if total_estimate else 0
                        eta = (total_estimate - copied) / rate if rate else 0
                        print(
                            f"    {copied:>12,} / {total_estimate:,} "
                            f"({pct:5.1f}%)  {rate:>8,.0f} rows/s  ETA {eta/60:5.1f} мин"
                        )
                        last_report = copied

            pg_conn.commit()
            elapsed = time.perf_counter() - t0
            print(f"  Загружено в staging: {copied:,} за {elapsed/60:.1f} мин")

            print("\n[3/5] Временно отключаю индекс и FK на snapshot_prices...")
            cur.execute("ALTER TABLE snapshot_prices DROP CONSTRAINT snapshot_prices_snapshot_id_fkey")
            cur.execute("DROP INDEX IF EXISTS idx_snapshot_prices_snapshot_id")
            pg_conn.commit()

            print("[4/5] Переношу из staging в snapshot_prices с JOIN...")
            t1 = time.perf_counter()
            cur.execute(
                """
                INSERT INTO snapshot_prices (snapshot_id, price_date, price)
                SELECT ls.id, s.price_date, s.price
                FROM snapshot_prices_staging s
                JOIN listing_snapshots ls
                  ON ls.external_id = s.external_id
                 AND ls.snapshot_dt = s.snapshot_dt
                """
            )
            inserted = cur.rowcount
            pg_conn.commit()
            print(f"  Перенесено: {inserted:,} за {(time.perf_counter()-t1)/60:.1f} мин")

            if inserted != copied:
                print(f"  ⚠️  Расхождение: в staging {copied:,}, вставлено {inserted:,}")
                print(f"     Осиротевшие цены (нет соответствующего снимка): {copied - inserted:,}")

            print("\n[5/5] Восстанавливаю индекс и FK, дропаю staging...")
            t2 = time.perf_counter()
            cur.execute(
                "CREATE INDEX idx_snapshot_prices_snapshot_id "
                "ON snapshot_prices (snapshot_id)"
            )
            cur.execute(
                "ALTER TABLE snapshot_prices "
                "ADD CONSTRAINT snapshot_prices_snapshot_id_fkey "
                "FOREIGN KEY (snapshot_id) REFERENCES listing_snapshots(id) "
                "ON DELETE CASCADE"
            )
            cur.execute("DROP TABLE snapshot_prices_staging")
            pg_conn.commit()
            print(f"  Готово за {(time.perf_counter()-t2)/60:.1f} мин")

            print("\n" + "=" * 60)
            print(f"  ВСЕГО: {inserted:,} цен за {(time.perf_counter()-t0)/60:.1f} мин")
            print("=" * 60)

    except Exception as e:
        pg_conn.rollback()
        print(f"\n[ОШИБКА] {type(e).__name__}: {e}")
        print("  Транзакция откачена. Staging-таблица могла остаться — почистите вручную:")
        print("  DROP TABLE IF EXISTS snapshot_prices_staging;")
        raise
    finally:
        pg_conn.close()
        sqlite_conn.close()


if __name__ == "__main__":
    main()
