"""Скрипт создания таблицы comparison_events в PostgreSQL.

Идемпотентный скрипт — безопасен для повторного запуска. Все объекты БД
создаются через IF NOT EXISTS, что позволяет использовать скрипт как
для первичной установки, так и для проверки схемы.

Запуск:
    python -m scripts.create_comparison_events

Настройки подключения читаются из .env (переменные PG_HOST, PG_PORT,
PG_NAME, PG_USER, PG_PASSWORD). Требуется DB_TYPE=postgresql.
"""

import sys

try:
    import psycopg
except ImportError:
    print(  # noqa: T201
        "[ОШИБКА] Драйвер psycopg не установлен. "
        "Установите зависимости: pip install -e \".[postgresql]\"",
        file=sys.stderr,
    )
    sys.exit(1)

from src.config.logger import configure as configure_logging
from src.config.logger import get_logger
from src.config.settings import Settings

# ── SQL-выражения для создания таблицы и индексов ─────────────

# Создание основной таблицы событий сравнения снимков
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS comparison_events (
    id              BIGSERIAL PRIMARY KEY,
    event_type      TEXT NOT NULL,
    external_id     TEXT NOT NULL,
    listing_title   TEXT,
    deal_dt         TIMESTAMPTZ NOT NULL,
    checkin_date    DATE NOT NULL,
    checkout_date   DATE NOT NULL,
    nights          INTEGER NOT NULL,
    depth_days      INTEGER NOT NULL,
    price_per_night DOUBLE PRECISION,
    total_price     DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Уникальный индекс для дедупликации: одно и то же событие
# (по объявлению, типу, дате заезда и моменту фиксации) не должно
# записываться дважды при повторных прогонах или ретраях
_CREATE_UNIQUE_INDEX_SQL = """
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_dedupe
    ON comparison_events (external_id, event_type, checkin_date, deal_dt);
"""

# Индекс для аналитических запросов по типу события и дате сделки
# (например, «все брони за последние сутки»)
_CREATE_TYPE_DEAL_DT_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_events_type_deal_dt
    ON comparison_events (event_type, deal_dt DESC);
"""

# Индекс для выборки всех событий конкретного объявления
_CREATE_EXTERNAL_ID_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_events_external_id
    ON comparison_events (external_id);
"""

# Индекс для фильтров по дате заезда (событийная лента, отмены за период)
_CREATE_CHECKIN_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_events_checkin
    ON comparison_events (checkin_date);
"""


def _build_dsn(settings: Settings) -> str:
    """Формирует строку подключения (DSN) для psycopg.

    Args:
        settings: Загруженные настройки приложения.

    Returns:
        DSN-строка в формате psycopg (host=... port=... ...).
    """
    return (
        f"host={settings.pg_host} "
        f"port={settings.pg_port} "
        f"dbname={settings.pg_name} "
        f"user={settings.pg_user} "
        f"password={settings.pg_password}"
    )


def _create_schema(dsn: str, logger: object) -> None:
    """Создаёт таблицу comparison_events и все индексы.

    Все выражения выполняются в одной транзакции — при ошибке
    любого из них ни один объект не будет создан.

    Args:
        dsn: Строка подключения psycopg.
        logger: Логгер для записи шагов создания схемы.

    Raises:
        psycopg.Error: При ошибке подключения или выполнения SQL.
    """
    logger.info("подключение_к_postgres", step="schema")

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            logger.info("создание_таблицы_comparison_events", step="schema")
            cur.execute(_CREATE_TABLE_SQL)

            logger.info("создание_уникального_индекса_дедупликации", step="schema")
            cur.execute(_CREATE_UNIQUE_INDEX_SQL)

            logger.info("создание_индекса_type_deal_dt", step="schema")
            cur.execute(_CREATE_TYPE_DEAL_DT_INDEX_SQL)

            logger.info("создание_индекса_external_id", step="schema")
            cur.execute(_CREATE_EXTERNAL_ID_INDEX_SQL)

            logger.info("создание_индекса_checkin", step="schema")
            cur.execute(_CREATE_CHECKIN_INDEX_SQL)

        conn.commit()

    logger.info("схема_создана_успешно", step="schema")


def _verify_schema(dsn: str, logger: object) -> None:
    """Проверяет, что таблица и индексы действительно созданы.

    Выполняет запрос к information_schema и pg_indexes для подтверждения,
    что все объекты присутствуют. Полезно для диагностики после запуска.

    Args:
        dsn: Строка подключения psycopg.
        logger: Логгер для записи результатов проверки.
    """
    expected_indexes = {
        "uq_events_dedupe",
        "idx_events_type_deal_dt",
        "idx_events_external_id",
        "idx_events_checkin",
    }

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Проверяем таблицу
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = 'comparison_events';"
            )
            row = cur.fetchone()
            table_exists = bool(row and row[0] > 0)

            # Проверяем индексы
            cur.execute(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = 'public' AND tablename = 'comparison_events';"
            )
            existing_indexes = {r[0] for r in cur.fetchall()}

    if not table_exists:
        logger.error(
            "проверка_схемы_таблица_не_найдена",
            step="verify",
        )
        raise RuntimeError("Таблица comparison_events не была создана")

    missing_indexes = expected_indexes - existing_indexes
    if missing_indexes:
        logger.error(
            "проверка_схемы_отсутствуют_индексы",
            missing=sorted(missing_indexes),
            step="verify",
        )
        raise RuntimeError(
            f"Не созданы индексы: {', '.join(sorted(missing_indexes))}"
        )

    logger.info(
        "проверка_схемы_успешна",
        table="comparison_events",
        indexes_count=len(expected_indexes),
        step="verify",
    )


def main() -> None:
    """Точка входа скрипта — создаёт таблицу comparison_events и индексы.

    Порядок работы:
        1. Загружает настройки из .env.
        2. Проверяет, что DB_TYPE=postgresql (иначе выходит с ошибкой).
        3. Создаёт таблицу и все индексы (идемпотентно).
        4. Проверяет, что все объекты созданы.

    Raises:
        SystemExit: При ошибке загрузки конфигурации, неверном DB_TYPE
            или сбое подключения к PostgreSQL.
    """
    # ── Загрузка конфигурации ──
    try:
        settings = Settings.load()
    except RuntimeError as e:
        print(f"[ОШИБКА] Не удалось загрузить конфигурацию: {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    # ── Настройка логирования ──
    configure_logging(
        log_level=settings.log_level,
        log_file_path=settings.log_file_path,
    )
    logger = get_logger("scripts.create_comparison_events")

    logger.info("запуск_скрипта_создания_таблицы", step="init")

    # ── Проверка типа БД ──
    if settings.db_type != "postgresql":
        logger.error(
            "неверный_тип_бд",
            db_type=settings.db_type,
            step="init",
        )
        print(  # noqa: T201
            f"[ОШИБКА] Скрипт работает только с PostgreSQL. "
            f"Текущий DB_TYPE={settings.db_type}. "
            f"Установите DB_TYPE=postgresql в .env",
            file=sys.stderr,
        )
        sys.exit(1)

    # ── Проверка обязательных параметров подключения ──
    if not settings.pg_user or not settings.pg_password:
        logger.error("отсутствуют_учётные_данные_postgres", step="init")
        print(  # noqa: T201
            "[ОШИБКА] Не заданы PG_USER или PG_PASSWORD в .env",
            file=sys.stderr,
        )
        sys.exit(1)

    dsn = _build_dsn(settings)

    logger.info(
        "параметры_подключения",
        host=settings.pg_host,
        port=settings.pg_port,
        dbname=settings.pg_name,
        user=settings.pg_user,
        step="init",
    )

    # ── Создание схемы ──
    try:
        _create_schema(dsn, logger)
        _verify_schema(dsn, logger)
    except psycopg.Error as e:
        logger.exception(
            "ошибка_создания_схемы",
            error=str(e),
            error_type=type(e).__name__,
            step="schema",
        )
        print(f"[ОШИБКА] Не удалось создать схему: {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)
    except RuntimeError as e:
        # Ошибки проверки схемы (verify) — уже залогированы
        print(f"[ОШИБКА] {e}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    logger.info("скрипт_завершён_успешно", step="done")
    print("[OK] Таблица comparison_events создана и проверена.")  # noqa: T201


if __name__ == "__main__":
    main()
