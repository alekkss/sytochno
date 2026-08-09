"""PostgreSQL-реализация репозитория событий сравнения снимков."""

from src.config.logger import get_logger
from src.models.booking_event import AnyEvent, EventType
from src.repositories.base_comparison_events_repository import (
    BaseComparisonEventsRepository,
)

logger = get_logger("repository.pg.events")

try:
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool
except ImportError as e:
    raise ImportError(
        "Для работы с PostgreSQL необходимо установить psycopg: "
        "pip install 'psycopg[binary]' psycopg_pool"
    ) from e


# Размер пачки для executemany. Значение подобрано как компромисс между
# накладными расходами на round-trip к БД и размером буфера отправки.
_BATCH_SIZE: int = 1000

# SQL для вставки одного события. ON CONFLICT DO NOTHING защищает от дублей
# по уникальному индексу uq_events_dedupe (external_id, event_type,
# checkin_date, deal_dt). RETURNING id возвращает id только для реально
# вставленных строк — это позволяет точно посчитать вставки.
_INSERT_SQL: str = """
    INSERT INTO comparison_events (
        event_type, external_id, listing_title, deal_dt,
        checkin_date, checkout_date, nights, depth_days,
        price_per_night, total_price
    ) VALUES (
        %(event_type)s, %(external_id)s, %(listing_title)s, %(deal_dt)s,
        %(checkin_date)s, %(checkout_date)s, %(nights)s, %(depth_days)s,
        %(price_per_night)s, %(total_price)s
    )
    ON CONFLICT (external_id, event_type, checkin_date, deal_dt) DO NOTHING
    RETURNING id
"""


class PostgreSQLComparisonEventsRepository(BaseComparisonEventsRepository):
    """Репозиторий событий сравнения с хранением в PostgreSQL.

    Использует пул соединений (psycopg_pool.ConnectionPool) и защиту от
    дублирования через ON CONFLICT DO NOTHING по уникальному индексу
    (external_id, event_type, checkin_date, deal_dt).

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
                "Пул соединений PostgreSQL (события) не создан. "
                "Вызовите initialize() перед использованием."
            )
        return self._pool

    def initialize(self) -> None:
        """Создаёт пул соединений и проверяет доступность таблицы.

        Таблица comparison_events и её индексы создаются отдельным
        скриптом scripts/create_comparison_events.py — здесь только
        проверяется, что таблица существует.

        Raises:
            RuntimeError: Если таблица comparison_events не найдена в БД.
        """
        self._pool = ConnectionPool(
            conninfo=self._dsn,
            min_size=self._min_pool_size,
            max_size=self._max_pool_size,
            kwargs={"row_factory": dict_row},
        )

        self._verify_table_exists()

        logger.info(
            "postgresql_репозиторий_событий_инициализирован",
            dsn=self._dsn.split("@")[-1],  # Логируем только host:port/db (без пароля)
        )

    def _verify_table_exists(self) -> None:
        """Проверяет наличие таблицы comparison_events в БД.

        Raises:
            RuntimeError: Если таблица не найдена.
        """
        pool = self._get_pool()

        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name = 'comparison_events'
                    """
                )
                row = cur.fetchone()

        exists = bool(row and row["cnt"] > 0)
        if not exists:
            raise RuntimeError(
                "Таблица comparison_events не найдена в БД. "
                "Запустите скрипт создания схемы: "
                "python -m scripts.create_comparison_events"
            )

    def bulk_insert(self, events: list[AnyEvent]) -> int:
        """Сохраняет пачку событий с защитой от дублирования.

        Разбивает список на батчи по _BATCH_SIZE строк и выполняет
        каждый батч отдельным executemany. Все батчи выполняются в
        одной транзакции — при ошибке любого из них откатываются все.

        Дубликаты (по external_id, event_type, checkin_date, deal_dt)
        игнорируются на уровне БД через ON CONFLICT DO NOTHING.

        Args:
            events: Список событий бронирования и отмен.

        Returns:
            Количество реально вставленных строк (без дубликатов).
        """
        if not events:
            return 0

        pool = self._get_pool()
        inserted_total = 0

        with pool.connection() as conn:
            with conn.cursor() as cur:
                for batch_start in range(0, len(events), _BATCH_SIZE):
                    batch = events[batch_start : batch_start + _BATCH_SIZE]
                    params_list = [self._event_to_params(ev) for ev in batch]

                    # executemany + RETURNING в psycopg 3 возвращает строки
                    # каждого выполнения по очереди; собираем их через
                    # returning=True + итерацию по результатам.
                    cur.executemany(_INSERT_SQL, params_list, returning=True)

                    # Пробегаем по результатам всех executemany-вставок:
                    # для каждой успешной вставки будет строка с id,
                    # для конфликтов — пустой результат.
                    while True:
                        if cur.rowcount and cur.rowcount > 0:
                            inserted_total += cur.rowcount
                        if not cur.nextset():
                            break

            conn.commit()

        logger.info(
            "события_записаны_в_бд",
            received=len(events),
            inserted=inserted_total,
            duplicates=len(events) - inserted_total,
        )

        return inserted_total

    def close(self) -> None:
        """Закрывает пул соединений с базой данных."""
        if self._pool is not None:
            self._pool.close()
            self._pool = None
            logger.info("postgresql_пул_событий_закрыт")

    @staticmethod
    def _event_to_params(event: AnyEvent) -> dict:
        """Преобразует событие в словарь параметров для SQL-запроса.

        Args:
            event: Событие бронирования или отмены.

        Returns:
            Словарь параметров для именованных плейсхолдеров.
        """
        # event_type у нас в модели — Enum, в БД пишем строковое значение
        event_type_value = (
            event.event_type.value
            if isinstance(event.event_type, EventType)
            else str(event.event_type)
        )

        return {
            "event_type": event_type_value,
            "external_id": event.listing_external_id,
            "listing_title": event.listing_title,
            "deal_dt": event.snapshot_dt,
            "checkin_date": event.checkin_date,
            "checkout_date": event.checkout_date,
            "nights": event.nights,
            "depth_days": event.depth_days,
            "price_per_night": event.price_per_night,
            "total_price": event.total_price,
        }
