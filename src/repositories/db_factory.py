"""Фабрика репозиториев — создаёт нужную реализацию по типу БД."""

from dataclasses import dataclass

from src.config.logger import get_logger
from src.config.settings import Settings
from src.repositories.base import BaseListingRepository
from src.repositories.base_pool_repository import BasePoolRepository
from src.repositories.snapshot_repository import BaseSnapshotRepository

logger = get_logger("repository.factory")


@dataclass
class RepositoryPair:
    """Набор репозиториев, возвращаемый фабрикой.

    Attributes:
        listing: Репозиторий объявлений.
        snapshot: Репозиторий снимков.
        pool: Репозиторий пула ID объявлений (для этапа batch-обогащения).
    """

    listing: BaseListingRepository
    snapshot: BaseSnapshotRepository
    pool: BasePoolRepository


def create_repositories(settings: Settings) -> RepositoryPair:
    """Создаёт и инициализирует набор репозиториев по настройкам.

    Выбирает реализацию (SQLite или PostgreSQL) на основе settings.db_type.
    Все репозитории инициализируются (создание таблиц/пула) перед возвратом.

    Args:
        settings: Настройки приложения с параметрами подключения.

    Returns:
        Набор инициализированных репозиториев (listings + snapshots + pool).

    Raises:
        RuntimeError: Если указан неподдерживаемый тип БД.
    """
    if settings.db_type == "sqlite":
        return _create_sqlite_repositories(settings)
    elif settings.db_type == "postgresql":
        return _create_postgresql_repositories(settings)
    else:
        raise RuntimeError(
            f"Неподдерживаемый тип базы данных: '{settings.db_type}'. "
            f"Допустимые значения: 'sqlite', 'postgresql'."
        )


def _create_sqlite_repositories(settings: Settings) -> RepositoryPair:
    """Создаёт SQLite-репозитории.

    Args:
        settings: Настройки с путём к файлу БД.

    Returns:
        Набор инициализированных SQLite-репозиториев.
    """
    from src.repositories.sqlite_repository import SQLiteListingRepository
    from src.repositories.snapshot_repository import SQLiteSnapshotRepository
    from src.repositories.sqlite_pool_repository import SQLitePoolRepository

    listing_repo = SQLiteListingRepository(db_path=settings.db_path)
    listing_repo.initialize()

    snapshot_repo = SQLiteSnapshotRepository(db_path=settings.db_path)
    snapshot_repo.initialize()

    pool_repo = SQLitePoolRepository(db_path=settings.db_path)
    pool_repo.initialize()

    logger.info(
        "репозитории_созданы",
        db_type="sqlite",
        db_path=settings.db_path,
    )

    return RepositoryPair(
        listing=listing_repo,
        snapshot=snapshot_repo,
        pool=pool_repo,
    )


def _create_postgresql_repositories(settings: Settings) -> RepositoryPair:
    """Создаёт PostgreSQL-репозитории.

    Args:
        settings: Настройки с параметрами подключения PostgreSQL.

    Returns:
        Набор инициализированных PostgreSQL-репозиториев.
    """
    from src.repositories.postgresql_repository import PostgreSQLListingRepository
    from src.repositories.postgresql_snapshot_repository import (
        PostgreSQLSnapshotRepository,
    )
    from src.repositories.postgresql_pool_repository import (
        PostgreSQLPoolRepository,
    )

    dsn = settings.pg_dsn

    listing_repo = PostgreSQLListingRepository(dsn=dsn)
    listing_repo.initialize()

    snapshot_repo = PostgreSQLSnapshotRepository(dsn=dsn)
    snapshot_repo.initialize()

    pool_repo = PostgreSQLPoolRepository(dsn=dsn)
    pool_repo.initialize()

    # Логируем без пароля — только host:port/dbname
    safe_dsn = dsn.split("@")[-1] if "@" in dsn else dsn
    logger.info(
        "репозитории_созданы",
        db_type="postgresql",
        connection=safe_dsn,
    )

    return RepositoryPair(
        listing=listing_repo,
        snapshot=snapshot_repo,
        pool=pool_repo,
    )
