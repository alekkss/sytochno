"""Сервис создания и сохранения снимков объявлений."""

from datetime import datetime

from src.config.logger import get_logger
from src.models.snapshot import DayPrice, ListingSnapshot
from src.repositories.snapshot_repository import BaseSnapshotRepository

logger = get_logger("service.snapshot")


class SnapshotService:
    """Сервис создания снимков объявлений после каждого парсинга.

    Принимает сырые данные объявлений, формирует снимки
    и делегирует сохранение репозиторию.

    Зависит от абстракции BaseSnapshotRepository (DIP).
    """

    def __init__(self, repository: BaseSnapshotRepository) -> None:
        """Инициализирует сервис.

        Args:
            repository: Репозиторий для хранения снимков.
        """
        self._repository = repository

    def save_snapshots(self, listings: list) -> list[ListingSnapshot]:
        """Создаёт и сохраняет снимки для всех переданных объявлений.

        Время снимка — единое для всей партии (момент вызова метода),
        чтобы все объявления одного прогона имели одинаковую метку времени.

        Args:
            listings: Список объявлений RawListing после парсинга.

        Returns:
            Список сохранённых снимков с присвоенными ID.
        """
        snapshot_dt = datetime.now()
        saved: list[ListingSnapshot] = []

        logger.info(
            "начало_сохранения_снимков",
            total=len(listings),
            snapshot_dt=snapshot_dt.isoformat(),
        )

        for listing in listings:
            try:
                snapshot = self._build_snapshot(listing, snapshot_dt)
                snapshot_id = self._repository.save(snapshot)
                snapshot.snapshot_id = snapshot_id
                saved.append(snapshot)
            except Exception as e:
                logger.warning(
                    "снимок_не_сохранён",
                    external_id=getattr(listing, "external_id", "неизвестен"),
                    error=str(e),
                    error_type=type(e).__name__,
                )

        logger.info(
            "снимки_сохранены",
            total=len(saved),
            skipped=len(listings) - len(saved),
        )

        return saved

    def _build_snapshot(self, listing: object, snapshot_dt: datetime) -> ListingSnapshot:
        """Строит объект снимка из данных объявления RawListing.

        Args:
            listing: Объявление RawListing с атрибутами
                     external_id, calendar_60_days, prices_60_days.
            snapshot_dt: Единая дата и время для всей партии снимков.

        Returns:
            Готовый объект ListingSnapshot.

        Raises:
            AttributeError: Если у объявления отсутствуют обязательные атрибуты.
        """
        external_id: str = listing.external_id  # type: ignore[union-attr]

        # calendar_60_days — список int (0/1), преобразуем в строку '0'/'1'
        raw_calendar: list[int] = getattr(listing, "calendar_60_days", [])
        calendar = "".join(str(v) for v in raw_calendar)

        if len(calendar) != 60:
            logger.warning(
                "некорректная_длина_календаря",
                external_id=external_id,
                length=len(calendar),
            )
            calendar = calendar.ljust(60, "0")[:60]

        prices = self._extract_prices(listing)

        return ListingSnapshot(
            listing_external_id=external_id,
            snapshot_dt=snapshot_dt,
            calendar=calendar,
            prices=prices,
        )

    def _extract_prices(self, listing: object) -> list[DayPrice]:
        """Извлекает цены по дням из RawListing.prices_60_days.

        prices_60_days — список int, где 0 означает занятый день.
        Дата каждого дня вычисляется как snapshot_date + индекс.

        Args:
            listing: Объявление RawListing с атрибутами
                     prices_60_days и snapshot_date.

        Returns:
            Список DayPrice. Пустой список, если цены недоступны.
        """
        from datetime import timedelta

        raw_prices: list[int] = getattr(listing, "prices_60_days", [])
        if not raw_prices:
            return []

        # Базовая дата — дата снимка объявления
        base_date = getattr(listing, "snapshot_date", datetime.now()).date()

        result: list[DayPrice] = []
        for i, price in enumerate(raw_prices):
            try:
                result.append(
                    DayPrice(
                        date=base_date + timedelta(days=i),
                        price=float(price),
                    )
                )
            except (TypeError, ValueError) as e:
                logger.warning(
                    "цена_пропущена",
                    index=i,
                    error=str(e),
                    error_type=type(e).__name__,
                )

        return result