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
            listings: Список объявлений после парсинга.
                      Ожидаются объекты с атрибутами:
                      - external_id: str
                      - calendar: str (60 символов '0'/'1')
                      - day_prices: list[dict] с ключами 'date' и 'price'
                        или список объектов с атрибутами date/price.

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
        """Строит объект снимка из данных объявления.

        Args:
            listing: Объявление с атрибутами external_id, calendar, day_prices.
            snapshot_dt: Единая дата и время для всей партии снимков.

        Returns:
            Готовый объект ListingSnapshot.

        Raises:
            AttributeError: Если у объявления отсутствуют обязательные атрибуты.
            ValueError: Если calendar не содержит ровно 60 символов.
        """
        external_id: str = listing.external_id  # type: ignore[union-attr]
        calendar: str = listing.calendar or ("0" * 60)  # type: ignore[union-attr]

        if len(calendar) != 60:
            logger.warning(
                "некорректная_длина_календаря",
                external_id=external_id,
                length=len(calendar),
            )
            # Дополняем нулями или обрезаем до 60
            calendar = calendar.ljust(60, "0")[:60]

        prices = self._extract_prices(listing)

        return ListingSnapshot(
            listing_external_id=external_id,
            snapshot_dt=snapshot_dt,
            calendar=calendar,
            prices=prices,
        )

    def _extract_prices(self, listing: object) -> list[DayPrice]:
        """Извлекает цены по дням из объявления.

        Поддерживает два формата day_prices:
        - список объектов с атрибутами .date и .price
        - список словарей с ключами 'date' и 'price'

        Args:
            listing: Объявление с атрибутом day_prices.

        Returns:
            Список DayPrice. Пустой список, если цены недоступны.
        """
        raw_prices = getattr(listing, "day_prices", None)
        if not raw_prices:
            return []

        result: list[DayPrice] = []

        for item in raw_prices:
            try:
                if isinstance(item, dict):
                    result.append(
                        DayPrice(
                            date=item["date"],
                            price=float(item["price"]),
                        )
                    )
                else:
                    result.append(
                        DayPrice(
                            date=item.date,
                            price=float(item.price),
                        )
                    )
            except (KeyError, AttributeError, TypeError, ValueError) as e:
                logger.warning(
                    "цена_пропущена",
                    error=str(e),
                    error_type=type(e).__name__,
                )

        return result