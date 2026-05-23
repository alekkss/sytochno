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

        Снимки с пустым или некорректным календарём пропускаются —
        они не несут информации и провоцируют призрачные события
        при сравнении (переходы 0→1 из нулевого снимка в нормальный).

        Args:
            listings: Список объявлений RawListing после парсинга.

        Returns:
            Список сохранённых снимков с присвоенными ID.
        """
        snapshot_dt = datetime.now()
        saved: list[ListingSnapshot] = []
        skipped_empty = 0

        logger.info(
            "начало_сохранения_снимков",
            total=len(listings),
            snapshot_dt=snapshot_dt.isoformat(),
        )

        for listing in listings:
            try:
                snapshot = self._build_snapshot(listing, snapshot_dt)

                # _build_snapshot возвращает None если календарь пустой —
                # пропускаем такое объявление, не сохраняем в БД.
                if snapshot is None:
                    skipped_empty += 1
                    continue

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
            skipped_empty=skipped_empty,
        )

        return saved

    def _build_snapshot(
        self, listing: object, snapshot_dt: datetime
    ) -> ListingSnapshot | None:
        """Строит объект снимка из данных объявления RawListing.

        Возвращает None если календарь пустой — такой снимок
        не имеет смысла сохранять и может провоцировать призрачные
        события при сравнении.

        Args:
            listing: Объявление RawListing с атрибутами
                     external_id, calendar_60_days, prices_60_days.
            snapshot_dt: Единая дата и время для всей партии снимков.

        Returns:
            Готовый объект ListingSnapshot или None при пустом календаре.

        Raises:
            AttributeError: Если у объявления отсутствует external_id.
        """
        external_id: str = listing.external_id  # type: ignore[union-attr]

        # calendar_60_days — список int (0/1), преобразуем в строку '0'/'1'
        raw_calendar: list[int] = getattr(listing, "calendar_60_days", [])

        # Пустой календарь — парсинг карточки не удался или API не вернул данные.
        # Сохранять такой снимок нельзя: при следующем прогоне сравнение
        # обнаружит переходы 0→1 на всех 60 днях и создаст призрачные брони.
        if not raw_calendar:
            logger.warning(
                "снимок_пропущен_пустой_календарь",
                external_id=external_id,
            )
            return None

        calendar = "".join(str(v) for v in raw_calendar)

        # Нестандартная длина — предупреждаем, но не обрезаем:
        # лучше пропустить снимок с неполным календарём, чем сохранить мусор.
        if len(calendar) != 60:
            logger.warning(
                "снимок_пропущен_некорректная_длина_календаря",
                external_id=external_id,
                length=len(calendar),
            )
            return None

        # ИСПРАВЛЕНО: передаём snapshot_dt, чтобы даты цен совпадали
        # с датами, по которым comparison_service ищет цены.
        prices = self._extract_prices(listing, snapshot_dt)

        return ListingSnapshot(
            listing_external_id=external_id,
            snapshot_dt=snapshot_dt,
            calendar=calendar,
            prices=prices,
        )

    def _extract_prices(self, listing: object, snapshot_dt: datetime) -> list[DayPrice]:
        """Извлекает цены по дням из RawListing.prices_60_days.

        Дата каждого дня вычисляется как дата снимка батча + индекс,
        что гарантирует совпадение с датами в comparison_service.

        Args:
            listing: Объявление RawListing с атрибутом prices_60_days.
            snapshot_dt: Единая дата и время снимка для всей партии.

        Returns:
            Список DayPrice. Пустой список, если цены недоступны.
        """
        from datetime import timedelta

        raw_prices: list[int] = getattr(listing, "prices_60_days", [])
        if not raw_prices:
            return []

        # Базовая дата берётся из snapshot_dt батча — той же,
        # что используется в comparison_service при поиске цен.
        base_date = snapshot_dt.date()

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