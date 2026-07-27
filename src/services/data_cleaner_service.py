"""Сервис обработки данных — расчёт стоимости за м²."""

from src.config.logger import get_logger
from src.models.listing import RawListing

logger = get_logger("service.data_cleaner")


class DataCleanerService:
    """Сервис постобработки данных после batch-обогащения.

    Выполняет расчёт средней стоимости за м² (price_per_sqm)
    для каждого объявления на основе цен по свободным дням и площади.

    Календарь и цены НЕ модифицируются — данные сохраняются
    в том виде, в котором их вернул API.

    Модификация выполняется in-place — переданные объекты RawListing
    изменяются напрямую (только поле price_per_sqm).
    """

    def __init__(
        self,
        price_deviation_up: int = 100,
        price_deviation_down: int = 50,
    ) -> None:
        """Инициализирует сервис обработки данных.

        Args:
            price_deviation_up: Не используется (сохранён для совместимости интерфейса).
            price_deviation_down: Не используется (сохранён для совместимости интерфейса).
        """
        # Параметры сохранены для совместимости с вызовами из __main__.py,
        # но не используются — этапы очистки удалены.
        pass

    def clean_listings(self, listings: list[RawListing]) -> dict[str, int]:
        """Рассчитывает price_per_sqm для списка объявлений.

        Календарь и цены не модифицируются.

        Args:
            listings: Список объявлений (модифицируется in-place — только price_per_sqm).

        Returns:
            Словарь со статистикой (все счётчики очистки = 0, для совместимости).
        """
        for listing in listings:
            listing.calculate_price_per_sqm()

        logger.info(
            "расчёт_price_per_sqm_завершён",
            step=f"обработано={len(listings)}",
        )

        return {
            "listings_processed": len(listings),
            "listings_cleaned": 0,
            "days_cleaned": 0,
            "outlier_listings_cleaned": 0,
            "outlier_days_cleaned": 0,
        }
