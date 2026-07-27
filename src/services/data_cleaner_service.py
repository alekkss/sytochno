"""Сервис очистки данных — удаление технических блокировок из календарей."""

from src.config.logger import get_logger
from src.models.listing import RawListing

logger = get_logger("service.data_cleaner")


class DataCleanerService:
    """Сервис предобработки данных после batch-обогащения.

    Выявляет и нейтрализует «технические блокировки» — дни, которые
    помечены как занятые (calendar=1), но не имеют цены (prices=0).
    Это означает, что дата никогда не была открыта для бронирования —
    хозяин закрыл её вручную. Такие дни не являются реальными бронями
    и не должны учитываться при расчёте загрузки и сравнении.

    Алгоритм: если calendar_60_days[i] == 1 и prices_60_days[i] == 0,
    то calendar_60_days[i] устанавливается в 0 (свободен).

    Модификация выполняется in-place — переданные объекты RawListing
    изменяются напрямую.
    """

    def clean_listings(self, listings: list[RawListing]) -> dict[str, int]:
        """Очищает список объявлений от технических блокировок.

        Для каждого объявления проверяет пары calendar/prices на каждый день.
        Если день занят, но цена отсутствует — день переводится в «свободен».

        Args:
            listings: Список объявлений для очистки (модифицируется in-place).

        Returns:
            Словарь со статистикой очистки:
                - listings_processed: Количество обработанных объявлений.
                - listings_cleaned: Количество объявлений, в которых найдены блокировки.
                - days_cleaned: Общее количество очищенных дней (по всем объявлениям).
        """
        listings_processed = 0
        listings_cleaned = 0
        total_days_cleaned = 0

        for listing in listings:
            cleaned_days = self._clean_single_listing(listing)

            listings_processed += 1

            if cleaned_days > 0:
                listings_cleaned += 1
                total_days_cleaned += cleaned_days

        stats = {
            "listings_processed": listings_processed,
            "listings_cleaned": listings_cleaned,
            "days_cleaned": total_days_cleaned,
        }

        if total_days_cleaned > 0:
            logger.info(
                "очистка_технических_блокировок",
                step=f"обработано={listings_processed}, "
                     f"с_блокировками={listings_cleaned}, "
                     f"дней_очищено={total_days_cleaned}",
            )
        else:
            logger.debug(
                "очистка_блокировок_не_требуется",
                step=f"обработано={listings_processed}",
            )

        return stats

    def _clean_single_listing(self, listing: RawListing) -> int:
        """Очищает одно объявление от технических блокировок.

        Правило: если calendar_60_days[i] == 1 и prices_60_days[i] == 0,
        то это техническая блокировка — переводим calendar_60_days[i] в 0.

        Объявления без данных календаря или цен — пропускаются.
        Объявления с фатальной причиной пропуска — пропускаются.

        Args:
            listing: Объявление для очистки (модифицируется in-place).

        Returns:
            Количество очищенных дней в этом объявлении.
        """
        # Пропускаем объявления с фатальными ошибками
        if listing.enrichment_skip_reason is not None:
            return 0

        # Пропускаем если нет данных
        if not listing.calendar_60_days or not listing.prices_60_days:
            return 0

        # Определяем длину для итерации (минимум из двух массивов)
        length = min(len(listing.calendar_60_days), len(listing.prices_60_days))

        cleaned_count = 0

        for i in range(length):
            if listing.calendar_60_days[i] == 1 and listing.prices_60_days[i] == 0:
                # День помечен как занятый, но цены нет —
                # это техническая блокировка, а не реальная бронь.
                listing.calendar_60_days[i] = 0
                cleaned_count += 1

        return cleaned_count
