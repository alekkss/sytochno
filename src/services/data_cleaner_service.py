"""Сервис очистки данных — удаление технических блокировок и ценовых выбросов."""

import statistics

from src.config.logger import get_logger
from src.models.listing import RawListing

logger = get_logger("service.data_cleaner")


class DataCleanerService:
    """Сервис предобработки данных после batch-обогащения.

    Выполняет два этапа очистки:

    Этап 1 — Технические блокировки:
        Дни с calendar=1 и price=0 — хозяин закрыл дату вручную,
        это не реальная бронь. Переводятся в calendar=0.

    Этап 2 — Ценовые выбросы по медиане стоимости за м²:
        Для каждого дня (0–59) рассчитывается медиана цены за м²
        по всем объявлениям. Объявления с отклонением выше верхнего
        порога (заградительная цена) или ниже нижнего порога (замануха)
        обнуляются (calendar=0, price=0).

    После обоих этапов для каждого объявления рассчитывается итоговая
    средняя стоимость за м² (поле price_per_sqm).

    Модификация выполняется in-place — переданные объекты RawListing
    изменяются напрямую.
    """

    def __init__(
        self,
        price_deviation_up: int = 100,
        price_deviation_down: int = 50,
    ) -> None:
        """Инициализирует сервис очистки данных.

        Args:
            price_deviation_up: Порог отклонения вверх от медианы (проценты).
                100 означает: цена за м² > медиана × 2.0 → заградительная.
            price_deviation_down: Порог отклонения вниз от медианы (проценты).
                50 означает: цена за м² < медиана × 0.5 → замануха.
        """
        self._upper_multiplier = 1.0 + price_deviation_up / 100.0
        self._lower_multiplier = 1.0 - price_deviation_down / 100.0

    def clean_listings(self, listings: list[RawListing]) -> dict[str, int]:
        """Выполняет полную очистку списка объявлений.

        Последовательно применяет оба этапа очистки, затем рассчитывает
        итоговую стоимость за м² для каждого объявления.

        Args:
            listings: Список объявлений для очистки (модифицируется in-place).

        Returns:
            Словарь со статистикой очистки:
                - listings_processed: Количество обработанных объявлений.
                - listings_cleaned: Объявления с техническими блокировками (этап 1).
                - days_cleaned: Дней очищено на этапе 1.
                - outlier_listings_cleaned: Объявления с ценовыми выбросами (этап 2).
                - outlier_days_cleaned: Дней очищено на этапе 2.
        """
        # Этап 1: Технические блокировки (calendar=1, price=0)
        stage1_stats = self._clean_technical_blocks(listings)

        # Этап 2: Ценовые выбросы по медиане стоимости за м²
        stage2_stats = self._clean_price_outliers(listings)

        # Итоговый расчёт price_per_sqm после всех очисток
        for listing in listings:
            listing.calculate_price_per_sqm()

        stats = {
            "listings_processed": stage1_stats["listings_processed"],
            "listings_cleaned": stage1_stats["listings_cleaned"],
            "days_cleaned": stage1_stats["days_cleaned"],
            "outlier_listings_cleaned": stage2_stats["outlier_listings_cleaned"],
            "outlier_days_cleaned": stage2_stats["outlier_days_cleaned"],
        }

        return stats

    def _clean_technical_blocks(self, listings: list[RawListing]) -> dict[str, int]:
        """Этап 1: Удаление технических блокировок.

        Правило: calendar=1, price=0 → calendar=0.
        Дата закрыта, но цены нет — значит она не была открыта для бронирования.

        Args:
            listings: Список объявлений (модифицируется in-place).

        Returns:
            Статистика этапа 1.
        """
        listings_processed = 0
        listings_cleaned = 0
        total_days_cleaned = 0

        for listing in listings:
            cleaned_days = self._clean_single_technical(listing)
            listings_processed += 1

            if cleaned_days > 0:
                listings_cleaned += 1
                total_days_cleaned += cleaned_days

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

        return {
            "listings_processed": listings_processed,
            "listings_cleaned": listings_cleaned,
            "days_cleaned": total_days_cleaned,
        }

    def _clean_single_technical(self, listing: RawListing) -> int:
        """Очищает одно объявление от технических блокировок.

        Args:
            listing: Объявление (модифицируется in-place).

        Returns:
            Количество очищенных дней.
        """
        if listing.enrichment_skip_reason is not None:
            return 0

        if not listing.calendar_60_days or not listing.prices_60_days:
            return 0

        length = min(len(listing.calendar_60_days), len(listing.prices_60_days))
        cleaned_count = 0

        for i in range(length):
            if listing.calendar_60_days[i] == 1 and listing.prices_60_days[i] == 0:
                listing.calendar_60_days[i] = 0
                cleaned_count += 1

        return cleaned_count

    def _clean_price_outliers(self, listings: list[RawListing]) -> dict[str, int]:
        """Этап 2: Удаление ценовых выбросов по медиане стоимости за м².

        Для каждого дня (0–59):
        1. Собирает цену за м² (price[день] ÷ площадь) по всем объявлениям
           с ненулевой ценой и ненулевой площадью в этот день.
        2. Рассчитывает медиану дня.
        3. Обнуляет дни, где цена за м² выше верхнего или ниже нижнего порога.

        Args:
            listings: Список объявлений (модифицируется in-place).

        Returns:
            Статистика этапа 2.
        """
        # Фильтруем объявления, пригодные для анализа
        eligible = self._get_eligible_listings(listings)

        if not eligible:
            logger.debug(
                "очистка_выбросов_нет_данных",
                step="нет объявлений с площадью и ценами",
            )
            return {"outlier_listings_cleaned": 0, "outlier_days_cleaned": 0}

        # Определяем количество дней для анализа
        max_days = self._get_max_days(eligible)

        outlier_listings: set[str] = set()
        total_outlier_days = 0

        for day_idx in range(max_days):
            # Собираем цены за м² для этого дня
            day_prices_per_sqm = self._collect_day_prices_per_sqm(eligible, day_idx)

            if len(day_prices_per_sqm) < 3:
                # Недостаточно данных для расчёта медианы — пропускаем день
                continue

            median_value = statistics.median(day_prices_per_sqm)

            if median_value <= 0:
                continue

            upper_threshold = median_value * self._upper_multiplier
            lower_threshold = median_value * self._lower_multiplier

            # Обнуляем выбросы
            cleaned_this_day = self._nullify_outliers_for_day(
                eligible, day_idx, upper_threshold, lower_threshold, outlier_listings
            )
            total_outlier_days += cleaned_this_day

        outlier_listings_count = len(outlier_listings)

        if total_outlier_days > 0:
            logger.info(
                "очистка_ценовых_выбросов",
                step=f"объявлений_затронуто={outlier_listings_count}, "
                     f"дней_очищено={total_outlier_days}, "
                     f"порог_вверх=×{self._upper_multiplier:.2f}, "
                     f"порог_вниз=×{self._lower_multiplier:.2f}",
            )
        else:
            logger.debug(
                "ценовые_выбросы_не_обнаружены",
                step=f"проанализировано={len(eligible)}, дней={max_days}",
            )

        return {
            "outlier_listings_cleaned": outlier_listings_count,
            "outlier_days_cleaned": total_outlier_days,
        }

    def _get_eligible_listings(self, listings: list[RawListing]) -> list[RawListing]:
        """Отбирает объявления, пригодные для медианного анализа.

        Критерии: есть площадь > 0, есть календарь и цены,
        нет фатальной причины пропуска.

        Args:
            listings: Полный список объявлений.

        Returns:
            Отфильтрованный список.
        """
        eligible: list[RawListing] = []

        for listing in listings:
            if listing.enrichment_skip_reason is not None:
                continue
            if not listing.area_m2 or listing.area_m2 <= 0:
                continue
            if not listing.calendar_60_days or not listing.prices_60_days:
                continue
            eligible.append(listing)

        return eligible

    def _get_max_days(self, listings: list[RawListing]) -> int:
        """Определяет максимальное количество дней для анализа.

        Берёт минимальную длину из всех календарей и массивов цен,
        но не более 60.

        Args:
            listings: Список пригодных объявлений.

        Returns:
            Количество дней для итерации.
        """
        max_days = 60

        for listing in listings:
            cal_len = len(listing.calendar_60_days)
            price_len = len(listing.prices_60_days)
            day_count = min(cal_len, price_len)
            if day_count < max_days:
                max_days = day_count

        return max_days

    def _collect_day_prices_per_sqm(
        self, listings: list[RawListing], day_idx: int
    ) -> list[float]:
        """Собирает цены за м² для конкретного дня по всем объявлениям.

        Включает только объявления, у которых в этот день есть ненулевая цена
        (день свободен или имеет цену).

        Args:
            listings: Список пригодных объявлений.
            day_idx: Индекс дня (0–59).

        Returns:
            Список значений цены за м² для расчёта медианы.
        """
        prices_per_sqm: list[float] = []

        for listing in listings:
            price = listing.prices_60_days[day_idx]
            if price <= 0:
                continue

            # area_m2 уже проверен в _get_eligible_listings (> 0)
            price_sqm = price / listing.area_m2  # type: ignore[operator]
            prices_per_sqm.append(price_sqm)

        return prices_per_sqm

    def _nullify_outliers_for_day(
        self,
        listings: list[RawListing],
        day_idx: int,
        upper_threshold: float,
        lower_threshold: float,
        outlier_listings: set[str],
    ) -> int:
        """Обнуляет выбросы для конкретного дня.

        Если цена за м² объявления в этот день выходит за пороги —
        calendar и price обнуляются.

        Args:
            listings: Список пригодных объявлений.
            day_idx: Индекс дня.
            upper_threshold: Верхний порог (медиана × upper_multiplier).
            lower_threshold: Нижний порог (медиана × lower_multiplier).
            outlier_listings: Множество ID затронутых объявлений (обновляется in-place).

        Returns:
            Количество обнулённых дней в этой итерации.
        """
        cleaned = 0

        for listing in listings:
            price = listing.prices_60_days[day_idx]
            if price <= 0:
                continue

            price_sqm = price / listing.area_m2  # type: ignore[operator]

            if price_sqm > upper_threshold or price_sqm < lower_threshold:
                listing.calendar_60_days[day_idx] = 0
                listing.prices_60_days[day_idx] = 0
                outlier_listings.add(listing.external_id)
                cleaned += 1

        return cleaned
