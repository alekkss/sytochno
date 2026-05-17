"""Сервис сравнения снимков и детектирования событий бронирования."""

from datetime import date, timedelta

from src.config.logger import get_logger
from src.models.booking_event import BookingEvent, CancellationEvent, EventType
from src.models.snapshot import ListingSnapshot

logger = get_logger("service.comparison")

# Тип события: объединение для удобства аннотаций
AnyEvent = BookingEvent | CancellationEvent


class ComparisonService:
    """Сервис детектирования броней и отмен между двумя снимками.

    Алгоритм:
    1. Сравнивает календари снимка №1 и снимка №2 день за днём.
    2. Собирает блоки дней с одинаковым типом изменения (0→1 или 1→0).
    3. Для каждого блока вычисляет экономику: глубину, цену, итог.
    4. Возвращает список событий BookingEvent и CancellationEvent.
    """

    def compare(
        self,
        old_snapshot: ListingSnapshot,
        new_snapshot: ListingSnapshot,
        listing_title: str = "",
    ) -> list[AnyEvent]:
        """Сравнивает два снимка и возвращает список событий.

        Args:
            old_snapshot: Снимок №1 (предыдущий прогон).
            new_snapshot: Снимок №2 (текущий прогон).
            listing_title: Название объявления для отчёта.

        Returns:
            Список событий BookingEvent и CancellationEvent,
            отсортированных по дате заезда.
        """
        old_calendar = old_snapshot.calendar_as_list()
        new_calendar = new_snapshot.calendar_as_list()

        if len(old_calendar) != 60 or len(new_calendar) != 60:
            logger.warning(
                "некорректная_длина_календарей",
                external_id=new_snapshot.listing_external_id,
                old_len=len(old_calendar),
                new_len=len(new_calendar),
            )
            return []

        # Базовая дата — день, с которого начинается календарь снимка №2
        base_date: date = new_snapshot.snapshot_dt.date()

        # Собираем список изменений по дням
        changes: list[tuple[date, int, int]] = []  # (дата, старое, новое)
        for i in range(60):
            old_val = old_calendar[i]
            new_val = new_calendar[i]
            if old_val != new_val:
                day = base_date + timedelta(days=i)
                changes.append((day, old_val, new_val))

        if not changes:
            logger.info(
                "изменений_не_обнаружено",
                external_id=new_snapshot.listing_external_id,
            )
            return []

        # Склеиваем изменения в блоки и строим события
        events = self._build_events(
            changes=changes,
            new_snapshot=new_snapshot,
            listing_title=listing_title,
        )

        logger.info(
            "события_обнаружены",
            external_id=new_snapshot.listing_external_id,
            total=len(events),
        )

        return sorted(events, key=lambda e: e.checkin_date)

    def _build_events(
        self,
        changes: list[tuple[date, int, int]],
        new_snapshot: ListingSnapshot,
        listing_title: str,
    ) -> list[AnyEvent]:
        """Склеивает отдельные дни изменений в блоки и строит события.

        Блок — это непрерывная последовательность дней с одинаковым
        типом изменения (0→1 или 1→0).

        Args:
            changes: Список (дата, старое_значение, новое_значение).
            new_snapshot: Снимок №2 для получения цен и метаданных.
            listing_title: Название объявления.

        Returns:
            Список событий.
        """
        events: list[AnyEvent] = []

        # Группируем изменения в непрерывные блоки
        blocks = self._group_into_blocks(changes)

        for block in blocks:
            event = self._build_single_event(
                block=block,
                new_snapshot=new_snapshot,
                listing_title=listing_title,
            )
            if event is not None:
                events.append(event)

        return events

    def _group_into_blocks(
        self,
        changes: list[tuple[date, int, int]],
    ) -> list[list[tuple[date, int, int]]]:
        """Группирует список изменений в непрерывные блоки одного типа.

        Блок разрывается если:
        - Тип изменения сменился (0→1 на 1→0 или наоборот).
        - Между датами пропуск больше одного дня.

        Args:
            changes: Отсортированный список изменений по дням.

        Returns:
            Список блоков, каждый блок — список изменений одного типа.
        """
        if not changes:
            return []

        blocks: list[list[tuple[date, int, int]]] = []
        current_block: list[tuple[date, int, int]] = [changes[0]]

        for i in range(1, len(changes)):
            prev_day, _, prev_new = changes[i - 1]
            curr_day, curr_old, curr_new = changes[i]

            # Тот же тип изменения и следующий день подряд
            same_type = (prev_new == curr_new and curr_old == (1 - curr_new))
            consecutive = (curr_day - prev_day == timedelta(days=1))

            if same_type and consecutive:
                current_block.append(changes[i])
            else:
                blocks.append(current_block)
                current_block = [changes[i]]

        blocks.append(current_block)
        return blocks

    def _build_single_event(
        self,
        block: list[tuple[date, int, int]],
        new_snapshot: ListingSnapshot,
        listing_title: str,
    ) -> AnyEvent | None:
        """Строит одно событие из блока дней.

        Args:
            block: Список дней одного блока (дата, старое, новое).
            new_snapshot: Снимок №2 для получения цен.
            listing_title: Название объявления.

        Returns:
            BookingEvent, CancellationEvent или None при ошибке.
        """
        if not block:
            return None

        checkin_date = block[0][0]
        last_day = block[-1][0]
        checkout_date = last_day + timedelta(days=1)
        nights = len(block)

        # Тип события определяется по направлению изменения
        _, old_val, new_val = block[0]
        if old_val == 0 and new_val == 1:
            event_type = EventType.BOOKING
        elif old_val == 1 and new_val == 0:
            event_type = EventType.CANCELLATION
        else:
            logger.warning(
                "неизвестный_тип_изменения",
                old_val=old_val,
                new_val=new_val,
            )
            return None

        # Глубина бронирования: разница между датой сделки и датой заезда
        snapshot_date = new_snapshot.snapshot_dt.date()
        depth_days = (checkin_date - snapshot_date).days

        # Средняя цена по дням блока
        price_per_night = self._calc_avg_price(
            block=block,
            snapshot=new_snapshot,
        )
        total_price = price_per_night * nights

        kwargs = dict(
            listing_external_id=new_snapshot.listing_external_id,
            listing_title=listing_title,
            event_type=event_type,
            snapshot_dt=new_snapshot.snapshot_dt,
            checkin_date=checkin_date,
            checkout_date=checkout_date,
            nights=nights,
            depth_days=depth_days,
            price_per_night=round(price_per_night, 2),
            total_price=round(total_price, 2),
        )

        if event_type == EventType.BOOKING:
            return BookingEvent(**kwargs)
        return CancellationEvent(**kwargs)

    def _calc_avg_price(
        self,
        block: list[tuple[date, int, int]],
        snapshot: ListingSnapshot,
    ) -> float:
        """Вычисляет среднюю цену за ночь по дням блока.

        Берёт цены из снимка №2 для каждого дня блока.
        Если цена для дня не найдена — день не учитывается в среднем.
        Если цены нет совсем — возвращает 0.0.

        Args:
            block: Список дней блока.
            snapshot: Снимок №2 с ценами.

        Returns:
            Средняя цена за ночь в рублях.
        """
        prices: list[float] = []

        for day_date, _, _ in block:
            price = snapshot.price_for_date(day_date)
            if price is not None and price > 0:
                prices.append(price)

        if not prices:
            return 0.0

        return sum(prices) / len(prices)