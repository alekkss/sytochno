"""Сервис сравнения снимков и детектирования событий бронирования."""

from datetime import date, timedelta

from src.config.logger import get_logger
from src.models.booking_event import BookingEvent, CancellationEvent, EventType
from src.models.snapshot import ListingSnapshot

logger = get_logger("service.comparison")

# Тип события: объединение для удобства аннотаций
AnyEvent = BookingEvent | CancellationEvent

# Виды переходов статуса дня
_KIND_BOOKING = "booking"
_KIND_CANCELLATION = "cancellation"


class ComparisonService:
    """Сервис детектирования броней и отмен между двумя снимками.

    Календарь использует три статуса дня:
    0 — свободен и бронируем, 1 — продан, 2 — техблок (не продан,
    но забронировать нельзя — ограничение минимального срока).

    Алгоритм:
    1. Строит словари {дата: значение} для каждого снимка на основе
       snapshot_dt.date() — реальной даты начала календаря.
    2. Находит пересечение дат (дни, присутствующие в обоих снимках).
    3. Определяет вид перехода для каждой даты (_event_kind):
       - бронь: 0→1 и 2→1 (день стал проданным);
       - отмена: 1→0 и 1→2 (день перестал быть проданным);
       - 0↔2 — НЕ событие: день не продан ни до, ни после — 
         изменилась только продаваемость ночи (сдвиг min-stay или соседней брони).,
         реальной сделки нет.
    4. Собирает блоки подряд идущих дней одного вида перехода.
    5. Для каждого блока вычисляет экономику: глубину, цену, итог.
    6. Возвращает список событий BookingEvent и CancellationEvent.
    """

    def compare(
        self,
        old_snapshot: ListingSnapshot,
        new_snapshot: ListingSnapshot,
        listing_title: str = "",
    ) -> list[AnyEvent]:
        """Сравнивает два снимка и возвращает список событий.

        Выравнивает календари по реальным датам — корректно работает
        даже если снимки сделаны в разные дни.

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

        # Базовые даты — день, с которого начинается календарь каждого снимка
        old_base_date: date = old_snapshot.snapshot_dt.date()
        new_base_date: date = new_snapshot.snapshot_dt.date()

        # Строим словари {дата: значение_занятости} для каждого снимка
        old_by_date: dict[date, int] = {
            old_base_date + timedelta(days=i): old_calendar[i]
            for i in range(60)
        }
        new_by_date: dict[date, int] = {
            new_base_date + timedelta(days=i): new_calendar[i]
            for i in range(60)
        }

        # Пересечение — даты, присутствующие в обоих снимках
        common_dates = sorted(set(old_by_date.keys()) & set(new_by_date.keys()))

        if not common_dates:
            logger.warning(
                "нет_пересекающихся_дат",
                external_id=new_snapshot.listing_external_id,
                old_base=old_base_date.isoformat(),
                new_base=new_base_date.isoformat(),
            )
            return []

        # Сравниваем значения для пересекающихся дат.
        # Переходы 0↔2 не являются событиями — считаем их отдельно
        # для мониторинга (смена владельцем минимального срока).
        changes: list[tuple[date, int, int]] = []
        boundary_shifts = 0

        for day in common_dates:
            old_val = old_by_date[day]
            new_val = new_by_date[day]

            if old_val == new_val:
                continue

            if self._event_kind(old_val, new_val) is not None:
                changes.append((day, old_val, new_val))
            else:
                boundary_shifts += 1

        if boundary_shifts > 0:
            logger.info(
                "переходы_границы_бронируемости_пропущены",
                external_id=new_snapshot.listing_external_id,
                count=boundary_shifts,
                note="0<->2 не события (сдвиг min-stay)",
            )

        if not changes:
            return []

        # Склеиваем изменения в блоки и строим события
        events = self._build_events(
            changes=changes,
            old_snapshot=old_snapshot,
            new_snapshot=new_snapshot,
            listing_title=listing_title,
        )

        return sorted(events, key=lambda e: e.checkin_date)

    @staticmethod
    def _event_kind(old_val: int, new_val: int) -> str | None:
        """Определяет вид перехода статуса дня.

        Единственная точка истины о том, что является событием:
        - бронь: день стал проданным (0→1 или 2→1);
        - отмена: день перестал быть проданным (1→0 или 1→2);
        - None: не событие — день не продан ни до, ни после (0↔2),
          изменилась только граница бронируемости (min-stay).

        Args:
            old_val: Значение дня в предыдущем снимке (0/1/2).
            new_val: Значение дня в текущем снимке (0/1/2).

        Returns:
            _KIND_BOOKING, _KIND_CANCELLATION или None.
        """
        if new_val == 1 and old_val != 1:
            return _KIND_BOOKING
        if old_val == 1 and new_val != 1:
            return _KIND_CANCELLATION
        return None

    def _build_events(
        self,
        changes: list[tuple[date, int, int]],
        old_snapshot: ListingSnapshot,
        new_snapshot: ListingSnapshot,
        listing_title: str,
    ) -> list[AnyEvent]:
        """Склеивает отдельные дни изменений в блоки и строит события.

        Блок — это непрерывная последовательность дней с одинаковым
        видом перехода (бронь или отмена), включая смешанные пары
        статусов: брони 0→1 и 2→1, отмены 1→0 и 1→2.

        Args:
            changes: Список (дата, старое_значение, новое_значение).
            old_snapshot: Снимок №1 — нужен для цен при брони.
            new_snapshot: Снимок №2 — нужен для цен при отмене.
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
                old_snapshot=old_snapshot,
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
        """Группирует список изменений в непрерывные блоки одного вида.

        Блок разрывается если:
        - Вид перехода сменился (бронь на отмену или наоборот).
          Сравнение ведётся по _event_kind, а не по арифметике
          значений: брони 0→1 и 2→1 — один вид, отмены 1→0 и 1→2 —
          тоже один вид, поэтому смешанные блоки корректно склеиваются.
        - Между датами пропуск больше одного дня.

        Args:
            changes: Отсортированный список изменений по дням.

        Returns:
            Список блоков, каждый блок — список изменений одного вида.
        """
        if not changes:
            return []

        blocks: list[list[tuple[date, int, int]]] = []
        current_block: list[tuple[date, int, int]] = [changes[0]]
        current_kind = self._event_kind(changes[0][1], changes[0][2])

        for i in range(1, len(changes)):
            prev_day = changes[i - 1][0]
            curr_day, curr_old, curr_new = changes[i]

            curr_kind = self._event_kind(curr_old, curr_new)
            consecutive = (curr_day - prev_day == timedelta(days=1))

            if curr_kind == current_kind and consecutive:
                current_block.append(changes[i])
            else:
                blocks.append(current_block)
                current_block = [changes[i]]
                current_kind = curr_kind

        blocks.append(current_block)
        return blocks

    def _build_single_event(
        self,
        block: list[tuple[date, int, int]],
        old_snapshot: ListingSnapshot,
        new_snapshot: ListingSnapshot,
        listing_title: str,
    ) -> AnyEvent | None:
        """Строит одно событие из блока дней.

        Вид события определяется по направлению перехода первого дня
        блока через _event_kind: бронь — день стал проданным (0→1/2→1),
        отмена — перестал быть проданным (1→0/1→2). Прочие переходы
        в блок попасть не могут (отфильтрованы в compare) — на всякий
        случай блок пропускается с предупреждением.

        Цены: для брони берутся из old_snapshot (день был не продан:
        свободен или техблок — цена валидна в обоих случаях, у дней
        техблока цена сохраняется). Для отмены — из new_snapshot (день
        снова не продан: свободен или техблок — цена валидна).

        Args:
            block: Список дней одного блока (дата, старое, новое).
            old_snapshot: Снимок №1 (предыдущий прогон).
            new_snapshot: Снимок №2 (текущий прогон).
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

        # Вид события определяется по направлению изменения первого дня
        _, old_val, new_val = block[0]
        kind = self._event_kind(old_val, new_val)

        if kind == _KIND_BOOKING:
            event_type = EventType.BOOKING
        elif kind == _KIND_CANCELLATION:
            event_type = EventType.CANCELLATION
        else:
            logger.warning(
                "неизвестный_тип_изменения",
                old_val=old_val,
                new_val=new_val,
            )
            return None

        # Глубина бронирования: разница между датой сделки и датой заезда.
        # Может быть отрицательной если событие обнаружено на уже прошедшую
        # дату (редкий запуск, длинный интервал между снимками). Логируем
        # как предупреждение — данные сохраняем, но помечаем.
        snapshot_date = new_snapshot.snapshot_dt.date()
        depth_days = (checkin_date - snapshot_date).days

        if depth_days < 0:
            logger.warning(
                "отрицательная_глубина_бронирования",
                step=f"id={new_snapshot.listing_external_id}, "
                     f"заезд={checkin_date}, снимок={snapshot_date}, "
                     f"глубина={depth_days}",
            )

        # Для брони берём цены из старого снимка (день был не продан →
        # цена есть: свободный день или техблок с сохранённой ценой).
        # Для отмены берём цены из нового снимка (день снова не продан →
        # цена есть по той же причине).
        price_snapshot = old_snapshot if event_type == EventType.BOOKING else new_snapshot

        price_per_night = self._calc_avg_price(
            block=block,
            snapshot=price_snapshot,
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

        Берёт цены из переданного снимка для каждого дня блока.
        Если цена для дня не найдена или равна нулю — день не учитывается.
        Если цен нет совсем — возвращает 0.0.

        Args:
            block: Список дней блока.
            snapshot: Снимок, из которого берутся цены.

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
