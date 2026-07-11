"""Контроллер адаптивного параллелизма — глобальный регулятор нагрузки (AIMD).

Реализует алгоритм Additive Increase / Multiplicative Decrease:
- При низком проценте ошибок — быстро увеличивает лимит (+3).
- При высоком проценте ошибок — мягко снижает (×0.75) и ставит cooldown.

Единый экземпляр на весь прогон — передаётся всем воркерам.
Все воркеры вызывают acquire() перед обработкой карточки и release() после.
При срабатывании cooldown все воркеры автоматически приостанавливаются.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field

from src.config.logger import get_logger

logger = get_logger("concurrency_controller")


# ── Параметры алгоритма AIMD ──

# Размер скользящего окна для подсчёта ошибок (секунды).
# 60 секунд — больше событий в окне, меньше шума от единичных сбоев.
# При 70+ воркерах за 60 секунд накапливается 200+ событий —
# статистика стабильная, ложные срабатывания маловероятны.
_WINDOW_SECONDS: float = 60.0

# Порог ошибок для удержания текущего лимита (нижняя граница «жёлтой зоны»).
# Ниже этого — увеличиваем лимит.
_ERROR_RATE_LOW: float = 0.15  # 15%

# Порог ошибок для снижения лимита (верхняя граница «жёлтой зоны»).
# Выше этого — снижаем лимит и включаем cooldown.
# 50% — срабатывает только при реальных массовых проблемах,
# а не при единичных таймаутах или min_nights ошибках.
_ERROR_RATE_HIGH: float = 0.50  # 50%

# Множитель снижения лимита при обвале (Multiplicative Decrease).
# 0.75 — мягкое снижение (со 140 → 105 → 79 → 59).
# Даёт быстрее вернуться к потолку после кратковременного сбоя.
_DECREASE_FACTOR: float = 0.75

# Шаг увеличения лимита (Additive Increase).
# +3 за шаг — от 13 до 140 за ~7 минут (вместо 63 минут при +1).
_INCREASE_STEP: int = 3

# Минимальный интервал между увеличениями лимита (секунды).
# 10 секунд — агрессивный рост при стабильной работе.
# При 0% ошибок лимит достигнет потолка за ceiling/step × interval секунд.
_INCREASE_INTERVAL_SECONDS: float = 10.0

# Длительность cooldown после снижения лимита (секунды).
# 5 секунд — короткая пауза, достаточная чтобы сбросить нагрузку.
_COOLDOWN_SECONDS: float = 5.0

# Минимальное количество событий в окне для принятия решений.
# Если меньше — не хватает статистики, держим текущий лимит.
_MIN_EVENTS_FOR_DECISION: int = 10


@dataclass
class _Event:
    """Одно событие (успех или провал) с временной меткой."""

    timestamp: float
    is_failure: bool


class ConcurrencyController:
    """Глобальный контроллер адаптивного параллелизма.

    Single Responsibility: управляет только лимитом параллелизма.
    Не управляет воркерами, прокси, браузерами — только выдаёт/забирает
    разрешения и адаптирует лимит по обратной связи.

    Open/Closed: параметры алгоритма вынесены в константы модуля.
    Для изменения поведения достаточно подкласса или новых параметров.

    Dependency Inversion: воркеры зависят от интерфейса контроллера
    (acquire/release/report), а не от деталей реализации.

    Использование:
        controller = ConcurrencyController(floor=5, ceiling=100, start=50)

        # В каждом воркере перед обработкой карточки:
        await controller.acquire()
        try:
            result = await process_card(...)
            controller.report_success()
        except NetworkError:
            controller.report_failure()
        finally:
            controller.release()
    """

    def __init__(
        self,
        floor: int = 5,
        ceiling: int = 100,
        start: int | None = None,
    ) -> None:
        """Инициализирует контроллер.

        Args:
            floor: Минимальный лимит параллелизма. Ниже этого значения
                лимит не опустится даже при 100% ошибок.
            ceiling: Максимальный лимит параллелизма. Выше этого значения
                лимит не поднимется даже при 0% ошибок.
            start: Начальный лимит. По умолчанию ceiling // 2 —
                стартуем с половины мощности и наращиваем при стабильности.
        """
        if floor < 1:
            raise ValueError("floor должен быть >= 1")
        if ceiling < floor:
            raise ValueError("ceiling должен быть >= floor")

        self._floor = floor
        self._ceiling = ceiling
        self._current_limit = start if start is not None else max(floor, ceiling // 2)

        # Семафор — ядро контроля. Воркеры ждут разрешения через acquire().
        self._semaphore = asyncio.Semaphore(self._current_limit)

        # Скользящее окно событий (deque с автоматической очисткой старых).
        self._events: deque[_Event] = deque()

        # Блокировка для потокобезопасного изменения лимита.
        self._lock = asyncio.Lock()

        # Время последнего увеличения лимита — для throttling роста.
        self._last_increase_time: float = 0.0

        # Cooldown: Event сброшен = воркеры ждут, установлен = работают.
        self._cooldown_event = asyncio.Event()
        self._cooldown_event.set()  # Изначально нет cooldown

        # Флаг активности cooldown (для предотвращения повторных срабатываний).
        self._cooldown_active = False

        # Статистика за всё время работы
        self._total_successes: int = 0
        self._total_failures: int = 0
        self._total_decreases: int = 0
        self._total_increases: int = 0

        logger.info(
            "контроллер_параллелизма_создан",
            step=f"floor={floor}, ceiling={ceiling}, "
                 f"start={self._current_limit}, "
                 f"окно={_WINDOW_SECONDS}с, "
                 f"порог_низкий={_ERROR_RATE_LOW * 100:.0f}%, "
                 f"порог_высокий={_ERROR_RATE_HIGH * 100:.0f}%, "
                 f"шаг_роста=+{_INCREASE_STEP}, "
                 f"интервал_роста={_INCREASE_INTERVAL_SECONDS}с, "
                 f"снижение=×{_DECREASE_FACTOR}, "
                 f"cooldown={_COOLDOWN_SECONDS}с",
        )

    @property
    def current_limit(self) -> int:
        """Текущий лимит параллелизма.

        Returns:
            Количество одновременно разрешённых операций.
        """
        return self._current_limit

    @property
    def floor(self) -> int:
        """Минимальный лимит параллелизма.

        Returns:
            Нижняя граница лимита.
        """
        return self._floor

    @property
    def ceiling(self) -> int:
        """Максимальный лимит параллелизма.

        Returns:
            Верхняя граница лимита.
        """
        return self._ceiling

    @property
    def error_rate(self) -> float:
        """Текущий процент ошибок в скользящем окне.

        Returns:
            Значение от 0.0 до 1.0 (0% — 100%).
        """
        self._purge_old_events()
        total = len(self._events)
        if total == 0:
            return 0.0
        failures = sum(1 for e in self._events if e.is_failure)
        return failures / total

    @property
    def stats(self) -> dict[str, int | float]:
        """Сводная статистика контроллера.

        Returns:
            Словарь с метриками работы.
        """
        return {
            "current_limit": self._current_limit,
            "floor": self._floor,
            "ceiling": self._ceiling,
            "error_rate": round(self.error_rate * 100, 1),
            "total_successes": self._total_successes,
            "total_failures": self._total_failures,
            "total_increases": self._total_increases,
            "total_decreases": self._total_decreases,
            "events_in_window": len(self._events),
        }

    async def acquire(self) -> None:
        """Запрашивает разрешение на выполнение операции.

        Блокирует воркер, если:
        - Достигнут текущий лимит параллелизма (семафор исчерпан).
        - Активен cooldown (все воркеры ждут его окончания).

        Воркер вызывает перед началом обработки карточки.
        """
        # Сначала ждём окончания cooldown (если активен)
        await self._cooldown_event.wait()

        # Затем ждём разрешения семафора
        await self._semaphore.acquire()

    def release(self) -> None:
        """Освобождает разрешение после завершения операции.

        Воркер вызывает после обработки карточки (в finally-блоке).
        """
        self._semaphore.release()

    def report_success(self) -> None:
        """Фиксирует успешную операцию.

        Вызывается после успешной загрузки страницы или API-запроса.
        Добавляет событие в скользящее окно и проверяет условие
        для увеличения лимита.
        """
        now = time.monotonic()
        self._events.append(_Event(timestamp=now, is_failure=False))
        self._total_successes += 1

        # Проверяем: можно ли увеличить лимит (неблокирующая попытка)
        self._try_increase(now)

    def report_failure(self) -> None:
        """Фиксирует провал операции (сетевая ошибка, ERR_EMPTY_RESPONSE).

        Вызывается при сетевых ошибках, таймаутах, отказах сервера.
        Добавляет событие и проверяет условие для снижения лимита.

        НЕ вызывается при логических ошибках API (min_nights, not_found) —
        это не проблема нагрузки, а особенность конкретной карточки.
        """
        now = time.monotonic()
        self._events.append(_Event(timestamp=now, is_failure=True))
        self._total_failures += 1

        # Проверяем: нужно ли снизить лимит (неблокирующая попытка)
        self._try_decrease(now)

    def _try_increase(self, now: float) -> None:
        """Пытается увеличить лимит (Additive Increase).

        Увеличение происходит если:
        - Прошло достаточно времени с последнего увеличения.
        - Достаточно событий в окне для статистики.
        - Error rate ниже порога _ERROR_RATE_LOW.
        - Текущий лимит ниже ceiling.

        Args:
            now: Текущее время (monotonic).
        """
        # Throttling: не чаще раза в _INCREASE_INTERVAL_SECONDS
        if now - self._last_increase_time < _INCREASE_INTERVAL_SECONDS:
            return

        self._purge_old_events()
        total = len(self._events)

        # Недостаточно статистики — ждём
        if total < _MIN_EVENTS_FOR_DECISION:
            return

        failures = sum(1 for e in self._events if e.is_failure)
        current_error_rate = failures / total

        if current_error_rate < _ERROR_RATE_LOW and self._current_limit < self._ceiling:
            new_limit = min(self._current_limit + _INCREASE_STEP, self._ceiling)
            self._adjust_limit(new_limit)
            self._last_increase_time = now
            self._total_increases += 1

            logger.info(
                "лимит_увеличен",
                step=f"новый={new_limit}, "
                     f"error_rate={current_error_rate * 100:.1f}%, "
                     f"событий={total}",
            )

    def _try_decrease(self, now: float) -> None:
        """Пытается снизить лимит (Multiplicative Decrease).

        Снижение происходит если:
        - Достаточно событий в окне для статистики.
        - Error rate выше порога _ERROR_RATE_HIGH.
        - Cooldown не активен (предотвращает каскадные снижения).

        При срабатывании:
        - Лимит умножается на _DECREASE_FACTOR (но не ниже floor).
        - Запускается cooldown на _COOLDOWN_SECONDS.

        Args:
            now: Текущее время (monotonic).
        """
        self._purge_old_events()
        total = len(self._events)

        # Недостаточно статистики — ждём
        if total < _MIN_EVENTS_FOR_DECISION:
            return

        failures = sum(1 for e in self._events if e.is_failure)
        current_error_rate = failures / total

        if current_error_rate > _ERROR_RATE_HIGH and not self._cooldown_active:
            new_limit = max(
                int(self._current_limit * _DECREASE_FACTOR),
                self._floor,
            )
            old_limit = self._current_limit
            self._adjust_limit(new_limit)
            self._total_decreases += 1

            logger.warning(
                "лимит_снижен_cooldown",
                step=f"старый={old_limit}, новый={new_limit}, "
                     f"error_rate={current_error_rate * 100:.1f}%, "
                     f"событий={total}, cooldown={_COOLDOWN_SECONDS}с",
            )

            # Запускаем cooldown — все воркеры замрут
            asyncio.create_task(self._run_cooldown())

    async def _run_cooldown(self) -> None:
        """Выполняет cooldown — глобальную паузу для всех воркеров.

        Сбрасывает cooldown_event (воркеры блокируются на acquire()),
        ждёт _COOLDOWN_SECONDS, затем устанавливает event обратно.
        Очищает скользящее окно — начинаем измерения с чистого листа.
        """
        self._cooldown_active = True
        self._cooldown_event.clear()  # Блокируем все acquire()

        logger.info(
            "cooldown_начат",
            step=f"пауза={_COOLDOWN_SECONDS}с, лимит={self._current_limit}",
        )

        await asyncio.sleep(_COOLDOWN_SECONDS)

        # Очищаем окно — после cooldown начинаем измерения заново
        self._events.clear()

        self._cooldown_active = False
        self._cooldown_event.set()  # Разблокируем все acquire()

        logger.info(
            "cooldown_завершён",
            step=f"лимит={self._current_limit}, воркеры_разблокированы",
        )

    def _adjust_limit(self, new_limit: int) -> None:
        """Изменяет текущий лимит семафора.

        Если новый лимит больше текущего — добавляем разрешения в семафор.
        Если новый лимит меньше — уменьшаем внутренний счётчик.

        Семафор asyncio не имеет метода для уменьшения — поэтому
        при снижении мы «забираем» разрешения через acquire без ожидания.
        Если все разрешения заняты — эффект проявится когда воркеры
        начнут вызывать release() (семафор не вырастет обратно).

        Args:
            new_limit: Новое значение лимита.
        """
        diff = new_limit - self._current_limit

        if diff > 0:
            # Увеличиваем: добавляем разрешения
            for _ in range(diff):
                self._semaphore.release()
        elif diff < 0:
            # Уменьшаем: забираем разрешения (если есть свободные)
            # Используем _value напрямую — это внутренний счётчик asyncio.Semaphore
            for _ in range(-diff):
                # Пытаемся забрать без блокировки
                if self._semaphore._value > 0:  # noqa: SLF001
                    self._semaphore._value -= 1  # noqa: SLF001

        self._current_limit = new_limit

    def _purge_old_events(self) -> None:
        """Удаляет события старше _WINDOW_SECONDS из скользящего окна."""
        cutoff = time.monotonic() - _WINDOW_SECONDS
        while self._events and self._events[0].timestamp < cutoff:
            self._events.popleft()

    def log_stats(self) -> None:
        """Выводит текущую статистику контроллера в лог.

        Вызывается периодически из основного цикла или при завершении.
        """
        stats = self.stats
        logger.info(
            "статистика_контроллера",
            step=f"лимит={stats['current_limit']}/{stats['ceiling']}, "
                 f"error_rate={stats['error_rate']}%, "
                 f"успехов={stats['total_successes']}, "
                 f"провалов={stats['total_failures']}, "
                 f"увеличений={stats['total_increases']}, "
                 f"снижений={stats['total_decreases']}, "
                 f"в_окне={stats['events_in_window']}",
        )
