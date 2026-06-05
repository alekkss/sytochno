"""Монитор здоровья соединения — детектор массовых сбоев прокси/сети."""

import asyncio
from dataclasses import dataclass, field

from src.config.logger import get_logger

logger = get_logger("connection_monitor")

# Порог последовательных полных провалов для срабатывания перезапуска
CONSECUTIVE_FAILURES_THRESHOLD: int = 2


@dataclass
class ConnectionMonitor:
    """Детектор массовых сбоев соединения для одного браузера/воркера.

    Считает последовательные полные провалы загрузки (когда все retry
    внутри page_loader исчерпаны). При достижении порога выставляет
    сигнал restart_needed — все вкладки должны прекратить обработку
    и дождаться перезапуска браузера.

    Потокобезопасен для asyncio (использует asyncio.Lock).
    """

    _consecutive_failures: int = field(default=0, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _restart_event: asyncio.Event = field(
        default_factory=asyncio.Event, init=False, repr=False
    )
    _total_failures: int = field(default=0, init=False, repr=False)
    _total_successes: int = field(default=0, init=False, repr=False)

    @property
    def restart_needed(self) -> bool:
        """Проверяет, требуется ли перезапуск браузера.

        Returns:
            True если количество последовательных сбоев достигло порога.
        """
        return self._restart_event.is_set()

    @property
    def consecutive_failures(self) -> int:
        """Текущее количество последовательных сбоев.

        Returns:
            Счётчик последовательных провалов.
        """
        return self._consecutive_failures

    @property
    def total_failures(self) -> int:
        """Общее количество провалов за время жизни монитора.

        Returns:
            Суммарное число провалов.
        """
        return self._total_failures

    @property
    def total_successes(self) -> int:
        """Общее количество успехов за время жизни монитора.

        Returns:
            Суммарное число успешных загрузок.
        """
        return self._total_successes

    async def report_failure(self, object_id: str = "") -> bool:
        """Фиксирует полный провал загрузки карточки.

        Вызывается когда все retry внутри page_loader исчерпаны и
        goto_with_retry вернул False.

        Args:
            object_id: ID объявления (для логов).

        Returns:
            True если порог достигнут и требуется перезапуск.
        """
        async with self._lock:
            self._consecutive_failures += 1
            self._total_failures += 1
            current = self._consecutive_failures

            logger.debug(
                "сбой_зафиксирован",
                step=f"id={object_id}, подряд={current}/{CONSECUTIVE_FAILURES_THRESHOLD}",
            )

            if current >= CONSECUTIVE_FAILURES_THRESHOLD and not self._restart_event.is_set():
                self._restart_event.set()
                logger.warning(
                    "порог_сбоев_достигнут_перезапуск_требуется",
                    step=f"подряд={current}, порог={CONSECUTIVE_FAILURES_THRESHOLD}",
                    total=f"всего_сбоев={self._total_failures}, "
                          f"всего_успехов={self._total_successes}",
                )
                return True

            return False

    async def report_success(self, object_id: str = "") -> None:
        """Фиксирует успешную загрузку карточки.

        Сбрасывает счётчик последовательных сбоев. Вызывается
        когда goto_with_retry вернул True.

        Args:
            object_id: ID объявления (для логов).
        """
        async with self._lock:
            if self._consecutive_failures > 0:
                logger.debug(
                    "счётчик_сбоев_сброшен",
                    step=f"id={object_id}, было_подряд={self._consecutive_failures}",
                )
            self._consecutive_failures = 0
            self._total_successes += 1

    async def reset(self) -> None:
        """Сбрасывает монитор после перезапуска браузера.

        Очищает счётчик последовательных сбоев и снимает сигнал
        restart_needed. Вызывается после успешного перезапуска
        браузера с новой прокси.
        """
        async with self._lock:
            old_failures = self._consecutive_failures
            self._consecutive_failures = 0
            self._restart_event.clear()

            logger.info(
                "монитор_сброшен",
                step=f"было_подряд={old_failures}",
                total=f"всего_сбоев={self._total_failures}, "
                      f"всего_успехов={self._total_successes}",
            )

    async def wait_for_restart_signal(self) -> None:
        """Ожидает сигнала о необходимости перезапуска.

        Используется вкладками для блокировки до момента, когда
        монитор просигнализирует о проблеме. Неблокирующая проверка
        доступна через свойство restart_needed.
        """
        await self._restart_event.wait()

    def should_skip(self) -> bool:
        """Быстрая неблокирующая проверка — нужно ли пропустить обработку.

        Вкладки вызывают этот метод перед началом обработки очередной
        карточки. Если restart_needed=True — вкладка не должна начинать
        новую загрузку.

        Returns:
            True если требуется перезапуск и обработку нужно пропустить.
        """
        return self._restart_event.is_set()
