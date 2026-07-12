"""Сервис мониторинга оперативной памяти — контроль потребления RAM.

Обеспечивает:
- Статический расчёт безопасного количества воркеров перед стартом.
- Динамическую проверку текущего потребления во время работы.
- Сигнализацию при приближении к порогу памяти.

Работает без сторонних зависимостей (psutil не требуется):
- Linux: читает /proc/meminfo (точные данные).
- macOS/Windows: использует resource или shutil как fallback.
"""

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path

from src.config.logger import get_logger

logger = get_logger("memory_monitor")

# Оценка потребления памяти одним воркером (МБ):
# Chromium (~500 МБ) + вкладки (~100 МБ × max_tabs) + Python-overhead (~50 МБ).
# При max_tabs=5: 500 + 500 + 50 = ~1050 МБ ≈ 1000 МБ с запасом.
_BASE_BROWSER_MB: int = 550
_PER_TAB_MB: int = 100
_PYTHON_OVERHEAD_MB: int = 50

# Резерв RAM для ОС и прочих процессов (МБ).
# Оставляем 1.5 ГБ — достаточно для Linux-сервера без GUI.
_OS_RESERVE_MB: int = 1536

# Интервал проверки памяти в динамическом мониторе (секунды)
_CHECK_INTERVAL_SECONDS: float = 10.0

# Путь к meminfo на Linux
_PROC_MEMINFO_PATH: str = "/proc/meminfo"


@dataclass
class MemoryInfo:
    """Информация о текущем состоянии оперативной памяти.

    Все значения в мегабайтах (МБ).
    """

    total_mb: int
    available_mb: int
    used_mb: int

    @property
    def usage_percent(self) -> float:
        """Процент использованной памяти.

        Returns:
            Значение от 0.0 до 100.0.
        """
        if self.total_mb <= 0:
            return 0.0
        return (self.used_mb / self.total_mb) * 100.0


class MemoryMonitor:
    """Сервис мониторинга оперативной памяти.

    Отвечает за:
    - Получение текущей информации о RAM.
    - Расчёт безопасного количества воркеров.
    - Динамический мониторинг с сигнализацией при превышении порога.

    Single Responsibility: только мониторинг памяти, не управляет воркерами.
    Dependency Inversion: внешний код решает, что делать с сигналом.
    """

    def __init__(self, memory_limit_mb: int, max_tabs: int) -> None:
        """Инициализирует монитор.

        Args:
            memory_limit_mb: Порог потребления RAM в МБ. При приближении
                к этому значению монитор сигнализирует о необходимости
                остановки воркера. 0 = мониторинг отключён.
            max_tabs: Количество вкладок на воркер (для расчёта потребления).
        """
        self._memory_limit_mb = memory_limit_mb
        self._max_tabs = max_tabs
        self._should_reduce = asyncio.Event()
        self._monitoring_task: asyncio.Task | None = None  # type: ignore[type-arg]
        self._is_running = False

    @property
    def is_enabled(self) -> bool:
        """Проверяет, включён ли мониторинг.

        Returns:
            True если memory_limit_mb > 0.
        """
        return self._memory_limit_mb > 0

    @property
    def should_reduce_workers(self) -> bool:
        """Проверяет, нужно ли сократить количество воркеров.

        Returns:
            True если потребление RAM приближается к порогу.
        """
        return self._should_reduce.is_set()

    def estimate_worker_mb(self) -> int:
        """Оценивает потребление памяти одним воркером в МБ.

        Расчёт: базовый Chromium + (вкладки × потребление на вкладку) + Python.

        Returns:
            Оценка в мегабайтах.
        """
        return _BASE_BROWSER_MB + (_PER_TAB_MB * self._max_tabs) + _PYTHON_OVERHEAD_MB

    def calculate_safe_workers(self, requested_workers: int) -> int:
        """Рассчитывает безопасное количество воркеров по доступной RAM.

        Формула:
        safe_workers = (available_mb - os_reserve) / worker_mb

        Если расчётное количество меньше запрошенного — возвращает
        уменьшенное значение с предупреждением в логах.
        Минимум — 1 воркер (если хотя бы на него хватает памяти).

        Args:
            requested_workers: Запрошенное количество воркеров.

        Returns:
            Безопасное количество воркеров (≥ 1, ≤ requested_workers).
        """
        if not self.is_enabled:
            return requested_workers

        mem_info = self.get_memory_info()
        if mem_info is None:
            logger.warning(
                "не_удалось_определить_ram_лимит_не_применён",
                step=f"запрошено_воркеров={requested_workers}",
            )
            return requested_workers

        worker_mb = self.estimate_worker_mb()

        # Сколько памяти доступно для воркеров
        # (от лимита отнимаем текущее использование и резерв ОС)
        budget_mb = self._memory_limit_mb - mem_info.used_mb - _OS_RESERVE_MB

        if budget_mb <= 0:
            logger.warning(
                "недостаточно_ram_для_воркеров",
                step=f"лимит={self._memory_limit_mb}МБ, "
                     f"используется={mem_info.used_mb}МБ, "
                     f"резерв_ос={_OS_RESERVE_MB}МБ, "
                     f"бюджет={budget_mb}МБ",
            )
            return 1

        safe_count = budget_mb // worker_mb

        # Минимум 1, максимум — запрошенное количество
        safe_count = max(1, min(safe_count, requested_workers))

        if safe_count < requested_workers:
            logger.warning(
                "количество_воркеров_ограничено_по_ram",
                step=f"запрошено={requested_workers}, безопасно={safe_count}, "
                     f"бюджет={budget_mb}МБ, на_воркер={worker_mb}МБ, "
                     f"ram_всего={mem_info.total_mb}МБ, "
                     f"ram_используется={mem_info.used_mb}МБ, "
                     f"ram_доступно={mem_info.available_mb}МБ",
            )
        else:
            logger.info(
                "ram_достаточно_для_всех_воркеров",
                step=f"воркеров={safe_count}, бюджет={budget_mb}МБ, "
                     f"на_воркер={worker_mb}МБ, "
                     f"ram_всего={mem_info.total_mb}МБ, "
                     f"ram_доступно={mem_info.available_mb}МБ",
            )

        return int(safe_count)

    async def start_monitoring(self) -> None:
        """Запускает фоновый мониторинг потребления RAM.

        Периодически проверяет текущее потребление. Если приближается
        к порогу (memory_limit_mb) — устанавливает флаг should_reduce_workers.

        Мониторинг работает как asyncio.Task и не блокирует основной цикл.
        """
        if not self.is_enabled:
            logger.debug("мониторинг_ram_отключён_лимит_не_задан")
            return

        if self._is_running:
            return

        self._is_running = True
        self._should_reduce.clear()
        self._monitoring_task = asyncio.create_task(self._monitor_loop())

        logger.info(
            "мониторинг_ram_запущен",
            step=f"лимит={self._memory_limit_mb}МБ, "
                 f"интервал={_CHECK_INTERVAL_SECONDS}с",
        )

    async def stop_monitoring(self) -> None:
        """Останавливает фоновый мониторинг."""
        self._is_running = False

        if self._monitoring_task is not None:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None

        logger.debug("мониторинг_ram_остановлен")

    async def check_memory_now(self) -> bool:
        """Выполняет одноразовую проверку памяти прямо сейчас.

        Returns:
            True если потребление RAM превышает порог и нужно сокращать воркеры.
        """
        if not self.is_enabled:
            return False

        mem_info = self.get_memory_info()
        if mem_info is None:
            return False

        is_over_limit = mem_info.used_mb >= self._memory_limit_mb

        if is_over_limit:
            logger.warning(
                "ram_превышает_порог",
                step=f"используется={mem_info.used_mb}МБ, "
                     f"лимит={self._memory_limit_mb}МБ, "
                     f"всего={mem_info.total_mb}МБ",
            )
            self._should_reduce.set()

        return is_over_limit

    async def _monitor_loop(self) -> None:
        """Фоновый цикл мониторинга RAM.

        Проверяет потребление каждые _CHECK_INTERVAL_SECONDS.
        При превышении порога устанавливает флаг should_reduce_workers.
        Если потребление вернулось в норму — сбрасывает флаг.
        """
        while self._is_running:
            try:
                await asyncio.sleep(_CHECK_INTERVAL_SECONDS)

                mem_info = self.get_memory_info()
                if mem_info is None:
                    continue

                if mem_info.used_mb >= self._memory_limit_mb:
                    if not self._should_reduce.is_set():
                        logger.warning(
                            "ram_порог_достигнут_сокращение_воркеров",
                            step=f"используется={mem_info.used_mb}МБ, "
                                 f"лимит={self._memory_limit_mb}МБ, "
                                 f"доступно={mem_info.available_mb}МБ",
                        )
                        self._should_reduce.set()
                else:
                    if self._should_reduce.is_set():
                        logger.info(
                            "ram_вернулась_в_норму",
                            step=f"используется={mem_info.used_mb}МБ, "
                                 f"лимит={self._memory_limit_mb}МБ",
                        )
                        self._should_reduce.clear()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(
                    "ошибка_мониторинга_ram",
                    error=str(e),
                    error_type=type(e).__name__,
                )

    @staticmethod
    def get_memory_info() -> MemoryInfo | None:
        """Получает текущую информацию о RAM.

        Linux: читает /proc/meminfo (точные данные о MemTotal, MemAvailable).
        Другие ОС: возвращает None с предупреждением.

        Returns:
            MemoryInfo с данными в МБ или None если не удалось определить.
        """
        proc_meminfo = Path(_PROC_MEMINFO_PATH)

        if proc_meminfo.exists():
            return MemoryMonitor._read_proc_meminfo(proc_meminfo)

        logger.debug(
            "proc_meminfo_не_найден_мониторинг_ram_недоступен",
            step="поддерживается_только_linux",
        )
        return None

    @staticmethod
    def _read_proc_meminfo(path: Path) -> MemoryInfo | None:
        """Читает /proc/meminfo и возвращает информацию о RAM.

        Парсит строки вида:
            MemTotal:       20396944 kB
            MemFree:         1234567 kB
            MemAvailable:   12345678 kB

        Args:
            path: Путь к /proc/meminfo.

        Returns:
            MemoryInfo или None при ошибке парсинга.
        """
        try:
            content = path.read_text(encoding="utf-8")
            values: dict[str, int] = {}

            for line in content.splitlines():
                match = re.match(r"^(\w+):\s+(\d+)\s+kB", line)
                if match:
                    key = match.group(1)
                    # Переводим из кБ в МБ
                    value_mb = int(match.group(2)) // 1024
                    values[key] = value_mb

            total = values.get("MemTotal", 0)
            available = values.get("MemAvailable", 0)

            # MemAvailable может отсутствовать на старых ядрах (< 3.14).
            # В этом случае приближаем: available ≈ MemFree + Buffers + Cached.
            if available == 0:
                mem_free = values.get("MemFree", 0)
                buffers = values.get("Buffers", 0)
                cached = values.get("Cached", 0)
                available = mem_free + buffers + cached

            used = total - available

            if total <= 0:
                logger.warning(
                    "некорректные_данные_meminfo",
                    step=f"total={total}, available={available}",
                )
                return None

            return MemoryInfo(
                total_mb=total,
                available_mb=available,
                used_mb=used,
            )

        except Exception as e:
            logger.debug(
                "ошибка_чтения_proc_meminfo",
                error=str(e),
                error_type=type(e).__name__,
            )
            return None
