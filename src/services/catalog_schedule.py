"""Планировщик этапа 1 — запуск синхронизации каталога по расписанию.

Этап 1 (сбор каталога sutochno.ru) выполняется не в каждом прогоне,
а по слотам: например, в 01:00 и 19:00 МСК. Основной цикл перед каждым
прогоном спрашивает у планировщика is_due() и запускает каталог только
при положительном ответе.

Семантика закрытия слота:
- Слот считается закрытым, если метка последней синхронизации
  (last_synced_at) не раньше момента наступления слота.
- Если этап 1 завершился с ошибкой — метка НЕ пишется, и следующий
  прогон (через 2 минуты) повторит попытку.
- Если сервис был остановлен и пропустил слот — при первом прогоне
  после старта метка окажется меньше прошедшего слота, и синхронизация
  выполнится (добор пропущенного слота).
"""

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config.logger import get_logger

logger = get_logger("catalog_schedule")

# Имя файла состояния (метка последней синхронизации) в папке data/
CATALOG_SYNC_STATE_FILENAME: str = "last_catalog_sync.json"

# Часовой пояс Москвы (UTC+3) — слоты расписания интерпретируются по МСК
_MSK_TZ: timezone = timezone(timedelta(hours=3))

# Сколько суток назад искать незакрытые слоты. Окна «вчера + сегодня»
# достаточно для любой конфигурации слотов (минимум один слот в сутки).
_LOOKBACK_DAYS: int = 1


class CatalogSchedule:
    """Планировщик синхронизации каталога по слотам времени (МСК).

    Отвечает на вопрос «нужно ли запускать этап 1 сейчас» и хранит
    метку последней успешной синхронизации в JSON-файле состояния.
    """

    def __init__(
        self,
        slots: list[tuple[int, int]],
        state_file: Path,
    ) -> None:
        """Инициализирует планировщик.

        Args:
            slots: Список слотов (часы, минуты) по МСК. Например,
                [(1, 0), (19, 0)] для 01:00 и 19:00.
            state_file: Путь к JSON-файлу состояния
                (обычно data/last_catalog_sync.json).

        Raises:
            ValueError: Если слоты не заданы или содержат некорректное
                время.
        """
        if not slots:
            raise ValueError(
                "Слоты синхронизации каталога не заданы: планировщику "
                "нужен хотя бы один слот HH:MM."
            )

        for hours, minutes in slots:
            if not (0 <= hours <= 23):
                raise ValueError(
                    f"Часы слота должны быть от 0 до 23, получено: {hours}."
                )
            if not (0 <= minutes <= 59):
                raise ValueError(
                    f"Минуты слота должны быть от 0 до 59, получено: {minutes}."
                )

        # Сортировка + дедупликация: стабильный порядок и отсутствие
        # повторяющихся слотов независимо от порядка в настройках
        self._slots: list[tuple[int, int]] = sorted(set(slots))
        self._state_file = state_file

    def describe_slots(self) -> str:
        """Формирует человекочитаемое описание слотов для логов.

        Returns:
            Строка вида «01:00, 19:00 МСК».
        """
        parts = [f"{h:02d}:{m:02d}" for h, m in self._slots]
        return ", ".join(parts) + " МСК"

    def is_due(self, now: datetime) -> bool:
        """Проверяет, нужно ли запускать синхронизацию каталога сейчас.

        Сравнивает последний прошедший слот с меткой последней
        синхронизации: если слот наступил, а метка старше него —
        пора синхронизироваться.

        Args:
            now: Текущее время (aware datetime, любой часовой пояс).

        Returns:
            True если есть незакрытый прошедший слот.

        Raises:
            ValueError: Если передано время без tzinfo (naive datetime).
        """
        now_msk = self._to_msk(now)
        pending = self.pending_slot(now_msk)
        return pending is not None

    def pending_slot(self, now: datetime) -> datetime | None:
        """Возвращает ближайший незакрытый прошедший слот.

        Args:
            now: Текущее время (aware datetime, любой часовой пояс).

        Returns:
            Момент слота (МСК), который наступил, но ещё не закрыт
            меткой синхронизации. None — если все прошедшие слоты
            закрыты или слоты ещё не наступали.

        Raises:
            ValueError: Если передано время без tzinfo (naive datetime).
        """
        now_msk = self._to_msk(now)

        latest_passed = self._latest_passed_slot(now_msk)
        if latest_passed is None:
            return None

        last_sync = self._load_last_sync()
        if last_sync is None:
            # Метки нет вовсе (первый запуск или файл удалён) —
            # любой прошедший слот считается незакрытым
            return latest_passed

        last_sync_msk = last_sync.astimezone(_MSK_TZ)
        if last_sync_msk < latest_passed:
            return latest_passed

        return None

    def mark_synced(self, now: datetime) -> None:
        """Записывает метку успешной синхронизации.

        Вызывается ТОЛЬКО после успешного завершения этапа 1
        (каталог собран, пул синхронизирован). При ошибке этапа 1
        метка не пишется — следующий прогон повторит попытку.

        Args:
            now: Момент завершения синхронизации (aware datetime).

        Raises:
            ValueError: Если передано время без tzinfo (naive datetime).
        """
        now_utc = self._to_utc(now)

        data = {
            "last_synced_at": now_utc.isoformat(),
            "slots": self.describe_slots(),
        }

        try:
            self._state_file.parent.mkdir(parents=True, exist_ok=True)
            self._state_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except OSError as e:
            # Ошибка записи метки не критична, но опасна: следующий прогон
            # повторно запустит каталог. Логируем явно.
            logger.warning(
                "ошибка_записи_метки_синхронизации",
                path=str(self._state_file),
                error=str(e)[:200],
                error_type=type(e).__name__,
            )

    def _latest_passed_slot(self, now_msk: datetime) -> datetime | None:
        """Находит самый поздний слот, момент которого уже наступил.

        Просматривает окно _LOOKBACK_DAYS назад от текущего дня.

        Args:
            now_msk: Текущее время в МСК.

        Returns:
            Момент слота или None, если в окне нет прошедших слотов.
        """
        candidates: list[datetime] = []

        for day_offset in range(_LOOKBACK_DAYS + 1):
            day = (now_msk - timedelta(days=day_offset)).date()
            for hours, minutes in self._slots:
                slot_dt = datetime(
                    day.year, day.month, day.day,
                    hours, minutes,
                    tzinfo=_MSK_TZ,
                )
                if slot_dt <= now_msk:
                    candidates.append(slot_dt)

        return max(candidates) if candidates else None

    def _load_last_sync(self) -> datetime | None:
        """Читает метку последней синхронизации из файла состояния.

        Отсутствие файла — нормальная ситуация (первый запуск),
        возвращает None. Повреждённый файл также даёт None с warning:
        безопаснее лишняя синхронизация, чем пропущенная.

        Returns:
            Момент последней синхронизации (UTC, aware) или None.
        """
        if not self._state_file.exists():
            return None

        try:
            data = json.loads(
                self._state_file.read_text(encoding="utf-8")
            )
            raw = data.get("last_synced_at", "")
            if not raw:
                return None
            parsed = datetime.fromisoformat(raw)
            if parsed.tzinfo is None:
                # Повреждённая метка без зоны — трактуем как UTC
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except (OSError, ValueError, json.JSONDecodeError) as e:
            logger.warning(
                "файл_состояния_синхронизации_повреждён",
                path=str(self._state_file),
                error=str(e)[:200],
                error_type=type(e).__name__,
                step="метка_считается_отсутствующей",
            )
            return None

    @staticmethod
    def _to_msk(now: datetime) -> datetime:
        """Конвертирует время в МСК.

        Args:
            now: Время с tzinfo.

        Returns:
            То же время в МСК.

        Raises:
            ValueError: Если передан naive datetime.
        """
        if now.tzinfo is None:
            raise ValueError(
                "Ожидался datetime с tzinfo (aware). "
                "Передача naive datetime запрещена: невозможно однозначно "
                "интерпретировать слоты МСК."
            )
        return now.astimezone(_MSK_TZ)

    @staticmethod
    def _to_utc(now: datetime) -> datetime:
        """Конвертирует время в UTC.

        Args:
            now: Время с tzinfo.

        Returns:
            То же время в UTC.

        Raises:
            ValueError: Если передан naive datetime.
        """
        if now.tzinfo is None:
            raise ValueError(
                "Ожидался datetime с tzinfo (aware). "
                "Передача naive datetime запрещена."
            )
        return now.astimezone(timezone.utc)
