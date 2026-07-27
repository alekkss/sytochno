"""Модуль конфигурации — загрузка и валидация переменных окружения."""

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    """Загружает переменные окружения из файла .env в корне проекта."""
    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(dotenv_path=env_path)


def _get_required(key: str) -> str:
    """Получает обязательную переменную окружения или выбрасывает исключение.

    Args:
        key: Имя переменной окружения.

    Returns:
        Значение переменной.

    Raises:
        RuntimeError: Если переменная не задана или пуста.
    """
    value = os.getenv(key)
    if not value or not value.strip():
        raise RuntimeError(
            f"Обязательная переменная окружения не задана: {key}. "
            f"Проверьте файл .env (см. .env.example)."
        )
    return value.strip()


def _get_bool(key: str, default: str = "false") -> bool:
    """Получает булеву переменную окружения.

    Args:
        key: Имя переменной окружения.
        default: Значение по умолчанию ("true" или "false").

    Returns:
        True если значение "true", "1" или "yes" (без учёта регистра).
    """
    value = os.getenv(key, default).strip().lower()
    return value in ("true", "1", "yes")


def _get_int(key: str, default: str) -> int:
    """Получает целочисленную переменную окружения.

    Args:
        key: Имя переменной окружения.
        default: Значение по умолчанию (строка).

    Returns:
        Целочисленное значение.

    Raises:
        RuntimeError: Если значение не может быть преобразовано в int.
    """
    value = os.getenv(key, default).strip()
    try:
        return int(value)
    except ValueError:
        raise RuntimeError(
            f"Переменная окружения {key} должна быть целым числом, получено: '{value}'."
        )


def _parse_time_hhmm(key: str) -> tuple[int, int] | None:
    """Парсит переменную окружения с временем в формате HH:MM.

    Время интерпретируется как московское (UTC+3).
    Пустое значение — пауза отключена.

    Args:
        key: Имя переменной окружения.

    Returns:
        Кортеж (часы, минуты) или None если переменная не задана.

    Raises:
        RuntimeError: Если формат невалиден.
    """
    value = os.getenv(key, "").strip()
    if not value:
        return None

    parts = value.split(":")
    if len(parts) != 2:
        raise RuntimeError(
            f"Переменная {key} должна быть в формате HH:MM, получено: '{value}'."
        )

    try:
        hours = int(parts[0])
        minutes = int(parts[1])
    except ValueError:
        raise RuntimeError(
            f"Переменная {key} должна быть в формате HH:MM, получено: '{value}'."
        )

    if not (0 <= hours <= 23):
        raise RuntimeError(
            f"Часы в {key} должны быть от 0 до 23, получено: {hours}."
        )
    if not (0 <= minutes <= 59):
        raise RuntimeError(
            f"Минуты в {key} должны быть от 0 до 59, получено: {minutes}."
        )

    return (hours, minutes)


def _load_search_urls() -> list[str]:
    """Загружает список URL поиска из переменных окружения.

    Читает SUTOCHNO_SEARCH_URL_1 ... SUTOCHNO_SEARCH_URL_8,
    фильтрует пустые значения. Хотя бы одна должна быть заполнена.

    Returns:
        Список непустых URL поиска (от 1 до 8 штук).

    Raises:
        RuntimeError: Если ни одна ссылка не задана.
    """
    urls: list[str] = []

    for i in range(1, 9):
        key = f"SUTOCHNO_SEARCH_URL_{i}"
        value = os.getenv(key, "").strip()
        if value:
            urls.append(value)

    if not urls:
        raise RuntimeError(
            "Ни одна ссылка поиска не задана. "
            "Заполните хотя бы SUTOCHNO_SEARCH_URL_1 в файле .env (см. .env.example)."
        )

    return urls


@dataclass(frozen=True)
class Settings:
    """Неизменяемые настройки приложения.

    Загружаются один раз при создании экземпляра.
    frozen=True гарантирует, что настройки не будут случайно изменены.
    """

    # URL поиска (от 1 до 8 ссылок)
    search_urls: tuple[str, ...] = field(default_factory=tuple)

    # Браузер
    headless_mode: bool = False
    navigation_timeout: int = 60000
    min_delay_ms: int = 2000
    max_delay_ms: int = 5000

    # Парсинг
    max_pages: int = 5

    # Параллельные вкладки
    max_tabs: int = 5
    tab_delay_ms: int = 3000

    # Хранилище
    db_path: str = "data/sutochno_listings.db"
    export_path: str = "data/sutochno_report.xlsx"

    # Логирование
    log_level: str = "INFO"
    log_file_path: str = "logs/app.log"

    # Прокси
    use_proxy: bool = False
    proxies_path: str = "data/proxies.txt"
    max_proxy_workers: int = 5

    # Мониторинг памяти
    memory_limit_mb: int = 0

    # Повторное обогащение — чёрный список
    blacklist_threshold: int = 2

    # Повторное обогащение — досрочное завершение цикла
    retry_min_cards_threshold: int = 300

    # Адаптивный контроль параллелизма (AIMD)
    concurrency_min: int = 5
    concurrency_max: int = 0
    concurrency_start: int = 0

    # Таймаут обработки одной карточки (секунды)
    enrich_timeout_seconds: int = 240

    # Ночная пауза (время по Москве, формат HH:MM)
    # Парсер не начинает новый прогон, если текущее время МСК
    # попадает в окно pause_start–pause_end.
    # None = пауза отключена.
    pause_start: tuple[int, int] | None = None
    pause_end: tuple[int, int] | None = None

    @classmethod
    def load(cls) -> "Settings":
        """Фабричный метод — загружает настройки из переменных окружения.

        Выполняет валидацию обязательных переменных и преобразование типов.

        Returns:
            Экземпляр Settings с загруженными настройками.

        Raises:
            RuntimeError: Если обязательные переменные не заданы или невалидны.
        """
        _load_env()

        search_urls = _load_search_urls()

        # Парсинг времени паузы
        pause_start = _parse_time_hhmm("PAUSE_START")
        pause_end = _parse_time_hhmm("PAUSE_END")

        settings = cls(
            search_urls=tuple(search_urls),
            headless_mode=_get_bool("HEADLESS_MODE", "false"),
            navigation_timeout=_get_int("NAVIGATION_TIMEOUT", "60000"),
            min_delay_ms=_get_int("MIN_DELAY_MS", "2000"),
            max_delay_ms=_get_int("MAX_DELAY_MS", "5000"),
            max_pages=_get_int("MAX_PAGES", "5"),
            max_tabs=_get_int("MAX_TABS", "5"),
            tab_delay_ms=_get_int("TAB_DELAY_MS", "3000"),
            db_path=os.getenv("DB_PATH", "data/sutochno_listings.db").strip(),
            export_path=os.getenv("EXPORT_PATH", "data/sutochno_report.xlsx").strip(),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            log_file_path=os.getenv("LOG_FILE_PATH", "logs/app.log").strip(),
            use_proxy=_get_bool("USE_PROXY", "false"),
            proxies_path=os.getenv("PROXIES_PATH", "data/proxies.txt").strip(),
            max_proxy_workers=_get_int("MAX_PROXY_WORKERS", "5"),
            memory_limit_mb=_get_int("MEMORY_LIMIT_MB", "0"),
            blacklist_threshold=_get_int("BLACKLIST_THRESHOLD", "2"),
            retry_min_cards_threshold=_get_int("RETRY_MIN_CARDS_THRESHOLD", "300"),
            concurrency_min=_get_int("CONCURRENCY_MIN", "5"),
            concurrency_max=_get_int("CONCURRENCY_MAX", "0"),
            concurrency_start=_get_int("CONCURRENCY_START", "0"),
            enrich_timeout_seconds=_get_int("ENRICH_TIMEOUT_SECONDS", "240"),
            pause_start=pause_start,
            pause_end=pause_end,
        )

        # Валидация диапазонов
        if settings.min_delay_ms < 0:
            raise RuntimeError("MIN_DELAY_MS не может быть отрицательным.")
        if settings.max_delay_ms < settings.min_delay_ms:
            raise RuntimeError("MAX_DELAY_MS не может быть меньше MIN_DELAY_MS.")
        if settings.max_pages < 0:
            raise RuntimeError("MAX_PAGES не может быть отрицательным.")
        if settings.log_level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise RuntimeError(
                f"LOG_LEVEL должен быть одним из: DEBUG, INFO, WARNING, ERROR, CRITICAL. "
                f"Получено: '{settings.log_level}'."
            )

        # Валидация параллельных вкладок
        if settings.max_tabs < 1:
            raise RuntimeError("MAX_TABS должен быть не менее 1.")
        if settings.max_tabs > 20:
            raise RuntimeError(
                "MAX_TABS не может быть больше 20. "
                "Рекомендуется 3–5 для стабильной работы."
            )
        if settings.tab_delay_ms < 0:
            raise RuntimeError("TAB_DELAY_MS не может быть отрицательным.")

        # Валидация прокси
        if settings.use_proxy:
            proxies_file = Path(settings.proxies_path)
            if not proxies_file.exists():
                raise RuntimeError(
                    f"USE_PROXY=true, но файл прокси не найден: {settings.proxies_path}. "
                    f"Создайте файл или укажите корректный путь в PROXIES_PATH."
                )

        if settings.max_proxy_workers < 1:
            raise RuntimeError(
                "MAX_PROXY_WORKERS должен быть не менее 1."
            )

        # Валидация мониторинга памяти
        if settings.memory_limit_mb < 0:
            raise RuntimeError("MEMORY_LIMIT_MB не может быть отрицательным.")
        if 0 < settings.memory_limit_mb < 1024:
            raise RuntimeError(
                "MEMORY_LIMIT_MB должен быть не менее 1024 (1 ГБ) или 0 (отключён). "
                f"Получено: {settings.memory_limit_mb}."
            )

        # Валидация чёрного списка
        if settings.blacklist_threshold < 1:
            raise RuntimeError(
                "BLACKLIST_THRESHOLD должен быть не менее 1. "
                f"Получено: {settings.blacklist_threshold}."
            )

        # Валидация порога досрочного завершения повторного обогащения
        if settings.retry_min_cards_threshold < 0:
            raise RuntimeError(
                "RETRY_MIN_CARDS_THRESHOLD не может быть отрицательным. "
                f"Получено: {settings.retry_min_cards_threshold}."
            )

        # Валидация адаптивного контроля параллелизма
        if settings.concurrency_min < 1:
            raise RuntimeError(
                "CONCURRENCY_MIN должен быть не менее 1. "
                f"Получено: {settings.concurrency_min}."
            )
        if settings.concurrency_max < 0:
            raise RuntimeError(
                "CONCURRENCY_MAX не может быть отрицательным. "
                f"Получено: {settings.concurrency_max}."
            )
        if settings.concurrency_max > 0 and settings.concurrency_max < settings.concurrency_min:
            raise RuntimeError(
                "CONCURRENCY_MAX не может быть меньше CONCURRENCY_MIN. "
                f"CONCURRENCY_MIN={settings.concurrency_min}, "
                f"CONCURRENCY_MAX={settings.concurrency_max}."
            )
        if settings.concurrency_start < 0:
            raise RuntimeError(
                "CONCURRENCY_START не может быть отрицательным. "
                f"Получено: {settings.concurrency_start}."
            )
        if (
            settings.concurrency_start > 0
            and settings.concurrency_max > 0
            and settings.concurrency_start > settings.concurrency_max
        ):
            raise RuntimeError(
                "CONCURRENCY_START не может быть больше CONCURRENCY_MAX. "
                f"CONCURRENCY_START={settings.concurrency_start}, "
                f"CONCURRENCY_MAX={settings.concurrency_max}."
            )

        # Валидация таймаута обогащения
        if settings.enrich_timeout_seconds < 0:
            raise RuntimeError(
                "ENRICH_TIMEOUT_SECONDS не может быть отрицательным. "
                f"Получено: {settings.enrich_timeout_seconds}."
            )

        # Валидация паузы: обе должны быть заданы или обе пусты
        if (settings.pause_start is None) != (settings.pause_end is None):
            raise RuntimeError(
                "PAUSE_START и PAUSE_END должны быть заданы одновременно "
                "или обе оставлены пустыми. "
                "Формат: HH:MM (время по Москве)."
            )

        return settings
