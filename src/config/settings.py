"""Модуль конфигурации — загрузка и валидация переменных окружения."""

import os
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Settings:
    """Неизменяемые настройки приложения.

    Загружаются один раз при создании экземпляра.
    frozen=True гарантирует, что настройки не будут случайно изменены.
    """

    # Обязательные
    sutochno_search_url: str

    # Браузер
    headless_mode: bool
    navigation_timeout: int
    min_delay_ms: int
    max_delay_ms: int

    # Парсинг
    max_pages: int

    # Хранилище
    db_path: str
    export_path: str

    # Логирование
    log_level: str
    log_file_path: str

    # Прокси
    use_proxy: bool
    proxies_path: str
    max_proxy_workers: int

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

        settings = cls(
            sutochno_search_url=_get_required("SUTOCHNO_SEARCH_URL"),
            headless_mode=_get_bool("HEADLESS_MODE", "false"),
            navigation_timeout=_get_int("NAVIGATION_TIMEOUT", "60000"),
            min_delay_ms=_get_int("MIN_DELAY_MS", "2000"),
            max_delay_ms=_get_int("MAX_DELAY_MS", "5000"),
            max_pages=_get_int("MAX_PAGES", "5"),
            db_path=os.getenv("DB_PATH", "data/sutochno_listings.db").strip(),
            export_path=os.getenv("EXPORT_PATH", "data/sutochno_report.xlsx").strip(),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
            log_file_path=os.getenv("LOG_FILE_PATH", "logs/app.log").strip(),
            use_proxy=_get_bool("USE_PROXY", "false"),
            proxies_path=os.getenv("PROXIES_PATH", "data/proxies.txt").strip(),
            max_proxy_workers=_get_int("MAX_PROXY_WORKERS", "5"),
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

        return settings
