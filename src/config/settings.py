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


def _load_search_urls() -> list[str]:
    """Загружает список URL поиска из переменных окружения.

    Читает SUTOCHNO_SEARCH_URL_1 ... SUTOCHNO_SEARCH_URL_6,
    фильтрует пустые значения. Хотя бы одна должна быть заполнена.

    Returns:
        Список непустых URL поиска (от 1 до 6 штук).

    Raises:
        RuntimeError: Если ни одна ссылка не задана.
    """
    urls: list[str] = []

    for i in range(1, 7):
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

    # URL поиска (от 1 до 6 ссылок)
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

        return settings
