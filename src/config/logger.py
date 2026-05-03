"""Модуль логирования — структурированные логи в двух форматах."""

import json
import logging
import sys
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any


# Глобальный trace_id для текущего запуска приложения
_TRACE_ID: str = uuid.uuid4().hex[:12]

# Белый список ключей контекста для вывода в консоль
_CONSOLE_CONTEXT_KEYS: set[str] = {
    "path",
    "file",
    "error",
    "error_type",
    "step",
    "current",
    "total",
    "directory",
}


def get_trace_id() -> str:
    """Возвращает trace_id текущего запуска."""
    return _TRACE_ID


class ConsoleFormatter(logging.Formatter):
    """Человекочитаемый однострочный формат для консоли.

    Формат: {timestamp} | {level} | {message} | {key=value, ...}
    Контекст фильтруется по белому списку ключей.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Форматирует запись лога для консольного вывода.

        Args:
            record: Запись лога.

        Returns:
            Отформатированная строка.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(5)
        message = record.getMessage()

        # Извлекаем контекст из extra
        context: dict[str, Any] = getattr(record, "context", {})
        filtered_context = {
            k: v for k, v in context.items() if k in _CONSOLE_CONTEXT_KEYS
        }

        parts = [timestamp, level, message]

        if filtered_context:
            context_str = ", ".join(f"{k}={v}" for k, v in filtered_context.items())
            parts.append(context_str)

        return " | ".join(parts)


class JsonFileFormatter(logging.Formatter):
    """Полный JSON-формат для файла логов.

    Включает все поля: timestamp, level, message, logger, trace_id, context, exception.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Форматирует запись лога в JSON для файлового вывода.

        Args:
            record: Запись лога.

        Returns:
            JSON-строка с полной информацией.
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "trace_id": _TRACE_ID,
        }

        # Контекст из extra
        context: dict[str, Any] = getattr(record, "context", {})
        if context:
            log_entry["context"] = context

        # Информация об исключении
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, ensure_ascii=False, default=str)


class ContextLogger:
    """Обёртка над стандартным логгером с поддержкой контекста.

    Позволяет передавать произвольные ключ-значение данные
    в каждое сообщение лога через параметр context.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Инициализирует обёртку.

        Args:
            logger: Стандартный логгер Python.
        """
        self._logger = logger

    def debug(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня DEBUG с контекстом."""
        self._log(logging.DEBUG, message, context)

    def info(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня INFO с контекстом."""
        self._log(logging.INFO, message, context)

    def warning(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня WARNING с контекстом."""
        self._log(logging.WARNING, message, context)

    def error(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня ERROR с контекстом."""
        self._log(logging.ERROR, message, context)

    def critical(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня CRITICAL с контекстом."""
        self._log(logging.CRITICAL, message, context)

    def exception(self, message: str, **context: Any) -> None:
        """Логирует сообщение уровня ERROR с информацией об исключении."""
        self._log(logging.ERROR, message, context, exc_info=True)

    def _log(
        self,
        level: int,
        message: str,
        context: dict[str, Any],
        exc_info: bool = False,
    ) -> None:
        """Внутренний метод логирования с передачей контекста через extra.

        Args:
            level: Уровень логирования.
            message: Текст сообщения.
            context: Словарь с контекстными данными.
            exc_info: Включать ли информацию об исключении.
        """
        self._logger.log(
            level,
            message,
            extra={"context": context},
            exc_info=exc_info,
        )


def _setup_logger(name: str, log_level: str, log_file_path: str) -> logging.Logger:
    """Настраивает логгер с двумя обработчиками (консоль + файл).

    Args:
        name: Имя логгера.
        log_level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file_path: Путь к файлу логов.

    Returns:
        Настроенный логгер.
    """
    logger = logging.getLogger(name)

    # Предотвращаем дублирование обработчиков при повторном вызове
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, log_level, logging.INFO))
    logger.propagate = False

    # Обработчик консоли — человекочитаемый формат
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ConsoleFormatter())
    logger.addHandler(console_handler)

    # Обработчик файла — JSON-формат с ротацией
    if log_file_path:
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=str(log_path),
            maxBytes=10 * 1024 * 1024,  # 10 МБ
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(JsonFileFormatter())
        logger.addHandler(file_handler)

    return logger


# Кэш настроенных логгеров
_loggers_cache: dict[str, ContextLogger] = {}

# Настройки по умолчанию (перезаписываются при вызове configure)
_log_level: str = "INFO"
_log_file_path: str = "logs/app.log"


def configure(log_level: str, log_file_path: str) -> None:
    """Конфигурирует параметры логирования для всего приложения.

    Должна вызываться один раз при старте приложения.
    Обновляет уровень логирования для всех уже созданных логгеров,
    а также добавляет файловый обработчик тем логгерам, которые были
    созданы до вызова configure() (при импорте модулей).

    Args:
        log_level: Уровень логирования.
        log_file_path: Путь к файлу логов.
    """
    global _log_level, _log_file_path  # noqa: PLW0603
    _log_level = log_level
    _log_file_path = log_file_path

    # Обновляем все уже существующие логгеры
    target_level = getattr(logging, log_level, logging.INFO)

    for name, context_logger in _loggers_cache.items():
        raw_logger = context_logger._logger

        # Обновляем уровень логирования
        raw_logger.setLevel(target_level)

        # Проверяем, есть ли уже файловый обработчик
        has_file_handler = any(
            isinstance(h, RotatingFileHandler) for h in raw_logger.handlers
        )

        # Добавляем файловый обработчик, если его нет и путь указан
        if not has_file_handler and log_file_path:
            log_path = Path(log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                filename=str(log_path),
                maxBytes=10 * 1024 * 1024,  # 10 МБ
                backupCount=5,
                encoding="utf-8",
            )
            file_handler.setFormatter(JsonFileFormatter())
            raw_logger.addHandler(file_handler)


def get_logger(name: str = "sutochno_parser") -> ContextLogger:
    """Получает или создаёт логгер с указанным именем.

    Args:
        name: Имя логгера (рекомендуется имя модуля: service, repository и т.д.).

    Returns:
        Экземпляр ContextLogger с настроенными обработчиками.
    """
    if name not in _loggers_cache:
        raw_logger = _setup_logger(name, _log_level, _log_file_path)
        _loggers_cache[name] = ContextLogger(raw_logger)
    return _loggers_cache[name]
