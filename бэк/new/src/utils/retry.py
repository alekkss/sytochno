"""Retry-декоратор для повторных попыток при временных сбоях."""

import asyncio
import functools
import time
from collections.abc import Callable
from typing import Any, TypeVar

from src.config.logger import get_logger

logger = get_logger("retry")

F = TypeVar("F", bound=Callable[..., Any])


def retry(
    max_attempts: int = 3,
    delay_seconds: float = 5.0,
    exceptions: tuple[type[BaseException], ...] = (Exception,),
    backoff_factor: float = 2.0,
) -> Callable[[F], F]:
    """Декоратор повторных попыток с экспоненциальной задержкой.

    Поддерживает как синхронные, так и асинхронные функции.

    Args:
        max_attempts: Максимальное количество попыток (включая первую).
        delay_seconds: Начальная задержка между попытками в секундах.
        exceptions: Кортеж исключений, при которых выполняется повтор.
        backoff_factor: Множитель задержки после каждой неудачной попытки.

    Returns:
        Декорированная функция с логикой повторных попыток.

    Example:
        @retry(max_attempts=3, delay_seconds=2.0)
        async def fetch_page(url: str) -> str:
            ...
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            """Обёртка для асинхронных функций."""
            current_delay = delay_seconds

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            "все_попытки_исчерпаны",
                            error=str(e),
                            error_type=type(e).__name__,
                            step=f"{attempt}/{max_attempts}",
                        )
                        raise
                    logger.warning(
                        "повторная_попытка",
                        error=str(e),
                        error_type=type(e).__name__,
                        step=f"{attempt}/{max_attempts}",
                    )
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff_factor

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            """Обёртка для синхронных функций."""
            current_delay = delay_seconds

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        logger.error(
                            "все_попытки_исчерпаны",
                            error=str(e),
                            error_type=type(e).__name__,
                            step=f"{attempt}/{max_attempts}",
                        )
                        raise
                    logger.warning(
                        "повторная_попытка",
                        error=str(e),
                        error_type=type(e).__name__,
                        step=f"{attempt}/{max_attempts}",
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff_factor

        if asyncio.iscoroutinefunction(func):
            return async_wrapper  # type: ignore[return-value]
        return sync_wrapper  # type: ignore[return-value]

    return decorator  # type: ignore[return-value]
