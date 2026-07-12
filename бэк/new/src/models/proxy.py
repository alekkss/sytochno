"""Модель конфигурации прокси-сервера."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ProxyConfig:
    """Данные одного прокси-сервера.

    Attributes:
        host: IP-адрес прокси.
        port: Порт прокси.
        username: Логин для авторизации.
        password: Пароль для авторизации.
    """

    host: str
    port: int
    username: str
    password: str

    @property
    def server_url(self) -> str:
        """Формирует URL прокси-сервера для Playwright.

        Returns:
            Строка вида 'http://ip:port'.
        """
        return f"http://{self.host}:{self.port}"

    @classmethod
    def from_string(cls, line: str) -> "ProxyConfig":
        """Создаёт экземпляр из строки формата 'ip:port:login:password'.

        Args:
            line: Строка с данными прокси.

        Returns:
            Экземпляр ProxyConfig.

        Raises:
            ValueError: Если строка не соответствует формату.
        """
        line = line.strip()
        if not line:
            raise ValueError("Строка прокси не может быть пустой.")

        parts = line.split(":")
        if len(parts) != 4:
            raise ValueError(
                f"Неверный формат прокси: '{line}'. "
                f"Ожидается: ip:port:login:password."
            )

        host = parts[0].strip()
        port_str = parts[1].strip()
        username = parts[2].strip()
        password = parts[3].strip()

        if not host:
            raise ValueError(f"IP-адрес прокси пуст: '{line}'.")
        if not username:
            raise ValueError(f"Логин прокси пуст: '{line}'.")
        if not password:
            raise ValueError(f"Пароль прокси пуст: '{line}'.")

        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(
                f"Порт прокси должен быть числом, получено: '{port_str}' в строке '{line}'."
            )

        if port < 1 or port > 65535:
            raise ValueError(
                f"Порт прокси должен быть от 1 до 65535, получено: {port}."
            )

        return cls(host=host, port=port, username=username, password=password)

    def __str__(self) -> str:
        """Строковое представление (без пароля для безопасности).

        Returns:
            Строка вида 'ip:port:login:***'.
        """
        return f"{self.host}:{self.port}:{self.username}:***"
