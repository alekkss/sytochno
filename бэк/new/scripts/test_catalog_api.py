"""Тестовый скрипт — перехват API-запросов каталога sutochno.ru.

Открывает страницу поиска, перехватывает ВСЕ запросы к /api/,
сохраняет тела запросов и ответов в файлы.
Затем переходит на вторую страницу пагинации и повторяет перехват.

Цель: понять, какие эндпоинты использует фронтенд для загрузки
каталога объявлений, и можно ли получить данные напрямую через API
без парсинга DOM.

Запуск:
    python -m scripts.test_catalog_api
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from playwright.async_api import Request, Response, async_playwright


# ── Настройки ──────────────────────────────────────────────
# URL поиска — берём из .env или используем дефолтный
_DEFAULT_SEARCH_URL = (
    "https://sutochno.ru/front/searchapp/search"
    "?type=city&id=397367&term=Санкт-Петербург"
    "&price_per=1&guests_adults=2&price_min=0&price_max=2000"
)

# Папка для сохранения перехваченных данных
_OUTPUT_DIR = Path("data/api_debug")

# Фильтр URL — перехватываем только запросы к API
_API_URL_PATTERNS = [
    "/api/",
    "/json/",
    "searchapp",
]

# Таймаут ожидания загрузки (мс)
_NAV_TIMEOUT_MS = 60000

# Аргументы Chromium — stealth
_BROWSER_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-gpu",
]

_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {
        get: () => ['ru-RU', 'ru', 'en-US', 'en']
    });
    window.chrome = {runtime: {}};
"""

_CONTEXT_OPTIONS = {
    "viewport": {"width": 1920, "height": 1080},
    "locale": "ru-RU",
    "timezone_id": "Europe/Moscow",
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


class ApiInterceptor:
    """Перехватчик API-запросов.

    Собирает все запросы и ответы к API-эндпоинтам,
    сохраняет их в структурированном виде для анализа.
    """

    def __init__(self, output_dir: Path) -> None:
        """Инициализирует перехватчик.

        Args:
            output_dir: Папка для сохранения результатов.
        """
        self._output_dir = output_dir
        self._captured: list[dict] = []
        self._counter: int = 0

    def _matches_api_pattern(self, url: str) -> bool:
        """Проверяет, относится ли URL к API.

        Args:
            url: URL запроса.

        Returns:
            True если URL содержит один из паттернов API.
        """
        return any(pattern in url for pattern in _API_URL_PATTERNS)

    async def on_request(self, request: Request) -> None:
        """Обработчик исходящего запроса.

        Логирует метод, URL и тело POST-запросов.

        Args:
            request: Объект запроса Playwright.
        """
        url = request.url
        if not self._matches_api_pattern(url):
            return

        self._counter += 1
        entry: dict = {
            "index": self._counter,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "method": request.method,
            "url": url,
            "request_headers": dict(request.headers),
            "request_body": None,
            "response_status": None,
            "response_headers": None,
            "response_body": None,
        }

        # Тело POST-запроса
        post_data = request.post_data
        if post_data:
            try:
                entry["request_body"] = json.loads(post_data)
            except (json.JSONDecodeError, TypeError):
                entry["request_body"] = post_data

        self._captured.append(entry)

        # Вывод в консоль
        print(  # noqa: T201
            f"\n{'─' * 70}\n"
            f"[{self._counter}] ЗАПРОС → {request.method} {url}\n"
            f"{'─' * 70}"
        )
        if entry["request_body"]:
            body_str = json.dumps(
                entry["request_body"], ensure_ascii=False, indent=2
            )
            # Обрезаем слишком длинные тела
            if len(body_str) > 2000:
                body_str = body_str[:2000] + "\n... (обрезано)"
            print(f"  Тело запроса:\n{body_str}")  # noqa: T201

    async def on_response(self, response: Response) -> None:
        """Обработчик входящего ответа.

        Дополняет ранее перехваченный запрос данными ответа.

        Args:
            response: Объект ответа Playwright.
        """
        url = response.url
        if not self._matches_api_pattern(url):
            return

        # Находим соответствующую запись запроса (последний с таким URL)
        entry = None
        for item in reversed(self._captured):
            if item["url"] == url and item["response_status"] is None:
                entry = item
                break

        if entry is None:
            return

        entry["response_status"] = response.status
        entry["response_headers"] = dict(response.headers)

        # Читаем тело ответа
        try:
            body_bytes = await response.body()
            body_text = body_bytes.decode("utf-8", errors="replace")

            try:
                entry["response_body"] = json.loads(body_text)
            except (json.JSONDecodeError, TypeError):
                entry["response_body"] = body_text[:5000]
        except Exception as e:
            entry["response_body"] = f"<ошибка чтения: {e}>"

        # Вывод в консоль
        print(  # noqa: T201
            f"\n  ОТВЕТ ← {response.status} {url}"
        )

        resp_body = entry["response_body"]
        if isinstance(resp_body, dict):
            # Показываем ключи верхнего уровня и размер
            keys = list(resp_body.keys())
            print(f"  Ключи ответа: {keys}")  # noqa: T201

            # Если есть data.objects — показываем количество и первый объект
            data = resp_body.get("data", {})
            if isinstance(data, dict):
                objects = data.get("objects", data.get("items", []))
                if isinstance(objects, list) and objects:
                    print(  # noqa: T201
                        f"  Количество объектов: {len(objects)}"
                    )
                    # Показываем ключи первого объекта
                    first = objects[0]
                    if isinstance(first, dict):
                        first_keys = list(first.keys())
                        print(  # noqa: T201
                            f"  Ключи первого объекта: {first_keys}"
                        )
                        # Показываем первый объект целиком (до 3000 символов)
                        first_str = json.dumps(
                            first, ensure_ascii=False, indent=2
                        )
                        if len(first_str) > 3000:
                            first_str = first_str[:3000] + "\n... (обрезано)"
                        print(  # noqa: T201
                            f"  Первый объект:\n{first_str}"
                        )

                # Проверяем наличие пагинации в ответе
                for page_key in ("page", "pages", "total", "count",
                                 "total_count", "totalCount", "pagination",
                                 "pager", "offset", "limit"):
                    if page_key in data:
                        print(  # noqa: T201
                            f"  Пагинация [{page_key}]: {data[page_key]}"
                        )
                    if page_key in resp_body:
                        print(  # noqa: T201
                            f"  Пагинация (root)[{page_key}]: "
                            f"{resp_body[page_key]}"
                        )

    def save_results(self, phase: str) -> None:
        """Сохраняет все перехваченные данные в JSON-файл.

        Args:
            phase: Название фазы (например, 'page1', 'page2').
        """
        self._output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"intercepted_{phase}.json"
        filepath = self._output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(self._captured, f, ensure_ascii=False, indent=2)

        print(  # noqa: T201
            f"\n{'═' * 70}\n"
            f"Перехвачено запросов: {len(self._captured)}\n"
            f"Сохранено в: {filepath}\n"
            f"{'═' * 70}"
        )

    def save_summary(self) -> None:
        """Сохраняет краткую сводку всех перехваченных эндпоинтов."""
        self._output_dir.mkdir(parents=True, exist_ok=True)

        summary: list[dict] = []
        for entry in self._captured:
            item: dict = {
                "index": entry["index"],
                "method": entry["method"],
                "url": entry["url"],
                "status": entry["response_status"],
            }

            # Размер ответа
            resp = entry.get("response_body")
            if isinstance(resp, dict):
                data = resp.get("data", {})
                if isinstance(data, dict):
                    objects = data.get("objects", data.get("items", []))
                    if isinstance(objects, list):
                        item["objects_count"] = len(objects)

            # Тело запроса (краткое)
            req_body = entry.get("request_body")
            if isinstance(req_body, dict):
                item["request_params"] = list(req_body.keys())

            summary.append(item)

        filepath = self._output_dir / "summary.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # Выводим сводку в консоль
        print(  # noqa: T201
            f"\n{'═' * 70}\n"
            f"СВОДКА ВСЕХ ПЕРЕХВАЧЕННЫХ ЭНДПОИНТОВ\n"
            f"{'═' * 70}"
        )
        for item in summary:
            objects_info = ""
            if "objects_count" in item:
                objects_info = f" | объектов: {item['objects_count']}"
            params_info = ""
            if "request_params" in item:
                params_info = f" | params: {item['request_params']}"
            print(  # noqa: T201
                f"  [{item['index']}] {item['method']} "
                f"{item['url'][:100]} "
                f"→ {item['status']}"
                f"{objects_info}{params_info}"
            )

        print(f"\nСводка сохранена в: {filepath}")  # noqa: T201


async def run() -> None:
    """Основная логика скрипта.

    1. Открывает браузер со stealth-настройками.
    2. Подключает перехватчик ко всем запросам и ответам.
    3. Переходит на страницу поиска — ждёт полной загрузки.
    4. Сохраняет перехваченные данные (фаза: page1).
    5. Кликает «Далее» для перехода на вторую страницу.
    6. Сохраняет перехваченные данные (фаза: page1_and_page2).
    7. Сохраняет итоговую сводку.
    """
    # Определяем URL поиска
    search_url = _DEFAULT_SEARCH_URL

    # Проверяем .env — если есть, берём URL оттуда
    env_path = Path(".env")
    if env_path.exists():
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("SUTOCHNO_SEARCH_URL_1=") and "=" in line:
                    value = line.split("=", 1)[1].strip()
                    if value:
                        search_url = value
                        break

    print(  # noqa: T201
        f"{'═' * 70}\n"
        f"ТЕСТ: Перехват API-запросов каталога sutochno.ru\n"
        f"{'═' * 70}\n"
        f"URL поиска: {search_url}\n"
        f"Результаты: {_OUTPUT_DIR}/\n"
        f"{'═' * 70}\n"
    )

    interceptor = ApiInterceptor(output_dir=_OUTPUT_DIR)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            args=_BROWSER_ARGS,
            ignore_default_args=["--enable-automation"],
        )

        context = await browser.new_context(**_CONTEXT_OPTIONS)
        await context.add_init_script(_STEALTH_SCRIPT)

        page = await context.new_page()
        page.set_default_navigation_timeout(_NAV_TIMEOUT_MS)

        # Подключаем перехватчики
        page.on("request", interceptor.on_request)
        page.on("response", interceptor.on_response)

        # ── Фаза 1: Загрузка первой страницы каталога ──
        print(  # noqa: T201
            f"\n{'═' * 70}\n"
            f"ФАЗА 1: Загрузка первой страницы каталога\n"
            f"{'═' * 70}"
        )

        await page.goto(search_url, wait_until="networkidle")

        # Ждём карточки
        try:
            await page.wait_for_selector(
                ".card[data-observe-id]", timeout=30000
            )
            cards = await page.query_selector_all(".card[data-observe-id]")
            print(f"\nКарточек на странице: {len(cards)}")  # noqa: T201
        except Exception:
            print(  # noqa: T201
                "\nКарточки не найдены — возможно, сайт заблокировал запрос"
            )

        # Ждём ещё немного — подгрузка дополнительных данных
        await asyncio.sleep(5)

        # Сохраняем результаты фазы 1
        interceptor.save_results(phase="page1")

        # ── Фаза 2: Переход на вторую страницу ──
        print(  # noqa: T201
            f"\n{'═' * 70}\n"
            f"ФАЗА 2: Переход на вторую страницу (пагинация)\n"
            f"{'═' * 70}"
        )

        # Ищем и кликаем кнопку «Далее»
        clicked = await page.evaluate("""
            () => {
                const items = document.querySelectorAll('li.navigation');
                for (const item of items) {
                    const text = item.querySelector('.pagination-arrow__text');
                    if (text && text.textContent.trim() === 'Далее') {
                        const link = item.querySelector('a');
                        if (link) {
                            link.click();
                            return true;
                        }
                    }
                }
                return false;
            }
        """)

        if clicked:
            print("Кнопка 'Далее' нажата, ждём загрузку...")  # noqa: T201

            # Ждём загрузку новой страницы
            try:
                await page.wait_for_load_state(
                    "domcontentloaded", timeout=15000
                )
            except Exception:
                pass

            try:
                await page.wait_for_selector(
                    ".card[data-observe-id]", timeout=30000
                )
                cards = await page.query_selector_all(
                    ".card[data-observe-id]"
                )
                print(  # noqa: T201
                    f"Карточек после пагинации: {len(cards)}"
                )
            except Exception:
                print("Карточки не загрузились после пагинации")  # noqa: T201

            await asyncio.sleep(5)
        else:
            print(  # noqa: T201
                "Кнопка 'Далее' не найдена — "
                "возможно, в каталоге только одна страница"
            )

        # Сохраняем результаты обеих фаз
        interceptor.save_results(phase="page1_and_page2")

        # ── Итоговая сводка ──
        interceptor.save_summary()

        # Закрываем
        await browser.close()

    print(  # noqa: T201
        f"\n{'═' * 70}\n"
        f"ГОТОВО. Проверьте файлы в папке: {_OUTPUT_DIR}/\n"
        f"\n"
        f"Файлы:\n"
        f"  intercepted_page1.json       — запросы при загрузке 1-й страницы\n"
        f"  intercepted_page1_and_page2.json — все запросы (1-я + 2-я стр.)\n"
        f"  summary.json                 — краткая сводка эндпоинтов\n"
        f"\n"
        f"Что смотреть:\n"
        f"  1. Есть ли POST-запрос с телом, содержащим параметры поиска?\n"
        f"  2. Возвращает ли ответ массив объектов (объявлений) с полями?\n"
        f"  3. Есть ли в ответе пагинация (total, page, limit)?\n"
        f"  4. Какие поля есть у объекта — совпадают ли с тем, что мы парсим?\n"
        f"  5. Меняется ли запрос при переходе на вторую страницу?\n"
        f"{'═' * 70}"
    )


if __name__ == "__main__":
    asyncio.run(run())
