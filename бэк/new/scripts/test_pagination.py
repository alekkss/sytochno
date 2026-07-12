"""Тестовый скрипт — обход пагинации каталога без обогащения карточек.

Заходит по указанной ссылке, собирает ID и названия объявлений,
переходит по страницам пагинации до конца (или до лимита).
Выводит прогресс в консоль. Браузер открывается в видимом режиме (headless=false).

Запуск:
    python -m scripts.test_pagination
"""

import asyncio
import re
import time

from playwright.async_api import Page, async_playwright

# ──────────────────────────────────────────────────────────────────────
# НАСТРОЙКИ — измени под себя
# ──────────────────────────────────────────────────────────────────────

# Ссылка на каталог sutochno.ru с фильтрами
SEARCH_URL = (
    "https://sutochno.ru/front/searchapp/search?"
    "guests_adults=2"
    "&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3"
    "&id=397367&type=city"
    "&SW.lat=59.754147210556404&SW.lng=29.918670401855454&NE.lat=60.080977339121056"
    "&NE.lat=60.080977339121056&NE.lng=30.69114659814453"
    "&price_per=1&occupied=2026-06-03;2026-06-10"
)

# https://sutochno.ru/front/searchapp/search?guests_adults=2&term=%D0%A1%D0%B0%D0%BD%D0%BA%D1%82-%D0%9F%D0%B5%D1%82%D0%B5%D1%80%D0%B1%D1%83%D1%80%D0%B3&id=397367&type=city&SW.lat=59.754147210556404&SW.lng=29.918670401855454&NE.lat=60.080977339121056&NE.lng=30.69114659814453&price_per=1&occupied=2026-06-03;2026-06-10

# Максимум страниц (0 = все)
MAX_PAGES = 0

# Очищать DOM после каждой страницы (True/False)
CLEANUP_DOM = True

# Headless режим (False = браузер виден)
HEADLESS = False

# Время прокрутки страницы в секундах
SCROLL_DURATION_SEC = 6.0


# ──────────────────────────────────────────────────────────────────────
# ЛОГИКА
# ──────────────────────────────────────────────────────────────────────


async def wait_for_cards(page: Page) -> bool:
    """Ждёт появления карточек на странице."""
    try:
        await page.wait_for_selector(".card[data-observe-id]", timeout=30000)
        return True
    except Exception:
        return False


async def scroll_page_slowly(page: Page, duration: float = SCROLL_DURATION_SEC) -> None:
    """Плавная прокрутка страницы вниз за указанное время.

    Прокручивает порциями по ~300px с паузами, чтобы ленивые элементы
    успели подгрузиться. Общее время прокрутки — примерно duration секунд.

    Args:
        page: Страница Playwright.
        duration: Целевое время прокрутки в секундах.
    """
    page_height = await page.evaluate("document.body.scrollHeight")
    viewport_height = 1080

    scroll_step = 300
    total_steps = max(1, (page_height - viewport_height) // scroll_step)
    pause_between_steps = duration / total_steps

    current_position = 0

    for _ in range(total_steps + 5):
        current_position += scroll_step
        await page.evaluate(f"window.scrollTo(0, {current_position})")
        await asyncio.sleep(pause_between_steps)

        new_height = await page.evaluate("document.body.scrollHeight")
        if current_position >= new_height:
            break

    final_height = await page.evaluate("document.body.scrollHeight")
    await page.evaluate(f"window.scrollTo(0, {final_height})")
    await asyncio.sleep(0.5)


async def parse_page(page: Page) -> list[dict[str, str]]:
    """Извлекает ID и название из всех карточек на странице."""
    cards = await page.query_selector_all(".card[data-observe-id]")
    results: list[dict[str, str]] = []

    for card in cards:
        external_id = await card.get_attribute("data-observe-id")
        if not external_id:
            continue

        title_el = await card.query_selector("h2.card-content__object-title")
        if not title_el:
            title_el = await card.query_selector(".card-content__object-title")
        title = (await title_el.inner_text()).strip() if title_el else "—"

        price_el = await card.query_selector(".price-total__number")
        price = None
        if price_el:
            price_text = await price_el.inner_text()
            digits = re.sub(r"[^\d]", "", price_text)
            price = int(digits) if digits else None

        results.append({
            "id": external_id,
            "title": title[:50],
            "price": str(price) if price else "—",
        })

    return results


async def cleanup_dom(page: Page) -> int:
    """Удаляет карточки из DOM, возвращает количество удалённых."""
    removed = await page.evaluate("""
        () => {
            const cards = document.querySelectorAll('.card[data-observe-id]');
            const count = cards.length;
            cards.forEach(card => card.remove());
            return count;
        }
    """)
    return removed


async def get_first_card_id(page: Page) -> str | None:
    """Получает ID первой карточки на странице (для детекции смены страницы)."""
    first_card = await page.query_selector(".card[data-observe-id]")
    if first_card:
        return await first_card.get_attribute("data-observe-id")
    return None


async def go_to_next_page(page: Page, current_first_id: str | None) -> bool:
    """Кликает 'Далее' и ждёт появления новых карточек.

    Вместо networkidle (который зависает на Vue SPA) — ждём момента,
    когда первая карточка на странице изменится (значит Vue отрисовал
    новую страницу).

    Args:
        page: Страница Playwright.
        current_first_id: ID первой карточки на текущей странице.

    Returns:
        True если переход выполнен.
    """
    # Скролл к пагинации
    await page.evaluate("""
        () => {
            const p = document.querySelector('.pagination-wrapper');
            if (p) p.scrollIntoView({behavior: 'smooth', block: 'center'});
        }
    """)
    await asyncio.sleep(0.5)

    # Проверяем наличие кнопки «Далее»
    has_next = await page.evaluate("""
        () => {
            const items = document.querySelectorAll('li.navigation');
            for (const item of items) {
                const text = item.querySelector('.pagination-arrow__text');
                if (text && text.textContent.trim() === 'Далее') return true;
            }
            return false;
        }
    """)

    if not has_next:
        return False

    # Кликаем
    clicked = await page.evaluate("""
        () => {
            const items = document.querySelectorAll('li.navigation');
            for (const item of items) {
                const text = item.querySelector('.pagination-arrow__text');
                if (text && text.textContent.trim() === 'Далее') {
                    const link = item.querySelector('a');
                    if (link) { link.click(); return true; }
                }
            }
            return false;
        }
    """)

    if not clicked:
        return False

    # Ждём появления НОВЫХ карточек (ID первой карточки должен измениться)
    # Это быстрее чем networkidle — сразу реагируем на обновление Vue
    max_wait = 15.0  # Максимум 15 секунд
    poll_interval = 0.3
    elapsed = 0.0

    while elapsed < max_wait:
        await asyncio.sleep(poll_interval)
        elapsed += poll_interval

        # Проверяем, появились ли карточки с новым ID
        new_first_id = await get_first_card_id(page)
        if new_first_id and new_first_id != current_first_id:
            # Новая страница загрузилась
            break

        # Если карточек нет вообще (DOM очищен, ещё не загрузились)
        cards_count = await page.evaluate(
            "document.querySelectorAll('.card[data-observe-id]').length"
        )
        if cards_count > 0 and current_first_id is None:
            break
    else:
        # Таймаут — проверяем есть ли хоть что-то
        cards = await page.query_selector_all(".card[data-observe-id]")
        if not cards:
            return False

    # Скролл наверх
    await page.evaluate("window.scrollTo(0, 0)")
    await asyncio.sleep(0.3)

    return True


async def run() -> None:
    """Основной цикл тестового скрипта."""
    max_pages = MAX_PAGES or 999

    print("=" * 70)
    print("ТЕСТ ПАГИНАЦИИ КАТАЛОГА SUTOCHNO.RU")
    print("=" * 70)
    print(f"URL: {SEARCH_URL[:80]}...")
    print(f"Лимит страниц: {'все' if MAX_PAGES == 0 else MAX_PAGES}")
    print(f"Очистка DOM: {'Да' if CLEANUP_DOM else 'Нет'}")
    print(f"Headless: {'Да' if HEADLESS else 'Нет (браузер виден)'}")
    print(f"Время прокрутки: {SCROLL_DURATION_SEC} сек")
    print("=" * 70)
    print()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )

        # Stealth
        await context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
            window.chrome = {runtime: {}};
        """)

        page = await context.new_page()
        page.set_default_navigation_timeout(60000)

        # Переходим на URL
        print("[*] Загрузка страницы...")
        await page.goto(SEARCH_URL, wait_until="domcontentloaded")

        if not await wait_for_cards(page):
            print("[!] Карточки не найдены на первой странице. Выход.")
            await browser.close()
            return

        total_listings = 0
        seen_ids: set[str] = set()
        pages_processed = 0
        start_time = time.time()

        while pages_processed < max_pages:
            page_start = time.time()
            current_page = pages_processed + 1

            # Плавная прокрутка для подгрузки всех ленивых карточек
            await scroll_page_slowly(page)

            # Парсим карточки
            listings = await parse_page(page)

            # Запоминаем ID первой карточки (для детекции смены страницы)
            first_card_id = listings[0]["id"] if listings else None

            # Дедупликация
            new_listings = [l for l in listings if l["id"] not in seen_ids]
            for l in new_listings:
                seen_ids.add(l["id"])

            total_listings += len(new_listings)
            pages_processed += 1
            page_time = time.time() - page_start
            total_time = time.time() - start_time

            # Вывод прогресса
            print(
                f"  Стр. {current_page:>3} | "
                f"Карточек: {len(listings):>2} (новых: {len(new_listings):>2}) | "
                f"Всего: {total_listings:>5} | "
                f"Время стр.: {page_time:.1f}с | "
                f"Общее: {total_time:.0f}с | "
                f"Сред.: {total_time/pages_processed:.1f}с/стр"
            )

            # Очистка DOM
            if CLEANUP_DOM:
                await cleanup_dom(page)
                # После очистки first_card_id уже нет в DOM —
                # это поможет детектить новую страницу
                first_card_id_for_detection = first_card_id
            else:
                first_card_id_for_detection = first_card_id

            # Проверяем лимит
            if pages_processed >= max_pages:
                print(f"\n[*] Достигнут лимит страниц ({max_pages}).")
                break

            # Следующая страница
            has_next = await go_to_next_page(page, first_card_id_for_detection)
            if not has_next:
                print(f"\n[*] Последняя страница достигнута (стр. {current_page}).")
                break

        # Итоги
        elapsed = time.time() - start_time
        print()
        print("=" * 70)
        print("ИТОГИ")
        print("=" * 70)
        print(f"  Страниц обработано: {pages_processed}")
        print(f"  Уникальных объявлений: {total_listings}")
        print(f"  Общее время: {elapsed:.0f} сек ({elapsed/60:.1f} мин)")
        print(f"  Среднее время на страницу: {elapsed/pages_processed:.1f} сек")
        print(f"  Очистка DOM: {'Да' if CLEANUP_DOM else 'Нет'}")
        print("=" * 70)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(run())
