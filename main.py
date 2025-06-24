import asyncio
import logging
import sqlite3
from aiogram import Bot, Dispatcher, Router, types, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.utils.markdown import hbold
from playwright.async_api import async_playwright
import re
import os
import aiohttp
from aiogram.client.default import DefaultBotProperties
import hashlib
from aiogram.types import CallbackQuery
from openai import OpenAI
import html
from html import escape
import json
import time

# === Настройки ===
API_TOKEN = "8138380518:AAHt-pjc94XFKnQW8MfJHX-WeBhZPaIJvJY"
CHANNEL_ID = 1685580880
DB_PATH = "profiles.db"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
CACHE_FILE = "stat_cache.json"
DB_PATH1 = "tracked_posts.db"
bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
dp = Dispatcher()
router = Router()
dp.include_router(router)
logging.basicConfig(level=logging.INFO)
TRACKED_TOKENS = set()

openai_client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key="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",
)

def migrate_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE profiles ADD COLUMN post_ids TEXT DEFAULT ''")
        conn.commit()
        print("🟢 Колонка post_ids успешно добавлена.")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("ℹ️ Колонка post_ids уже существует.")
        else:
            raise
    finally:
        conn.close()

def init_db():
    # === БД 1: tracked_posts.db ===
    with sqlite3.connect(DB_PATH1) as conn1:
        c1 = conn1.cursor()
        c1.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                token TEXT,
                post_id TEXT,
                PRIMARY KEY (token, post_id)
            )
        """)
        conn1.commit()

    # === БД 2: profiles.db ===
    with sqlite3.connect("profiles.db") as conn2:
        c2 = conn2.cursor()
        c2.execute('''
            CREATE TABLE IF NOT EXISTS profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                post_ids TEXT DEFAULT ''
            )
        ''')
        conn2.commit()


def add_profile(url: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO profiles (url) VALUES (?)", (url,))
    conn.commit()
    conn.close()

def get_profiles():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT url, post_ids FROM profiles")
    profiles = cur.fetchall()
    conn.close()
    return [(url, post_ids.split(',')) if post_ids else (url, []) for url, post_ids in profiles]

def update_post_ids(url: str, new_id: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT post_ids FROM profiles WHERE url = ?", (url,))
    row = cur.fetchone()
    old_ids = row[0].split(',') if row and row[0] else []
    if new_id not in old_ids:
        old_ids = ([new_id] + old_ids)[:5]  
    cur.execute("UPDATE profiles SET post_ids = ? WHERE url = ?", (','.join(old_ids), url))
    conn.commit()
    conn.close()


async def fetch_latest_post(profile_url: str):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()
        
        try:
            await page.goto(profile_url, timeout=60000)
            await page.wait_for_load_state("domcontentloaded")
            # Увеличим время ожидания и добавим проверку альтернативных селекторов
            try:
                await page.wait_for_selector("div.card-content-box", timeout=30000)
            except:
                await page.wait_for_selector("div.css-1s5s0hx", timeout=30000)  # Альтернативный селектор
        except Exception as e:
            logging.error(f"Ошибка загрузки страницы: {e}")
            await browser.close()
            return None

        # Пробуем разные варианты поиска карточек
        cards = await page.query_selector_all("div.card-content-box")
        if not cards:
            cards = await page.query_selector_all("div.css-1s5s0hx")  # Альтернативный селектор
        
        if not cards:
            logging.warning("Посты не найдены на странице")
            await browser.close()
            return None

        card = cards[0]
        try:
            # Пробуем разные методы получения текста
            try:
                text = await card.inner_text()
            except:
                text = await card.evaluate("el => el.textContent")
            
            # Удаляем технические элементы перед обработкой
            for unwanted in [
                "см. оригинал", "subscribe to", "подробнее", "see original", 
                "likecomment", "share", "comment", "like", "repost"
            ]:
                text = re.sub(unwanted, "", text, flags=re.IGNORECASE)

            lines = text.strip().split("\n")
            seen = set()
            cleaned_lines = []

            for line in lines:
                line = line.strip()
                if not line:
                    continue
                if line in seen:
                    continue
                if re.match(r"^\d+(\.\d+)?[kK]?$", line):  # Числа
                    continue
                if re.fullmatch(r"[A-Z0-9]+", line):  # HEX-коды
                    continue
                if any(phrase in line.lower() for phrase in ["см. оригинал", "subscribe to"]):
                    continue
                seen.add(line)
                cleaned_lines.append(line)

            # Удаляем служебную информацию в начале
            while cleaned_lines and cleaned_lines[0].lower().startswith(("binance", "bibi")):
                cleaned_lines.pop(0)

            # Извлекаем заголовок
            title_el = await card.query_selector("div.font-bold, .font-bold")
            if not title_el:
                title_el = await card.query_selector("div.css-1k5hq0n")  # Альтернативный селектор
                
            header = await title_el.inner_text() if title_el else ""

            # Удаляем дубликат заголовка
            if cleaned_lines and cleaned_lines[0] == header:
                cleaned_lines.pop(0)

            text_to_process = "\n".join(cleaned_lines).strip()

            # Важная проверка: если текста недостаточно
            if not text_to_process or len(text_to_process) < 20:
                logging.warning(f"Текст слишком короткий: {text_to_process}")
                await browser.close()
                return None

            # Генерация ID до обработки ИИ
            cleaned_for_id = re.sub(r'\s+', '', (header + text_to_process).strip())
            post_id = hashlib.md5(cleaned_for_id.encode()).hexdigest()

            # Перевод только если текст преимущественно на английском
            russian_chars = len(re.findall(r'[а-яА-Я]', text_to_process))
            english_chars = len(re.findall(r'[a-zA-Z]', text_to_process))
            
            if english_chars > russian_chars:
                try:
                    logging.info("Перевод поста на русский...")
                    response = openai_client.chat.completions.create(
                        model="meta-llama/llama-4-maverick:free",
                        messages=[{"role": "user", "content": f"Переведи на русский(и не пиши фразы вроде переведено на русский)): {text_to_process}"}],
                        max_tokens=2000
                    )
                    translated = response.choices[0].message.content.strip()
                    if translated and len(translated) > 10:
                        text_to_process = translated
                except Exception as e:
                    logging.warning(f"Ошибка перевода: {e}")

            # Сжатие только для длинных текстов
            if len(text_to_process) > 300:
                try:
                    logging.info("Сжатие длинного поста...")
                    compress_response = openai_client.chat.completions.create(
                        model="meta-llama/llama-4-maverick:free",
                        messages=[{
                            "role": "user",
                            "content": f"Сократи текст, оставив суть (без примеров и повторов, также не пиши фразы по типу: вот ващ сокращенный текст): {text_to_process}"
                        }],
                        max_tokens=1000
                    )
                    compressed = compress_response.choices[0].message.content.strip()
                    if compressed and len(compressed) > 20:
                        text_to_process = compressed
                except Exception as e:
                    logging.warning(f"Ошибка сжатия: {e}")

            # Форматирование текста
            formatted_text = (
                f"<b>{escape(header)}</b>\n\n" if header else ""
            ) + f"<blockquote>{escape(text_to_process)}</blockquote>"

            # Получение изображений
            image_urls = []
            image_elements = await card.query_selector_all("img")
            for img in image_elements:
                src = await img.get_attribute("src")
                if src:
                    if src.startswith("//"):
                        src = "https:" + src
                    elif src.startswith("/"):
                        src = "https://www.binance.com" + src
                    if src.startswith("http"):
                        image_urls.append(src)

            # Информация об авторе
            nickname = profile_url.split("/")[-1]
            button = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔗 Источник", url=profile_url)]]
            )

            await browser.close()
            return {
                "id": post_id,
                "text": formatted_text,
                "images": image_urls,
                "footer": f"<i>Автор: @{nickname}</i>",
                "button": button
            }

        except Exception as e:
            logging.exception(f"Ошибка обработки поста: {e}")
            await browser.close()
            return None




async def download_image(url: str, filename: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(filename, 'wb') as f:
                    f.write(await resp.read())


@dp.message(Command("add"))
async def cmd_add(message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.reply("❌ Формат: /add <ссылка>")
        return
    url = parts[1]
    if not (
        "binance.com/ru/square/profile/" in url or
        "binance.com/en/square/profile/" in url
    ):
        await message.reply("❌ Неверная ссылка на профиль Binance Square")
        return
    add_profile(url)
    await message.reply("✅ Профиль добавлен!")


@dp.message(Command("last"))
async def cmd_last(message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.reply("❌ Формат: /last <ссылка>")
        return
    url = parts[1]
    post = await fetch_latest_post(url)
    if not post:
        await message.reply("⚠️ Пост не найден.")
        return
    _, old_ids = next(((u, ids) for u, ids in get_profiles() if u == url), (None, []))
    if post['id'] in old_ids:
        await message.reply("ℹ️ Этот пост уже отправлялся ранее.")
        return
    await bot.send_message(CHANNEL_ID, f"<b>{hbold('Новый пост')}</b>\n{post['text']}\n\n{post['footer']}", reply_markup=post['button'])
    for i, img_url in enumerate(post["images"]):
        filename = f"image_{i}.jpg"
        await download_image(img_url, filename)
        with open(filename, "rb") as photo:
            await bot.send_photo(CHANNEL_ID, photo)
        os.remove(filename)
    update_post_ids(url, post['id'])
    await message.reply(f"✅ Пост отправлен. ID: <code>{post['id']}</code>")

@dp.message(Command("list"))
async def cmd_list(message: Message):
    profiles = get_profiles()
    text = "<b>📋 Список профилей:</b>\n\n" + "\n".join(f"🔹 {url}" for url, _ in profiles)
    await message.reply(text or "📭 Список профилей пуст.")

@dp.message(Command("del"))
async def cmd_delete(message: Message):
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.reply("❌ Формат: /del <ссылка>")
        return
    url = parts[1]
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM profiles WHERE url = ?", (url,))
    conn.commit()
    conn.close()
    await message.reply("✅ Профиль удалён.")


async def check_new_posts():
    while True:
        logging.info("🔄 Цикл проверки новых постов")
        for url, old_ids in get_profiles():
            try:
                post = await fetch_latest_post(url)
                if post and post['id'] not in old_ids:
                    text = f"<b>{hbold('Новый пост')}</b>\n{post['text']}\n\n{post['footer']}"
                    await bot.send_message(CHANNEL_ID, text, reply_markup=post['button'])
                    for i, img_url in enumerate(post['images']):
                        filename = f"image_{i}.jpg"
                        await download_image(img_url, filename)
                        with open(filename, "rb") as photo:
                            await bot.send_photo(CHANNEL_ID, photo)
                        os.remove(filename)
                    update_post_ids(url, post['id'])
            except Exception as e:
                logging.exception(f"Ошибка при обработке {url}: {e}")
        await asyncio.sleep(60)


@dp.message(Command("news"))
async def cmd_news(message: Message, bot: Bot):
    args = message.text.strip().split()
    if len(args) < 2 or len(args) > 3:
        await message.reply("❌ Формат: /news <токен> [кол-во постов от 1 до 101]")
        return

    token = args[1].lower()
    limit = 10
    if len(args) == 3:
        try:
            limit = min(101, max(1, int(args[2])))
        except:
            await message.reply("❌ Кол-во постов должно быть числом от 1 до 101.")
            return

    url = f"https://www.binance.com/ru/square/search?s={token}"
    await message.reply(f"🔍 Парсим <b>{limit}</b> постов по токену <b>{token.upper()}</b>...")

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="ru-RU",
                extra_http_headers={"Accept-Language": "ru-RU,ru;q=0.9"},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await page.goto(url)
            await page.wait_for_selector("div.card-content-box", timeout=60000)

            cards = await page.query_selector_all("div.card-content-box")
            if not cards:
                await message.reply("⚠️ Посты не найдены.")
                return

            for i in range(min(limit, len(cards))):
                try:
                    card = cards[i]
                    await card.evaluate("el => el.click()")
                    await page.wait_for_selector("div#articleBody", timeout=20000)

                    # Перевод (если есть кнопка)
                    try:
                        translate_btn = page.locator("div.common-trans-btn-list-item-text.css-vurnku >> text=Перевести")
                        if await translate_btn.count() > 0:
                            await translate_btn.click()
                            await asyncio.sleep(1.5)
                    except Exception as e:
                        logging.warning(f"⚠️ Не удалось нажать кнопку перевода: {e}")

                    raw_content = await page.locator("div.richtext-container").inner_text()
                    text_to_process = raw_content.strip()

                    # === Перевод текста на русский, если он на английском ===
                    if not re.search(r'[а-яА-Я]', text_to_process):
                        try:
                            logging.info("🌐 Переводим пост на русский...")
                            response = openai_client.chat.completions.create(
                                model="meta-llama/llama-4-maverick:free",
                                messages=[
                                    {
                                        "role": "user",
                                        "content": f"Переведи этот текст на русский(и не пиши фразы вроде переведено на русский):\n\n{text_to_process}"
                                    }
                                ]
                            )
                            text_to_process = response.choices[0].message.content.strip()
                        except Exception as e:
                            logging.warning(f"⚠️ Ошибка при переводе: {e}")

                    # === Сокращение текста ===
                    try:
                        logging.info("✂️ Сокращаем текст...")
                        compress_response = openai_client.chat.completions.create(
                            model="meta-llama/llama-4-maverick:free",
                            messages=[
                                {
                                    "role": "user",
                                    "content": (
                                            "Сократи этот текст, оставь только основную суть. "
                                            "Не добавляй пояснений, заголовков и фраз вроде 'Вот сокращённая версия'. "
                                            "Верни только сжатый текст:\n\n"
                                            f"{text_to_process}"
                                        )
                                }
                            ]
                        )
                        text_to_process = compress_response.choices[0].message.content.strip()
                    except Exception as e:
                        logging.warning(f"⚠️ Ошибка при сокращении: {e}")

                    safe_content = escape(text_to_process)

                    # Автор
                    try:
                        profile_link = await page.locator("div.nick-username a").first.get_attribute("href")
                        username = profile_link.split("/")[-1] if profile_link else "Автор"
                        is_verified = await page.locator("div.avatar-name-container svg").count() > 0
                        verified_prefix = "✅" if is_verified else ""
                        formatted_nick = f"{verified_prefix}@{username}"
                    except Exception as e:
                        logging.error(f"❗ Не удалось получить имя автора: {e}")
                        formatted_nick = "Автор неизвестен"

                    # Время
                    try:
                        post_time = await page.locator("div.css-12fealn > span").first.inner_text()
                    except Exception as e:
                        logging.warning(f"⚠️ Не удалось получить время поста: {e}")
                        post_time = "время неизвестно"

                    author_block = escape(f"{formatted_nick} | 🕒 {post_time}")

                    # Ссылка на источник
                    source_url = page.url
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🔗 Ссылка на источник", url=source_url)]]
                    )

                    full_text = f"<pre>{safe_content}</pre>\n<blockquote>{author_block}</blockquote>"

                    chunks = [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]
                    for j, chunk in enumerate(chunks):
                        await bot.send_message(
                            chat_id=CHANNEL_ID,
                            text=chunk,
                            reply_markup=keyboard if j == len(chunks) - 1 else None,
                            parse_mode=ParseMode.HTML,
                        )

                    await page.go_back()
                    await page.wait_for_selector("div.card-content-box", timeout=15000)
                    cards = await page.query_selector_all("div.card-content-box")
                    await asyncio.sleep(2)

                except Exception as e:
                    logging.error(f"❗ Ошибка при обработке поста #{i+1}\n{e}")
                    continue

            await browser.close()

    except Exception as e:
        logging.error(f"❌ Ошибка загрузки страницы: {e}")
        await message.reply("⚠️ Не удалось загрузить посты. Попробуй позже.")

def format_monospaced_table(positions_dict: dict, old_positions: dict, old_tokens: set) -> str:
    lines = []
    header = f"{'#':<2} {'Token':<10} {'Price':<10} {'Change':<7} Note"
    lines.append(header)
    lines.append("-" * len(header))

    sorted_items = sorted(positions_dict.items(), key=lambda x: x[1]['pos'])

    for i, (name, data) in enumerate(sorted_items):
        price = data['price']
        change = data['change']
        badge = data['badge']
        note = ""

        if name not in old_tokens:
            note = "🆕"
        elif old_positions[name]['pos'] != data['pos']:
            old_pos = old_positions[name]['pos'] + 1
            new_pos = data['pos'] + 1
            arrow = "🔺" if new_pos < old_pos else "🔻"
            note = f"{arrow} {old_pos}→{new_pos}"

        if badge:
            note += f" ({badge})" if note else f"({badge})"

        lines.append(f"{i+1:<2} {name:<10} {price:<10} {change:<7} {note}")

    result = "<pre>\n" + "\n".join(lines) + "\n</pre>"
    logging.info(f"📄 Сформирована таблица:\n{result}")
    return result

async def fetch_stat_text(force_send=False):
    url = "https://www.binance.com/ru/square/"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        page = await context.new_page()

        await page.route("**/*", lambda route, request: route.abort()
                         if request.resource_type in ["image", "stylesheet", "font", "media"]
                         else route.continue_())

        start = time.time()
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state("networkidle")
        logging.info(f"⏱ Загрузка страницы: {round(time.time() - start, 2)} сек")

        # === Проверка заголовка ===
        found = False
        try:
            await page.wait_for_selector("h2:has-text('Most Searched')", timeout=20000)
            logging.info("✅ Найден заголовок: Most Searched")
            found = True
        except:
            try:
                await page.wait_for_selector("h2.css-1ld3mhe:has-text('Самые популярные по запросам')", timeout=30000)
                logging.info("✅ Найден заголовок: Самые популярные по запросам")
                found = True
            except:
                logging.error("❌ Не удалось найти заголовок статистики")

        if not found:
            raise RuntimeError("Заголовок 'Most Searched' или 'Самые популярные по запросам' не найден")

        # Пробуем раскрыть весь блок (если требуется)
        try:
            await page.locator("div.css-1h8s7v0").click(timeout=3000)
            await page.wait_for_timeout(1000)
        except:
            pass

        # Получаем монеты
        stat_section = page.locator("div.css-6srrto")
        links = stat_section.locator("a")
        count = await links.count()
        logging.info(f"🔢 Найдено монет: {count}")

        positions = {}
        for i in range(count):
            try:
                logging.info(f"🔍 Обрабатываем монету #{i+1}")
                coin = links.nth(i)

                name = await coin.locator("div.css-1q7imhr").inner_text()
                price = await coin.locator("div.css-1dru1te").inner_text()
                change = await coin.locator("div.css-1qhsfgf, div.css-1wsvtgi").inner_text()

                badge = ""
                try:
                    badge = await coin.locator("div.css-75hguj").inner_text()
                except:
                    pass

                positions[name] = {
                    "pos": i,
                    "price": price,
                    "change": change,
                    "badge": badge
                }

                logging.info(f"✅ {name}: {price} ({change}) {badge}")

            except Exception as e:
                logging.warning(f"⚠️ Ошибка при разборе монеты #{i+1}: {e}")

        # Загрузка предыдущей статистики
        old_positions = {}
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                old_positions = json.load(f)

            # Старый формат кэша
            if isinstance(list(old_positions.values())[0], int):
                old_positions = {
                    k: {"pos": v, "price": "", "change": "", "badge": ""}
                    for k, v in old_positions.items()
                }

        old_tokens = set(old_positions.keys())

        # Проверка изменений
        changed = False
        for name in positions:
            if name not in old_positions or positions[name]["pos"] != old_positions[name]["pos"]:
                changed = True
                break

        # Сохраняем новый кэш
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(positions, f, ensure_ascii=False, indent=2)

        await browser.close()

        if changed or force_send:
            table = format_monospaced_table(positions, old_positions, old_tokens)
            logging.info(f"📊 Возвращаем таблицу длиной {len(table)} символов")
            return f"📊 <b>Самые популярные по запросам (6 ч.)</b>\n{table}"
        else:
            logging.info("ℹ️ Нет изменений — не отправляем")
            return None


@router.message(F.text == "/stat")
async def stat_command(message: types.Message):
    logging.info(f"📥 Получена команда /stat от user_id={message.from_user.id}")
    await message.answer("⏳ Получаю данные с Binance Square...")

    try:
        text = await fetch_stat_text()
        logging.info(f"📊 fetch_stat_text вернул: {'есть текст' if text else 'пусто'}")

        if text:
            await message.answer(text, parse_mode=ParseMode.HTML)
            await message.answer("✅ Данные отправлены.")
        else:
            logging.info("📁 Попытка отправки из кэша...")
            from_cache = await fetch_stat_text(force_send=True)
            logging.info(f"📦 fetch_stat_text(force_send=True) вернул: {'есть кэш' if from_cache else 'тоже пусто'}")

            if from_cache:
                await message.answer(from_cache, parse_mode=ParseMode.HTML)
                await message.answer("📋 Отправлен текущий мониторинг.")
            else:
                await message.answer("⚠️ Нет новых данных и кэш тоже пуст.")
    except Exception as e:
        logging.exception("❌ Ошибка внутри /stat")
        await message.answer(f"❌ Ошибка: {e}")




async def check_stat_periodically():
    while True:
        print("🔁 Авто-проверка...")
        try:
            text = await fetch_stat_text()
            if text:
                await bot.send_message(chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML)
        except Exception as e:
            print("❌ Ошибка автообновления:", e)
        await asyncio.sleep(360)  

def save_post_id(token: str, post_id: str):
    with sqlite3.connect(DB_PATH1) as conn:
        c = conn.cursor()
        c.execute("INSERT OR IGNORE INTO posts (token, post_id) VALUES (?, ?)", (token, post_id))
        conn.commit()


def is_new_post(token: str, post_id: str) -> bool:
    with sqlite3.connect(DB_PATH1) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM posts WHERE token = ? AND post_id = ?", (token, post_id))
        return c.fetchone() is None


# === Парсинг поста ===
async def parse_latest_post(token: str):
    url = f"https://www.binance.com/ru/square/search?s={token}"
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        context = await browser.new_context(locale="ru-RU", user_agent=USER_AGENT)
        page = await context.new_page()

        await page.goto(url)
        await page.wait_for_selector(".card-content-box", timeout=30000)
        card = await page.query_selector(".card-content-box")
        if not card:
            return None

        await card.click()
        await page.wait_for_selector("div#articleBody", timeout=20000)

        try:
            translate_btn = page.locator("div.common-trans-btn-list-item-text.css-vurnku >> text=Перевести")
            if await translate_btn.count() > 0:
                await translate_btn.click()
                await asyncio.sleep(1.5)
        except:
            pass

        raw_content = await page.locator("div.richtext-container").inner_text()
        text_to_process = raw_content.strip()

        if not text_to_process or len(text_to_process) < 10:
            return None

        # Перевод
        if not re.search(r'[а-яА-Я]', text_to_process):
            try:
                response = openai_client.chat.completions.create(
                    model="meta-llama/llama-4-maverick:free",
                    messages=[{"role": "user", "content": f"Переведи этот текст на русский:\n\n{text_to_process}"}]
                )
                text_to_process = response.choices[0].message.content.strip()
            except:
                pass

        # Сжатие
        if text_to_process.strip():
            try:
                compress_response = openai_client.chat.completions.create(
                    model="meta-llama/llama-4-maverick:free",
                    messages=[
                        {
                            "role": "user",
                            "content": (
                                "Сократи этот текст, сохранив суть. Удали лишние детали, примеры и повторы. "
                                "Верни только краткий текст:\n\n" + text_to_process.strip()
                            )
                        }
                    ]
                )
                text_to_process = compress_response.choices[0].message.content.strip()
            except:
                pass

        safe_content = escape(text_to_process)

        try:
            profile_link = await page.locator("div.nick-username a").first.get_attribute("href")
            username = profile_link.split("/")[-1] if profile_link else "Автор"
            is_verified = await page.locator("div.avatar-name-container svg").count() > 0
            formatted_nick = f"{'✅' if is_verified else ''}@{username}"
        except:
            formatted_nick = "Автор неизвестен"

        try:
            post_time = await page.locator("div.css-12fealn > span").first.inner_text()
        except:
            post_time = "время неизвестно"

        cleaned_for_id = re.sub(r'\s+', '', text_to_process.strip())
        post_id = hashlib.md5(cleaned_for_id.encode()).hexdigest()

        return {
            "id": post_id,
            "text": safe_content,
            "author": f"{formatted_nick} | 🕒 {post_time}",
            "url": page.url
        }


    
@dp.message(Command("laster"))
async def cmd_last(message: Message):
    args = message.text.strip().split()
    if len(args) != 2:
        await message.reply("❌ Формат: /laster <токен>")
        return

    token = args[1].lower()
    await message.reply(f"⏳ Получаем последний пост по <b>{token.upper()}</b>...")

    post = await parse_latest_post(token)
    if not post:
        await message.reply("⚠️ Пост не найден.")
        return

    text = f"<pre>{post['text']}</pre>\n<blockquote>{escape(post['author'])}</blockquote>"
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔗 Ссылка на источник", url=post['url'])]]
    )

    await bot.send_message(CHANNEL_ID, text=text, reply_markup=keyboard)


@dp.message(Command("track"))
async def cmd_track(message: Message):
    args = message.text.strip().split()
    if len(args) != 2:
        await message.reply("❌ Формат: /track <токен>")
        return

    token = args[1].lower()
    if token in TRACKED_TOKENS:
        TRACKED_TOKENS.remove(token)
        await message.reply(f"⛔ Трекинг токена <b>{token.upper()}</b> выключен.")
    else:
        TRACKED_TOKENS.add(token)
        await message.reply(f"✅ Трекинг токена <b>{token.upper()}</b> включен.")


async def tracker_loop():
    while True:
        for token in list(TRACKED_TOKENS):
            try:
                post = await parse_latest_post(token)
                if post and is_new_post(token, post['id']):
                    save_post_id(token, post['id'])
                    text = f"<pre>{post['text']}</pre>\n<blockquote>{escape(post['author'])}</blockquote>"
                    keyboard = InlineKeyboardMarkup(
                        inline_keyboard=[[InlineKeyboardButton(text="🔗 Ссылка на источник", url=post['url'])]]
                    )
                    await bot.send_message(CHANNEL_ID, text=text, reply_markup=keyboard)
            except Exception as e:
                logging.warning(f"⚠️ Ошибка трекинга для {token}: {e}")
        await asyncio.sleep(180)


async def main():
    init_db()
    migrate_db()
    asyncio.create_task(check_new_posts())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
