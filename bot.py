import os
import logging
import json
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, Filters
from telegram.error import RetryAfter
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Конфигурация из переменных окружения ---
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 8443))
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"

# Загрузка каналов и кнопок из переменных окружения
try:
    CHANNELS = json.loads(os.environ.get("CHANNEL_IDS", "[]"))
    CHANNEL_BUTTONS = json.loads(os.environ.get("CHANNEL_BUTTONS", "[]"))
    if not CHANNELS or not CHANNEL_BUTTONS:
        logger.error("CHANNEL_IDS или CHANNEL_BUTTONS пусты или не заданы.")
        raise ValueError("CHANNEL_IDS и CHANNEL_BUTTONS должны быть заданы в переменных окружения.")
    if len(CHANNELS) != len(CHANNEL_BUTTONS):
        logger.error("Количество каналов и кнопок не совпадает.")
        raise ValueError("Количество CHANNEL_IDS и CHANNEL_BUTTONS должно совпадать.")
except json.JSONDecodeError as e:
    logger.error(f"Ошибка парсинга JSON в CHANNEL_IDS или CHANNEL_BUTTONS: {e}")
    raise
except ValueError as e:
    logger.error(f"Ошибка в конфигурации каналов: {e}")
    raise

# Проверка обязательных переменных окружения
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL не задан в переменных окружения!")
    raise ValueError("WEBHOOK_URL не задан в переменных окружения!")
if not TOKEN:
    logger.error("BOT_TOKEN не задан в переменных окружения!")
    raise ValueError("BOT_TOKEN не задан в переменных окружения!")

# --- Настройка Google Sheets ---
sheet = None
try:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        logger.error(f"Файл с учетными данными не найден по пути: {GOOGLE_CREDENTIALS_PATH}")
        raise FileNotFoundError(f"Файл с учетными данными не найден по пути: {GOOGLE_CREDENTIALS_PATH}")

    with open(GOOGLE_CREDENTIALS_PATH, 'r') as f:
        creds_json = json.load(f)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms").sheet1
    logger.info("Google Sheets успешно инициализирован.")
except Exception as e:
    logger.error(f"Ошибка при инициализации Google Sheets: {e}")
    raise

def start(update: Update, context: CallbackContext) -> None:
    user = update.message.from_user
    logger.info(f"Пользователь {user.id} {user.first_name} запустил бота.")
    welcome_text = (
        "Привет! 👋\n"
        "Напиши код фильма, и я помогу тебе узнать его название. 🎬\n\n"
    )
    try:
        update.message.reply_text(welcome_text, parse_mode='Markdown')
    except RetryAfter as e:
        logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
        time.sleep(e.retry_after)
        update.message.reply_text(welcome_text, parse_mode='Markdown')

def prompt_subscribe(update: Update, context: CallbackContext, message_id=None) -> None:
    promo_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим!"
    )
    keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in CHANNEL_BUTTONS]
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    try:
        if message_id:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=promo_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            update.message.reply_text(promo_text, reply_markup=reply_markup, parse_mode='Markdown')
    except RetryAfter as e:
        logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
        time.sleep(e.retry_after)
        if message_id:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=promo_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        else:
            update.message.reply_text(promo_text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logger.warning(f"Не удалось отправить/отредактировать сообщение: {e}. Пропускаем.")

def check_subscription(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    bot = context.bot
    all_subscribed = True
    failed_channel = None

    # Prevent duplicate processing
    query_id = query.id
    if context.user_data.get('last_processed_query') == query_id:
        logger.info(f"Повторный запрос {query_id} от пользователя {user_id}. Игнорируем.")
        return
    context.user_data['last_processed_query'] = query_id

    # Limit subscription attempts
    context.user_data['subscription_attempts'] = context.user_data.get('subscription_attempts', 0) + 1
    if context.user_data['subscription_attempts'] > 3:
        try:
            query.message.edit_text(
                "😔 Слишком много попыток. Попробуй позже или свяжись с поддержкой.",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}.")
        return

    for channel_id in CHANNELS:
        try:
            member = bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                all_subscribed = False
                failed_channel = channel_id
                break
        except Exception as e:
            logger.error(f"Ошибка при проверке подписки на канал {channel_id}: {e}")
            all_subscribed = False
            failed_channel = channel_id
            break

    if all_subscribed:
        context.user_data['subscription_confirmed'] = True
        context.user_data['subscription_attempts'] = 0
        logger.info(f"Пользователь {user_id} успешно подтвердил подписку.")
        try:
            query.message.edit_text(
                "🎉 Поздравляю! Ты подписался на все каналы.\n"
                "Теперь можешь отправить код фильма, и я найду его название! 🍿",
                parse_mode='Markdown'
            )
            # Process pending movie code
            if 'pending_movie_code' in context.user_data:
                code = context.user_data.pop('pending_movie_code')
                movie = find_movie_by_code(code)
                try:
                    if movie:
                        query.message.reply_text(f"🎥 Фильм по коду {code}: {movie['title']}", parse_mode='Markdown')
                    else:
                        query.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')
                except RetryAfter as e:
                    logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
                    time.sleep(e.retry_after)
                    if movie:
                        query.message.reply_text(f"🎥 Фильм по коду {code}: {movie['title']}", parse_mode='Markdown')
                    else:
                        query.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')
        except RetryAfter as e:
            logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
            time.sleep(e.retry_after)
            query.message.edit_text(
                "🎉 Поздравляю! Ты подписался на все каналы.\n"
                "Теперь можешь отправить код фильма, и я найду его название! 🍿",
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}.")
    else:
        logger.info(f"Пользователь {user_id} не подписан на канал: {failed_channel}")
        try:
            query.message.edit_text(
                f"😕 Похоже, ты не подписан на канал {failed_channel or 'один из каналов'}.\n"
                "Пожалуйста, проверь ещё раз и нажми '✅ Я ПОДПИСАЛСЯ!'.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(btn["text"], url=btn["url"]) for btn in CHANNEL_BUTTONS],
                    [InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")]
                ])
            )
        except RetryAfter as e:
            logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
            time.sleep(e.retry_after)
            query.message.edit_text(
                f"😕 Похоже, ты не подписан на канал {failed_channel or 'один из каналов'}.\n"
                "Пожалуйста, проверь ещё раз и нажми '✅ Я ПОДПИСАЛСЯ!'.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(btn["text"], url=btn["url"]) for btn in CHANNEL_BUTTONS],
                    [InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")]
                ])
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение: {e}. Пропускаем.")

def find_movie_by_code(code: str) -> dict:
    if sheet is None:
        logger.error("Google Sheets не инициализирован. Невозможно выполнить поиск.")
        return None

    try:
        all_values = sheet.get_all_values()
        for row_index, row_data in enumerate(all_values):
            if not row_data or len(row_data) < 2:
                continue
            sheet_code = row_data[0].strip()
            sheet_title = row_data[1].strip()
            if sheet_code == code:
                logger.info(f"Найден фильм с кодом {code}: {sheet_title}")
                return {"code": sheet_code, "title": sheet_title}
        logger.info(f"Фильм с кодом {code} не найден.")
        return None
    except gspread.exceptions.APIError as e:
        logger.error(f"Ошибка API Google Sheets при доступе: {e}")
        return None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при доступе к Google Sheets: {e}")
        return None

def handle_movie_code(update: Update, context: CallbackContext) -> None:
    code = update.message.text.strip()
    user_id = update.message.from_user.id

    if not code.isdigit():
        logger.info(f"Пользователь {user_id} ввел нечисловой код: {code}")
        try:
            update.message.reply_text("Пожалуйста, введи только числовой код фильма. 🔢", parse_mode='Markdown')
        except RetryAfter as e:
            logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
            time.sleep(e.retry_after)
            update.message.reply_text("Пожалуйста, введи только числовой код фильма. 🔢", parse_mode='Markdown')
        return

    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"Пользователь {user_id} не подтвердил подписку. Код {code} сохранен как ожидающий.")
        context.user_data['pending_movie_code'] = code
        prompt_subscribe(update, context)
        return

    logger.info(f"Пользователь {user_id} подтвердил подписку. Обрабатываем код: {code}")
    movie = find_movie_by_code(code)
    if movie:
        try:
            update.message.reply_text(f"🎥 Фильм по коду {code}: {movie['title']}", parse_mode='Markdown')
        except RetryAfter as e:
            logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
            time.sleep(e.retry_after)
            update.message.reply_text(f"🎥 Фильм по коду {code}: {movie['title']}", parse_mode='Markdown')
    else:
        try:
            update.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')
        except RetryAfter as e:
            logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
            time.sleep(e.retry_after)
            update.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')

def handle_non_numeric_text(update: Update, context: CallbackContext) -> None:
    if update.message.from_user.id == context.bot.id:
        return  # Ignore messages sent by the bot itself
    try:
        update.message.reply_text("Пожалуйста, введи *только числовой* код фильма. 🔢", parse_mode='Markdown')
    except RetryAfter as e:
        logger.warning(f"Сработал flood control: {e}. Ждем {e.retry_after} секунд.")
        time.sleep(e.retry_after)
        update.message.reply_text("Пожалуйста, введи *только числовой* код фильма. 🔢", parse_mode='Markdown')

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command & Filters.regex(r'^\d+$'), handle_movie_code))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command & ~Filters.regex(r'^\d+$'), handle_non_numeric_text))

    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Установка вебхука на: {full_webhook_url}")
    logger.info(f"Использование порта: {PORT}")

    try:
        updater.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=full_webhook_url
        )
        logger.info("Вебхук успешно запущен.")
        updater.idle()
    except Exception as e:
        logger.error(f"Не удалось запустить вебхук: {e}")
        raise

if __name__ == "__main__":
    main()