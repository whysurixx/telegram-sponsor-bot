import os
import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, Filters
from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация Flask
app = Flask(__name__)

# --- Конфигурация из переменных окружения ---
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 443))

# --- Путь к файлу с секретными учетными данными Google ---
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"

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

# --- Конфигурация каналов ---
CHANNELS = [
    "-1002657330561",
    "-1002243633174",
    "-1002484534545",
    "-1002578865225",
    "-1002617434713",
]

CHANNEL_BUTTONS = [
    {"text": "Канал 1 — Смотри новинки", "url": "https://t.me/+8qO35jVzZVs5MjMy"},
    {"text": "Канал 2 — Лучше фильмы", "url": "https://t.me/+ZAvb9OTIrU9mOWIy"},
    {"text": "Канал 3 — Премии и хиты", "url": "https://t.me/+PAu2GRMZuUU0ZWQy"},
    {"text": "Канал 4 — Кино без рекламы", "url": "https://t.me/+kO2CPJZgxediMmZi"},
    {"text": "Канал 5 — Эксклюзивы", "url": "https://t.me/+DUDDSAYIDl8yN2Ni"},
]

@app.route('/')
def health_check():
    return "Bot is alive", 200

def start(update: Update, context: CallbackContext) -> None:
    user = update.message.from_user
    logger.info(f"Пользователь {user.id} {user.first_name} запустил бота.")
    welcome_text = (
        "Привет! 👋\n"
        "Напиши код фильма, и я помогу тебе узнать его название. 🎬\n\n"
    )
    update.message.reply_text(welcome_text)

def prompt_subscribe(update: Update, context: CallbackContext, message_id=None) -> None:
    promo_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим!"
    )
    keyboard = []
    for btn_info in CHANNEL_BUTTONS:
        keyboard.append([InlineKeyboardButton(btn_info["text"], url=btn_info["url"])])
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if message_id:
        try:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=promo_text,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение {message_id}: {e}. Отправляем новое.")
            update.effective_message.reply_text(promo_text, reply_markup=reply_markup)
    else:
        update.message.reply_text(promo_text, reply_markup=reply_markup)

def check_subscription(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    bot = context.bot
    all_subscribed = True

    for channel_id in CHANNELS:
        try:
            member = bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                all_subscribed = False
                break
        except Exception as e:
            logger.error(f"Ошибка при проверке подписки на канал {channel_id}: {e}")
            all_subscribed = False
            break

    if all_subscribed:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"Пользователь {user_id} успешно подтвердил подписку.")
        query.message.reply_text(
            "🎉 Поздравляю! Ты подписался на все каналы.\n"
            "Теперь можешь отправить код фильма, и я найду его название! 🍿"
        )
        if 'pending_movie_code' in context.user_data:
            code = context.user_data.pop('pending_movie_code')
            movie = find_movie_by_code(code)
            if movie:
                query.message.reply_text(f"🎥 Фильм по коду \"{code}\": \"{movie['title']}\"", parse_mode='Markdown')
            else:
                query.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')
    else:
        logger.info(f"Пользователь {user_id} не подписан на все каналы.")
        error_message = (
            "😕 Похоже, ты подписался не на все каналы.\n"
            "Пожалуйста, проверь ещё раз и нажми '✅ Я ПОДПИСАЛСЯ!'.\n"
        )
        query.message.reply_text(error_message, parse_mode='Markdown')
        prompt_subscribe(update, context, message_id=query.message.message_id)

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
        update.message.reply_text("Пожалуйста, введи только числовой код фильма. 🔢")
        return

    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"Пользователь {user_id} не подтвердил подписку. Код {code} сохранен как ожидающий.")
        context.user_data['pending_movie_code'] = code
        prompt_subscribe(update, context)
        return

    logger.info(f"Пользователь {user_id} подтвердил подписку. Обрабатываем код: {code}")
    movie = find_movie_by_code(code)
    if movie:
        update.message.reply_text(f"🎥 Фильм по коду \"{code}\": \"{movie['title']}\"", parse_mode='Markdown')
    else:
        update.message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command & Filters.regex(r'^\d+$'), handle_movie_code))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, введи *только числовой* код фильма. 🔢", parse_mode='Markdown')))

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
    except Exception as e:
        logger.error(f"Не удалось запустить вебхук: {e}")
        raise

    logger.info("Запуск Flask-приложения...")
    app.run(host='0.0.0.0', port=PORT)

if __name__ == "__main__":
    main()