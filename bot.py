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

# Получаем токен и URL вебхука из переменных окружения
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 443))

# Проверка переменных окружения
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL не задан в переменных окружения!")
    raise ValueError("WEBHOOK_URL не задан в переменных окружения!")
if not TOKEN:
    logger.error("BOT_TOKEN не задан в переменных окружения!")
    raise ValueError("BOT_TOKEN не задан в переменных окружения!")

# Настройка Google Sheets
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS")
if not GOOGLE_CREDENTIALS:
    logger.error("GOOGLE_CREDENTIALS не задан в переменных окружения!")
    raise ValueError("GOOGLE_CREDENTIALS не задан в переменных окружения!")

try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS), scope)
    client = gspread.authorize(creds)
    sheet = client.open("MovieDatabase").sheet1
except Exception as e:
    logger.error(f"Ошибка при инициализации Google Sheets: {e}")
    raise

CHANNELS = [
    "-1002657330561",
    "-1002243633174",
    "-1002484534545",
    "-1002578865225",
    "-1002617434713",
]

# Простой обработчик для корневого URL (healthcheck)
@app.route('/')
def health_check():
    return "Bot is alive", 200

def start(update: Update, context: CallbackContext) -> None:
    user = update.message.from_user
    logger.info(f"User {user.id} started the bot")
    welcome_text = (
        "Привет! 👋\n"
        "Напиши код фильма, и я помогу тебе узнать его название. 🎬\n\n"
    )
    update.message.reply_text(welcome_text)

def prompt_subscribe(update: Update, context: CallbackContext) -> None:
    if context.user_data.get('subscribed'):
        update.message.reply_text("Фильм не найден! Попробуй другой код.")
        return
    promo_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим!"
    )
    keyboard = [
        [InlineKeyboardButton("Канал 1 — Смотри новинки", url="https://t.me/+8qO35jVzZVs5MjMy")],
        [InlineKeyboardButton("Канал 2 — Лучше фильмы", url="https://t.me/+ZAvb9OTIrU9mOWIy")],
        [InlineKeyboardButton("Канал 3 — Премии и хиты", url="https://t.me/+PAu2GRMZuUU0ZWQy")],
        [InlineKeyboardButton("Канал 4 — Кино без рекламы", url="https://t.me/+kO2CPJZgxediMmZi")],
        [InlineKeyboardButton("Канал 5 — Эксклюзивы", url="https://t.me/+DUDDSAYIDl8yN2Ni")],
        [InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text(promo_text, reply_markup=reply_markup)

def check_subscription(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()
    user_id = query.from_user.id
    bot = context.bot
    all_subscribed = True
    for channel in CHANNELS:
        try:
            member = bot.get_chat_member(chat_id=channel, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                all_subscribed = False
                break
        except Exception as e:
            logger.error(f"Error checking subscription for {channel}: {e}")
            all_subscribed = False
            break
    if all_subscribed:
        context.user_data['subscribed'] = True
        query.message.reply_text(
            "🎉 Поздравляю! Ты подписался на все каналы.\nТеперь можешь отправить код фильма, и я найду его название! 🍿"
        )
    else:
        query.message.reply_text(
            "😕 Похоже, ты подписался не на все каналы.\nПроверь ещё раз и нажми 'Я ПОДПИСАЛСЯ!'."
        )

def find_movie_by_code(code: str) -> dict:
    try:
        data = sheet.get_all_records()
        for row in data:
            if row.get("Код") == code:
                return {"code": row["Код"], "title": row["Название"]}
        return None
    except Exception as e:
        logger.error(f"Ошибка при доступе к Google Sheets: {e}")
        return None

def handle_movie_code(update: Update, context: CallbackContext) -> None:
    code = update.message.text.strip()
    if not context.user_data.get('subscribed'):
        prompt_subscribe(update, context)
        return
    movie = find_movie_by_code(code)
    if movie:
        update.message.reply_text(f"Фильм: {movie['title']}")
    else:
        update.message.reply_text("Фильм не найден! Попробуй другой код.")

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_movie_code))
    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    logger.info(f"Using port: {PORT}")
    try:
        updater.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=full_webhook_url
        )
        logger.info("Webhook started successfully")
    except Exception as e:
        logger.error(f"Failed to start webhook: {e}")
        raise
    app.run(host='0.0.0.0', port=PORT)

if __name__ == "__main__":
    main()