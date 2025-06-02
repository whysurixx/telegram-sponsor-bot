import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, Filters
from flask import Flask

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация Flask
app = Flask(__name__)

# Получаем токен и URL вебхука из переменных окружения
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # Например https://telegram-sponsor-bot.onrender.com
PORT = int(os.environ.get("PORT", 443))  # Обычно 443 для HTTPS

# Проверка переменных окружения
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL не задан в переменных окружения!")
    raise ValueError("WEBHOOK_URL не задан в переменных окружения!")
if not TOKEN:
    logger.error("BOT_TOKEN не задан в переменных окружения!")
    raise ValueError("BOT_TOKEN не задан в переменных окружения!")

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
        "Напиши код фильма, и я помогу тебе узнать его название и детали. 🎬\n\n"
    )
    update.message.reply_text(welcome_text)

def prompt_subscribe(update: Update, context: CallbackContext) -> None:
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
        [InlineKeyboardButton("Я ПОДПИСАЛСЯ!", callback_data="check_subscription")],
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

        query.message.reply_text("Похоже, ты подписался не на все каналы. Проверь ещё раз и нажми 'Я ПОДПИСАЛСЯ!'.")

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))

    # Обработчик всех текстовых сообщений (кроме команд) — выводим подписку
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, prompt_subscribe))

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
