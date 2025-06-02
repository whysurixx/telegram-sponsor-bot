import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN")

# Список ID твоих каналов
CHANNELS = [
    "-1002657330561",
    "-1002243633174",
    "-1002484534545",
    "-1002578865225",
    "-1002617434713",
]

def start(update: Update, context: CallbackContext) -> None:
    user = update.message.from_user
    logger.info(f"User {user.id} started the bot")

    welcome_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим!"
    )
    update.message.reply_text(welcome_text)

    keyboard = [
        [InlineKeyboardButton("Канал 1 — Смотри новинки", url="https://t.me/+8qO35jVzZVs5MjMy")],
        [InlineKeyboardButton("Канал 2 — Лучше фильмы", url="https://t.me/+ZAvb9OTIrU9mOWIy")],
        [InlineKeyboardButton("Канал 3 — Премии и хиты", url="https://t.me/+PAu2GRMZuUU0ZWQy")],
        [InlineKeyboardButton("Канал 4 — Кино без рекламы", url="https://t.me/+kO2CPJZgxediMmZi")],
        [InlineKeyboardButton("Канал 5 — Эксклюзивы", url="https://t.me/+DUDDSAYIDl8yN2Ni")],
        [InlineKeyboardButton("Я ПОДПИСАЛСЯ!", callback_data="check_subscription")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    update.message.reply_text("Упс, подпишись на наших спонсоров и нажми на кнопку ниже!", reply_markup=reply_markup)

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
        query.message.reply_text("Ты подписался на все каналы! Теперь можно продолжить поиск фильма.")
    else:
        query.message.reply_text("Похоже, ты подписался не на все каналы. Проверь ещё раз и нажми 'Я ПОДПИСАЛСЯ!'.")

def main() -> None:
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))

    PORT = int(os.environ.get("PORT", 8443))
    RENDER_EXTERNAL_URL = os.environ.get("RENDER_EXTERNAL_URL")

    updater.start_webhook(listen="0.0.0.0", port=PORT, url_path=TOKEN)
    updater.bot.set_webhook(f"https://{RENDER_EXTERNAL_URL}/{TOKEN}")
    updater.idle()

if __name__ == "__main__":
    main()
