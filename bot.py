import os
import logging
import json
import time
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, Filters
from telegram.error import RetryAfter
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Optional, Dict, List

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 8443))
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"

# Load channels and buttons from environment variables
try:
    CHANNELS = json.loads(os.environ.get("CHANNEL_IDS", "[]"))
    CHANNEL_BUTTONS = json.loads(os.environ.get("CHANNEL_BUTTONS", "[]"))
    if not CHANNELS or not CHANNEL_BUTTONS:
        logger.error("CHANNEL_IDS or CHANNEL_BUTTONS are empty or not set.")
        raise ValueError("CHANNEL_IDS and CHANNEL_BUTTONS must be set in environment variables.")
    if len(CHANNELS) != len(CHANNEL_BUTTONS):
        logger.error("Number of channels and buttons do not match.")
        raise ValueError("Number of CHANNEL_IDS and CHANNEL_BUTTONS must match.")
except json.JSONDecodeError as e:
    logger.error(f"Error parsing JSON in CHANNEL_IDS or CHANNEL_BUTTONS: {e}")
    raise
except ValueError as e:
    logger.error(f"Configuration error for channels: {e}")
    raise

# Validate essential environment variables
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL is not set in environment variables!")
    raise ValueError("WEBHOOK_URL is not set in environment variables!")
if not TOKEN:
    logger.error("BOT_TOKEN is not set in environment variables!")
    raise ValueError("BOT_TOKEN is not set in environment variables!")

# Initialize Google Sheets
sheet = None
try:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
        raise FileNotFoundError(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")

    with open(GOOGLE_CREDENTIALS_PATH, 'r') as f:
        creds_json = json.load(f)

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_json, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key("1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms").sheet1
    logger.info("Google Sheets initialized successfully.")
except Exception as e:
    logger.error(f"Error initializing Google Sheets: {e}")
    raise

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    user = update.message.from_user
    logger.info(f"User {user.id} {user.first_name} started the bot.")
    welcome_text = (
        "Привет! 👋\n"
        "Напиши код фильма, и я помогу тебе узнать его название. 🎬\n\n"
    )
    await send_message_with_retry(update.message, welcome_text)

async def send_message_with_retry(message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Send a message with retry on flood control."""
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        time.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as e:
        logger.warning(f"Failed to send message: {e}")

async def edit_message_with_retry(context, chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edit a message with retry on flood control."""
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        time.sleep(e.retry_after)
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.warning(f"Failed to edit message: {e}")

async def prompt_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: Optional[int] = None) -> None:
    """Prompt user to subscribe to channels."""
    promo_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим! 🚀"
    )
    # Ensure buttons are vertical (one per row)
    keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in CHANNEL_BUTTONS]
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if message_id:
        await edit_message_with_retry(context, update.effective_chat.id, message_id, promo_text, reply_markup)
    else:
        await send_message_with_retry(update.message, promo_text, reply_markup)

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check if the user is subscribed to all required channels."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    bot = context.bot
    unsubscribed_channels = []

    # Check subscription status for each channel
    for channel_id, button in zip(CHANNELS, CHANNEL_BUTTONS):
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                unsubscribed_channels.append(button)
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            unsubscribed_channels.append(button)

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"User {user_id} successfully confirmed subscription.")
        success_text = (
            "🎉 Поздравляю! Ты подписался на все каналы.\n"
            "Теперь можешь отправить код фильма, и я найду его название! 🍿"
        )
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, success_text)

        # Process pending movie code if exists
        if 'pending_movie_code' in context.user_data:
            code = context.user_data.pop('pending_movie_code')
            movie = find_movie_by_code(code)
            result_text = (
                f"🎥 Фильм по коду {code}: {movie['title']}" if movie
                else f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код."
            )
            await send_message_with_retry(query.message, result_text)
    else:
        logger.info(f"User {user_id} is not subscribed to some channels.")
        promo_text = (
            "😕 Похоже, ты не подписан на некоторые каналы.\n"
            "Пожалуйста, подпишись на них и нажми '✅ Я ПОДПИСАЛСЯ!'."
        )
        # Show only unsubscribed channels, one per row
        keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in unsubscribed_channels]
        keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, promo_text, reply_markup)

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    """Find a movie by its code in Google Sheets."""
    if sheet is None:
        logger.error("Google Sheets not initialized. Cannot perform search.")
        return None

    try:
        all_values = sheet.get_all_values()
        for row_data in all_values:
            if not row_data or len(row_data) < 2:
                continue
            sheet_code = row_data[0].strip()
            sheet_title = row_data[1].strip()
            if sheet_code == code:
                logger.info(f"Found movie with code {code}: {sheet_title}")
                return {"code": sheet_code, "title": sheet_title}
        logger.info(f"Movie with code {code} not found.")
        return None
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error: {e}")
        return None
    except Exception as e:
        logger.error(f"Unknown error accessing Google Sheets: {e}")
        return None

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle numeric movie code input."""
    code = update.message.text.strip()
    user_id = update.message.from_user.id

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Пожалуйста, введи только числовой код фильма. 🔢")
        return

    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"User {user_id} has not confirmed subscription. Saving code {code} as pending.")
        context.user_data['pending_movie_code'] = code
        await prompt_subscribe(update, context)
        return

    logger.info(f"User {user_id} confirmed subscription. Processing code: {code}")
    movie = find_movie_by_code(code)
    result_text = (
        f"🎥 Фильм по коду {code}: {movie['title']}" if movie
        else f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код."
    )
    await send_message_with_retry(update.message, result_text)

async def handle_non_numeric_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle non-numeric text input."""
    if update.message.from_user.id == context.bot.id:
        return  # Ignore messages sent by the bot itself
    await send_message_with_retry(update.message, "Пожалуйста, введи *только числовой* код фильма. 🔢")

async def main() -> None:
    """Main function to run the bot."""
    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application.add_handler(MessageHandler(Filters.text & ~Filters.command & Filters.regex(r'^\d+$'), handle_movie_code))
    application.add_handler(MessageHandler(Filters.text & ~Filters.command & ~Filters.regex(r'^\d+$'), handle_non_numeric_text))

    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    logger.info(f"Using port: {PORT}")

    try:
        await application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=full_webhook_url
        )
        logger.info("Webhook started successfully.")
    except Exception as e:
        logger.error(f"Failed to start webhook: {e}")
        raise

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())