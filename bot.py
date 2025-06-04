import os
import logging
import json
import time
import random
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes
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
PORT = int(os.environ.get("PORT", 10000))
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

# Initialize Telegram application
application_tg = Application.builder().token(TOKEN).build()

# List of random emojis for positive responses
POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']

# Custom keyboard
def get_main_keyboard():
    keyboard = [
        [KeyboardButton("🔍 Поиск фильма")],
        [KeyboardButton("👥 Реферальная система")],
        [KeyboardButton("❓ Как работает бот")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command."""
    user = update.message.from_user
    logger.info(f"User {user.id} {user.first_name} started the bot.")
    welcome_text = (
        "Привет, *киноман*! 🎥✨\n"
        "Я твой личный гид в мир кино! 🍿\n"
        "Выбери действие ниже, и давай начнём приключение! 😎"
    )
    await send_message_with_retry(update.message, welcome_text, reply_markup=get_main_keyboard())

async def send_message_with_retry(message, text: str, reply_markup: Optional[ReplyKeyboardMarkup | InlineKeyboardMarkup] = None) -> None:
    """Send a message with retry on flood control."""
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        time.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as e:
        logger.warning(f"Failed to send message: {e}")

async def edit_message_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
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
        "Эй, *кинофан*! 🎬\n"
        "Чтобы продолжить, подпишись на наших крутых спонсоров! 🚀\n"
        "Жми на кнопки ниже и затем на *Я ПОДПИСАЛСЯ!* 😎"
    )
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
            "Ура, *ты молодец*! 🎉\n"
            "Теперь ты подписан на все каналы! 😍\n"
            f"{'Введи *числовой код* для поиска фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Выбери действие ниже! 😎'}"
        )
        reply_markup = get_main_keyboard() if not context.user_data.get('awaiting_code', False) else None
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, success_text, reply_markup)
    else:
        logger.info(f"User {user_id} is not subscribed to some channels.")
        promo_text = (
            "Ой-ой! 😕 Кажется, ты ещё не подписан на *все каналы*! \n"
            "Подпишись на них и снова нажми *Я ПОДПИСАЛСЯ!* 🚀"
        )
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

    if not context.user_data.get('awaiting_code', False):
        logger.info(f"User {user_id} sent code without activating search mode.")
        await send_message_with_retry(update.message, "Эй, *киноман*! 😅 Нажми *🔍 Поиск фильма* и затем введи код! 🍿", reply_markup=get_main_keyboard())
        return

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Эй, мне нужен *числовой код*! 😅 Введи только цифры, пожалуйста! 🔢")
        return

    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"User {user_id} has not confirmed subscription. Prompting to subscribe.")
        await prompt_subscribe(update, context)
        return

    logger.info(f"User {user_id} confirmed subscription. Processing code: {code}")
    movie = find_movie_by_code(code)
    context.user_data['awaiting_code'] = False
    result_text = (
        f"*Вот твой фильм!* 🎥 Код {code}: *{movie['title']}* {random.choice(POSITIVE_EMOJIS)}" if movie
        else f"Ой, фильм с кодом *{code}* не найден! 😢 Попробуем ещё раз? 🔍"
    )
    await send_message_with_retry(update.message, result_text, reply_markup=get_main_keyboard())

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle custom button presses."""
    user_id = update.message.from_user.id
    text = update.message.text

    if text == "🔍 Поиск фильма":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Search without subscription.")
            await prompt_subscribe(update, context)
            return
        context.user_data['awaiting_code'] = True
        await send_message_with_retry(update.message, "Круто! 😎 Введи *числовой код* фильма! 🍿")
    elif text == "👥 Реферальная система":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Referral without subscription.")
            await prompt_subscribe(update, context)
            return
        await send_message_with_retry(update.message, "Реферальная система пока в разработке! 😅 Скоро будет что-то крутое! 🚀", reply_markup=get_main_keyboard())
    elif text == "❓ Как работает бот":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed How-to without subscription.")
            await prompt_subscribe(update, context)
            return
        await send_message_with_retry(update.message, "Я помогу найти фильм по коду! 🎥 Просто нажми *🔍 Поиск фильма*, подпишись на каналы и введи код! 😊", reply_markup=get_main_keyboard())
    else:
        logger.info(f"User {user_id} sent unknown command: {text}")
        await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Выбери действие из меню ниже! 👇", reply_markup=get_main_keyboard())

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle non-button text input."""
    if update.message.from_user.id == context.bot.id:
        return
    logger.info(f"User {update.message.from_user.id} sent non-button text: {update.message.text}")
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Выбери действие из меню ниже! 👇", reply_markup=get_main_keyboard())

# Define the webhook endpoint
async def webhook_endpoint(request):
    try:
        body = await request.body()
        update = Update.de_json(json.loads(body.decode()), application_tg.bot)
        if update:
            await application_tg.process_update(update)
        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return PlainTextResponse("Error", status_code=500)

# Define the ASGI application
app = Starlette(
    routes=[
        Route(f"/{TOKEN}", endpoint=webhook_endpoint, methods=["POST"])
    ]
)

async def startup():
    # Add handlers to the application
    application_tg.add_handler(CommandHandler("start", start))
    application_tg.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\d+$'), handle_movie_code))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\d+$'), handle_non_button_text))

    # Initialize the application
    await application_tg.initialize()

    # Set the webhook
    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    try:
        await application_tg.bot.set_webhook(url=full_webhook_url)
        logger.info("Webhook set successfully.")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

    # Start the application
    await application_tg.start()
    logger.info("Application started successfully.")

# Add startup event handler
app.add_event_handler("startup", startup)

# Optional: Add shutdown event handler
async def shutdown():
    await application_tg.stop()
    await application_tg.shutdown()
    logger.info("Application shut down successfully.")

app.add_event_handler("shutdown", shutdown)