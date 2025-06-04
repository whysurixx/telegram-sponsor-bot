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
PORT = int(os.environ.get("PORT", 10000))  # Use Render's PORT environment variable
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"
BOT_USERNAME = os.environ.get("BOT_USERNAME")  # e.g., @YourBotName
MOVIE_SHEET_ID = "1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms"
USER_SHEET_ID = "1XYFfqmC5boLBB8HjjkyKA6AyN3WNCKy6U8LEmN8KvrA"

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
if not BOT_USERNAME:
    logger.error("BOT_USERNAME is not set in environment variables!")
    raise ValueError("BOT_USERNAME is not set in environment variables!")

# Initialize Google Sheets
movie_sheet = None
user_sheet = None
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
    
    # Movie sheet
    movie_spreadsheet = client.open_by_key(MOVIE_SHEET_ID)
    movie_sheet = movie_spreadsheet.sheet1  # Movie data sheet
    logger.info(f"Movie sheet initialized successfully (ID: {MOVIE_SHEET_ID}).")
    
    # User sheet
    user_spreadsheet = client.open_by_key(USER_SHEET_ID)
    try:
        user_sheet = user_spreadsheet.worksheet("Users")  # User data sheet
    except gspread.exceptions.WorksheetNotFound:
        user_sheet = user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
        user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
        logger.info(f"Created new 'Users' worksheet in user spreadsheet (ID: {USER_SHEET_ID}).")
    logger.info(f"User sheet initialized successfully (ID: {USER_SHEET_ID}).")
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
    """Handle the /start command, including referral links."""
    user = update.message.from_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    logger.info(f"User {user_id} {first_name} started the bot.")

    # Check for referral
    referrer_id = None
    if update.message.text.startswith("/start invite_"):
        try:
            referrer_id = int(update.message.text.split("invite_")[1])
            if referrer_id == user_id:
                logger.info(f"User {user_id} tried to invite themselves.")
                referrer_id = None
        except (IndexError, ValueError):
            logger.warning(f"Invalid referral link for user {user_id}: {update.message.text}")
            referrer_id = None

    # Register or update user in Users sheet
    user_data = get_user_data(user_id)
    if not user_data:
        # New user: initialize with 5 search queries
        add_user(user_id, username, first_name, search_queries=5, invited_users=0)
        logger.info(f"Registered new user {user_id} with 5 search queries.")
    else:
        # Update username and first_name if changed
        update_user(user_id, username=username, first_name=first_name)
        logger.info(f"Updated existing user {user_id}.")

    # Process referral if valid
    if referrer_id and user_data is None:  # Only for new users
        referrer_data = get_user_data(referrer_id)
        if referrer_data:
            # Increment referrer's invited_users and add 2 search queries
            update_user(
                referrer_id,
                invited_users=int(referrer_data.get("invited_users", 0)) + 1,
                search_queries=int(referrer_data.get("search_queries", 0)) + 2
            )
            logger.info(f"User {user_id} invited by {referrer_id}. Updated referrer's data.")

    welcome_text = (
        "Привет, *киноман*! 🎬\n"
        "Добро пожаловать в твой личный кино-гид! 🍿 Я помогу найти фильмы по секретным кодам и открою мир кино! 🚀\n"
        f"{'Ты был приглашён другом! 😎 ' if referrer_id else ''}"
        "Выбери действие в меню ниже, и начнём приключение! 😎"
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
        "Эй, *кинофан*! 🎥\n"
        "Чтобы открыть доступ к фильмам, подпишись на наших крутых спонсоров! 🌟\n"
        "Кликни на кнопки ниже, подпишись и нажми *Я ПОДПИСАЛСЯ!* 😎"
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
            "Супер, *ты в деле*! 🎉\n"
            "Вы подписаны на все каналы! 😍 Теперь ты можешь продолжить работать с ботом!\n"
            f"{'Введи *числовой код* для поиска фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Выбери действие в меню ниже! 😎'}"
        )
        # Send a new message instead of editing to avoid inline keyboard issues
        await send_message_with_retry(
            query.message,
            success_text,
            reply_markup=get_main_keyboard() if not context.user_data.get('awaiting_code', False) else None
        )
        # Optionally, delete the original inline keyboard message to clean up
        try:
            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
        except Exception as e:
            logger.warning(f"Failed to delete subscription prompt message: {e}")
    else:
        logger.info(f"User {user_id} is not subscribed to some channels.")
        promo_text = (
            "Ой-ой! 😕 Похоже, ты пропустил пару каналов! 🚨\n"
            "Подпишись на все каналы ниже и снова нажми *Я ПОДПИСАЛСЯ!* 🌟"
        )
        keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in unsubscribed_channels]
        keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, promo_text, reply_markup)

def get_user_data(user_id: int) -> Optional[Dict[str, str]]:
    """Retrieve user data from Users sheet."""
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return None
    try:
        all_values = user_sheet.get_all_values()[1:]  # Skip header
        for row in all_values:
            if not row or len(row) < 1:
                continue
            if row[0] == str(user_id):
                return {
                    "user_id": row[0],
                    "username": row[1] if len(row) > 1 else "",
                    "first_name": row[2] if len(row) > 2 else "",
                    "search_queries": row[3] if len(row) > 3 else "0",
                    "invited_users": row[4] if len(row) > 4 else "0"
                }
        return None
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in get_user_data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unknown error in get_user_data: {e}")
        return None

def add_user(user_id: int, username: str, first_name: str, search_queries: int, invited_users: int) -> None:
    """Add a new user to Users sheet."""
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    try:
        user_sheet.append_row([
            str(user_id),
            username,
            first_name,
            str(search_queries),
            str(invited_users)
        ])
        logger.info(f"Added user {user_id} to Users sheet.")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in add_user: {e}")
    except Exception as e:
        logger.error(f"Unknown error in add_user: {e}")

def update_user(user_id: int, **kwargs) -> None:
    """Update user data in Users sheet."""
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    try:
        all_values = user_sheet.get_all_values()
        for idx, row in enumerate(all_values[1:], start=2):  # Skip header
            if not row or len(row) < 1 or row[0] != str(user_id):
                continue
            updates = {
                "username": row[1] if len(row) > 1 else "",
                "first_name": row[2] if len(row) > 2 else "",
                "search_queries": row[3] if len(row) > 3 else "0",
                "invited_users": row[4] if len(row) > 4 else "0"
            }
            updates.update(kwargs)
            user_sheet.update(f"A{idx}:E{idx}", [[
                str(user_id),
                updates["username"],
                updates["first_name"],
                str(updates["search_queries"]),
                str(updates["invited_users"])
            ]])
            logger.info(f"Updated user {user_id} in Users sheet.")
            return
        logger.warning(f"User {user_id} not found for update.")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in update_user: {e}")
    except Exception as e:
        logger.error(f"Unknown error in update_user: {e}")

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    """Find a movie by its code in Google Sheets."""
    if movie_sheet is None:
        logger.error("Movie sheet not initialized. Cannot perform search.")
        return None

    try:
        all_values = movie_sheet.get_all_values()
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
        await send_message_with_retry(update.message, "Эй, *киноман*! 😅 Сначала нажми *🔍 Поиск фильма*, а потом введи код! 🍿", reply_markup=get_main_keyboard())
        return

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Ой, нужен *только числовой код*! 😊 Введи цифры, и мы найдём твой фильм! 🔢")
        return

    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"User {user_id} has not confirmed subscription. Prompting to subscribe.")
        await prompt_subscribe(update, context)
        return

    # Check search queries
    user_data = get_user_data(user_id)
    if not user_data:
        logger.error(f"User {user_id} not found in Users sheet.")
        await send_message_with_retry(update.message, "Упс, что-то пошло не так! 😢 Попробуй снова или напиши в поддержку.", reply_markup=get_main_keyboard())
        return
    search_queries = int(user_data.get("search_queries", 0))
    if search_queries <= 0:
        logger.info(f"User {user_id} has no remaining search queries.")
        await send_message_with_retry(
            update.message,
            "Ой, у тебя закончились поиски! 😕 Приглашай друзей через *👥 Реферальная система* и получай +2 поиска за каждого! 🚀",
            reply_markup=get_main_keyboard()
        )
        context.user_data['awaiting_code'] = False
        return

    logger.info(f"User {user_id} confirmed subscription. Processing code: {code}")
    movie = find_movie_by_code(code)
    context.user_data['awaiting_code'] = False
    if movie:
        # Decrement search queries
        update_user(user_id, search_queries=search_queries - 1)
        result_text = (
            f"*Бинго!* 🎥 Код {code}: *{movie['title']}* {random.choice(POSITIVE_EMOJIS)}\n"
            f"Осталось поисков: *{search_queries - 1}* 🔍\n"
            "Хочешь найти ещё один шедевр? Нажми *🔍 Поиск фильма*! 🍿"
        )
    else:
        result_text = f"Упс, фильм с кодом *{code}* не найден! 😢 Проверь код или попробуй другой! 🔍"
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
        await send_message_with_retry(update.message, "Отлично! 😎 Введи *числовой код* фильма, и я найду его для тебя! 🍿")
    elif text == "👥 Реферальная система":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Referral without subscription.")
            await prompt_subscribe(update, context)
            return
        user_data = get_user_data(user_id)
        if not user_data:
            logger.error(f"User {user_id} not found in Users sheet.")
            await send_message_with_retry(update.message, "Упс, что-то пошло не так! 😢 Попробуй снова или напиши в поддержку.", reply_markup=get_main_keyboard())
            return
        referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
        invited_users = user_data.get("invited_users", "0")
        search_queries = user_data.get("search_queries", "0")
        referral_text = (
            "🔥 *Реферальная система* 🔥\n\n"
            "Приглашай друзей и получай *+2 поиска* за каждого, кто начнёт использовать бота по твоей ссылке! 🚀\n"
            f"Твоя реферальная ссылка: `{referral_link}`\n"
            "Скопируй её и отправь друзьям! 😎\n\n"
            f"👥 *Количество добавленных пользователей*: *{invited_users}*\n"
            f"🔍 *Количество оставшихся запросов*: *{search_queries}*"
        )
        await send_message_with_retry(update.message, referral_text, reply_markup=get_main_keyboard())
    elif text == "❓ Как работает бот":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed How-to without subscription.")
            await prompt_subscribe(update, context)
            return
        how_it_works_text = (
            "🎬 *Как работает наш кино-бот?* 🎥\n\n"
            "Я — твой личный помощник в мире кино! 🍿 Моя главная задача — помочь тебе найти фильмы по секретным числовым кодам. Вот как это работает:\n\n"
            "🔍 *Поиск фильмов*:\n"
            "1. Нажми на кнопку *🔍 Поиск фильма* в меню.\n"
            "2. Подпишись на наши крутые спонсорские каналы (это обязательно! 😎).\n"
            "3. Введи *числовой код* фильма (только цифры!).\n"
            "4. Я найду фильм в нашей базе и покажу его название! 🎉\n\n"
            "👥 *Реферальная система*:\n"
            "- У тебя есть *5 бесплатных поисков* при старте! 🚀\n"
            "- Приглашай друзей в бота, и за каждого нового пользователя ты получишь *+2 поиска*! 🌟\n"
            "- Если поиски закончились, приглашай друзей, чтобы продолжить! 😍\n\n"
            "❗ *Важно*:\n"
            "- Подписка на каналы обязательна для доступа к поиску.\n"
            "- Вводи только числовые коды после нажатия *🔍 Поиск фильма*.\n"
            "- Если что-то пошло не так, просто следуй подсказкам, и я помогу! 😊\n\n"
            "Готов к кино-приключению? Выбери действие в меню! 👇"
        )
        await send_message_with_retry(update.message, how_it_works_text, reply_markup=get_main_keyboard())
    else:
        logger.info(f"User {user_id} sent unknown command: {text}")
        await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇", reply_markup=get_main_keyboard())

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle non-button text input."""
    if update.message.from_user.id == context.bot.id:
        return
    logger.info(f"User {update.message.from_user.id} sent non-button text: {update.message.text}")
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇", reply_markup=get_main_keyboard())

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

# Define a health check endpoint
async def health_check(request):
    return PlainTextResponse("OK", status_code=200)

# Define the ASGI application
app = Starlette(
    routes=[
        Route(f"/{TOKEN}", endpoint=webhook_endpoint, methods=["POST"]),
        Route("/", endpoint=health_check, methods=["GET", "HEAD"])  # Health check endpoint
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