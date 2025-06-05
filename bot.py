import os
import logging
import json
import time
import random
import asyncio
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ChatJoinRequestHandler, ContextTypes
from telegram.ext import filters
from telegram.error import RetryAfter
from google.oauth2.service_account import Credentials
import gspread
from typing import Optional, Dict, List
import telegram  # Для логирования версии

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Логируем версию python-telegram-bot
logger.info(f"python-telegram-bot version: {telegram.__version__}")

# Configuration from environment variables
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"
BOT_USERNAME = os.environ.get("BOT_USERNAME")

if BOT_USERNAME.startswith("@"):
    BOT_USERNAME = BOT_USERNAME[1:]
    logger.info(f"Removed '@' from BOT_USERNAME: {BOT_USERNAME}")

MOVIE_SHEET_ID = "1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms"
USER_SHEET_ID = "1XYFfqmC5boLBB8HjjkyKA6AyN3WNCKy6U8LEmN8KvrA"
JOIN_REQUESTS_SHEET_ID = "1OKteXrJFjKC7B2qbwoVkt-rfbkCGdYt2VjMcZRjtQ84"

# Load channels and buttons
try:
    CHANNELS = json.loads(os.environ.get("CHANNEL_IDS", "[]"))
    CHANNEL_BUTTONS = json.loads(os.environ.get("CHANNEL_BUTTONS", "[]"))
    if not CHANNELS or not CHANNEL_BUTTONS:
        logger.error("CHANNEL_IDS or CHANNEL_BUTTONS are empty or not set.")
        raise ValueError("CHANNEL_IDS and CHANNEL_BUTTONS must be set.")
    if len(CHANNELS) != len(CHANNEL_BUTTONS):
        logger.error("Number of channels and buttons do not match.")
        raise ValueError("Number of CHANNEL_IDS and CHANNEL_BUTTONS must match.")
except json.JSONDecodeError as e:
    logger.error(f"Error parsing JSON in CHANNEL_IDS or CHANNEL_BUTTONS: {e}")
    raise
except ValueError as e:
    logger.error(f"Configuration error for channels: {e}")
    raise

# Validate environment variables
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL is not set!")
    raise ValueError("WEBHOOK_URL is not set!")
if not TOKEN:
    logger.error("BOT_TOKEN is not set!")
    raise ValueError("BOT_TOKEN is not set!")
if not BOT_USERNAME:
    logger.error("BOT_USERNAME is not set!")
    raise ValueError("BOT_USERNAME is not set!")

# Initialize Google Sheets
movie_sheet = None
user_sheet = None
join_requests_sheet = None
MOVIE_DICT = {}  # Cache for movie data
try:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
        raise FileNotFoundError(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    
    # Movie sheet
    movie_spreadsheet = client.open_by_key(MOVIE_SHEET_ID)
    movie_sheet = movie_spreadsheet.sheet1
    logger.info(f"Movie sheet initialized (ID: {MOVIE_SHEET_ID}).")
    
    # User sheet
    user_spreadsheet = client.open_by_key(USER_SHEET_ID)
    try:
        user_sheet = user_spreadsheet.worksheet("Users")
    except gspread.exceptions.WorksheetNotFound:
        user_sheet = user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
        user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
        logger.info(f"Created new 'Users' worksheet (ID: {USER_SHEET_ID}).")
    logger.info(f"User sheet initialized (ID: {USER_SHEET_ID}).")
    
    # Join Requests sheet
    join_requests_spreadsheet = client.open_by_key(JOIN_REQUESTS_SHEET_ID)
    try:
        join_requests_sheet = join_requests_spreadsheet.worksheet("JoinRequests")
    except gspread.exceptions.WorksheetNotFound:
        join_requests-sheet = join_requests_spreadsheet.add_worksheet(title="JoinRequests", rows=1000, cols=2)
        join_requests_sheet.append_row(["user_id", "channel_id"])
        logger.info(f"Created new 'JoinRequests' worksheet (ID: {JOIN_REQUESTS_SHEET_ID}).")
    logger.info(f"Join Requests sheet initialized (ID: {JOIN_REQUESTS_SHEET_ID}).")
except Exception as e:
    logger.error(f"Error initializing Google Sheets: {e}")
    raise

# Initialize Telegram application
application_tg = Application.builder().token(TOKEN).build()

# Random emojis for responses
POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']

# Custom reply keyboard
def get_main_reply_keyboard():
    keyboard = [
        [KeyboardButton("🔍 Поиск фильма"), KeyboardButton("👥 Реферальная система")],
        [KeyboardButton("❓ Как работает бот")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command, including referral links."""
    user = update.message.from_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    logger.info(f"User {user_id} {first_name} started the bot with message: {update.message.text}")

    # Handle referral
    referrer_id = None
    if update.message.text.startswith("/start invite_"):
        try:
            referrer_id = int(update.message.text.split("invite_")[1])
            if referrer_id == user_id:
                logger.info(f"User {user_id} tried to invite themselves.")
                await send_message_with_retry(update.message, "❌ Вы не можете пригласить себя!", reply_markup=get_main_reply_keyboard())
                return
            else:
                logger.info(f"Referral detected for user {user_id} from referrer {referrer_id}")
                context.user_data['referrer_id'] = referrer_id
        except (IndexError, ValueError):
            logger.warning(f"Invalid referral link for user {user_id}: {update.message.text}")
            referrer_id = None

    # Register or update user
    user_data = get_user_data(user_id)
    if not user_data:
        try:
            add_user(user_id, username, first_name, search_queries=5, invited_users=0)
            logger.info(f"Added user {user_id} to Users sheet with 5 search queries.")
        except Exception as e:
            logger.error(f"Failed to add user {user_id} to Users sheet: {e}")
    else:
        update_user(user_id, username=username, first_name=first_name)
        logger.info(f"Updated existing user {user_id}.")

    welcome_text = (
        "Привет, *киноман*! 🎬\n"
 memos/2025-06-05-22-26-05.txt
        "Добро пожаловать в твой личный кино-гид! 🍿 Я помогу найти фильмы по секретным кодам и открою мир кино! 🚀\n"
        f"{'Ты был приглашён другом! 😎 ' if referrer_id else ''}"
        "Выбери действие в меню ниже, и начнём приключение! 😎"
    )
    await send_message_with_retry(update.message, welcome_text, reply_markup=get_main_reply_keyboard())

async def send_message_with_retry(message, text: str, reply_markup=None) -> None:
    """Send a message with retry on flood control."""
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        time.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as-standard
        logger.error(f"Failed to send message: {e}, Response: {e.__dict__}")

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
        logger.error(f"Failed to edit message: {e}, Response: {e.__dict__}")

async def prompt_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: Optional[int] = None) -> None:
    """Prompt user to subscribe to channels."""
    promo_text = (
        "Эй, *кинофан*! 🎥\n"
        "Чтобы открыть доступ к фильмам, подпишись на наших крутых спонсоров! 🌟\n"
        "Кликни на кнопки ниже, подпишись или отправь заявку на вступление и нажми *Я ПОДПИСАЛСЯ!* 😎"
    )
    keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in CHANNEL_BUTTONS]
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if message_id:
        await edit_message_with_retry(context, update.effective_chat.id, message_id, promo_text, reply_markup)
    else:
        await send_message_with_retry(update.message, promo_text, reply_markup=reply_markup)

def has_sent_join_request(user_id: int, channel_id: int) -> bool:
    """Check if user has sent a join request to the channel."""
    if join_requests_sheet is None:
        logger.error("JoinRequests sheet not initialized.")
        return False
    try:
        all_values = join_requests_sheet.get_all_values()[1:]  # Skip header
        for row in all_values:
            if row and len(row) >= 2 and row[0] == str(user_id) and row[1] == str(channel_id):
                return True
        return False
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in has_sent_join_request: {e}")
        return False
    except Exception as e:
        logger.error(f"Unknown error in has_sent_join_request: {e}")
        return False

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Check if the user is subscribed to all required channels or has sent join requests."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    bot = context.bot
    unsubscribed_channels = []

    for channel_id, button in zip(CHANNELS, CHANNEL_BUTTONS):
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status in ["member", "administrator", "creator"]:
                continue
            elif has_sent_join_request(user_id, channel_id):
                continue
            else:
                unsubscribed_channels.append(button)
            await asyncio.sleep(0.1)  # Add 100ms delay to avoid rate limits
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            unsubscribed_channels.append(button)

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"User {user_id} successfully confirmed subscription for all channels.")

        # Process referral reward
        referrer_id = context.user_data.get('referrer_id')
        if referrer_id:
            referrer_data = get_user_data(referrer_id)
            if referrer_data:
                new_invited_users = int(referrer_data.get("invited_users", 0)) + 1
                new_search_queries = int(referrer_data.get("search_queries", "0")) + 2
                update_user(
                    user_id=referrer_id,
                    invited_users=new_invited_users,
                    search_queries=new_search_queries
                )
                logger.info(f"Added 2 search queries to referrer {referrer_id} for inviting user {user_id}")
                try:
                    await bot.send_message(
                        user_id=referrer_id,
                        text=f"User {user_id} successfully confirmed subscription. Вам начислено *+2 поиска*!",
                        parse_mode='Markdown'
                    )
                    logger.info(f"Sent referral reward notification to referrer {referrer_id}")
                except Exception as e:
                    logger.error(f"Failed to send referral reward notification to {referrer_id}: {e}")

                del context.user_data['referrer_id']

        success_text = (
            "Супер, *ты в деле*! 🎉\n"
            "Вы подписаны на все каналы или отправили заявки! 😍 Теперь ты можешь продолжить работать с ботом!\n"
            f"{'Введи *числовой код* для поиска фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Выбери действие в меню ниже! 😎'}"
        )
        reply_markup = get_main_reply_keyboard() if not context.user_data.get('awaiting_code', False) else ReplyKeyboardRemove()

        await asyncio.sleep(0.5)
        await edit_message_with_retry(
            context,
            query.message.chat_id,
            query.message.message_id,
            success_text,
            reply_markup=None  # Inline keyboard not needed here
        )
        if not context.user_data.get('awaiting_code', False):
            await send_message_with_retry(
                query.message,
                "Что дальше? 😎",
                reply_markup=reply_markup
            )
    else:
        logger.info(f"User {user_id} is not subscribed to some channels.")
        promo_text = (
            "Ой-ой! 😜 Похоже, ты пропустил пару каналов! 🚨\n"
            "Подпишись или отправь заявку на вступление на все каналы ниже и снова нажми *Я ПОДПИСАЛСЯ!* 🌟"
        )
        keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in unsubscribed_channels]
        keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await edit_message_with_retry(
            context,
            query.message.chat_id,
            query.message.message_id,
            promo_text,
            reply_markup=reply_markup
        )

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
        row_to_add = [str(user_id), username, first_name, str(search_queries), str(invited_users)]
        user_sheet.append_row(row_to_add)
        logger.info(f"Added user {user_id} to Users sheet with {search_queries} search queries.")
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

def add_join_request(user_id: int, channel_id: int) -> None:
    """Add a join request to JoinRequests sheet."""
    if join_requests_sheet is None:
        logger.error("JoinRequests sheet not initialized.")
        return
    try:
        all_values = join_requests_sheet.get_all_values()[1:]  # Skip header
        for row in all_values:
            if row and len(row) >= 2 and row[0] == str(user_id) and row[1] == str(channel_id):
                return  # Already exists
        join_requests_sheet.append_row([str(user_id), str(channel_id)])
        logger.info(f"Added join request for user {user_id} to channel {channel_id}")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in add_join_request: {e}")
    except Exception as e:
        logger.error(f"Unknown error in add_join_request: {e}")

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    """Find a movie by its code in cached MOVIE_DICT."""
    if code in MOVIE_DICT:
        return {"code": code, "title": MOVIE_DICT[code]}
    return None

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle numeric movie code input."""
    code = update.message.text.strip()
    user_id = update.message.from_user.id

    if not context.user_data.get('awaiting_code', False):
        logger.info(f"User {user_id} sent code without activating search mode.")
        await send_message_with_retry(update.message, "Эй, *киноман*! 😅 Сначала нажми *🔍 Поиск фильма*, а потом введи код! 🍿", reply_markup=get_main_reply_keyboard())
        return

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Ой, нужен *только числовой код*! 😊 Введи цифры, и мы найдём твой фильм! 🔢", reply_markup=ReplyKeyboardRemove())
        return

    # Check search queries
    user_data = get_user_data(user_id)
    if not user_data:
        logger.error(f"User {user_id} not found in Users sheet.")
        await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота или напиши в поддержку.", reply_markup=get_main_reply_keyboard())
        return
    search_queries = int(user_data.get("search_queries", 0))
    if search_queries <= 0:
        logger.info(f"User {user_id} has no remaining search queries.")
        await send_message_with_retry(
            update.message,
            "Ой, у тебя закончились поиски! 😕 Приглашай друзей через *👥 Реферальная система* и получай +2 поиска за каждого! 🚀",
            reply_markup=get_main_reply_keyboard()
        )
        context.user_data['awaiting_code'] = False
        return

    logger.info(f"User {user_id} processing code: {code}")
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
    await send_message_with_retry(update.message, result_text, reply_markup=get_main_reply_keyboard())

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle custom button presses from reply keyboard."""
    if update.message and update.message.from_user:
        user_id = update.message.from_user.id
        text = update.message.text

        if text == "🔍 Поиск фильма":
            if not context.user_data.get('subscription_confirmed', False):
                logger.info(f"User {user_id} pressed Search without subscription.")
                await prompt_subscribe(update, context)
                return
            context.user_data['awaiting_code'] = True
            await send_message_with_retry(update.message, "Отлично! 😎 Введи *числовой код* фильма, и я найду его для тебя! 🍿", reply_markup=ReplyKeyboardRemove())
        elif text == "👥 Реферальная система":
            if not context.user_data.get('subscription_confirmed', False):
                logger.info(f"User {user_id} pressed Referral without subscription.")
                await prompt_subscribe(update, context)
                return
            user_data = get_user_data(user_id)
            if not user_data:
                logger.error(f"User {user_id} not found in Users sheet.")
                await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота или напиши в поддержку.", reply_markup=get_main_reply_keyboard())
                return
            referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
            logger.info(f"Generated referral link for user {user_id}: {referral_link}")
            invited_users = user_data.get("invited_users", "0")
            search_queries = user_data.get("search_queries", "0")
            referral_text = (
                "🔥 *Реферальная система* 🔥\n\n"
                "Приглашай друзей и получай *+2 поиска* за каждого, кто перейдёт по твоей ссылке и подпишется на наши каналы! 🚀\n\n"
                f"Твоя реферальная ссылка: `{referral_link}`\n"
                "Нажми на ссылку выше, чтобы скопировать её, и отправь друзьям! 😎\n\n"
                f"👥 *Количество добавленных пользователей*: *{invited_users}*\n"
                f"🔍 *Количество оставшихся запросов*: *{search_queries}*"
            )
            await send_message_with_retry(update.message, referral_text, reply_markup=get_main_reply_keyboard())
        elif text == "❓ Как работает бот":
            how_it_works_text = (
                "🎬 *Как работает наш кино-бот?* 🎥\n\n"
                "Я — твой личный помощник в мире кино! 🍿 Моя главная задача — помочь тебе найти фильмы по секретным числовым кодам. Вот как это работает:\n\n"
                "🔍 *Поиск фильмов*:\n"
                "1. Нажми на кнопку *🔍 Поиск фильма* в меню.\n"
                "2. Подпишись на наши крутые спонсорские каналы или отправь заявку на вступление (это обязательно! 😎).\n"
                "3. Введи *числовой код* фильма (только цифры!).\n"
                "4. Я найду фильм в нашей базе и покажу его название! 🎉\n\n"
                "👥 *Реферальная система*:\n"
                "- У тебя есть *5 бесплатных поисков* при старте! 🚀\n"
                "- Приглашай друзей в бота, и за каждого, кто подпишется на каналы, ты получишь *+2 поиска*! 🌟\n"
                "- Если поиски закончились, приглашай друзей, чтобы продолжить! 😍\n\n"
                "❗ *Важно*:\n"
                "- Подписка или заявка на вступление в каналы обязательна для доступа к поиску.\n"
                "- Вводи только числовые коды после нажатия *🔍 Поиск фильма*.\n"
                "- Если что-то пошло не так, просто следуй подсказкам, и я помогу! 😊\n\n"
                "Готов к кино-приключению? Выбери действие в меню! 👇"
            )
            await send_message_with_retry(update.message, how_it_works_text, reply_markup=get_main_reply_keyboard())
        else:
            logger.info(f"User {user_id} sent unknown command: {text}")
            await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇", reply_markup=get_main_reply_keyboard())
    elif update.channel_post:
        logger.warning("Ignoring channel post update")
        return

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle non-button text input."""
    if update.message.from_user.id == context.bot["id"]:
        return
    logger.info(f"User {update.message.from_user.id} sent non-button text: {update.message.text}")
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню! 👇", reply_markup=get_main_reply_keyboard())

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle chat join request updates."""
    join_request = update.chat_join_request
    user = join_request.from_user
    user_id = user.id
    chat_id = join_request.chat.id
    if str(chat_id) in CHANNELS:
        add_join_request(user_id, chat_id)
        logger.info(f"User {user_id} sent join request to channel {chat_id}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors gracefully."""
    logger.error(f"Update {update} caused error: {context.error}")
    if update.callback_query:
        await update.callback_query.answer()
        await edit_message_with_retry(
            context,
            update.callback_query.message.chat_id,
            update.callback_query.message.message_id,
            "Упс, что-то пошло не так! 😢 Попробуй снова или напиши в поддержку.",
            reply_markup=None
        )
        await send_message_with_retry(
            update.callback_query.message,
            "Выбери действие в меню ниже! 😎",
            reply_markup=get_main_reply_keyboard()
        )

# Webhook endpoint
async def webhook_endpoint(request):
    try:
        body = await request.body()
        update = Update.de_json(json.loads(body.decode()), application_tg.bot)
        if update:
            logger.info(f"Received update: {update.to_json()}")
            await application_tg.process_update(update)
        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return PlainTextResponse("Error", status_code=500)

# Health check endpoint
async def health_check(request):
    return PlainTextResponse("OK", status_code=200)

# ASGI application
app = Starlette(
    routes=[
        Route(f"/{TOKEN}", endpoint=webhook_endpoint, methods=["POST"]),
        Route("/", endpoint=health_check, methods=["GET", "HEAD"])
    ]
)

async def startup():
    """Initialize the bot and load movie data into cache."""
    # Add handlers
    application_tg.add_error_handler(error_handler)
    application_tg.add_handler(CommandHandler("start", start))
    application_tg.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\d+$'), handle_movie_code))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\d+$'), handle_non_button_text))
    application_tg.add_handler(ChatJoinRequestHandler(handle_join_request))

    # Initialize application
    await application_tg.initialize()

    # Load movie data into cache
    global MOVIE_DICT
    if movie_sheet:
        try:
            all_values = movie_sheet.get_all_values()[1:]  # Skip header
            MOVIE_DICT = {row[0].strip(): row[1].strip() for row in all_values if row and len(row) >= 2}
            logger.info(f"Loaded {len(MOVIE_DICT)} movies into cache.")
        except Exception as e:
            logger.error(f"Error loading movie data into cache: {e}")

    # Set webhook
    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    try:
        await application_tg.bot.set_webhook(url=full_webhook_url)
        logger.info("Webhook set successfully.")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

    # Start application
    await application_tg.start()
    logger.info("Application started successfully.")

async def shutdown():
    """Shut down the application."""
    await application_tg.stop()
    await application_tg.shutdown()
    logger.info("Application shut down successfully.")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)