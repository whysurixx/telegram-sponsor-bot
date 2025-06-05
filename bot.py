import os
import logging
import json
import time
import random
import asyncio
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.responses import PlainTextResponse
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ChatJoinRequestHandler, ContextTypes
from telegram.ext import filters
from telegram.error import RetryAfter
from google.oauth2.service_account import Credentials
import gspread
from typing import Optional, Dict, List
import telegram

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logger.info(f"python-telegram-bot version: {telegram.__version__}")

# Configuration from environment variables
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").lstrip("@")

MOVIE_SHEET_ID = "1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms"
USER_SHEET_ID = "1XYFfqmC5boLBB8HjjkyKA6AyN3WNCKy6U8LEmN8KvrA"
JOIN_REQUESTS_SHEET_ID = "1OKteXrJFjKC7B2qbwoVkt-rfbkCGdYt2VjMcZRjtQ84"

# Validate environment variables
if not all([TOKEN, WEBHOOK_URL, BOT_USERNAME]):
    logger.error("Missing required environment variables: BOT_TOKEN, WEBHOOK_URL, or BOT_USERNAME")
    raise ValueError("BOT_TOKEN, WEBHOOK_URL, and BOT_USERNAME must be set")

# Load channels and buttons
try:
    CHANNELS = json.loads(os.environ.get("CHANNEL_IDS", "[]"))
    CHANNEL_BUTTONS = json.loads(os.environ.get("CHANNEL_BUTTONS", "[]"))
    if not CHANNELS or not CHANNEL_BUTTONS or len(CHANNELS) != len(CHANNEL_BUTTONS):
        logger.error("Invalid CHANNEL_IDS or CHANNEL_BUTTONS configuration")
        raise ValueError("CHANNEL_IDS and CHANNEL_BUTTONS must be non-empty and equal in length")
except json.JSONDecodeError as e:
    logger.error(f"Error parsing JSON in CHANNEL_IDS or CHANNEL_BUTTONS: {e}")
    raise

# Initialize Google Sheets
movie_sheet = None
user_sheet = None
join_requests_sheet = None
MOVIE_DICT = {}  # Cache for movie data

try:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        raise FileNotFoundError(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)

    # Movie sheet
    movie_spreadsheet = client.open_by_key(MOVIE_SHEET_ID)
    movie_sheet = movie_spreadsheet.sheet1
    logger.info(f"Movie sheet initialized (ID: {MOVIE_SHEET_ID})")

    # User sheet
    user_spreadsheet = client.open_by_key(USER_SHEET_ID)
    try:
        user_sheet = user_spreadsheet.worksheet("Users")
    except gspread.exceptions.WorksheetNotFound:
        user_sheet = user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
        user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
        logger.info(f"Created new 'Users' worksheet (ID: {USER_SHEET_ID})")

    # Join Requests sheet
    join_requests_spreadsheet = client.open_by_key(JOIN_REQUESTS_SHEET_ID)
    try:
        join_requests_sheet = join_requests_spreadsheet.worksheet("JoinRequests")
    except gspread.exceptions.WorksheetNotFound:
        join_requests_sheet = join_requests_spreadsheet.add_worksheet(title="JoinRequests", rows=1000, cols=2)
        join_requests_sheet.append_row(["user_id", "channel_id"])
        logger.info(f"Created new 'JoinRequests' worksheet (ID: {JOIN_REQUESTS_SHEET_ID})")
except Exception as e:
    logger.error(f"Error initializing Google Sheets: {e}")
    raise

# Initialize Telegram application
application_tg = Application.builder().token(TOKEN).build()

# Random emojis for responses
POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']

def get_main_inline_keyboard() -> InlineKeyboardMarkup:
    """Create the main inline keyboard."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Поиск фильма", callback_data="search_movie")],
        [InlineKeyboardButton("👥 Реферальная система", callback_data="referral_system")],
        [InlineKeyboardButton("❓ Как работает бот", callback_data="how_it_works")]
    ])

async def send_message_with_retry(message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Send a message with retry on flood control."""
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds")
        await asyncio.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Failed to send message: {e}")

async def edit_message_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    """Edit a message with retry on flood control."""
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup
        )
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds")
        await asyncio.sleep(e.retry_after)
        await context.bot.edit_message_text(
            chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Failed to edit message: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle the /start command, including referral links."""
    user = update.effective_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    logger.info(f"User {user_id} ({first_name}) started the bot")

    # Handle referral
    referrer_id = None
    if update.message.text.startswith("/start invite_"):
        try:
            referrer_id = int(update.message.text.split("invite_")[1])
            if referrer_id == user_id:
                logger.info(f"User {user_id} tried to invite themselves")
                await send_message_with_retry(
                    update.message, "❌ Вы не можете пригласить себя!", reply_markup=get_main_inline_keyboard()
                )
                return
            context.user_data['referrer_id'] = referrer_id
            logger.info(f"Referral detected for user {user_id} from referrer {referrer_id}")
        except (IndexError, ValueError):
            logger.warning(f"Invalid referral link for user {user_id}")

    # Register or update user
    user_data = get_user_data(user_id)
    if not user_data:
        add_user(user_id, username, first_name, search_queries=5, invited_users=0)
        logger.info(f"Added user {user_id} with 5 search queries")
    else:
        update_user(user_id, username=username, first_name=first_name)
        logger.info(f"Updated user {user_id}")

    welcome_text = (
        f"Привет, *киноман*! 🎬\n"
        f"Я твой кино-гид! 🍿 Найду фильмы по секретным кодам! 🚀\n"
        f"{'Ты пришёл по приглашению друга! 😎 ' if referrer_id else ''}"
        f"Выбери действие в меню! 😎"
    )
    await send_message_with_retry(update.message, welcome_text, reply_markup=get_main_inline_keyboard())

async def prompt_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: Optional[int] = None) -> None:
    """Prompt user to subscribe to channels."""
    promo_text = (
        "*Киноман*! 🎥\n"
        "Для доступа к фильмам подпишись на наши каналы! 🌟\n"
        "Кликни кнопки ниже, подпишись или отправь заявку и нажми *Я ПОДПИСАЛСЯ!* 😎"
    )
    keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in CHANNEL_BUTTONS]
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if message_id:
        await edit_message_with_retry(context, update.effective_chat.id, message_id, promo_text, reply_markup)
    else:
        await send_message_with_retry(update.message, promo_text, reply_markup)

def has_sent_join_request(user_id: int, channel_id: int) -> bool:
    """Check if user has sent a join request to the channel."""
    if not join_requests_sheet:
        logger.error("JoinRequests sheet not initialized")
        return False
    try:
        records = join_requests_sheet.get_all_records()
        return any(row['user_id'] == str(user_id) and row['channel_id'] == str(channel_id) for row in records)
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
            if member.status in ["member", "administrator", "creator"] or has_sent_join_request(user_id, channel_id):
                continue
            unsubscribed_channels.append(button)
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            unsubscribed_channels.append(button)

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"User {user_id} confirmed subscription")

        # Process referral reward
        referrer_id = context.user_data.get('referrer_id')
        if referrer_id:
            referrer_data = get_user_data(referrer_id)
            if referrer_data:
                new_invited_users = int(referrer_data.get("invited_users", 0)) + 1
                new_search_queries = int(referrer_data.get("search_queries", 0)) + 2
                update_user(referrer_id, invited_users=new_invited_users, search_queries=new_search_queries)
                logger.info(f"Added 2 searches to referrer {referrer_id} for user {user_id}")
                try:
                    await bot.send_message(
                        referrer_id,
                        f"Друг присоединился по твоей ссылке! 🎉 Тебе +2 поиска!",
                        parse_mode='Markdown'
                    )
                except Exception as e:
                    logger.error(f"Failed to notify referrer {referrer_id}: {e}")
                del context.user_data['referrer_id']

        success_text = (
            f"*Ты в деле*! 🎉\n"
            f"Подписка подтверждена! 😍\n"
            f"{'Введи *числовой код* фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Выбери действие в меню! 😎'}"
        )
        reply_markup = get_main_inline_keyboard() if not context.user_data.get('awaiting_code', False) else None
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, success_text, reply_markup)
    else:
        logger.info(f"User {user_id} not subscribed to all channels")
        promo_text = (
            "Ой! 😜 Ты пропустил пару каналов! 🚨\n"
            "Подпишись или отправь заявку и нажми *Я ПОДПИСАЛСЯ!* 🌟"
        )
        keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in unsubscribed_channels]
        keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, promo_text, InlineKeyboardMarkup(keyboard))

async def handle_inline_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle inline button presses."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data

    if data == "search_movie":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Search without subscription")
            await prompt_subscribe(update, context, query.message.message_id)
            return
        context.user_data['awaiting_code'] = True
        await edit_message_with_retry(
            context, query.message.chat_id, query.message.message_id,
            "Введи *числовой код* фильма! 🍿", reply_markup=None
        )
    elif data == "referral_system":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Referral without subscription")
            await prompt_subscribe(update, context, query.message.message_id)
            return
        user_data = get_user_data(user_id)
        if not user_data:
            logger.error(f"User {user_id} not found")
            await edit_message_with_retry(
                context, query.message.chat_id, query.message.message_id,
                "Упс, данные не найдены! 😢 Перезапусти бота.", reply_markup=get_main_inline_keyboard()
            )
            return
        referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
        referral_text = (
            f"🔥 *Реферальная система* 🔥\n\n"
            f"Приглашай друзей и получай *+2 поиска* за каждого, кто подпишется на все каналы! 🚀\n"
            f"Твоя ссылка: `{referral_link}`\n"
            f"👥 *Приглашено*: {user_data.get('invited_users', '0')}\n"
            f"🔍 *Осталось поисков*: {user_data.get('search_queries', '0')}"
        )
        await edit_message_with_retry(
            context, query.message.chat_id, query.message.message_id,
            referral_text, reply_markup=get_main_inline_keyboard()
        )
    elif data == "how_it_works":
        how_it_works_text = (
            f"🎬 *Как работает бот?* 🎥\n\n"
            f"Я нахожу фильмы по числовым кодам! 🍿 Вот как:\n\n"
            f"🔍 *Поиск*:\n"
            f"1. Нажми *🔍 Поиск фильма*.\n"
            f"2. Подпишись на каналы или отправь заявку.\n"
            f"3. Введи *числовой код*.\n"
            f"4. Получи название фильма! 🎉\n\n"
            f"👥 *Рефералы*:\n"
            f"- Стартуешь с *5 поисками*! 🚀\n"
            f"- За каждого друга +2 поиска! 🌟\n\n"
            f"❗ *Важно*:\n"
            f"- Подписка на каналы обязательна.\n"
            f"- Вводи только цифры в поиске.\n"
            f"Готов? Выбери действие! 👇"
        )
        await edit_message_with_retry(
            context, query.message.chat_id, query.message.message_id,
            how_it_works_text, reply_markup=get_main_inline_keyboard()
        )

def get_user_data(user_id: int) -> Optional[Dict[str, str]]:
    """Retrieve user data from Users sheet."""
    if not user_sheet:
        logger.error("Users sheet not initialized")
        return None
    try:
        records = user_sheet.get_all_records()
        for row in records:
            if row.get('user_id') == str(user_id):
                return row
        return None
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in get_user_data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unknown error in get_user_data: {e}")
        return None

def add_user(user_id: int, username: str, first_name: str, search_queries: int, invited_users: int) -> None:
    """Add a new user to Users sheet."""
    if not user_sheet:
        logger.error("Users sheet not initialized")
        return
    try:
        user_sheet.append_row([str(user_id), username, first_name, str(search_queries), str(invited_users)])
        logger.info(f"Added user {user_id} with {search_queries} searches")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in add_user: {e}")
    except Exception as e:
        logger.error(f"Unknown error in add_user: {e}")

def update_user(user_id: int, **kwargs) -> None:
    """Update user data in Users sheet."""
    if not user_sheet:
        logger.error("Users sheet not initialized")
        return
    try:
        cell = user_sheet.find(str(user_id), in_column=1)
        if not cell:
            logger.warning(f"User {user_id} not found for update")
            return
        row = user_sheet.row_values(cell.row)
        updates = {
            "username": row[1] if len(row) > 1 else "",
            "first_name": row[2] if len(row) > 2 else "",
            "search_queries": row[3] if len(row) > 3 else "0",
            "invited_users": row[4] if len(row) > 4 else "0"
        }
        updates.update(kwargs)
        user_sheet.update(f"A{cell.row}:E{cell.row}", [[
            str(user_id),
            updates["username"],
            updates["first_name"],
            str(updates["search_queries"]),
            str(updates["invited_users"])
        ]])
        logger.info(f"Updated user {user_id}")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in update_user: {e}")
    except Exception as e:
        logger.error(f"Unknown error in update_user: {e}")

def add_join_request(user_id: int, channel_id: int) -> None:
    """Add a join request to JoinRequests sheet."""
    if not join_requests_sheet:
        logger.error("JoinRequests sheet not initialized")
        return
    try:
        if not has_sent_join_request(user_id, channel_id):
            join_requests_sheet.append_row([str(user_id), str(channel_id)])
            logger.info(f"Added join request for user {user_id} to channel {channel_id}")
    except gspread.exceptions.APIError as e:
        logger.error(f"Google Sheets API error in add_join_request: {e}")
    except Exception as e:
        logger.error(f"Unknown error in add_join_request: {e}")

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    """Find a movie by its code in cached MOVIE_DICT."""
    return {"code": code, "title": MOVIE_DICT[code]} if code in MOVIE_DICT else None

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle numeric movie code input."""
    code = update.message.text.strip()
    user_id = update.effective_user.id

    if not context.user_data.get('awaiting_code', False):
        logger.info(f"User {user_id} sent code without search mode")
        await send_message_with_retry(
            update.message, "Сначала нажми *🔍 Поиск фильма*! 🍿", reply_markup=get_main_inline_keyboard()
        )
        return

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Нужен *числовой код*! 🔢 Введи цифры!")
        return

    user_data = get_user_data(user_id)
    if not user_data:
        logger.error(f"User {user_id} not found")
        await send_message_with_retry(
            update.message, "Данные не найдены! 😢 Перезапусти бота.", reply_markup=get_main_inline_keyboard()
        )
        return

    search_queries = int(user_data.get("search_queries", 0))
    if search_queries <= 0:
        logger.info(f"User {user_id} has no searches left")
        await send_message_with_retry(
            update.message,
            "Поиски закончились! 😕 Приглашай друзей через *👥 Реферальная система*! 🚀",
            reply_markup=get_main_inline_keyboard()
        )
        context.user_data['awaiting_code'] = False
        return

    movie = find_movie_by_code(code)
    context.user_data['awaiting_code'] = False
    update_user(user_id, search_queries=search_queries - 1)
    result_text = (
        f"*Бинго!* 🎥 Код {code}: *{movie['title']}* {random.choice(POSITIVE_EMOJIS)}\n"
        f"Осталось поисков: *{search_queries - 1}*\n"
        f"Ищешь ещё? Нажми *🔍 Поиск фильма*! 🍿"
    ) if movie else f"Фильм с кодом *{code}* не найден! 😢 Проверь код!"
    await send_message_with_retry(update.message, result_text, reply_markup=get_main_inline_keyboard())

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle custom button presses."""
    user_id = update.effective_user.id
    text = update.message.text

    if text == "🔍 Поиск фильма":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Search without subscription")
            await prompt_subscribe(update, context)
            return
        context.user_data['awaiting_code'] = True
        await send_message_with_retry(update.message, "Введи *числовой код* фильма! 🍿")
    elif text == "👥 Реферальная система":
        if not context.user_data.get('subscription_confirmed', False):
            logger.info(f"User {user_id} pressed Referral without subscription")
            await prompt_subscribe(update, context)
            return
        user_data = get_user_data(user_id)
        if not user_data:
            logger.error(f"User {user_id} not found")
            await send_message_with_retry(
                update.message, "Данные не найдены! 😢 Перезапусти бота.", reply_markup=get_main_inline_keyboard()
            )
            return
        referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
        referral_text = (
            f"🔥 *Реферальная система* 🔥\n\n"
            f"Приглашай друзей и получай *+2 поиска* за каждого, кто подпишется на все каналы! 🚀\n"
            f"Твоя ссылка: `{referral_link}`\n"
            f"👥 *Приглашено*: {user_data.get('invited_users', '0')}\n"
            f"🔍 *Осталось поисков*: {user_data.get('search_queries', '0')}"
        )
        await send_message_with_retry(update.message, referral_text, reply_markup=get_main_inline_keyboard())
    elif text == "❓ Как работает бот":
        how_it_works_text = (
            f"🎬 *Как работает бот?* 🎥\n\n"
            f"Я нахожу фильмы по числовым кодам! 🍿 Вот как:\n\n"
            f"🔍 *Поиск*:\n"
            f"1. Нажми *🔍 Поиск фильма*.\n"
            f"2. Подпишись на каналы или отправь заявку.\n"
            f"3. Введи *числовой код*.\n"
            f"4. Получи название фильма! 🎉\n\n"
            f"👥 *Рефералы*:\n"
            f"- Стартуешь с *5 поисками*! 🚀\n"
            f"- За каждого друга +2 поиска! 🌟\n\n"
            f"❗ *Важно*:\n"
            f"- Подписка на каналы обязательна.\n"
            f"- Вводи только цифры в поиске.\n"
            f"Готов? Выбери действие! 👇"
        )
        await send_message_with_retry(update.message, how_it_works_text, reply_markup=get_main_inline_keyboard())
    else:
        logger.info(f"User {user_id} sent unknown command: {text}")
        await send_message_with_retry(update.message, "Неизвестная команда! 😕 Выбери действие!", reply_markup=get_main_inline_keyboard())

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle non-button text input."""
    if update.effective_user.id != context.bot.id:
        logger.info(f"User {update.effective_user.id} sent non-button text: {update.message.text}")
        await send_message_with_retry(update.message, "Неизвестная команда! 😕 Выбери действие!", reply_markup=get_main_inline_keyboard())

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle chat join request updates."""
    join_request = update.chat_join_request
    user_id = join_request.from_user.id
    chat_id = join_request.chat.id
    if str(chat_id) in CHANNELS:
        add_join_request(user_id, chat_id)
        logger.info(f"User {user_id} sent join request to channel {chat_id}")

async def webhook_endpoint(request):
    """Handle incoming webhook updates."""
    try:
        body = await request.json()
        update = Update.de_json(body, application_tg.bot)
        if update:
            logger.info(f"Received update: {update.update_id}")
            await application_tg.process_update(update)
        return PlainTextResponse("OK")
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return PlainTextResponse("Error", status_code=500)

async def health_check(request):
    """Health check endpoint."""
    return PlainTextResponse("OK", status_code=200)

app = Starlette(
    routes=[
        Route(f"/{TOKEN}", endpoint=webhook_endpoint, methods=["POST"]),
        Route("/", endpoint=health_check, methods=["GET", "HEAD"])
    ]
)

async def startup():
    """Initialize the bot and load movie data."""
    # Add handlers
    application_tg.add_handler(CommandHandler("start", start))
    application_tg.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application_tg.add_handler(CallbackQueryHandler(handle_inline_buttons, pattern="^(search_movie|referral_system|how_it_works)$"))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\d+$'), handle_movie_code))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\d+$'), handle_non_button_text))
    application_tg.add_handler(ChatJoinRequestHandler(handle_join_request))

    # Initialize application
    await application_tg.initialize()

    # Load movie data
    global MOVIE_DICT
    if movie_sheet:
        try:
            records = movie_sheet.get_all_records()
            MOVIE_DICT = {row['code']: row['title'] for row in records if 'code' in row and 'title' in row}
            logger.info(f"Loaded {len(MOVIE_DICT)} movies into cache")
        except Exception as e:
            logger.error(f"Error loading movie data: {e}")

    # Set webhook
    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    try:
        await application_tg.bot.set_webhook(url=full_webhook_url)
        logger.info("Webhook set successfully")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

    await application_tg.start()
    logger.info("Application started")

async def shutdown():
    """Shut down the application."""
    await application_tg.stop()
    await application_tg.shutdown()
    logger.info("Application shut down")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)