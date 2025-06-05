import os
import logging
import json
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
import telegram
import redis.asyncio as redis

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
USER_CACHE = {}  # Cache for user data
JOIN_REQUESTS_CACHE = set()  # Cache for join requests
PENDING_USER_UPDATES = []  # Queue for batch updates
PENDING_JOIN_REQUESTS = []  # Queue for batch join request updates
REDIS_CLIENT = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

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
    
    movie_spreadsheet = client.open_by_key(MOVIE_SHEET_ID)
    movie_sheet = movie_spreadsheet.sheet1
    logger.info(f"Movie sheet initialized (ID: {MOVIE_SHEET_ID}).")
    
    user_spreadsheet = client.open_by_key(USER_SHEET_ID)
    try:
        user_sheet = user_spreadsheet.worksheet("Users")
    except gspread.exceptions.WorksheetNotFound:
        user_sheet = user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
        user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
        logger.info(f"Created new 'Users' worksheet (ID: {USER_SHEET_ID}).")
    logger.info(f"User sheet initialized (ID: {USER_SHEET_ID}).")
    
    join_requests_spreadsheet = client.open_by_key(JOIN_REQUESTS_SHEET_ID)
    try:
        join_requests_sheet = join_requests_spreadsheet.worksheet("JoinRequests")
    except gspread.exceptions.WorksheetNotFound:
        join_requests_sheet = join_requests_spreadsheet.add_worksheet(title="JoinRequests", rows=1000, cols=2)
        join_requests_sheet.append_row(["user_id", "channel_id"])
        logger.info(f"Created new 'JoinRequests' worksheet (ID: {JOIN_REQUESTS_SHEET_ID}).")
    logger.info(f"Join Requests sheet initialized (ID: {JOIN_REQUESTS_SHEET_ID}).")
except Exception as e:
    logger.error(f"Error initializing Google Sheets: {e}")
    raise

# Initialize Telegram application
application_tg = Application.builder().token(TOKEN).concurrent_updates(True).rate_limiter(True).connection_pool_size(20).build()

# Random emojis for responses
POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']

# Custom reply keyboard
def get_main_reply_keyboard():
    keyboard = [
        [KeyboardButton("🔍 Поиск фильма"), KeyboardButton("👥 Реферальная система")],
        [KeyboardButton("❓ Как работает бот")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

# Cache sync functions
async def sync_cache_periodically():
    while True:
        await asyncio.sleep(300)  # Sync every 5 minutes
        try:
            global USER_CACHE, JOIN_REQUESTS_CACHE
            if user_sheet:
                all_values = user_sheet.get_all_values()[1:]
                USER_CACHE = {
                    row[0]: {
                        "user_id": row[0],
                        "username": row[1] if len(row) > 1 else "",
                        "first_name": row[2] if len(row) > 2 else "",
                        "search_queries": row[3] if len(row) > 3 else "0",
                        "invited_users": row[4] if len(row) > 4 else "0"
                    } for row in all_values if row
                }
                for user_id, data in USER_CACHE.items():
                    await REDIS_CLIENT.hset(f"user:{user_id}", mapping=data)
            if join_requests_sheet:
                all_values = join_requests_sheet.get_all_values()[1:]
                JOIN_REQUESTS_CACHE = {(row[0], row[1]) for row in all_values if row and len(row) >= 2}
                for user_id, channel_id in JOIN_REQUESTS_CACHE:
                    await REDIS_CLIENT.sadd(f"join_requests:{user_id}", channel_id)
            logger.info("Caches synced with Google Sheets and Redis.")
        except Exception as e:
            logger.error(f"Error syncing caches: {e}")

async def batch_sync_to_sheets():
    while True:
        await asyncio.sleep(60)  # Sync every 60 seconds
        try:
            if PENDING_USER_UPDATES and user_sheet:
                user_sheet.batch_update(PENDING_USER_UPDATES)
                logger.info(f"Batch updated {len(PENDING_USER_UPDATES)} user records to Google Sheets.")
                PENDING_USER_UPDATES.clear()
            if PENDING_JOIN_REQUESTS and join_requests_sheet:
                join_requests_sheet.batch_update(PENDING_JOIN_REQUESTS)
                logger.info(f"Batch updated {len(PENDING_JOIN_REQUESTS)} join requests to Google Sheets.")
                PENDING_JOIN_REQUESTS.clear()
        except Exception as e:
            logger.error(f"Error in batch sync to sheets: {e}")

# Data access functions
def get_user_data(user_id: int) -> Optional[Dict[str, str]]:
    return USER_CACHE.get(str(user_id))

def has_sent_join_request(user_id: int, channel_id: int) -> bool:
    return (str(user_id), str(channel_id)) in JOIN_REQUESTS_CACHE

def add_user(user_id: int, username: str, first_name: str, search_queries: int, invited_users: int) -> None:
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    user_id_str = str(user_id)
    data = {
        "user_id": user_id_str,
        "username": username,
        "first_name": first_name,
        "search_queries": str(search_queries),
        "invited_users": str(invited_users)
    }
    USER_CACHE[user_id_str] = data
    PENDING_USER_UPDATES.append({
        "range": f"A{len(USER_CACHE) + 1}:E{len(USER_CACHE) + 1}",
        "values": [[user_id_str, username, first_name, str(search_queries), str(invited_users)]]
    })
    asyncio.create_task(REDIS_CLIENT.hset(f"user:{user_id_str}", mapping=data))
    logger.info(f"Queued user {user_id} addition with {search_queries} search queries.")

def update_user(user_id: int, **kwargs) -> None:
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    user_id_str = str(user_id)
    user_data = USER_CACHE.get(user_id_str)
    if not user_data:
        logger.warning(f"User {user_id} not found for update.")
        return
    updates = user_data.copy()
    updates.update(kwargs)
    USER_CACHE[user_id_str] = updates
    PENDING_USER_UPDATES.append({
        "range": f"A{list(USER_CACHE.keys()).index(user_id_str) + 2}:E{list(USER_CACHE.keys()).index(user_id_str) + 2}",
        "values": [[user_id_str, updates["username"], updates["first_name"], str(updates["search_queries"]), str(updates["invited_users"])]]
    })
    asyncio.create_task(REDIS_CLIENT.hset(f"user:{user_id_str}", mapping=updates))
    logger.info(f"Queued update for user {user_id} in cache.")

def add_join_request(user_id: int, channel_id: int) -> None:
    if join_requests_sheet is None:
        logger.error("JoinRequests sheet not initialized.")
        return
    user_id_str, channel_id_str = str(user_id), str(channel_id)
    if (user_id_str, channel_id_str) in JOIN_REQUESTS_CACHE:
        return
    JOIN_REQUESTS_CACHE.add((user_id_str, channel_id_str))
    PENDING_JOIN_REQUESTS.append({
        "range": f"A{len(JOIN_REQUESTS_CACHE) + 1}:B{len(JOIN_REQUESTS_CACHE) + 1}",
        "values": [[user_id_str, channel_id_str]]
    })
    asyncio.create_task(REDIS_CLIENT.sadd(f"join_requests:{user_id_str}", channel_id_str))
    logger.info(f"Queued join request for user {user_id} to channel {channel_id}")

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    if code in MOVIE_DICT:
        return {"code": code, "title": MOVIE_DICT[code]}
    return None

# Telegram handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user
    user_id = user.id
    username = user.username or ""
    first_name = user.first_name or ""
    logger.info(f"User {user_id} {first_name} started the bot with message: {update.message.text}")

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

    user_data = get_user_data(user_id)
    if not user_data:
        add_user(user_id, username, first_name, search_queries=5, invited_users=0)
        logger.info(f"Added user {user_id} to cache with 5 search queries.")
    else:
        update_user(user_id, username=username, first_name=first_name)
        logger.info(f"Updated existing user {user_id}.")

    unsubscribed_channels = []
    async def check_channel(channel_id, button):
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"] and not has_sent_join_request(user_id, channel_id):
                return button
            return None
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            return button

    tasks = [check_channel(channel_id, button) for channel_id, button in zip(CHANNELS, CHANNEL_BUTTONS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    unsubscribed_channels = [r for r in results if r is not None]

    if unsubscribed_channels:
        await prompt_subscribe(update, context)
    else:
        context.user_data['subscription_confirmed'] = True
        welcome_text = (
            "Привет, *киноман*! 🎬\n"
            "Добро пожаловать в твой личный кино-гид! 🍿 Я помогу найти фильмы по секретным кодам и открою мир кино! 🚀\n"
            f"{'Ты был приглашён другом! 😎 ' if referrer_id else ''}"
            "Выбери действие в меню ниже, и начнём приключение! 😎"
        )
        await send_message_with_retry(update.message, welcome_text, reply_markup=get_main_reply_keyboard())

async def send_message_with_retry(message, text: str, reply_markup=None) -> None:
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        await asyncio.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Failed to send message: {e}, Response: {e.__dict__}")

async def edit_message_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
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
        await asyncio.sleep(e.retry_after)
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

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    bot = context.bot
    unsubscribed_channels = []

    async def check_channel(channel_id, button):
        try:
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status in ["member", "administrator", "creator"]:
                return None
            elif has_sent_join_request(user_id, channel_id):
                return None
            else:
                return button
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            return button

    tasks = [check_channel(channel_id, button) for channel_id, button in zip(CHANNELS, CHANNEL_BUTTONS)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    unsubscribed_channels = [r for r in results if r is not None]

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"User {user_id} successfully confirmed subscription for all channels.")

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
            reply_markup=None
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

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    user_data = get_user_data(user_id)
    if not user_data:
        logger.error(f"User {user_id} not found in cache.")
        await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота.", reply_markup=get_main_reply_keyboard())
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
        update_user(user_id, search_queries=search_queries - 1)
        result_text = (
            f"*Бинго!* 🎥 Код {code}: *{movie['title']}* {random.choice(POSITIVE_EMOJIS)}\n"
            f"Осталось поисков: *{search_queries - 1}* 🔍\n"
            "Хочешь найти ещё один шедевр? Нажми *🔍 Поиск фильма*! 🍿"
        )
    else:
        result_text = f"Упс, фильм с кодом *{code}* не найден! 😢 Проверь код или попробуй другой! 🔍"
    await send_message_with_retry(update.message, result_text, reply_markup=get_main_reply_keyboard())

async def search_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"User {update.message.from_user.id} pressed Search without subscription.")
        await prompt_subscribe(update, context)
        return
    context.user_data['awaiting_code'] = True
    await send_message_with_retry(update.message, "Отлично! 😎 Введи *числовой код* фильма, и я найду его для тебя! 🍿", reply_markup=ReplyKeyboardRemove())

async def referral_system(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get('subscription_confirmed', False):
        logger.info(f"User {update.message.from_user.id} pressed Referral without subscription.")
        await prompt_subscribe(update, context)
        return
    user_id = update.message.from_user.id
    user_data = get_user_data(user_id)
    if not user_data:
        await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота.", reply_markup=get_main_reply_keyboard())
        return
    referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
    invited_users = user_data.get("invited_users", "0")
    search_queries = user_data.get("search_queries", "0")
    referral_text = (
        "🔥 *Реферальная система* 🔥\n\n"
        "Приглашай друзей и получай *+2 поиска* за каждого, кто перейдёт по твоей ссылке и подпишется на наши каналы! 🚀\n\n"
        f"Твоя реферальная ссылка: {referral_link}\n"
        "Нажми на ссылку, чтобы поделиться, или скопируй её для друзей! 😎\n\n"
        f"👥 *Количество добавленных пользователей*: *{invited_users}*\n"
        f"🔍 *Количество оставшихся запросов*: *{search_queries}*"
    )
    await send_message_with_retry(update.message, referral_text, reply_markup=get_main_reply_keyboard())

async def how_it_works(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

BUTTON_HANDLERS = {
    "🔍 Поиск фильма": search_movie,
    "👥 Реферальная система": referral_system,
    "❓ Как работает бот": how_it_works
}

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.from_user:
        user_id = update.message.from_user.id
        text = update.message.text
        handler = BUTTON_HANDLERS.get(text)
        if handler:
            await handler(update, context)
        else:
            logger.info(f"User {user_id} sent unknown command: {text}")
            await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇", reply_markup=get_main_reply_keyboard())
    elif update.channel_post:
        logger.warning("Ignoring channel post update")
        return

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id == context.bot["id"]:
        return
    logger.info(f"User {update.message.from_user.id} sent non-button text: {update.message.text}")
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню! 👇", reply_markup=get_main_reply_keyboard())

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    join_request = update.chat_join_request
    user = join_request.from_user
    user_id = user.id
    chat_id = join_request.chat.id
    if str(chat_id) in CHANNELS:
        add_join_request(user_id, chat_id)
        logger.info(f"User {user_id} sent join request to channel {chat_id}")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error: {context.error}")
    if update.callback_query:
        await update.callback_query.answer()
        await edit_message_with_retry(
            context,
            update.callback_query.message.chat_id,
            update.callback_query.message.message_id,
            "Упс, что-то пошло не так! 😢 Попробуй снова.",
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
            all_values = movie_sheet.get_all_values()[1:]
            MOVIE_DICT = {row[0].strip(): row[1].strip() for row in all_values if row and len(row) >= 2}
            logger.info(f"Loaded {len(MOVIE_DICT)} movies into cache.")
        except Exception as e:
            logger.error(f"Error loading movie data into cache: {e}")

    # Load user and join request data into cache
    global USER_CACHE, JOIN_REQUESTS_CACHE
    if user_sheet:
        try:
            all_values = user_sheet.get_all_values()[1:]
            USER_CACHE = {
                row[0]: {
                    "user_id": row[0],
                    "username": row[1] if len(row) > 1 else "",
                    "first_name": row[2] if len(row) > 2 else "",
                    "search_queries": row[3] if len(row) > 3 else "0",
                    "invited_users": row[4] if len(row) > 4 else "0"
                } for row in all_values if row
            }
            for user_id, data in USER_CACHE.items():
                await REDIS_CLIENT.hset(f"user:{user_id}", mapping=data)
            logger.info(f"Loaded {len(USER_CACHE)} users into cache.")
        except Exception as e:
            logger.error(f"Error loading user data into cache: {e}")
    if join_requests_sheet:
        try:
            all_values = join_requests_sheet.get_all_values()[1:]
            JOIN_REQUESTS_CACHE = {(row[0], row[1]) for row in all_values if row and len(row) >= 2}
            for user_id, channel_id in JOIN_REQUESTS_CACHE:
                await REDIS_CLIENT.sadd(f"join_requests:{user_id}", channel_id)
            logger.info(f"Loaded {len(JOIN_REQUESTS_CACHE)} join requests into cache.")
        except Exception as e:
            logger.error(f"Error loading join requests into cache: {e}")

    # Connect to Redis
    try:
        await REDIS_CLIENT.ping()
        logger.info("Connected to Redis.")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")

    # Start periodic sync tasks
    asyncio.create_task(sync_cache_periodically())
    asyncio.create_task(batch_sync_to_sheets())

    # Set webhook
    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Setting webhook to: {full_webhook_url}")
    try:
        await application_tg.bot.set_webhook(url=full_webhook_url)
        logger.info("Webhook set successfully.")
    except Exception as e:
        logger.error(f"Failed to set webhook: {e}")
        raise

async def shutdown():
    await application_tg.stop()
    await application_tg.shutdown()
    await REDIS_CLIENT.close()
    logger.info("Application shut down successfully.")

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)