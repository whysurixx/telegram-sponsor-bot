import os
import logging
import json
import time
import random
import asyncio
import sys
import tempfile
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ChatJoinRequestHandler, ContextTypes
from telegram.ext import filters
from telegram.error import RetryAfter
from google.oauth2.service_account import Credentials
from gspread_asyncio import AsyncioGspreadClientManager
from typing import Optional, Dict, List
import telegram
from tenacity import retry, stop_after_attempt, wait_fixed
from cachetools import LRUCache
from aiohttp import web

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

logger.info(f"python-telegram-bot version: {telegram.__version__}")

# Configuration from environment variables
TOKEN = os.environ.get("BOT_TOKEN")
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_PATH = "google-credentials.json"
BOT_USERNAME = os.environ.get("BOT_USERNAME")

if BOT_USERNAME and BOT_USERNAME.startswith("@"):
    BOT_USERNAME = BOT_USERNAME[1:]
    logger.info(f"Removed '@' from BOT_USERNAME: {BOT_USERNAME}")

MOVIE_SHEET_ID = "1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms"
USER_SHEET_ID = "1XYFfqmC5boLBB8HjjkyKA6AyN3WNCKy6U8LEmN8KvrA"
JOIN_REQUESTS_SHEET_ID = "1OKteXrJFjKC7B2qbwoVkt-rfbkCGdYt2VjMcZRjtQ84"

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

if not TOKEN:
    logger.error("BOT_TOKEN is not set!")
    raise ValueError("BOT_TOKEN is not set!")
if not BOT_USERNAME:
    logger.error("BOT_USERNAME is not set!")
    raise ValueError("BOT_USERNAME is not set!")

movie_sheet = None
user_sheet = None
join_requests_sheet = None
MOVIE_DICT = LRUCache(maxsize=5000)
USER_DICT = LRUCache(maxsize=5000)
JOIN_REQUESTS_DICT = {}
MOVIE_CACHE_REFRESH_INTERVAL = 60
OTHER_CACHE_REFRESH_INTERVAL = 300

async def init_google_sheets():
    global movie_sheet, user_sheet, join_requests_sheet
    try:
        if GOOGLE_CREDENTIALS_JSON:
            logger.info("Using Google credentials from GOOGLE_CREDENTIALS_JSON environment variable.")
            credentials_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(credentials_dict, temp_file)
                temp_file_path = temp_file.name
        else:
            if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
                logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
                raise FileNotFoundError(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
            temp_file_path = GOOGLE_CREDENTIALS_PATH

        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(temp_file_path, scopes=scope)
        client_manager = AsyncioGspreadClientManager(lambda: creds)
        client = await client_manager.authorize()

        movie_spreadsheet = await client.open_by_key(MOVIE_SHEET_ID)
        movie_sheet = await movie_spreadsheet.get_worksheet(0)
        logger.info(f"Movie sheet initialized (ID: {MOVIE_SHEET_ID}).")
        
        user_spreadsheet = await client.open_by_key(USER_SHEET_ID)
        try:
            user_sheet = await user_spreadsheet.worksheet("Users")
        except Exception:
            user_sheet = await user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
            await user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
            logger.info(f"Created new 'Users' worksheet (ID: {USER_SHEET_ID}).")
        logger.info(f"User sheet initialized (ID: {USER_SHEET_ID}).")
        
        join_requests_spreadsheet = await client.open_by_key(JOIN_REQUESTS_SHEET_ID)
        try:
            join_requests_sheet = await join_requests_spreadsheet.worksheet("JoinRequests")
        except Exception:
            join_requests_sheet = await join_requests_spreadsheet.add_worksheet(title="JoinRequests", rows=1000, cols=2)
            await join_requests_sheet.append_row(["user_id", "channel_id"])
            logger.info(f"Created new 'JoinRequests' worksheet (ID: {JOIN_REQUESTS_SHEET_ID}).")
        logger.info(f"Join Requests sheet initialized (ID: {JOIN_REQUESTS_SHEET_ID}).")

        if GOOGLE_CREDENTIALS_JSON:
            os.unlink(temp_file_path)
            logger.info(f"Temporary credentials file deleted: {temp_file_path}")
    except Exception as e:
        logger.error(f"Error initializing Google Sheets: {e}")
        if GOOGLE_CREDENTIALS_JSON and 'temp_file_path' in locals():
            os.unlink(temp_file_path)
            logger.info(f"Temporary credentials file deleted after error: {temp_file_path}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def load_movie_cache():
    global MOVIE_DICT
    try:
        last_row = getattr(load_movie_cache, "last_row", 0)
        range_val = f"A{last_row+1}:B"
        new_values = await movie_sheet.get_values(range_val) or []
        added_movies = 0
        for row in new_values:
            if len(row) >= 2:
                code = row[0].strip()
                if last_row == 0 and code.lower() in ["code", "код"]:
                    continue
                MOVIE_DICT[code] = row[1].strip()
                added_movies += 1
        total_rows = last_row + len(new_values)
        load_movie_cache.last_row = total_rows
        logger.info(f"Loaded {added_movies} new movies into cache. Total in cache: {len(MOVIE_DICT)}")
    except Exception as e:
        logger.error(f"Error loading movie data into cache: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def load_user_cache():
    global USER_DICT
    try:
        all_values = await user_sheet.get_all_values()
        new_dict = {
            row[0]: {
                "user_id": row[0],
                "username": row[1] if len(row) > 1 else "",
                "first_name": row[2] if len(row) > 2 else "",
                "search_queries": row[3] if len(row) > 3 else "0",
                "invited_users": row[4] if len(row) > 4 else "0"
            } for row in all_values[1:] if row and len(row) >= 1
        }
        USER_DICT.clear()
        USER_DICT.update(new_dict)
        logger.info(f"Loaded {len(USER_DICT)} users into cache.")
    except Exception as e:
        logger.error(f"Error loading user cache: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def load_join_requests_cache():
    global JOIN_REQUESTS_DICT
    try:
        all_values = await join_requests_sheet.get_all_values()
        new_dict = {(row[0], row[1]): True for row in all_values[1:] if row and len(row) >= 2}
        if len(new_dict) > 10000:
            new_dict = dict(list(new_dict.items())[-10000:])
        JOIN_REQUESTS_DICT.clear()
        JOIN_REQUESTS_DICT.update(new_dict)
        logger.info(f"Loaded {len(JOIN_REQUESTS_DICT)} join requests into cache.")
    except Exception as e:
        logger.error(f"Error loading join requests cache: {e}")

async def log_cache_size():
    while True:
        try:
            movie_size = sys.getsizeof(MOVIE_DICT) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in MOVIE_DICT.items())
            user_size = sys.getsizeof(USER_DICT) + sum(sys.getsizeof(k) + sys.getsizeof(v) for k, v in USER_DICT.items())
            join_requests_size = sys.getsizeof(JOIN_REQUESTS_DICT) + sum(sys.getsizeof(k) for k in JOIN_REQUESTS_DICT)
            logger.info(f"Cache sizes: movies={movie_size/1024:.2f} KB, users={user_size/1024:.2f} KB, join_requests={join_requests_size/1024:.2f} KB")
            await asyncio.sleep(3600)
        except Exception as e:
            logger.error(f"Error logging cache size: {e}")
            await asyncio.sleep(3600)

async def refresh_movie_cache_periodically():
    while True:
        try:
            await load_movie_cache()
            await asyncio.sleep(MOVIE_CACHE_REFRESH_INTERVAL)
        except Exception as e:
            logger.error(f"Error during movie cache refresh: {e}")
            await asyncio.sleep(MOVIE_CACHE_REFRESH_INTERVAL)

async def refresh_other_caches_periodically():
    while True:
        try:
            await load_user_cache()
            await load_join_requests_cache()
            await asyncio.sleep(OTHER_CACHE_REFRESH_INTERVAL)
        except Exception as e:
            logger.error(f"Error during other caches refresh: {e}")
            await asyncio.sleep(OTHER_CACHE_REFRESH_INTERVAL)

application_tg = Application.builder().token(TOKEN).build()

POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']

def get_main_reply_keyboard():
    keyboard = [
        [KeyboardButton("🔍 Поиск фильма"), KeyboardButton("👥 Реферальная система")],
        [KeyboardButton("❓ Как работает бот")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def get_search_reply_keyboard():
    keyboard = [
        [KeyboardButton("❌ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True, one_time_keyboard=False)

def escape_markdown_v2(text: str) -> str:
    special_chars = r'_*[]()~`>#+-=|{}.!'
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text

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
            logger.info(f"Referral detected for user {user_id} from referrer {referrer_id}")
            context.user_data['referrer_id'] = referrer_id
        except (IndexError, ValueError):
            logger.warning(f"Invalid referral link for user {user_id}: {update.message.text}")
            referrer_id = None

    user_data = get_user_data(user_id)
    if not user_data:
        try:
            await add_user(user_id, username, first_name, search_queries=5, invited_users=0)
            logger.info(f"Added user {user_id} to Users sheet with 5 search queries.")
        except Exception as e:
            logger.error(f"Failed to add user {user_id} to Users sheet: {e}")
    else:
        await update_user(user_id, username=username, first_name=first_name)
        logger.info(f"Updated existing user {user_id}.")

    welcome_text = (
        "Привет, *киноман*! 🎬\n"
        "Добро пожаловать в твой личный кино-гид! 🍿 Я помогу найти фильмы по секретным кодам и открою мир кино! 🚀\n"
        f"{'Ты был приглашён другом! 😎 ' if referrer_id else ''}"
        "Выбери действие в меню ниже, и начнём приключение! 😎"
    )
    await send_message_with_retry(update.message, welcome_text, reply_markup=get_main_reply_keyboard())

async def send_message_with_retry(message, text: str, reply_markup=None, parse_mode: str = 'Markdown') -> None:
    try:
        await message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except RetryAfter as e:
        logger.warning(f"Flood control triggered: {e}. Waiting {e.retry_after} seconds.")
        await asyncio.sleep(e.retry_after)
        await message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Failed to send message: {e}, Response: {e.__dict__}")
        try:
            await message.reply_text(text, reply_markup=reply_markup)
        except Exception as e2:
            logger.error(f"Failed to send message without parse_mode: {e2}")

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
        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup
            )
        except Exception as e2:
            logger.error(f"Failed to edit message without Markdown: {e2}")

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

def has_sent_join_request(user_id: int, channel_id: int) -> bool:
    return (str(user_id), str(channel_id)) in JOIN_REQUESTS_DICT

async def check_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
            await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Error checking subscription for channel {channel_id}: {e}")
            unsubscribed_channels.append(button)

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        logger.info(f"User {user_id} successfully confirmed subscription for all channels.")

        referrer_id = context.user_data.get('referrer_id')
        if referrer_id:
            referrer_data = get_user_data(referrer_id)
            if referrer_data:
                new_invited_users = int(referrer_data.get("invited_users", 0)) + 1
                new_search_queries = int(referrer_data.get("search_queries", "0")) + 2
                await update_user(
                    user_id=referrer_id,
                    invited_users=new_invited_users,
                    search_queries=new_search_queries
                )
                logger.info(f"Added 2 search queries to referrer {referrer_id} for inviting user {user_id}")
                try:
                    await bot.send_message(
                        user_id=referrer_id,
                        text=f"Пользователь {user_id} успешно подтвердил подписку. Вам начислено *+2 поиска*!",
                        parse_mode='Markdown'
                    )
                    logger.info(f"Sent referral reward notification to referrer {referrer_id}")
                except Exception as e:
                    logger.error(f"Failed to send referral reward notification to {referrer_id}: {e}")

                del context.user_data['referrer_id']

        success_text = (
            "Супер, *ты в деле*! 🎉\n"
            "Вы подписаны на все каналы или отправили заявки! 😍 Теперь ты можешь искать фильмы!\n"
            f"{'Введи *числовой код* для поиска фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Нажми *🔍 Поиск фильма* в меню ниже! 😎'}"
        )
        reply_markup = get_main_reply_keyboard() if not context.user_data.get('awaiting_code', False) else get_search_reply_keyboard()

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
                "Выбери действие в меню ниже! 😎",
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
    return USER_DICT.get(str(user_id))

async def add_user(user_id: int, username: str, first_name: str, search_queries: int, invited_users: int) -> None:
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    try:
        user_id_str = str(user_id)
        row_to_add = [user_id_str, username, first_name, str(search_queries), str(invited_users)]
        await user_sheet.append_row(row_to_add)
        USER_DICT[user_id_str] = {
            "user_id": user_id_str,
            "username": username,
            "first_name": first_name,
            "search_queries": str(search_queries),
            "invited_users": str(invited_users)
        }
        logger.info(f"Added user {user_id} to Users sheet with {search_queries} search queries.")
    except Exception as e:
        logger.error(f"Failed to add user {user_id} to Users sheet: {e}")

async def update_user(user_id: int, **kwargs) -> None:
    if user_sheet is None:
        logger.error("Users sheet not initialized.")
        return
    try:
        user_id_str = str(user_id)
        all_values = await user_sheet.get_all_values()
        for idx, row in enumerate(all_values[1:], start=2):
            if not row or len(row) < 1 or row[0] != user_id_str:
                continue
            updates = {
                "username": row[1] if len(row) > 1 else "",
                "first_name": row[2] if len(row) > 2 else "",
                "search_queries": row[3] if len(row) > 3 else "0",
                "invited_users": row[4] if len(row) > 4 else "0"
            }
            updates.update(kwargs)
            await user_sheet.update(f"A{idx}:E{idx}", [[
                user_id_str,
                updates["username"],
                updates["first_name"],
                str(updates["search_queries"]),
                str(updates["invited_users"])
            ]])
            USER_DICT[user_id_str] = {
                "user_id": user_id_str,
                "username": updates["username"],
                "first_name": updates["first_name"],
                "search_queries": str(updates["search_queries"]),
                "invited_users": str(updates["invited_users"])
            }
            logger.info(f"Updated user {user_id} in Users sheet.")
            return
        logger.warning(f"User {user_id} not found for update.")
    except Exception as e:
        logger.error(f"Failed to update user {user_id}: {e}")

async def add_join_request(user_id: int, channel_id: int) -> None:
    if join_requests_sheet is None:
        logger.error("JoinRequests sheet not initialized.")
        return
    try:
        user_id_str, channel_id_str = str(user_id), str(channel_id)
        if (user_id_str, channel_id_str) in JOIN_REQUESTS_DICT:
            return
        await join_requests_sheet.append_row([user_id_str, channel_id_str])
        JOIN_REQUESTS_DICT[(user_id_str, channel_id_str)] = True
        if len(JOIN_REQUESTS_DICT) > 10000:
            oldest_key = next(iter(JOIN_REQUESTS_DICT))
            del JOIN_REQUESTS_DICT[oldest_key]
        logger.info(f"Added join request for user {user_id} to channel {channel_id}")
    except Exception as e:
        logger.error(f"Failed to add join request for user {user_id} to channel {channel_id}: {e}")

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    if code in MOVIE_DICT:
        return {"code": code, "title": MOVIE_DICT[code]}
    return None

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    code = update.message.text.strip()
    user_id = update.message.from_user.id

    if not context.user_data.get('awaiting_code', False):
        logger.info(f"User {user_id} sent code without activating search mode.")
        await send_message_with_retry(update.message, "Эй, *киноман*! 😅 Сначала нажми *🔍 Поиск фильма*, а потом введи код! 🍿", reply_markup=get_main_reply_keyboard())
        return

    if not code.isdigit():
        logger.info(f"User {user_id} entered non-numeric code: {code}")
        await send_message_with_retry(update.message, "Ой, нужен *только числовой код*! 😊 Введи цифры, и мы найдём твой фильм! 🔢", reply_markup=get_search_reply_keyboard())
        return

    user_data = get_user_data(user_id)
    if not user_data:
        logger.error(f"User {user_id} not found in Users sheet.")
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
        await update_user(user_id, search_queries=search_queries - 1)
        result_text = (
            f"*Бинго!* 🎥 Код {code}: *{escape_markdown_v2(movie['title'])}* {random.choice(POSITIVE_EMOJIS)}\n"
            f"Осталось поисков: *{search_queries - 1}* 🔍\n"
            "Хочешь найти ещё один шедевр? Нажми *🔍 Поиск фильма*! 🍿"
        )
    else:
        result_text = f"Упс, фильм с кодом *{code}* не найден! 😢 Проверь код или попробуй другой! 🔍"
    await send_message_with_retry(update.message, result_text, reply_markup=get_main_reply_keyboard())

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message and update.message.from_user:
        user_id = update.message.from_user.id
        text = update.message.text

        if text == "🔍 Поиск фильма":
            if not context.user_data.get('subscription_confirmed', False):
                logger.info(f"User {user_id} pressed Search without subscription.")
                await prompt_subscribe(update, context)
                return
            context.user_data['awaiting_code'] = True
            await send_message_with_retry(
                update.message,
                "Отлично! 😎 Введи *числовой код* фильма, и я найду его для тебя! 🍿",
                reply_markup=get_search_reply_keyboard()
            )
        elif text == "❌ Назад":
            if context.user_data.get('awaiting_code', False):
                context.user_data['awaiting_code'] = False
                logger.info(f"User {user_id} cancelled search mode.")
                await send_message_with_retry(
                    update.message,
                    "Поиск отменён! 😊 Выбери действие в меню ниже! 👇",
                    reply_markup=get_main_reply_keyboard()
                )
            else:
                await send_message_with_retry(
                    update.message,
                    "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇",
                    reply_markup=get_main_reply_keyboard()
                )
        elif text == "👥 Реферальная система":
            user_data = get_user_data(user_id)
            if not user_data:
                logger.error(f"User {user_id} not found in Users sheet.")
                await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота.", reply_markup=get_main_reply_keyboard())
                return
            referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
            logger.info(f"Generated referral link for user {user_id}: {referral_link}")
            invited_users = user_data.get("invited_users", "0")
            search_queries = user_data.get("search_queries", "0")
            referral_text = (
                "<b>🔥 Реферальная система 🔥</b>\n\n"
                "Приглашай друзей и получай <b>+2 поиска</b> за каждого, кто перейдёт по твоей ссылке и подпишется на наши каналы! 🚀\n\n"
                f"Твоя реферальная ссылка: <a href='{referral_link}'>{referral_link}</a>\n"
                "Нажми на ссылку, чтобы поделиться, или скопируй её для друзей! 😎\n\n"
                f"👥 <b>Количество добавленных пользователей</b>: <b>{invited_users}</b>\n"
                f"🔍 <b>Количество оставшихся запросов</b>: <b>{search_queries}</b>"
            )
            await send_message_with_retry(update.message, referral_text, reply_markup=get_main_reply_keyboard(), parse_mode='HTML')
        elif text == "❓ Как работает бот":
            how_it_works_text = (
                "🎬 *Как работает наш кино-бот?* 🎥\n\n"
                "Я — твой личный помощник в мире кино! 🍿 Моя главная задача — помочь тебе найти фильмы по секретным числовым кодам. Вот как это работает:\n\n"
                "🔍 *Поиск фильмов*:\n"
                "1. Нажми на кнопку *🔍 Поиск фильма* в меню.\n"
                "2. Подпишись на наши крутые спонсорские каналы или отправь заявку на вступление (это обязательно для поиска! 😎).\n"
                "3. Введи *числовой код* фильма (только цифры!).\n"
                "4. Я найду фильм в нашей базе и покажу его название! 🎉\n\n"
                "👥 *Реферальная система*:\n"
                "- У тебя есть *5 бесплатных поисков* при старте! 🚀\n"
                "- Приглашай друзей в бота, и за каждого, кто подпишется на каналы, ты получишь *+2 поиска*! 🌟\n"
                "- Если поиски закончились, приглашай друзей, чтобы продолжить! 😍\n\n"
                "❗ *Важно*:\n"
                "- Подписка или заявка на вступление в каналы обязательна только для поиска фильмов.\n"
                "- Вводи только числовые коды после нажатия *🔍 Поиск фильма*.\n"
                "- Нажми *❌ Назад*, чтобы отменить поиск и вернуться в меню.\n"
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
    if update.message.from_user.id == context.bot.id:
        return
    logger.info(f"User {update.message.from_user.id} sent non-button text: {update.message.text}")
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню! 👇", reply_markup=get_main_reply_keyboard())

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    join_request = update.chat_join_request
    user = join_request.from_user
    user_id = user.id
    chat_id = join_request.chat.id
    if str(chat_id) in CHANNELS:
        await add_join_request(user_id, chat_id)
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

async def webhook(request):
    """Handle incoming Telegram webhook updates."""
    try:
        update = Update.de_json(await request.json(), application_tg.bot)
        await application_tg.process_update(update)
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"Error processing webhook update: {e}")
        return web.Response(status=500)

async def main():
    await init_google_sheets()
    application_tg.add_error_handler(error_handler)
    application_tg.add_handler(CommandHandler("start", start))
    application_tg.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\d+$'), handle_movie_code))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\d+$'), handle_non_button_text))
    application_tg.add_handler(ChatJoinRequestHandler(handle_join_request))
    await load_movie_cache()
    await load_user_cache()
    await load_join_requests_cache()
    asyncio.create_task(refresh_movie_cache_periodically())
    asyncio.create_task(refresh_other_caches_periodically())
    asyncio.create_task(log_cache_size())
    logger.info("Starting bot with webhook...")

    # Initialize the application
    await application_tg.initialize()
    await application_tg.start()

    # Set up the webhook
    port = int(os.environ.get("PORT", 8443))
    webhook_url = f"https://{os.environ.get('RENDER_EXTERNAL_HOSTNAME')}/webhook"
    await application_tg.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook set to {webhook_url}")

    # Start the web server
    app = web.Application()
    app.router.add_post('/webhook', webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Webhook server started on port {port}")

    # Keep the bot running
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())