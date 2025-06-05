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

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)
logger = logging.getLogger(__name__)

# Configuration from environment variables
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
GOOGLE_CREDENTIALS_PATH = "/etc/secrets/GOOGLE_CREDENTIALS"
BOT_USERNAME = os.environ.get("BOT_USERNAME", "").lstrip("@")

# Load channels and buttons
try:
    CHANNELS = json.loads(os.environ.get("CHANNEL_IDS", "[]"))
    CHANNEL_BUTTONS = json.loads(os.environ.get("CHANNEL_BUTTONS", "[]"))
    if not CHANNELS or not CHANNEL_BUTTONS or len(CHANNELS) != len(CHANNEL_BUTTONS):
        logger.error("Invalid CHANNEL_IDS or CHANNEL_BUTTONS configuration.")
        raise ValueError("CHANNEL_IDS and CHANNEL_BUTTONS must be set and match in length.")
except (json.JSONDecodeError, ValueError) as e:
    logger.error(f"Configuration error: {e}")
    raise

# Validate environment variables
if not all([WEBHOOK_URL, TOKEN, BOT_USERNAME]):
    logger.error("Missing required environment variables!")
    raise ValueError("BOT_TOKEN, WEBHOOK_URL, and BOT_USERNAME must be set!")

# Initialize Google Sheets
movie_sheet = None
user_sheet = None
join_requests_sheet = None
MOVIE_DICT = {}
USER_CACHE = {}
JOIN_REQUESTS_CACHE = set()
PENDING_USER_UPDATES = []
PENDING_JOIN_REQUESTS = []

try:
    if not os.path.exists(GOOGLE_CREDENTIALS_PATH):
        logger.error(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")
        raise FileNotFoundError(f"Credentials file not found at: {GOOGLE_CREDENTIALS_PATH}")

    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GOOGLE_CREDENTIALS_PATH, scopes=scope)
    client = gspread.authorize(creds)
    
    movie_spreadsheet = client.open_by_key("1hmm-rfUlDcA31QD04XRXIyaa_EpN8ObuHFc8cp7Rwms")
    movie_sheet = movie_spreadsheet.sheet1
    
    user_spreadsheet = client.open_by_key("1XYFfqmC5boLBB8HjjkyKA6AyN3WNCKy6U8LEmN8KvrA")
    try:
        user_sheet = user_spreadsheet.worksheet("Users")
    except gspread.exceptions.WorksheetNotFound:
        user_sheet = user_spreadsheet.add_worksheet(title="Users", rows=1000, cols=5)
        user_sheet.append_row(["user_id", "username", "first_name", "search_queries", "invited_users"])
    
    join_requests_spreadsheet = client.open_by_key("1OKteXrJFjKC7B2qbwoVkt-rfbkCGdYt2VjMcZRjtQ84")
    try:
        join_requests_sheet = join_requests_spreadsheet.worksheet("JoinRequests")
    except gspread.exceptions.WorksheetNotFound:
        join_requests_sheet = join_requests_spreadsheet.add_worksheet(title="JoinRequests", rows=1000, cols=2)
        join_requests_sheet.append_row(["user_id", "channel_id"])
except Exception as e:
    logger.error(f"Error initializing Google Sheets: {e}")
    raise

# Initialize Telegram application
application_tg = Application.builder().token(TOKEN).concurrent_updates(True).rate_limiter(True).connection_pool_size(50).build()

# Constants
POSITIVE_EMOJIS = ['😍', '🎉', '😎', '👍', '🔥', '😊', '😁', '⭐']
MAIN_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton("🔍 Поиск фильма"), KeyboardButton("👥 Реферальная система")],
    [KeyboardButton("❓ Как работает бот")]
], resize_keyboard=True, one_time_keyboard=False)

# Cache sync and batch updates
async def sync_cache_periodically():
    while True:
        await asyncio.sleep(180)
        try:
            global USER_CACHE, JOIN_REQUESTS_CACHE
            if user_sheet:
                all_values = user_sheet.get_all_values()[1:]
                USER_CACHE.clear()
                USER_CACHE.update({
                    row[0]: {
                        "user_id": row[0],
                        "username": row[1] if len(row) > 1 else "",
                        "first_name": row[2] if len(row) > 2 else "",
                        "search_queries": row[3] if len(row) > 3 else "0",
                        "invited_users": row[4] if len(row) > 4 else "0"
                    } for row in all_values if row
                })
            if join_requests_sheet:
                all_values = join_requests_sheet.get_all_values()[1:]
                JOIN_REQUESTS_CACHE.clear()
                JOIN_REQUESTS_CACHE.update({(row[0], row[1]) for row in all_values if row and len(row) >= 2})
        except Exception as e:
            logger.error(f"Error syncing caches: {e}")

async def batch_sync_to_sheets():
    while True:
        await asyncio.sleep(30)
        try:
            if PENDING_USER_UPDATES and user_sheet:
                user_sheet.batch_update(PENDING_USER_UPDATES)
                PENDING_USER_UPDATES.clear()
            if PENDING_JOIN_REQUESTS and join_requests_sheet:
                join_requests_sheet.batch_update(PENDING_JOIN_REQUESTS)
                PENDING_JOIN_REQUESTS.clear()
        except Exception as e:
            logger.error(f"Error in batch sync: {e}")

# Data access functions
def get_user_data(user_id: int) -> Optional[Dict[str, str]]:
    return USER_CACHE.get(str(user_id))

def has_sent_join_request(user_id: int, channel_id: int) -> bool:
    return (str(user_id), str(channel_id)) in JOIN_REQUESTS_CACHE

def add_user(user_id: int, username: str, first_name: str, search_queries: int, invited_users: int) -> None:
    if user_sheet is None: return
    user_id_str = str(user_id)
    data = {"user_id": user_id_str, "username": username, "first_name": first_name, 
            "search_queries": str(search_queries), "invited_users": str(invited_users)}
    USER_CACHE[user_id_str] = data
    PENDING_USER_UPDATES.append({
        "range": f"A{len(USER_CACHE) + 1}:E{len(USER_CACHE) + 1}",
        "values": [[user_id_str, username, first_name, str(search_queries), str(invited_users)]]
    })

def update_user(user_id: int, **kwargs) -> None:
    if user_sheet is None: return
    user_id_str = str(user_id)
    user_data = USER_CACHE.get(user_id_str)
    if not user_data: return
    updates = user_data.copy()
    updates.update(kwargs)
    USER_CACHE[user_id_str] = updates
    PENDING_USER_UPDATES.append({
        "range": f"A{list(USER_CACHE.keys()).index(user_id_str) + 2}:E{list(USER_CACHE.keys()).index(user_id_str) + 2}",
        "values": [[user_id_str, updates["username"], updates["first_name"], str(updates["search_queries"]), str(updates["invited_users"])]]
    })

def add_join_request(user_id: int, channel_id: int) -> None:
    if join_requests_sheet is None: return
    user_id_str, channel_id_str = str(user_id), str(channel_id)
    if (user_id_str, channel_id_str) in JOIN_REQUESTS_CACHE: return
    JOIN_REQUESTS_CACHE.add((user_id_str, channel_id_str))
    PENDING_JOIN_REQUESTS.append({
        "range": f"A{len(JOIN_REQUESTS_CACHE) + 1}:B{len(JOIN_REQUESTS_CACHE) + 1}",
        "values": [[user_id_str, channel_id_str]]
    })

def find_movie_by_code(code: str) -> Optional[Dict[str, str]]:
    return {"code": code, "title": MOVIE_DICT[code]} if code in MOVIE_DICT else None

# Telegram handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.message.from_user
    user_id, username, first_name = user.id, user.username or "", user.first_name or ""
    
    referrer_id = None
    if update.message.text.startswith("/start invite_"):
        try:
            referrer_id = int(update.message.text.split("invite_")[1])
            if referrer_id == user_id:
                await send_message_with_retry(update.message, "❌ Вы не можете пригласить себя!", reply_markup=MAIN_KEYBOARD)
                return
            context.user_data['referrer_id'] = referrer_id
        except (IndexError, ValueError):
            pass

    user_data = get_user_data(user_id)
    if not user_data:
        add_user(user_id, username, first_name, search_queries=5, invited_users=0)
    else:
        update_user(user_id, username=username, first_name=first_name)

    async def check_channel(channel_id, button):
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            return button if member.status not in ["member", "administrator", "creator"] and not has_sent_join_request(user_id, channel_id) else None
        except Exception:
            return button

    tasks = [check_channel(cid, btn) for cid, btn in zip(CHANNELS, CHANNEL_BUTTONS)]
    unsubscribed_channels = [r for r in await asyncio.gather(*tasks, return_exceptions=True) if r]
    
    if unsubscribed_channels:
        await prompt_subscribe(update, context)
    else:
        context.user_data['subscription_confirmed'] = True
        welcome_text = f"Привет, *киноман*! 🎬\nДобро пожаловать в твой личный кино-гид! 🍿 Я помогу найти фильмы по секретным кодам и открою мир кино! 🚀\n{'Ты был приглашён другом! 😎 ' if referrer_id else ''}Выбери действие в меню ниже! 😎"
        await send_message_with_retry(update.message, welcome_text, reply_markup=MAIN_KEYBOARD)

async def send_message_with_retry(message, text: str, reply_markup=None) -> None:
    try:
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup, disable_web_page_preview=True)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup, disable_web_page_preview=True)
    except Exception:
        pass

async def edit_message_with_retry(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup)
    except RetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, parse_mode='Markdown', reply_markup=reply_markup)
    except Exception:
        pass

async def prompt_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE, message_id: Optional[int] = None) -> None:
    promo_text = "Эй, *кинофан*! 🎥\nЧтобы открыть доступ к фильмам, подпишись на наших крутых спонсоров! 🌟\nКликни на кнопки ниже, подпишись или отправь заявку на вступление и нажми *Я ПОДПИСАЛСЯ!* 😎"
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

    async def check_channel(channel_id, button):
        try:
            member = await context.bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            return None if member.status in ["member", "administrator", "creator"] or has_sent_join_request(user_id, channel_id) else button
        except Exception:
            return button

    tasks = [check_channel(cid, btn) for cid, btn in zip(CHANNELS, CHANNEL_BUTTONS)]
    unsubscribed_channels = [r for r in await asyncio.gather(*tasks, return_exceptions=True) if r]

    if not unsubscribed_channels:
        context.user_data['subscription_confirmed'] = True
        referrer_id = context.user_data.get('referrer_id')
        if referrer_id:
            referrer_data = get_user_data(referrer_id)
            if referrer_data:
                update_user(referrer_id, invited_users=int(referrer_data.get("invited_users", 0)) + 1, search_queries=int(referrer_data.get("search_queries", "0")) + 2)
                try:
                    await context.bot.send_message(user_id=referrer_id, text=f"User {user_id} successfully confirmed subscription. Вам начислено *+2 поиска*!", parse_mode='Markdown')
                except Exception:
                    pass
                del context.user_data['referrer_id']
        success_text = f"Супер, *ты в деле*! 🎉\nВы подписаны на все каналы или отправили заявки! 😍 Теперь ты можешь продолжить работать с ботом!\n{'Введи *числовой код* для поиска фильма! 🍿' if context.user_data.get('awaiting_code', False) else 'Выбери действие в меню ниже! 😎'}"
        reply_markup = MAIN_KEYBOARD if not context.user_data.get('awaiting_code', False) else ReplyKeyboardRemove()
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, success_text)
        if not context.user_data.get('awaiting_code', False):
            await send_message_with_retry(query.message, "Что дальше? 😎", reply_markup=reply_markup)
    else:
        promo_text = "Ой-ой! 😜 Похоже, ты пропустил пару каналов! 🚨\nПодпишись или отправь заявку на вступление на все каналы ниже и снова нажми *Я ПОДПИСАЛСЯ!* 🌟"
        keyboard = [[InlineKeyboardButton(btn["text"], url=btn["url"])] for btn in unsubscribed_channels]
        keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
        await edit_message_with_retry(context, query.message.chat_id, query.message.message_id, promo_text, reply_markup=InlineKeyboardMarkup(keyboard))

async def handle_movie_code(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    code, user_id = update.message.text.strip(), update.message.from_user.id
    if not context.user_data.get('awaiting_code', False):
        await send_message_with_retry(update.message, "Эй, *киноман*! 😅 Сначала нажми *🔍 Поиск фильма*, а потом введи код! 🍿", reply_markup=MAIN_KEYBOARD)
        return
    if not code.isdigit():
        await send_message_with_retry(update.message, "Ой, нужен *только числовой код*! 😊 Введи цифры, и мы найдём твой фильм! 🔢", reply_markup=ReplyKeyboardRemove())
        return
    user_data = get_user_data(user_id)
    if not user_data:
        await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота.", reply_markup=MAIN_KEYBOARD)
        return
    search_queries = int(user_data.get("search_queries", 0))
    if search_queries <= 0:
        await send_message_with_retry(update.message, "Ой, у тебя закончились поиски! 😕 Приглашай друзей через *👥 Реферальная система* и получай +2 поиска за каждого! 🚀", reply_markup=MAIN_KEYBOARD)
        context.user_data['awaiting_code'] = False
        return
    movie = find_movie_by_code(code)
    context.user_data['awaiting_code'] = False
    result_text = f"*Бинго!* 🎥 Код {code}: *{movie['title']}* {random.choice(POSITIVE_EMOJIS)}\nОсталось поисков: *{search_queries - 1}* 🔍\nХочешь найти ещё один шедевр? Нажми *🔍 Поиск фильма*! 🍿" if movie else f"Упс, фильм с кодом *{code}* не найден! 😢 Проверь код или попробуй другой! 🔍"
    update_user(user_id, search_queries=search_queries - 1) if movie else None
    await send_message_with_retry(update.message, result_text, reply_markup=MAIN_KEYBOARD)

async def search_movie(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get('subscription_confirmed', False):
        await prompt_subscribe(update, context)
        return
    context.user_data['awaiting_code'] = True
    await send_message_with_retry(update.message, "Отлично! 😎 Введи *числовой код* фильма, и я найду его для тебя! 🍿", reply_markup=ReplyKeyboardRemove())

async def referral_system(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get('subscription_confirmed', False):
        await prompt_subscribe(update, context)
        return
    user_id = update.message.from_user.id
    user_data = get_user_data(user_id)
    if not user_data:
        await send_message_with_retry(update.message, "Упс, не удалось получить твои данные! 😢 Перезапусти бота.", reply_markup=MAIN_KEYBOARD)
        return
    referral_link = f"https://t.me/{BOT_USERNAME}?start=invite_{user_id}"
    referral_text = f"🔥 *Реферальная система* 🔥\n\nПриглашай друзей и получай *+2 поиска* за каждого, кто перейдёт по твоей ссылке и подпишется на наши каналы! 🚀\n\nТвоя реферальная ссылка: {referral_link}\nНажми на ссылку, чтобы поделиться, или скопируй её для друзей! 😎\n\n👥 *Количество добавленных пользователей*: *{user_data.get('invited_users', '0')}*\n🔍 *Количество оставшихся запросов*: *{user_data.get('search_queries', '0')}*"
    await send_message_with_retry(update.message, referral_text, reply_markup=MAIN_KEYBOARD)

async def how_it_works(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    how_it_works_text = "🎬 *Как работает наш кино-бот?* 🎥\n\nЯ — твой личный помощник в мире кино! 🍿 Моя главная задача — помочь тебе найти фильмы по секретным числовым кодам. Вот как это работает:\n\n🔍 *Поиск фильмов*:\n1. Нажми на кнопку *🔍 Поиск фильма* в меню.\n2. Подпишись на наши крутые спонсорские каналы или отправь заявку на вступление (это обязательно! 😎).\n3. Введи *числовой код* фильма (только цифры!).\n4. Я найду фильм в нашей базе и покажу его название! 🎉\n\n👥 *Реферальная система*:\n- У тебя есть *5 бесплатных поисков* при старте! 🚀\n- Приглашай друзей в бота, и за каждого, кто подпишется на каналы, ты получишь *+2 поиска*! 🌟\n- Если поиски закончились, приглашай друзей, чтобы продолжить! 😍\n\n❗ *Важно*:\n- Подписка или заявка на вступление в каналы обязательна для доступа к поиску.\n- Вводи только числовые коды после нажатия *🔍 Поиск фильма*.\n- Если что-то пошло не так, просто следуй подсказкам, и я помогу! 😊\n\nГотов к кино-приключению? Выбери действие в меню! 👇"
    await send_message_with_retry(update.message, how_it_works_text, reply_markup=MAIN_KEYBOARD)

BUTTON_HANDLERS = {
    "🔍 Поиск фильма": search_movie,
    "👥 Реферальная система": referral_system,
    "❓ Как работает бот": how_it_works
}

async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.from_user: return
    text = update.message.text
    handler = BUTTON_HANDLERS.get(text)
    if handler:
        await handler(update, context)
    else:
        await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню ниже! 👇", reply_markup=MAIN_KEYBOARD)

async def handle_non_button_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message.from_user.id == context.bot["id"]: return
    await send_message_with_retry(update.message, "Ой, *неизвестная команда*! 😕 Пожалуйста, выбери действие из меню! 👇", reply_markup=MAIN_KEYBOARD)

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    join_request = update.chat_join_request
    user_id, chat_id = join_request.from_user.id, join_request.chat.id
    if str(chat_id) in CHANNELS:
        add_join_request(user_id, chat_id)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.callback_query:
        await update.callback_query.answer()
        await edit_message_with_retry(context, update.callback_query.message.chat_id, update.callback_query.message.message_id, "Упс, что-то пошло не так! 😢 Попробуй снова.")
        await send_message_with_retry(update.callback_query.message, "Выбери действие в меню ниже! 😎", reply_markup=MAIN_KEYBOARD)

# Webhook and health check
async def webhook_endpoint(request):
    try:
        update = Update.de_json(json.loads(await request.body().decode()), application_tg.bot)
        if update: await application_tg.process_update(update)
        return PlainTextResponse("OK")
    except Exception:
        return PlainTextResponse("Error", status_code=500)

async def health_check(request):
    return PlainTextResponse("OK", status_code=200)

# ASGI application
app = Starlette(routes=[
    Route(f"/{TOKEN}", endpoint=webhook_endpoint, methods=["POST"]),
    Route("/", endpoint=health_check, methods=["GET", "HEAD"])
])

async def startup():
    application_tg.add_error_handler(error_handler)
    application_tg.add_handler(CommandHandler("start", start))
    application_tg.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.Regex(r'^\d+$'), handle_movie_code))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_buttons))
    application_tg.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & ~filters.Regex(r'^\d+$'), handle_non_button_text))
    application_tg.add_handler(ChatJoinRequestHandler(handle_join_request))
    
    await application_tg.initialize()
    
    global MOVIE_DICT, USER_CACHE, JOIN_REQUESTS_CACHE
    if movie_sheet:
        all_values = movie_sheet.get_all_values()[1:]
        MOVIE_DICT = {row[0].strip(): row[1].strip() for row in all_values if row and len(row) >= 2}
    if user_sheet:
        all_values = user_sheet.get_all_values()[1:]
        USER_CACHE = {row[0]: {"user_id": row[0], "username": row[1] if len(row) > 1 else "", "first_name": row[2] if len(row) > 2 else "", "search_queries": row[3] if len(row) > 3 else "0", "invited_users": row[4] if len(row) > 4 else "0"} for row in all_values if row}
    if join_requests_sheet:
        all_values = join_requests_sheet.get_all_values()[1:]
        JOIN_REQUESTS_CACHE = {(row[0], row[1]) for row in all_values if row and len(row) >= 2}
    
    asyncio.create_task(sync_cache_periodically())
    asyncio.create_task(batch_sync_to_sheets())
    
    await application_tg.bot.set_webhook(url=f"{WEBHOOK_URL}/{TOKEN}")

async def shutdown():
    await application_tg.stop()
    await application_tg.shutdown()

app.add_event_handler("startup", startup)
app.add_event_handler("shutdown", shutdown)