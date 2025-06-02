import os
import logging
import json
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, CallbackContext, MessageHandler, Filters
from flask import Flask
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация Flask
app = Flask(__name__)

# --- Конфигурация из переменных окружения ---
TOKEN = os.environ.get("BOT_TOKEN")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 443))
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS")

# Проверка обязательных переменных окружения
if not WEBHOOK_URL:
    logger.error("WEBHOOK_URL не задан в переменных окружения!")
    raise ValueError("WEBHOOK_URL не задан в переменных окружения!")
if not TOKEN:
    logger.error("BOT_TOKEN не задан в переменных окружения!")
    raise ValueError("BOT_TOKEN не задан в переменных окружения!")
if not GOOGLE_CREDENTIALS:
    logger.error("GOOGLE_CREDENTIALS не задан в переменных окружения!")
    raise ValueError("GOOGLE_CREDENTIALS не задан в переменных окружения!")

# --- Настройка Google Sheets ---
# Использование глобальной переменной для `sheet`
sheet = None
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(GOOGLE_CREDENTIALS), scope)
    client = gspread.authorize(creds)
    # Предполагаем, что "MovieDatabase" - это название таблицы, а sheet1 - первый лист
    sheet = client.open("MovieDatabase").sheet1
    logger.info("Google Sheets успешно инициализирован.")
except Exception as e:
    logger.error(f"Ошибка при инициализации Google Sheets: {e}")
    # Важно не вызывать raise здесь, если бот должен запускаться без подключения к Sheets
    # Но для критической зависимости, как здесь, можно оставить raise
    raise

# --- Конфигурация каналов (можно сделать динамической, если нужно) ---
# Для демонстрации оставим как есть, но в реальном проекте можно загружать из файла/БД
CHANNELS = [
    "-1002657330561", # Пример ID канала
    "-1002243633174",
    "-1002484534545",
    "-1002578865225",
    "-1002617434713",
]

# URL-адреса для кнопок подписки (лучше хранить их динамически, например, в той же таблице Sheets)
CHANNEL_BUTTONS = [
    {"text": "Канал 1 — Смотри новинки", "url": "https://t.me/+8qO35jVzZVs5MjMy"},
    {"text": "Канал 2 — Лучше фильмы", "url": "https://t.me/+ZAvb9OTIrU9mOWIy"},
    {"text": "Канал 3 — Премии и хиты", "url": "https://t.me/+PAu2GRMZuUU0ZWQy"},
    {"text": "Канал 4 — Кино без рекламы", "url": "https://t.me/+kO2CPJZgxediMmZi"},
    {"text": "Канал 5 — Эксклюзивы", "url": "https://t.me/+DUDDSAYIDl8yN2Ni"},
]

# Простой обработчик для корневого URL (healthcheck)
@app.route('/')
def health_check():
    return "Bot is alive", 200

def start(update: Update, context: CallbackContext) -> None:
    """Обработчик команды /start."""
    user = update.message.from_user
    logger.info(f"Пользователь {user.id} {user.first_name} запустил бота.")
    welcome_text = (
        "Привет! 👋\n"
        "Напиши код фильма, и я помогу тебе узнать его название. 🎬\n\n"
    )
    update.message.reply_text(welcome_text)

def prompt_subscribe(update: Update, context: CallbackContext, message_id=None) -> None:
    """
    Запрашивает у пользователя подписку на каналы.
    Может редактировать существующее сообщение или отправлять новое.
    """
    promo_text = (
        "Чтобы продолжить поиск фильма, сначала подпишись на наших спонсоров!\n"
        "Когда сделаешь всё, нажми кнопку и мы продолжим!"
    )
    keyboard = []
    for btn_info in CHANNEL_BUTTONS:
        keyboard.append([InlineKeyboardButton(btn_info["text"], url=btn_info["url"])])
    keyboard.append([InlineKeyboardButton("✅ Я ПОДПИСАЛСЯ!", callback_data="check_subscription")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    if message_id:
        # Пытаемся отредактировать сообщение (если это callback-запрос, например)
        try:
            context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=message_id,
                text=promo_text,
                reply_markup=reply_markup
            )
        except Exception as e:
            logger.warning(f"Не удалось отредактировать сообщение {message_id}: {e}. Отправляем новое.")
            update.effective_message.reply_text(promo_text, reply_markup=reply_markup)
    else:
        # Отправляем новое сообщение (если это текстовое сообщение)
        update.message.reply_text(promo_text, reply_markup=reply_markup)

def check_subscription(update: Update, context: CallbackContext) -> None:
    """Проверяет подписку пользователя на все каналы."""
    query = update.callback_query
    query.answer() # Всегда отвечаем на callback-запрос
    user_id = query.from_user.id
    bot = context.bot
    all_subscribed = True
    failed_channel = None

    for channel_id in CHANNELS:
        try:
            member = bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status not in ["member", "administrator", "creator"]:
                all_subscribed = False
                failed_channel = channel_id # Запоминаем, на какой канал не подписан
                break
        except Exception as e:
            logger.error(f"Ошибка при проверке подписки на канал {channel_id}: {e}")
            all_subscribed = False
            failed_channel = channel_id
            break

    if all_subscribed:
        context.user_data['subscribed'] = True
        query.message.reply_text(
            "🎉 Поздравляю! Ты подписался на все каналы.\n"
            "Теперь можешь отправить код фильма, и я найду его название! 🍿"
        )
        # Если у пользователя был незавершенный запрос на поиск фильма, можно его повторить
        if 'pending_movie_code' in context.user_data:
            code = context.user_data.pop('pending_movie_code')
            handle_movie_code_internal(query.message, context, code) # Вызываем внутренний обработчик
    else:
        # Добавляем более информативное сообщение о неподписке
        error_message = (
            "😕 Похоже, ты подписался не на все каналы.\n"
            "Пожалуйста, проверь ещё раз и нажми '✅ Я ПОДПИСАЛСЯ!'.\n"
        )
        if failed_channel:
            error_message += f"Возможно, ты не подписан на канал с ID: `{failed_channel}`." # Для отладки
        query.message.reply_text(error_message, parse_mode='Markdown')
        # Снова показываем кнопки подписки, чтобы пользователь мог вернуться к ним
        prompt_subscribe(update, context, message_id=query.message.message_id)


def find_movie_by_code(code: str) -> dict:
    """Ищет фильм по коду в Google Sheets."""
    if sheet is None:
        logger.error("Google Sheets не инициализирован. Невозможно выполнить поиск.")
        return None # Возвращаем None, если таблица недоступна

    try:
        # Для лучшей производительности, особенно с большими таблицами,
        # можно использовать `get_all_values()` и искать вручную,
        # или настроить `gspread` для работы с колонками по индексам,
        # если названия колонок могут меняться.
        # Здесь предполагаем, что заголовок "Код" и "Название" точно есть.
        data = sheet.get_all_records()
        for row in data:
            # Используем .get() для безопасного доступа к ключам
            if str(row.get("Код")) == code: # Преобразуем код из таблицы в строку для сравнения
                return {"code": row.get("Код"), "title": row.get("Название")}
        return None
    except gspread.exceptions.APIError as e:
        logger.error(f"Ошибка API Google Sheets при доступе: {e}")
        return None
    except Exception as e:
        logger.error(f"Неизвестная ошибка при доступе к Google Sheets: {e}")
        return None

def handle_movie_code_internal(message, context: CallbackContext, code: str) -> None:
    """Внутренний обработчик кода фильма, который может быть вызван из разных мест."""
    movie = find_movie_by_code(code)
    if movie:
        message.reply_text(f"🎥 Фильм: *{movie['title']}*", parse_mode='Markdown')
    else:
        message.reply_text(f"К сожалению, фильм с кодом `{code}` не найден! Попробуй другой код.", parse_mode='Markdown')

def handle_movie_code(update: Update, context: CallbackContext) -> None:
    """Основной обработчик текстовых сообщений с кодом фильма."""
    code = update.message.text.strip()

    # Проверяем, число ли это, чтобы отсеять другие сообщения
    if not code.isdigit():
        update.message.reply_text("Пожалуйста, введи только числовой код фильма. 🔢")
        return

    if not context.user_data.get('subscribed'):
        # Сохраняем код, чтобы обработать его после подписки
        context.user_data['pending_movie_code'] = code
        prompt_subscribe(update, context)
        return

    handle_movie_code_internal(update.message, context, code)

def main() -> None:
    """Запускает бота."""
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher

    # Добавляем обработчики
    dp.add_handler(CommandHandler("start", start))
    dp.add_handler(CallbackQueryHandler(check_subscription, pattern="check_subscription"))
    # Фильтруем только числовые текстовые сообщения, не команды
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command & Filters.regex(r'^\d+$'), handle_movie_code))
    # Добавляем fallback для нечисловых сообщений после подписки
    dp.add_handler(MessageHandler(Filters.text & ~Filters.command, lambda u, c: u.message.reply_text("Пожалуйста, введи *только числовой* код фильма. 🔢", parse_mode='Markdown')))


    full_webhook_url = f"{WEBHOOK_URL}/{TOKEN}"
    logger.info(f"Установка вебхука на: {full_webhook_url}")
    logger.info(f"Использование порта: {PORT}")

    try:
        updater.start_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=TOKEN,
            webhook_url=full_webhook_url,
            # drop_pending_updates=True # Можно добавить, чтобы не обрабатывать старые обновления при запуске
        )
        logger.info("Вебхук успешно запущен.")
    except Exception as e:
        logger.error(f"Не удалось запустить вебхук: {e}")
        # Если вебхук не запускается, бот не будет работать.
        # В производственной среде это может быть критично.
        raise

    # Flask приложение для обработки запросов от вебхука
    # В production-среде `app.run` обычно не используется напрямую в этом файле.
    # Вместо этого Flask запускается с помощью Gunicorn/uWSGI/etc.
    # Но для Heroku или подобных платформ, где gunicorn/uwsgi запускает Flask,
    # и `Updater` просто конфигурирует вебхук, такой подход может работать.
    # Если вы используете эту структуру, убедитесь, что ваш хостинг
    # правильно запускает Flask-приложение, а не только этот Python-скрипт.
    logger.info("Запуск Flask-приложения...")
    app.run(host='0.0.0.0', port=PORT)

if __name__ == "__main__":
    main()