# /root/scripts/5_verst/telegram_notifier.py
import configparser
from pathlib import Path
import requests
import re
import sys

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "5_verst.ini"

MAX_TELEGRAM_MESSAGE_LENGTH = 3500


def load_telegram_config():
    """
    Читаем token и chat_id из секции [telegram_notification] файла 5_verst.ini
    """
    if not CONFIG_PATH.exists():
        print(f"Файл настроек {CONFIG_PATH} не найден!", file=sys.stderr)
        return None, None

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH, encoding="utf-8")

    try:
        section = config["telegram_notification"]
        token = section["token"].strip()
        chat_id = section["chat_id"].strip()
        return token, chat_id
    except KeyError:
        print(
            "В ini-файле нет секции [telegram_notification] или полей token/chat_id.",
            file=sys.stderr,
        )
        return None, None


def clean_text_for_telegram(text: str) -> str:
    """
    Убираем только реальные управляющие символы,
    не трогая emoji и обычный unicode.
    """
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)
    if len(text) > MAX_TELEGRAM_MESSAGE_LENGTH:
        text = text[:MAX_TELEGRAM_MESSAGE_LENGTH - 3] + "..."
    return text


def send_telegram_notification(message: str) -> bool:
    token, chat_id = load_telegram_config()

    if not token or not chat_id:
        print("Telegram не настроен: пустой token или chat_id", file=sys.stderr)
        return False

    message = clean_text_for_telegram(message)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
        "parse_mode": "MarkdownV2"
    }

    try:
        resp = requests.post(url, data=data, timeout=30)
        if not resp.ok:
            print(
                f"Ошибка отправки уведомления в Telegram: "
                f"{resp.status_code} {resp.text}",
                file=sys.stderr,
            )
            resp.raise_for_status()
        return True
    except Exception as e:
        print(f"Исключение при отправке уведомления в Telegram: {e}", file=sys.stderr)
        return False

def escape_markdown(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in escape_chars else c for c in text)