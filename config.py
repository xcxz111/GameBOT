import os
from dotenv import load_dotenv

load_dotenv()

# Telegram
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))  # чат, в котором бот работает (в остальных не отвечает)
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

# MySQL
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "gamebot")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "game_bot")

# External/secondary MySQL (for integrations, e.g. 21 settings/billing)
# Prefer EXT_MYSQL_* variables; fallback to DB_HOST/DB_PORT + existing MySQL names if needed.
EXT_DB_SERVER_IP = os.getenv("EXT_DB_SERVER_IP", "")
EXT_MYSQL_HOST = os.getenv("EXT_MYSQL_HOST", os.getenv("DB_HOST", ""))
EXT_MYSQL_PORT = int(os.getenv("EXT_MYSQL_PORT", os.getenv("DB_PORT", "3306")))
EXT_MYSQL_USER = os.getenv("EXT_MYSQL_USER", os.getenv("MYSQL_USER", ""))
EXT_MYSQL_PASSWORD = os.getenv(
    "EXT_MYSQL_PASSWORD",
    os.getenv("MYSQL_PASSWORD", os.getenv("MYSQL_ROOT_PASSWORD", "")),
)
EXT_MYSQL_DATABASE = os.getenv("EXT_MYSQL_DATABASE", os.getenv("MYSQL_DATABASE", ""))
