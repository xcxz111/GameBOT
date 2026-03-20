"""Connection helper for the secondary/external MySQL database."""
import pymysql
from config import (
    EXT_MYSQL_HOST,
    EXT_MYSQL_PORT,
    EXT_MYSQL_USER,
    EXT_MYSQL_PASSWORD,
    EXT_MYSQL_DATABASE,
)


def get_external_connection():
    """Open connection to the secondary MySQL database."""
    host = (EXT_MYSQL_HOST or "").strip() or (EXT_DB_SERVER_IP or "").strip()
    # If DB_HOST is an internal alias like "db", prefer explicit server IP when provided.
    if host.lower() == "db" and (EXT_DB_SERVER_IP or "").strip():
        host = EXT_DB_SERVER_IP.strip()
    if not host:
        raise RuntimeError("External DB host is not configured (EXT_MYSQL_HOST/DB_HOST/EXT_DB_SERVER_IP).")
    if not EXT_MYSQL_DATABASE:
        raise RuntimeError("External DB database is not configured (EXT_MYSQL_DATABASE/MYSQL_DATABASE).")
    return pymysql.connect(
        host=host,
        port=EXT_MYSQL_PORT,
        user=EXT_MYSQL_USER,
        password=EXT_MYSQL_PASSWORD,
        database=EXT_MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
