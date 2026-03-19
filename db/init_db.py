"""
Создание таблиц. Запуск из корня проекта: python -m db.init_db
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE


def get_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )


TABLES = [
    """
    CREATE TABLE IF NOT EXISTS city (
        id INT AUTO_INCREMENT PRIMARY KEY,
        city VARCHAR(100) NOT NULL,
        INDEX idx_city (city)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id BIGINT NOT NULL PRIMARY KEY,
        user_name VARCHAR(100) NULL,
        name VARCHAR(200) NULL,
        city_id INT NULL,
        city_changed_at DATETIME NULL,
        language_code VARCHAR(10) NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_user_name (user_name),
        INDEX idx_created_at (created_at),
        FOREIGN KEY (city_id) REFERENCES city(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS games (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        game_type VARCHAR(20) NOT NULL DEFAULT 'dice',
        chat_id BIGINT NOT NULL,
        city_id INT NULL,
        start_time DATETIME NOT NULL,
        min_participants INT NOT NULL DEFAULT 2,
        max_participants INT NOT NULL DEFAULT 50,
        prize_places INT NOT NULL DEFAULT 3,
        status VARCHAR(20) NOT NULL DEFAULT 'draft',
        current_round INT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_status (status),
        INDEX idx_start_time (start_time),
        INDEX idx_city_id (city_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS prizes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        game_id INT NOT NULL,
        place_number INT NOT NULL,
        prize_name VARCHAR(255) NOT NULL DEFAULT '',
        coupon_text TEXT NOT NULL,
        FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
        INDEX idx_game_id (game_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS participants (
        id INT AUTO_INCREMENT PRIMARY KEY,
        game_id INT NOT NULL,
        user_id BIGINT NOT NULL,
        eliminated_round INT NULL,
        registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_game_user (game_id, user_id),
        FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
        INDEX idx_game_id (game_id),
        INDEX idx_user_id (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS throws (
        id INT AUTO_INCREMENT PRIMARY KEY,
        game_id INT NOT NULL,
        user_id BIGINT NOT NULL,
        round_number INT NOT NULL,
        throw_index TINYINT NOT NULL,
        value TINYINT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
        INDEX idx_game_round_user (game_id, round_number, user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS admins (
        user_id BIGINT NOT NULL PRIMARY KEY,
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS user_prizes (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        game_id INT NOT NULL,
        place_number INT NOT NULL,
        prize_name VARCHAR(255) NOT NULL DEFAULT '',
        coupon_text TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
        FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE,
        INDEX idx_user_id (user_id),
        INDEX idx_game_id (game_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
]


def _migrate_users_city(cur):
    """Если в users есть колонка city (varchar), заменить на city_id (ссылка на city)."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'users' AND COLUMN_NAME = 'city'
    """, (MYSQL_DATABASE,))
    if not cur.fetchone():
        return  # уже city_id или новая схема
    cur.execute("ALTER TABLE users ADD COLUMN city_id INT NULL AFTER name")
    cur.execute("ALTER TABLE users ADD CONSTRAINT fk_users_city FOREIGN KEY (city_id) REFERENCES city(id) ON DELETE SET NULL")
    cur.execute("ALTER TABLE users DROP COLUMN city")


def _migrate_games_city_id(cur):
    """Добавить в games колонку city_id (NULL = для всех, иначе id города)."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'games' AND COLUMN_NAME = 'city_id'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return  # уже есть
    cur.execute("ALTER TABLE games ADD COLUMN city_id INT NULL AFTER chat_id, ADD INDEX idx_city_id (city_id)")


def _migrate_games_reminder_5min(cur):
    """Добавить в games колонку reminder_5min_sent (напоминание за 5 мин отправлено)."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'games' AND COLUMN_NAME = 'reminder_5min_sent'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return
    cur.execute("ALTER TABLE games ADD COLUMN reminder_5min_sent TINYINT(1) NOT NULL DEFAULT 0 AFTER status")


def _migrate_prizes_prize_name(cur):
    """Добавить в prizes колонку prize_name для названия приза (до двоеточия)."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'prizes' AND COLUMN_NAME = 'prize_name'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return
    cur.execute("ALTER TABLE prizes ADD COLUMN prize_name VARCHAR(255) NOT NULL DEFAULT '' AFTER place_number")


def _migrate_users_city_changed_at(cur):
    """Добавить в users колонку city_changed_at для лимита смены города."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'users' AND COLUMN_NAME = 'city_changed_at'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return
    cur.execute("ALTER TABLE users ADD COLUMN city_changed_at DATETIME NULL AFTER city_id")


def create_tables():
    """Создать таблицы в БД (безопасно вызывать при каждом запуске — CREATE TABLE IF NOT EXISTS)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for sql in TABLES:
                cur.execute(sql)
            try:
                _migrate_users_city(cur)
            except Exception:
                pass  # уже мигрировано или новая установка
            try:
                _migrate_games_city_id(cur)
            except Exception:
                pass
            try:
                _migrate_games_reminder_5min(cur)
            except Exception:
                pass
            try:
                _migrate_prizes_prize_name(cur)
            except Exception:
                pass
            try:
                _migrate_users_city_changed_at(cur)
            except Exception:
                pass
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


def main():
    create_tables()
    print("Таблицы созданы: city, users, games, prizes, participants, throws, admins.")


if __name__ == "__main__":
    main()
