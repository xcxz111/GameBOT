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
    CREATE TABLE IF NOT EXISTS app_settings (
        `key` VARCHAR(64) NOT NULL PRIMARY KEY,
        `value` VARCHAR(255) NULL,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
    """
    CREATE TABLE IF NOT EXISTS game21_bot_settings (
        id TINYINT NOT NULL PRIMARY KEY,
        enabled TINYINT(1) NOT NULL DEFAULT 0,
        enabled_users TINYINT(1) NOT NULL DEFAULT 1,
        commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
        commission_users_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS game21_bot_sessions (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        status VARCHAR(20) NOT NULL DEFAULT 'active',
        bet_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
        result VARCHAR(20) NULL,
        winner VARCHAR(32) NULL,
        net_result DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        total_rounds INT NOT NULL DEFAULT 0,
        total_wins INT NOT NULL DEFAULT 0,
        total_losses INT NOT NULL DEFAULT 0,
        total_draws INT NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
        INDEX idx_21_user (user_id),
        INDEX idx_21_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS game21_bot_rounds (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        session_id BIGINT NOT NULL,
        round_number INT NOT NULL,
        player_cards VARCHAR(255) NULL,
        bot_cards VARCHAR(255) NULL,
        player_points TINYINT NULL,
        bot_points TINYINT NULL,
        result VARCHAR(20) NULL,
        winner VARCHAR(32) NULL,
        bet_amount DECIMAL(10,2) NULL,
        commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
        net_result DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (session_id) REFERENCES game21_bot_sessions(id) ON DELETE CASCADE,
        INDEX idx_21_round_session (session_id, round_number)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS game21_users_sessions (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        player1_id BIGINT NOT NULL,
        player2_id BIGINT NOT NULL,
        bet_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00,
        commission_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
        result VARCHAR(20) NOT NULL DEFAULT 'draw',
        winner_id BIGINT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_21_users_created (created_at),
        INDEX idx_21_users_winner (winner_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE IF NOT EXISTS game21_users_rounds (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        session_id BIGINT NOT NULL,
        phase VARCHAR(20) NOT NULL,
        user_id BIGINT NOT NULL,
        throw_order INT NOT NULL,
        value TINYINT NOT NULL,
        total_after INT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_21_users_rounds_session (session_id, throw_order),
        INDEX idx_21_users_rounds_user (user_id)
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


def _migrate_21_commission_users_percent(cur):
    """Добавить отдельную комиссию для режима 21 между пользователями."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'game21_bot_settings' AND COLUMN_NAME = 'commission_users_percent'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return
    cur.execute("ALTER TABLE game21_bot_settings ADD COLUMN commission_users_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00 AFTER commission_percent")


def _migrate_21_enabled_users(cur):
    """Добавить флаг enabled_users для режима 21 против пользователей."""
    cur.execute("""
        SELECT COLUMN_NAME FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'game21_bot_settings' AND COLUMN_NAME = 'enabled_users'
    """, (MYSQL_DATABASE,))
    if cur.fetchone():
        return
    cur.execute("ALTER TABLE game21_bot_settings ADD COLUMN enabled_users TINYINT(1) NOT NULL DEFAULT 1 AFTER enabled")


def _migrate_21_sessions_and_rounds(cur):
    """Добавить недостающие поля в таблицы истории 21."""
    for col_sql in [
        ("game21_bot_sessions", "bet_amount", "ALTER TABLE game21_bot_sessions ADD COLUMN bet_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00 AFTER status"),
        ("game21_bot_sessions", "commission_percent", "ALTER TABLE game21_bot_sessions ADD COLUMN commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00 AFTER bet_amount"),
        ("game21_bot_sessions", "result", "ALTER TABLE game21_bot_sessions ADD COLUMN result VARCHAR(20) NULL AFTER commission_percent"),
        ("game21_bot_sessions", "winner", "ALTER TABLE game21_bot_sessions ADD COLUMN winner VARCHAR(32) NULL AFTER result"),
        ("game21_bot_sessions", "net_result", "ALTER TABLE game21_bot_sessions ADD COLUMN net_result DECIMAL(10,2) NOT NULL DEFAULT 0.00 AFTER result"),
        ("game21_bot_rounds", "commission_percent", "ALTER TABLE game21_bot_rounds ADD COLUMN commission_percent DECIMAL(5,2) NOT NULL DEFAULT 0.00 AFTER bet_amount"),
        ("game21_bot_rounds", "winner", "ALTER TABLE game21_bot_rounds ADD COLUMN winner VARCHAR(32) NULL AFTER result"),
        ("game21_bot_rounds", "net_result", "ALTER TABLE game21_bot_rounds ADD COLUMN net_result DECIMAL(10,2) NOT NULL DEFAULT 0.00 AFTER commission_percent"),
    ]:
        table, col, alter = col_sql
        cur.execute(
            """SELECT COLUMN_NAME FROM information_schema.COLUMNS
               WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s""",
            (MYSQL_DATABASE, table, col),
        )
        if not cur.fetchone():
            cur.execute(alter)


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
            try:
                _migrate_21_commission_users_percent(cur)
            except Exception:
                pass
            try:
                _migrate_21_enabled_users(cur)
            except Exception:
                pass
            try:
                _migrate_21_sessions_and_rounds(cur)
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
