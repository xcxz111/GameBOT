"""Запросы к БД: пользователи, города, игры, призы."""
from typing import Optional
from datetime import datetime
from db.init_db import get_connection
from db.external_db import get_external_connection


def get_cities():
    """Список всех городов: [{"id": ..., "city": ...}, ...]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, city FROM city ORDER BY city")
            return cur.fetchall()


def create_city(name: str) -> Optional[int]:
    """Создать город, вернуть id или None при ошибке."""
    if not name or not str(name).strip():
        return None
    name = str(name).strip()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO city (city) VALUES (%s)", (name,))
            conn.commit()
            return cur.lastrowid


def get_city(city_id: int):
    """Город по id или None."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, city FROM city WHERE id = %s", (city_id,))
            return cur.fetchone()


def update_city(city_id: int, name: str) -> bool:
    """Обновить название города."""
    if not name or not str(name).strip():
        return False
    name = str(name).strip()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE city SET city = %s WHERE id = %s", (name, city_id))
            conn.commit()
            return cur.rowcount > 0


def delete_city(city_id: int) -> bool:
    """Удалить город; у пользователей с этим городом выставить city_id = NULL."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET city_id = NULL WHERE city_id = %s", (city_id,))
            cur.execute("DELETE FROM city WHERE id = %s", (city_id,))
            conn.commit()
            return cur.rowcount > 0


def get_user(user_id: int):
    """Пользователь по user_id или None."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
            return cur.fetchone()


def is_admin_user(user_id: int) -> bool:
    """Проверка, есть ли пользователь в таблице admins."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM admins WHERE user_id = %s LIMIT 1", (user_id,))
            return cur.fetchone() is not None


def add_admin_user(user_id: int) -> bool:
    """Добавить пользователя в таблицу admins."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT IGNORE INTO admins (user_id) VALUES (%s)", (user_id,))
            conn.commit()
            return cur.rowcount > 0


def get_active_chat_id() -> Optional[int]:
    """Получить активный chat_id из app_settings."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT `value` FROM app_settings WHERE `key` = 'active_chat_id' LIMIT 1")
            row = cur.fetchone()
            if not row:
                return None
            try:
                return int(row.get("value"))
            except Exception:
                return None


def set_active_chat_id(chat_id: int) -> bool:
    """Сохранить активный chat_id в app_settings."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO app_settings (`key`, `value`)
                   VALUES ('active_chat_id', %s)
                   ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)""",
                (str(int(chat_id)),),
            )
            conn.commit()
            return True


def save_user(user_id: int, user_name: Optional[str], name: Optional[str], city_id: Optional[int] = None, language_code: Optional[str] = None):
    """Создать или обновить пользователя (при первом заходе создаём, city_id ставим при выборе города)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO users (user_id, user_name, name, city_id, language_code)
                   VALUES (%s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                   user_name = VALUES(user_name),
                   name = VALUES(name),
                   city_id = COALESCE(VALUES(city_id), city_id),
                   language_code = COALESCE(VALUES(language_code), language_code)""",
                (user_id, user_name or None, name or None, city_id, language_code or None),
            )
        conn.commit()


def update_user_city(user_id: int, city_id: Optional[int]) -> bool:
    """Обновить город пользователя и timestamp смены города."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE users SET city_id = %s, city_changed_at = NOW() WHERE user_id = %s",
                (city_id, user_id),
            )
            conn.commit()
            return cur.rowcount > 0


def create_game(
    name: str,
    game_type: str,
    chat_id: int,
    start_time: datetime,
    min_participants: int,
    max_participants: int,
    prize_places: int,
    city_id: Optional[int] = None,
) -> Optional[int]:
    """Создать игру. city_id: NULL = для всех, иначе id города. Вернуть game_id или None."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO games (name, game_type, chat_id, city_id, start_time, min_participants, max_participants, prize_places, status)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'draft')""",
                (name, game_type, chat_id, city_id, start_time, min_participants, max_participants, prize_places),
            )
            conn.commit()
            return cur.lastrowid


def _split_prize_text(raw: str):
    """Разобрать строку формата 'Название приза: код/купон' на (name, coupon)."""
    raw = (raw or "").strip()
    if ":" not in raw:
        return "", raw
    name, coupon = raw.split(":", 1)
    return name.strip(), coupon.strip()


def add_prize(game_id: int, place_number: int, raw_text: str) -> bool:
    """Добавить приз к игре. raw_text: 'Название приза: код/купон'."""
    prize_name, coupon_text = _split_prize_text(raw_text or "")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO prizes (game_id, place_number, prize_name, coupon_text) VALUES (%s, %s, %s, %s)",
                (game_id, place_number, prize_name, coupon_text or ""),
            )
            conn.commit()
            return True


def get_games_current():
    """Текущие игры: статусы draft и active."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, game_type, start_time, min_participants, max_participants, prize_places, city_id "
                "FROM games "
                "WHERE status IN ('draft', 'active') "
                "ORDER BY start_time"
            )
            return cur.fetchall()


def get_games_finished():
    """Прошедшие игры: только со статусом finish."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, game_type, start_time, min_participants, max_participants, prize_places, city_id "
                "FROM games WHERE status = 'finish' ORDER BY start_time DESC"
            )
            return cur.fetchall()


def count_participants(game_id: int) -> int:
    """Количество записанных участников игры."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM participants WHERE game_id = %s", (game_id,))
            row = cur.fetchone() or {}
            return int(row.get("cnt") or 0)


def get_games_need_5min_reminder():
    """Игры, до начала которых 5–6 минут (окно 1 мин при проверке раз в минуту) и напоминание ещё не отправляли."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, chat_id FROM games
                   WHERE status = 'draft' AND reminder_5min_sent = 0
                     AND start_time > NOW() + INTERVAL 5 MINUTE
                     AND start_time <= NOW() + INTERVAL 6 MINUTE
                   ORDER BY start_time"""
            )
            return cur.fetchall()


def set_game_reminder_5min_sent(game_id: int) -> bool:
    """Отметить, что напоминание за 5 мин для игры отправлено."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE games SET reminder_5min_sent = 1 WHERE id = %s",
                (game_id,),
            )
            conn.commit()
            return cur.rowcount > 0


def get_games_to_start():
    """Игры, у которых время старта наступило не менее 1 минуты назад (буфер от рассинхрона часов)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT id, name, game_type, chat_id, city_id
                   FROM games
                   WHERE status = 'draft'
                     AND start_time <= NOW() - INTERVAL 1 MINUTE
                   ORDER BY start_time"""
            )
            return cur.fetchall()


def get_game(game_id: int):
    """Игра по id или None."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM games WHERE id = %s", (game_id,))
            return cur.fetchone()


def get_prizes(game_id: int):
    """Призы игры по place_number."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT place_number, prize_name, coupon_text FROM prizes WHERE game_id = %s ORDER BY place_number",
                (game_id,),
            )
            return cur.fetchall()


def get_participants_user_ids(game_id: int):
    """Список user_id участников игры."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM participants WHERE game_id = %s", (game_id,))
            return [row["user_id"] for row in cur.fetchall()]


def get_participants_for_display(game_id: int):
    """Участники игры для объявления: [(user_id, display_name), ...] по порядку записи."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT p.user_id, u.name, u.user_name
                   FROM participants p
                   LEFT JOIN users u ON u.user_id = p.user_id
                   WHERE p.game_id = %s ORDER BY p.registered_at, p.id""",
                (game_id,),
            )
            rows = cur.fetchall()
    result = []
    for row in rows:
        uid = row["user_id"]
        name = (row.get("name") or "").strip() or (row.get("user_name") or "").strip() or str(uid)
        result.append((uid, name))
    return result


def add_participant(game_id: int, user_id: int) -> bool:
    """Записать пользователя на игру. Вернуть True если записан, False если уже был записан."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT IGNORE INTO participants (game_id, user_id) VALUES (%s, %s)",
                (game_id, user_id),
            )
            conn.commit()
            return cur.rowcount > 0


def update_game(
    game_id: int,
    game_type: Optional[str] = None,
    min_participants: Optional[int] = None,
    max_participants: Optional[int] = None,
    prize_places: Optional[int] = None,
    start_time: Optional[datetime] = None,
    name: Optional[str] = None,
    status: Optional[str] = None,
) -> bool:
    """Обновить поля игры (только переданные)."""
    updates = []
    args = []
    if game_type is not None:
        updates.append("game_type = %s")
        args.append(game_type)
    if min_participants is not None:
        updates.append("min_participants = %s")
        args.append(min_participants)
    if max_participants is not None:
        updates.append("max_participants = %s")
        args.append(max_participants)
    if prize_places is not None:
        updates.append("prize_places = %s")
        args.append(prize_places)
    if start_time is not None:
        updates.append("start_time = %s")
        args.append(start_time)
    if name is not None:
        updates.append("name = %s")
        args.append(name)
    if status is not None:
        updates.append("status = %s")
        args.append(status)
    if not updates:
        return True
    args.append(game_id)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE games SET {', '.join(updates)} WHERE id = %s",
                args,
            )
            conn.commit()
            return cur.rowcount > 0


def add_throw(game_id: int, user_id: int, round_number: int, throw_index: int, value: int):
    """Записать один бросок участника в раунде."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO throws (game_id, user_id, round_number, throw_index, value)
                   VALUES (%s, %s, %s, %s, %s)""",
                (game_id, user_id, round_number, throw_index, value),
            )
            conn.commit()


def get_round_totals(game_id: int, round_number: int):
    """Сумма очков по участникам за раунд (только 3 основных броска): [(user_id, total), ...] по user_id."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT user_id, SUM(value) AS total
                   FROM throws WHERE game_id = %s AND round_number = %s AND throw_index < 3
                   GROUP BY user_id""",
                (game_id, round_number),
            )
            return [(row["user_id"], int(row["total"])) for row in cur.fetchall()]


def get_round_tiebreak_totals(game_id: int, round_number: int):
    """Сумма доп. бросков тай-брейка за раунд (throw_index >= 3): [(user_id, total), ...]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT user_id, COALESCE(SUM(value), 0) AS total
                   FROM throws
                   WHERE game_id = %s AND round_number = %s AND throw_index >= 3
                   GROUP BY user_id""",
                (game_id, round_number),
            )
            return [(row["user_id"], int(row["total"])) for row in cur.fetchall()]


def get_all_round_totals(game_id: int, max_round: int):
    """Тоталы по раундам 1..max_round: {round_number: [(user_id, total), ...]}."""
    result = {}
    for r in range(1, max_round + 1):
        result[r] = get_round_totals(game_id, r)
    return result


def delete_prizes(game_id: int):
    """Удалить все призы игры."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM prizes WHERE game_id = %s", (game_id,))
            conn.commit()


def set_prizes(game_id: int, prize_list: list):
    """Заменить призы игры: удалить старые, записать новые (список строк 'Название приза: код')."""
    delete_prizes(game_id)
    for place, raw_text in enumerate(prize_list, 1):
        add_prize(game_id, place, raw_text or "")


def delete_game(game_id: int) -> bool:
    """Удалить игру (CASCADE удалит prizes, participants). Вернуть True если удалено."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM games WHERE id = %s", (game_id,))
            conn.commit()
            return cur.rowcount > 0


def add_user_prize(user_id: int, game_id: int, place_number: int, prize_name: str, coupon_text: str) -> bool:
    """Записать выигранный пользователем приз."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO user_prizes (user_id, game_id, place_number, prize_name, coupon_text)
                   VALUES (%s, %s, %s, %s, %s)""",
                (user_id, game_id, place_number, prize_name or "", coupon_text or ""),
            )
            conn.commit()
            return True


def get_user_prizes(user_id: int):
    """История выигранных призов пользователя (новые сверху)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT up.id, up.game_id, up.place_number, up.prize_name, up.coupon_text, up.created_at,
                          g.game_type, g.start_time
                   FROM user_prizes up
                   LEFT JOIN games g ON g.id = up.game_id
                   WHERE up.user_id = %s
                   ORDER BY up.created_at DESC, up.id DESC""",
                (user_id,),
            )
            return cur.fetchall()


def get_user_prize_by_id(user_id: int, user_prize_id: int):
    """Один приз пользователя по id (защита от доступа к чужим призам)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT up.id, up.game_id, up.place_number, up.prize_name, up.coupon_text, up.created_at,
                          g.game_type, g.start_time
                   FROM user_prizes up
                   LEFT JOIN games g ON g.id = up.game_id
                   WHERE up.user_id = %s AND up.id = %s
                   LIMIT 1""",
                (user_id, user_prize_id),
            )
            return cur.fetchone()


def get_game_winners(game_id: int):
    """Победители игры из user_prizes: [(place_number, user_id, name, prize_name, coupon_text), ...]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT up.place_number, up.user_id, up.prize_name, up.coupon_text, u.name, u.user_name
                   FROM user_prizes up
                   LEFT JOIN users u ON u.user_id = up.user_id
                   WHERE up.game_id = %s
                   ORDER BY up.place_number ASC, up.id ASC""",
                (game_id,),
            )
            rows = cur.fetchall()
    result = []
    for row in rows:
        uid = row["user_id"]
        name = (row.get("name") or "").strip() or (row.get("user_name") or "").strip() or str(uid)
        result.append(
            (
                int(row["place_number"]),
                uid,
                name,
                (row.get("prize_name") or "").strip(),
                (row.get("coupon_text") or "").strip(),
            )
        )
    return result


def get_external_user_balance(user_id: int, username: Optional[str] = None):
    """Баланс пользователя из внешней БД (таблица users.balance). Только чтение."""
    try:
        with get_external_connection() as conn:
            with conn.cursor() as cur:
                # Строго по ТЗ: users.id -> users.balance
                cur.execute("SELECT balance FROM users WHERE id = %s LIMIT 1", (user_id,))
                row = cur.fetchone()
                if row is not None and "balance" in row:
                    return row.get("balance")
    except Exception:
        return None
    return None


def update_external_user_balance(user_id: int, delta: float) -> bool:
    """Изменить баланс пользователя во внешней БД (users.balance = balance + delta)."""
    try:
        with get_external_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET balance = balance + %s WHERE id = %s",
                    (delta, user_id),
                )
                conn.commit()
                return cur.rowcount > 0
    except Exception:
        return False


def is_21_vs_bot_enabled() -> bool:
    """Статус режима 21 против бота."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT enabled FROM game21_bot_settings WHERE id = 1")
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO game21_bot_settings (id, enabled, commission_percent) VALUES (1, 0, 0.00)")
                conn.commit()
                return False
            return bool(row.get("enabled"))


def is_21_vs_users_enabled() -> bool:
    """Статус режима 21 против пользователей (в чате)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT enabled_users FROM game21_bot_settings WHERE id = 1")
            row = cur.fetchone()
            if not row:
                # В старых версиях без колонки, но у миграции есть дефолт.
                cur.execute(
                    "INSERT INTO game21_bot_settings (id, enabled, enabled_users, commission_percent, commission_users_percent) VALUES (1, 0, 1, 0.00, 0.00)"
                )
                conn.commit()
                return True
            return bool(row.get("enabled_users"))


def set_21_users_enabled(enabled: bool) -> bool:
    """Включить/выключить режим 21 против пользователей."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_settings
                   (id, enabled, enabled_users, commission_percent, commission_users_percent)
                   VALUES (1, 0, %s, 0.00, 0.00)
                   ON DUPLICATE KEY UPDATE enabled_users = VALUES(enabled_users)""",
                (1 if enabled else 0,),
            )
            conn.commit()
            return True


def set_21_vs_bot_enabled(enabled: bool) -> bool:
    """Включить/выключить режим 21 против бота."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_settings (id, enabled, commission_percent)
                   VALUES (1, %s, 0.00)
                   ON DUPLICATE KEY UPDATE enabled = VALUES(enabled)""",
                (1 if enabled else 0,),
            )
            conn.commit()
            return True


def get_21_bot_commission_percent() -> float:
    """Комиссия 21 против бота в процентах."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT commission_percent FROM game21_bot_settings WHERE id = 1")
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO game21_bot_settings (id, enabled, commission_percent) VALUES (1, 0, 0.00)")
                conn.commit()
                return 0.0
            try:
                return float(row.get("commission_percent") or 0.0)
            except Exception:
                return 0.0


def set_21_bot_commission_percent(percent: float) -> bool:
    """Установить комиссию 21 против бота в процентах (0..100)."""
    p = float(percent)
    if p < 0:
        p = 0.0
    if p > 100:
        p = 100.0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_settings (id, enabled, commission_percent)
                   VALUES (1, 0, %s)
                   ON DUPLICATE KEY UPDATE commission_percent = VALUES(commission_percent)""",
                (p,),
            )
            conn.commit()
            return True


def get_21_users_commission_percent() -> float:
    """Комиссия 21 между пользователями в процентах."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT commission_users_percent FROM game21_bot_settings WHERE id = 1")
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "INSERT INTO game21_bot_settings (id, enabled, commission_percent, commission_users_percent) VALUES (1, 0, 0.00, 0.00)"
                )
                conn.commit()
                return 0.0
            try:
                return float(row.get("commission_users_percent") or 0.0)
            except Exception:
                return 0.0


def set_21_users_commission_percent(percent: float) -> bool:
    """Установить комиссию 21 между пользователями в процентах (0..100)."""
    p = float(percent)
    if p < 0:
        p = 0.0
    if p > 100:
        p = 100.0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_settings (id, enabled, commission_percent, commission_users_percent)
                   VALUES (1, 0, 0.00, %s)
                   ON DUPLICATE KEY UPDATE commission_users_percent = VALUES(commission_users_percent)""",
                (p,),
            )
            conn.commit()
            return True


def get_21_rules_bot_text() -> Optional[str]:
    """Кастомный HTML-текст правил «21 против бота» из БД или None (тогда — перевод по умолчанию)."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT rules_bot_text FROM game21_bot_settings WHERE id = 1")
                row = cur.fetchone()
                if not row:
                    return None
                r = row.get("rules_bot_text")
                if r is None:
                    return None
                s = str(r).strip()
                return s if s else None
    except Exception:
        return None


def get_21_rules_users_text() -> Optional[str]:
    """Кастомный HTML-текст правил «21 между пользователями» из БД или None."""
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT rules_users_text FROM game21_bot_settings WHERE id = 1")
                row = cur.fetchone()
                if not row:
                    return None
                r = row.get("rules_users_text")
                if r is None:
                    return None
                s = str(r).strip()
                return s if s else None
    except Exception:
        return None


def set_21_rules_bot_text(text: Optional[str]) -> bool:
    """Сохранить правила vs bot; пустая строка / None — сброс к переводу по умолчанию."""
    val = (text or "").strip() or None
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM game21_bot_settings WHERE id = 1")
                if cur.fetchone():
                    cur.execute(
                        "UPDATE game21_bot_settings SET rules_bot_text = %s WHERE id = 1",
                        (val,),
                    )
                else:
                    cur.execute(
                        """INSERT INTO game21_bot_settings
                           (id, enabled, enabled_users, commission_percent, commission_users_percent, rules_bot_text, rules_users_text)
                           VALUES (1, 0, 1, 0.00, 0.00, %s, NULL)""",
                        (val,),
                    )
                conn.commit()
                return True
    except Exception:
        return False


def set_21_rules_users_text(text: Optional[str]) -> bool:
    """Сохранить правила vs users; пустая строка / None — сброс."""
    val = (text or "").strip() or None
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM game21_bot_settings WHERE id = 1")
                if cur.fetchone():
                    cur.execute(
                        "UPDATE game21_bot_settings SET rules_users_text = %s WHERE id = 1",
                        (val,),
                    )
                else:
                    cur.execute(
                        """INSERT INTO game21_bot_settings
                           (id, enabled, enabled_users, commission_percent, commission_users_percent, rules_bot_text, rules_users_text)
                           VALUES (1, 0, 1, 0.00, 0.00, NULL, %s)""",
                        (val,),
                    )
                conn.commit()
                return True
    except Exception:
        return False


def create_21_bot_session(user_id: int, bet_amount: float, commission_percent: float) -> Optional[int]:
    """Создать сессию 21 против бота."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_sessions
                   (user_id, status, bet_amount, commission_percent, total_rounds, total_wins, total_losses, total_draws)
                   VALUES (%s, 'active', %s, %s, 0, 0, 0, 0)""",
                (user_id, float(bet_amount), float(commission_percent)),
            )
            conn.commit()
            return cur.lastrowid


def close_21_bot_session(session_id: int, result: str, winner: str, net_result: float) -> bool:
    """Закрыть сессию 21 против бота итоговым результатом."""
    wins = 1 if result == "win" else 0
    losses = 1 if result == "lose" else 0
    draws = 1 if result == "draw" else 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE game21_bot_sessions
                   SET status = 'finish',
                       result = %s,
                       winner = %s,
                       net_result = %s,
                       total_rounds = 1,
                       total_wins = %s,
                       total_losses = %s,
                       total_draws = %s
                   WHERE id = %s""",
                (result, winner, float(net_result), wins, losses, draws, session_id),
            )
            conn.commit()
            return cur.rowcount > 0


def add_21_bot_round(
    session_id: int,
    round_number: int,
    player_cards: str,
    bot_cards: str,
    player_points: int,
    bot_points: int,
    result: str,
    winner: str,
    bet_amount: float,
    commission_percent: float,
    net_result: float,
) -> bool:
    """Записать раунд 21 против бота."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_bot_rounds
                   (session_id, round_number, player_cards, bot_cards, player_points, bot_points, result, winner, bet_amount, commission_percent, net_result)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    session_id,
                    int(round_number),
                    player_cards,
                    bot_cards,
                    int(player_points),
                    int(bot_points),
                    result,
                    winner,
                    float(bet_amount),
                    float(commission_percent),
                    float(net_result),
                ),
            )
            conn.commit()
            return True


def get_21_bot_stats():
    """Статистика режима 21 против бота по завершённым сессиям."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                       COUNT(*) AS total_games,
                       COALESCE(SUM(CASE WHEN result = 'lose' THEN 1 ELSE 0 END), 0) AS bot_wins_count,
                       COALESCE(SUM(CASE WHEN result = 'lose' THEN -net_result ELSE 0 END), 0) AS bot_wins_sum,
                       COALESCE(SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END), 0) AS bot_losses_count,
                       COALESCE(SUM(CASE WHEN result = 'win' THEN net_result ELSE 0 END), 0) AS bot_losses_sum,
                       COALESCE(SUM(CASE WHEN result = 'draw' THEN 1 ELSE 0 END), 0) AS draws_count,
                       COALESCE(SUM(-net_result), 0) AS bot_profit
                   FROM game21_bot_sessions
                   WHERE status = 'finish'"""
            )
            row = cur.fetchone() or {}
            return {
                "total_games": int(row.get("total_games") or 0),
                "bot_wins_count": int(row.get("bot_wins_count") or 0),
                "bot_wins_sum": float(row.get("bot_wins_sum") or 0.0),
                "bot_losses_count": int(row.get("bot_losses_count") or 0),
                "bot_losses_sum": float(row.get("bot_losses_sum") or 0.0),
                "draws_count": int(row.get("draws_count") or 0),
                "bot_profit": float(row.get("bot_profit") or 0.0),
            }


def add_21_users_game(
    player1_id: int,
    player2_id: int,
    bet_amount: float,
    commission_percent: float,
    result: str,
    winner_id: Optional[int],
    commission_amount: float,
) -> Optional[int]:
    """Записать игру 21 между пользователями (для админской статистики)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO game21_users_sessions
                   (player1_id, player2_id, bet_amount, commission_percent, commission_amount, result, winner_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    int(player1_id),
                    int(player2_id),
                    float(bet_amount),
                    float(commission_percent),
                    float(commission_amount),
                    str(result or "draw"),
                    int(winner_id) if winner_id is not None else None,
                ),
            )
            conn.commit()
            return cur.lastrowid


def add_21_users_round_events(session_id: int, events: list) -> bool:
    """Сохранить события бросков 21 между пользователями."""
    if not events:
        return True
    with get_connection() as conn:
        with conn.cursor() as cur:
            for e in events:
                cur.execute(
                    """INSERT INTO game21_users_rounds
                       (session_id, phase, user_id, throw_order, value, total_after)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (
                        int(session_id),
                        str(e.get("phase") or "turn"),
                        int(e.get("user_id")),
                        int(e.get("throw_order") or 0),
                        int(e.get("value") or 0),
                        int(e.get("total_after")) if e.get("total_after") is not None else None,
                    ),
                )
            conn.commit()
            return True


def get_21_users_stats():
    """Статистика режима 21 между пользователями."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT
                       COUNT(*) AS total_games,
                       COALESCE(SUM(commission_amount), 0) AS bot_commission_sum
                   FROM game21_users_sessions"""
            )
            row = cur.fetchone() or {}
            return {
                "total_games": int(row.get("total_games") or 0),
                "bot_commission_sum": float(row.get("bot_commission_sum") or 0.0),
            }
