"""Запросы к БД: пользователи, города, игры, призы."""
from typing import Optional
from datetime import datetime
from db.init_db import get_connection


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
