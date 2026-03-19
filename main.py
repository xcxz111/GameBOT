import asyncio
import html
import logging
import random
import time
from pathlib import Path
from datetime import datetime as dt
from typing import Any, Awaitable, Callable, Dict, List

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message, CallbackQuery, TelegramObject,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ChatMemberLeft, ChatMemberBanned,
)
from aiogram.enums import ParseMode, ContentType
from aiogram.client.default import DefaultBotProperties
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramRetryAfter

from config import BOT_TOKEN, CHAT_ID, ADMIN_ID
from translations import t, LANG_NAMES, DEFAULT_LANG

# Папка для сохранения фото призов (в корне проекта)
PRIZES_DIR = Path(__file__).resolve().parent / "призы"
PRIZES_DIR.mkdir(parents=True, exist_ok=True)

# Маркер «выбыл» в списке тоталов по раундам (отображается как « выбыл» в закрепе)
ELIMINATED_MARKER = "__eliminated__"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class ChatFilter:
    """Игнорировать апдейты не из нашего чата. Личные чаты (DM с ботом) всегда пропускаем — там /start и выбор города."""

    def __init__(self, chat_id: int):
        self.chat_id = chat_id

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        chat = getattr(event, "chat", None) or (getattr(event, "message", None) and getattr(event.message, "chat", None))
        if chat is None:
            return await handler(event, data)
        chat_id = getattr(chat, "id", None)
        chat_type = str(getattr(chat, "type", "")).lower()
        # личка с ботом — пропускаем (там /start и выбор города)
        if chat_type == "private":
            return await handler(event, data)
        # в остальных чатах — только наш CHAT_ID
        if chat_id != self.chat_id:
            logger.debug("ChatFilter: пропуск чата id=%s (ожидается %s)", chat_id, self.chat_id)
            return
        return await handler(event, data)


async def log_game_chat_message(handler: Callable, event: TelegramObject, data: Dict[str, Any]) -> Any:
    """Лог каждого сообщения в игровом чате (чтобы видеть, доходит ли бросок и какой тип)."""
    msg = getattr(event, "message", event)
    chat = getattr(msg, "chat", None)
    chat_id = getattr(chat, "id", None) if chat else None
    if chat_id is not None and chat_id in _chat_to_game:
        content_type = getattr(msg, "content_type", None)
        dice = getattr(msg, "dice", None)
        has_dice = dice is not None
        text_len = len(getattr(msg, "text", None) or "")
        logger.info("game_chat message: chat_id=%s content_type=%s has_dice=%s dice_emoji=%s dice_value=%s text_len=%s", chat_id, content_type, has_dice, getattr(dice, "emoji", None), getattr(dice, "value", None), text_len)
    return await handler(event, data)


_pending_lang = {}  # user_id -> "en"|"ru"|"pl"|"uk" до сохранения в БД


def lang_keyboard(
    with_nav: bool = False,
    nav_lang: str = DEFAULT_LANG,
    back_callback: str = "menu:main",
    main_callback: str = "menu:main",
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    lang_count = 0
    for code, name in LANG_NAMES.items():
        builder.add(InlineKeyboardButton(text=name, callback_data=f"lang:{code}"))
        lang_count += 1
    if with_nav:
        builder.add(
            InlineKeyboardButton(text=t("admin_btn_back", nav_lang), callback_data=back_callback),
            InlineKeyboardButton(text=t("admin_btn_main", nav_lang), callback_data=main_callback),
        )
        builder.adjust(*(1 for _ in range(lang_count)), 2)
        return builder.as_markup()
    builder.adjust(*(1 for _ in range(lang_count)))
    return builder.as_markup()


def city_keyboard(language_code) -> InlineKeyboardMarkup:
    """Инлайн-кнопки: города из БД (как есть) + кнопка «Не указывать город» на языке пользователя."""
    try:
        from db.queries import get_cities
        cities = get_cities()
    except Exception:
        cities = []
    builder = InlineKeyboardBuilder()
    for row in cities:
        builder.add(InlineKeyboardButton(text=row["city"], callback_data=f"city:{row['id']}"))
    builder.add(InlineKeyboardButton(text=t("btn_no_city", language_code), callback_data="city:0"))
    builder.adjust(1)  # по одной кнопке в ряд
    return builder.as_markup()


def main_menu_keyboard(language_code, user_id: int = None) -> InlineKeyboardMarkup:
    """Кнопки главного меню: личный кабинет, записаться на игру, [админка только для админа], язык."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("btn_cabinet", language_code), callback_data="menu:cabinet"))
    builder.add(InlineKeyboardButton(text=t("btn_signup", language_code), callback_data="menu:signup"))
    if user_id and user_id == ADMIN_ID:
        builder.add(InlineKeyboardButton(text=t("btn_admin", language_code), callback_data="menu:admin"))
    builder.add(InlineKeyboardButton(text=t("btn_lang", language_code), callback_data="menu:lang"))
    builder.adjust(1)  # по одной в ряд
    return builder.as_markup()


def signup_games_keyboard(lang) -> InlineKeyboardMarkup:
    """Кнопки предстоящих игр для записи: тип + город (или «для всех») + дата; Назад и Главная."""
    from db.queries import get_games_current, get_city
    try:
        games = get_games_current()
    except Exception:
        games = []
    builder = InlineKeyboardBuilder()
    for g in games:
        city_label = (
            t("admin_game_city_all", lang)
            if not g.get("city_id")
            else (get_city(g["city_id"]) or {}).get("city") or str(g["city_id"])
        )
        label = f"{_game_type_label(lang, g['game_type'])} {city_label} {g['start_time'].strftime('%d.%m.%Y %H:%M')}"
        builder.add(InlineKeyboardButton(text=label, callback_data=f"menu:signup:game:{g['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="menu:signup:back"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="menu:main"),
    )
    builder.adjust(*(1 for _ in games), 2)
    return builder.as_markup()


def cabinet_main_keyboard(lang) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("cabinet_btn_my_prizes", lang), callback_data="menu:cabinet:prizes"))
    builder.add(InlineKeyboardButton(text=t("cabinet_btn_change_city", lang), callback_data="menu:cabinet:city"))
    builder.add(InlineKeyboardButton(text=t("cabinet_btn_change_lang", lang), callback_data="menu:cabinet:lang"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="menu:main"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="menu:main"),
    )
    builder.adjust(1, 1, 1, 2)
    return builder.as_markup()


def cabinet_city_keyboard(lang) -> InlineKeyboardMarkup:
    from db.queries import get_cities
    try:
        cities = get_cities()
    except Exception:
        cities = []
    builder = InlineKeyboardBuilder()
    for row in cities:
        builder.add(InlineKeyboardButton(text=row["city"], callback_data=f"menu:cabinet:setcity:{row['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="menu:cabinet"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="menu:main"),
    )
    builder.adjust(*(1 for _ in cities), 2)
    return builder.as_markup()


def cabinet_prizes_keyboard(lang, prizes: list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for p in prizes:
        game_type = _game_type_label(lang, p.get("game_type") or "dice")
        dt_val = p.get("start_time") or p.get("created_at")
        dt_label = dt_val.strftime("%d.%m.%Y") if dt_val else "?"
        label = f"{dt_label} {game_type}"
        builder.add(InlineKeyboardButton(text=label, callback_data=f"menu:cabinet:prize:{p['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="menu:cabinet"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="menu:main"),
    )
    builder.adjust(*(1 for _ in prizes), 2)
    return builder.as_markup()


# Состояние админа: user_id -> {"state": str, "city_id": int | None}
_admin_state = {}


def _admin_clear(user_id: int):
    _admin_state.pop(user_id, None)


def admin_main_keyboard(lang) -> InlineKeyboardMarkup:
    """Админка: настройка городов, создать игру, игры, назад (в главное меню пользователя)."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_cities", lang), callback_data="admin:cities"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_create_game", lang), callback_data="admin:create_game"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_games", lang), callback_data="admin:games"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_settings_21", lang), callback_data="admin:settings21"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:main"))
    builder.adjust(1)
    return builder.as_markup()


def admin_games_keyboard(lang) -> InlineKeyboardMarkup:
    """Игры: прошедшие, текущие; назад и главная в один ряд."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_games_past", lang), callback_data="admin:games:past"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_games_current", lang), callback_data="admin:games:current"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:back"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 2)
    return builder.as_markup()


GAME_TYPE_EMOJI = {"dice": "🎲", "bowling": "🎳", "darts": "🎯"}


# Состояние раунда: game_id -> {participant_ids, current_index, throw_count, chat_id, game_type, round_number}
_round_state: Dict[int, dict] = {}
_chat_to_game: Dict[int, int] = {}  # chat_id -> game_id для быстрого поиска по апдейту в чате


def _game_type_label(lang, game_type: str) -> str:
    """Название типа игры для отображения."""
    key = f"admin_btn_type_{game_type}"
    return t(key, lang) if game_type in ("dice", "bowling", "darts") else game_type


def admin_current_games_keyboard(lang) -> InlineKeyboardMarkup:
    """Кнопки текущих игр (тип + дата); назад и главная."""
    from db.queries import get_games_current
    try:
        games = get_games_current()
    except Exception:
        games = []
    builder = InlineKeyboardBuilder()
    for g in games:
        label = f"{_game_type_label(lang, g['game_type'])} {g['start_time'].strftime('%d.%m.%Y %H:%M')}"
        builder.add(InlineKeyboardButton(text=label, callback_data=f"admin:game:{g['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:games"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(*(1 for _ in games), 2)
    return builder.as_markup()


def admin_past_games_keyboard(lang) -> InlineKeyboardMarkup:
    """Кнопки прошедших игр (тип + дата); назад и главная."""
    from db.queries import get_games_finished
    try:
        games = get_games_finished()
    except Exception:
        games = []
    builder = InlineKeyboardBuilder()
    for g in games:
        label = f"{_game_type_label(lang, g['game_type'])} {g['start_time'].strftime('%d.%m.%Y %H:%M')}"
        builder.add(InlineKeyboardButton(text=label, callback_data=f"admin:game:{g['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:games"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(*(1 for _ in games), 2)
    return builder.as_markup()


def _format_game_detail(game, prizes, city_label: str, lang) -> str:
    """Текст с параметрами игры."""
    from db.queries import count_participants
    try:
        registered = count_participants(game["id"])
    except Exception:
        registered = 0
    lines = [
        f"{t('admin_game_label_type', lang)}: {_game_type_label(lang, game['game_type'])}",
        f"{t('admin_game_label_participants', lang)}: {game['min_participants']}-{game['max_participants']}",
        f"{t('admin_game_label_prize_places', lang)}: {game['prize_places']}",
        f"{t('admin_game_label_registered', lang)}: {registered}",
        f"{t('admin_game_label_date', lang)}: {game['start_time'].strftime('%d.%m.%Y %H:%M')}",
        f"{t('admin_game_label_city', lang)}: {city_label}",
        f"{t('admin_game_label_prizes', lang)}:",
    ]
    for p in prizes:
        lines.append(f"  {p['place_number']}. { (p['coupon_text'] or '')[:80] }{'…' if len(p.get('coupon_text') or '') > 80 else ''}")
    return "\n".join(lines)


def admin_game_detail_keyboard(game_id: int, lang) -> InlineKeyboardMarkup:
    """Клавиатура редактирования игры: все пункты + назад и главная."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_players", lang), callback_data=f"admin:game:{game_id}:players"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_type", lang), callback_data=f"admin:game:{game_id}:edit_type"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_participants", lang), callback_data=f"admin:game:{game_id}:edit_participants"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_prize_places", lang), callback_data=f"admin:game:{game_id}:edit_prize_places"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_prizes", lang), callback_data=f"admin:game:{game_id}:edit_prizes"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_datetime", lang), callback_data=f"admin:game:{game_id}:edit_datetime"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_cancel_game", lang), callback_data=f"admin:game:{game_id}:cancel"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:games:current"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 1, 1, 1, 1, 1, 2)
    return builder.as_markup()


def admin_finished_game_detail_keyboard(game_id: int, lang) -> InlineKeyboardMarkup:
    """Клавиатура прошедшей игры: список игроков, призов, победителей + назад/главная."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_players", lang), callback_data=f"admin:game:{game_id}:players"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_prizes_list", lang), callback_data=f"admin:game:{game_id}:prizes"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_winners_list", lang), callback_data=f"admin:game:{game_id}:winners"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:games:past"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 1, 2)
    return builder.as_markup()


def admin_game_confirm_cancel_keyboard(game_id: int, lang) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_yes", lang), callback_data=f"admin:game:{game_id}:confirm_cancel"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_no", lang), callback_data=f"admin:game:{game_id}"))
    builder.adjust(1)
    return builder.as_markup()


def _admin_game_edit_type_keyboard(game_id: int, lang) -> InlineKeyboardMarkup:
    """Выбор типа игры при редактировании; назад к карточке игры."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_dice", lang), callback_data=f"admin:game:{game_id}:settype:dice"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_bowling", lang), callback_data=f"admin:game:{game_id}:settype:bowling"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_darts", lang), callback_data=f"admin:game:{game_id}:settype:darts"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data=f"admin:game:{game_id}"))
    builder.adjust(1)
    return builder.as_markup()


def admin_create_game_keyboard(lang) -> InlineKeyboardMarkup:
    """Создать игру: для всех, для города; назад и главная в один ряд."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_game_for_all", lang), callback_data="admin:create_game:all"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_game_for_city", lang), callback_data="admin:create_game:city"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:back"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 2)  # две кнопки по одной, последняя строка — Назад и Главная
    return builder.as_markup()


def admin_create_game_city_keyboard(lang) -> InlineKeyboardMarkup:
    """Выбор города для игры «для города»; назад в меню «Создать игру»."""
    from db.queries import get_cities
    try:
        cities = get_cities()
    except Exception:
        cities = []
    builder = InlineKeyboardBuilder()
    for row in cities:
        builder.add(InlineKeyboardButton(text=row["city"], callback_data=f"admin:create_game:city:{row['id']}"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:create_game"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 2)
    return builder.as_markup()


def admin_create_game_type_keyboard(lang) -> InlineKeyboardMarkup:
    """Тип игры: кубики, боулинг, дротики; назад и главная в один ряд."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_dice", lang), callback_data="admin:create_game:type:dice"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_bowling", lang), callback_data="admin:create_game:type:bowling"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_type_darts", lang), callback_data="admin:create_game:type:darts"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:create_game"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 1, 2)  # три типа по одной, последняя строка — Назад и Главная
    return builder.as_markup()


def admin_cities_keyboard(lang) -> InlineKeyboardMarkup:
    """Настройка городов: создать, редактировать, удалить, назад, главная."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_create_city", lang), callback_data="admin:cities:create"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_edit_city", lang), callback_data="admin:cities:edit"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_delete_city", lang), callback_data="admin:cities:delete"))
    builder.add(
        InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:back"),
        InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
    )
    builder.adjust(1, 1, 1, 2)  # три кнопки по одной, последняя строка — Назад и Главная
    return builder.as_markup()


def admin_cities_list_keyboard(prefix: str, lang) -> InlineKeyboardMarkup:
    """Список городов как инлайн-кнопки (callback_data prefix:id, например admin:city:edit:5)."""
    from db.queries import get_cities
    try:
        cities = get_cities()
    except Exception:
        cities = []
    builder = InlineKeyboardBuilder()
    for row in cities:
        builder.add(InlineKeyboardButton(text=row["city"], callback_data=f"{prefix}:{row['id']}"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data="admin:cities"))
    builder.adjust(1)
    return builder.as_markup()


def admin_confirm_keyboard(city_id: int, lang) -> InlineKeyboardMarkup:
    """Да / Нет для подтверждения удаления."""
    builder = InlineKeyboardBuilder()
    builder.add(InlineKeyboardButton(text=t("admin_btn_yes", lang), callback_data=f"admin:confirm:yes:{city_id}"))
    builder.add(InlineKeyboardButton(text=t("admin_btn_no", lang), callback_data="admin:confirm:no"))
    builder.adjust(1)
    return builder.as_markup()


def _admin_input_prompt(main_key: str, lang) -> str:
    """Текст запроса ввода + подсказка «для отмены нажмите /back»."""
    return f"{t(main_key, lang)}\n{t('admin_cancel_hint', lang)}"


def _admin_prizes_initial_prompt(lang, prize_places: int, place: int = 1, new: bool = False) -> str:
    """Запрос призов: сколько нужно, для какого места, описание и /back."""
    count_hint = t("admin_prizes_count_hint", lang).format(count=prize_places)
    send_for_place = t("admin_send_prize_for_place", lang).format(place=place)
    desc = t("admin_enter_prizes_new", lang) if new else t("admin_enter_prizes", lang)
    # Фразу «Отправьте приз для N-го места» показываем внизу перед подсказкой /back
    return f"{count_hint}\n\n{desc}\n\n{send_for_place}\n{t('admin_cancel_hint', lang)}"


def _parse_prize_pair(raw: str):
    """Разбить строку «Название приза: сам приз» на (name, value). Если формат неверный — вернуть (None, None)."""
    raw = (raw or "").strip()
    if ":" not in raw:
        return None, None
    name, value = raw.split(":", 1)
    name, value = name.strip(), value.strip()
    if not name or not value:
        return None, None
    return name, value


def _user_lang(msg_or_cb):
    """Язык: pending (выбран, но ещё не сохранён), из БД, из Telegram, иначе None."""
    from_user = getattr(msg_or_cb, "from_user", None)
    if from_user is None:
        return None
    user_id = getattr(from_user, "id", None)
    if user_id and user_id in _pending_lang:
        return _pending_lang[user_id]
    if user_id:
        try:
            from db.queries import get_user
            user = get_user(user_id)
            if user and user.get("language_code"):
                return user["language_code"]
        except Exception:
            pass
    lang = getattr(from_user, "language_code", None)
    if lang and str(lang).strip():
        return str(lang).strip()
    return None


async def cmd_start(msg: Message):
    from db.queries import get_user, save_user
    user_id = msg.from_user.id
    lang = _user_lang(msg)
    try:
        user = get_user(user_id)
    except Exception as e:
        logger.exception("get_user failed: %s", e)
        user = None
    if user is None:
        logger.info("cmd_start: new user %s, sending language choice", user_id)
        await msg.answer(
            t("choose_language", lang),
            reply_markup=lang_keyboard(),
        )
        return
    logger.info("cmd_start: existing user %s, sending welcome", user_id)
    _admin_clear(user_id)
    await msg.answer(
        t("welcome_menu", lang),
        reply_markup=main_menu_keyboard(lang, user_id),
    )


async def on_lang_chosen(cb: CallbackQuery):
    from db.queries import get_user, save_user
    user_id = cb.from_user.id
    value = cb.data.split(":", 1)[1]
    if value not in LANG_NAMES:
        await cb.answer()
        return
    user = get_user(user_id)
    if user is not None:
        save_user(
            user_id=user_id,
            user_name=user.get("user_name"),
            name=user.get("name"),
            city_id=user.get("city_id"),
            language_code=value,
        )
        await cb.message.edit_text(
            t("welcome_menu", value),
            reply_markup=main_menu_keyboard(value, user_id),
        )
        await cb.answer()
        return
    _pending_lang[user_id] = value
    await cb.message.edit_text(
        t("choose_city", value),
        reply_markup=city_keyboard(value),
    )
    await cb.answer()


async def on_menu(cb: CallbackQuery):
    """Обработка кнопок главного меню: cabinet, signup, lang, signup:back, signup:game:id, main."""
    lang = _user_lang(cb)
    data = cb.data
    action = data.split(":", 1)[1] if ":" in data else ""
    if action == "cabinet":
        await cb.message.edit_text(
            t("cabinet_title", lang),
            reply_markup=cabinet_main_keyboard(lang),
        )
        await cb.answer()
    elif action == "cabinet:prizes":
        from db.queries import get_user_prizes
        prizes = get_user_prizes(cb.from_user.id)
        if not prizes:
            await cb.answer(t("cabinet_no_prizes", lang), show_alert=True)
            return
        await cb.message.edit_text(
            t("cabinet_prizes_title", lang),
            reply_markup=cabinet_prizes_keyboard(lang, prizes),
        )
        await cb.answer()
    elif action.startswith("cabinet:prize:"):
        from db.queries import get_user_prize_by_id, get_user_prizes
        parts = action.split(":")
        if len(parts) < 3 or not parts[2].isdigit():
            await cb.answer()
            return
        prize_id = int(parts[2])
        prize = get_user_prize_by_id(cb.from_user.id, prize_id)
        if not prize:
            await cb.answer()
            return
        text = f"{(prize.get('prize_name') or '').strip()}: {(prize.get('coupon_text') or '').strip()}".strip(": ").strip()
        if not text:
            text = t("cabinet_no_prizes", lang)
        prizes = get_user_prizes(cb.from_user.id)
        await cb.message.edit_text(
            text,
            reply_markup=cabinet_prizes_keyboard(lang, prizes),
        )
        await cb.answer()
    elif action == "cabinet:city":
        await cb.message.edit_text(
            t("cabinet_city_change_info", lang),
            reply_markup=cabinet_city_keyboard(lang),
        )
        await cb.answer()
    elif action.startswith("cabinet:setcity:"):
        from db.queries import get_user, update_user_city
        parts = action.split(":")
        if len(parts) < 3 or not parts[2].isdigit():
            await cb.answer()
            return
        new_city_id = int(parts[2])
        user = get_user(cb.from_user.id) or {}
        current_city_id = user.get("city_id")
        changed_at = user.get("city_changed_at")
        now = dt.now()
        if current_city_id == new_city_id:
            await cb.answer(t("cabinet_city_changed", lang), show_alert=True)
            return
        if changed_at:
            elapsed = now - changed_at
            total_days = elapsed.total_seconds() / 86400
            if total_days < 14:
                remaining = int(14 - total_days)
                if (14 - total_days) - remaining > 0:
                    remaining += 1
                remaining = max(1, remaining)
                await cb.answer(t("cabinet_city_change_wait", lang).format(days=remaining), show_alert=True)
                return
        update_user_city(cb.from_user.id, new_city_id)
        await cb.answer(t("cabinet_city_changed", lang), show_alert=True)
        await cb.message.edit_text(
            t("cabinet_title", lang),
            reply_markup=cabinet_main_keyboard(lang),
        )
    elif action == "cabinet:lang":
        await cb.message.edit_text(
            t("choose_language", lang),
            reply_markup=lang_keyboard(
                with_nav=True,
                nav_lang=lang,
                back_callback="menu:cabinet",
                main_callback="menu:main",
            ),
        )
        await cb.answer()
    elif action == "signup":
        from db.queries import get_games_current
        games = []
        try:
            games = get_games_current()
        except Exception:
            pass
        title = t("signup_upcoming_title", lang) if games else t("signup_no_games", lang)
        await cb.message.edit_text(title, reply_markup=signup_games_keyboard(lang))
        await cb.answer()
    elif action == "signup:back" or action == "main":
        await cb.message.edit_text(
            t("welcome_menu", lang),
            reply_markup=main_menu_keyboard(lang, cb.from_user.id),
        )
        await cb.answer()
    elif action.startswith("signup:game:"):
        from db.queries import get_game, get_user, add_participant, save_user, get_participants_user_ids
        parts = action.split(":")
        if len(parts) < 3:
            await cb.answer()
            return
        try:
            game_id = int(parts[2])
        except ValueError:
            await cb.answer()
            return
        game = get_game(game_id) if game_id else None
        if not game:
            await cb.answer()
            return
        max_players = game.get("max_participants")
        if max_players is not None:
            try:
                current_count = len(get_participants_user_ids(game_id))
                if current_count >= max_players:
                    await cb.answer(t("signup_game_full", lang), show_alert=True)
                    return
            except Exception:
                pass
        user_id = cb.from_user.id
        chat_id = game.get("chat_id")
        if chat_id is not None:
            try:
                member = await cb.bot.get_chat_member(chat_id, user_id)
                if isinstance(member, (ChatMemberLeft, ChatMemberBanned)):
                    try:
                        chat = await cb.bot.get_chat(chat_id)
                        chat_title = chat.title or str(chat_id)
                    except Exception:
                        chat_title = str(chat_id)
                    await cb.answer(
                        t("signup_must_join_chat", lang).format(chat_title=chat_title),
                        show_alert=True,
                    )
                    return
            except Exception:
                pass
        user = get_user(user_id)
        if user is None:
            save_user(user_id, cb.from_user.username, cb.from_user.first_name or "")
            user = get_user(user_id)
        game_city_id = game.get("city_id")
        user_city_id = (user or {}).get("city_id") if user else None
        if game_city_id is not None:
            if user_city_id is None:
                await cb.answer(t("signup_set_city_first", lang), show_alert=True)
                return
            if user_city_id != game_city_id:
                await cb.answer(t("signup_wrong_city", lang), show_alert=True)
                return
        added = add_participant(game_id, user_id)
        if added:
            await cb.answer(t("signup_success", lang), show_alert=True)
        else:
            await cb.answer(t("signup_already", lang), show_alert=True)
    elif action == "admin":
        if cb.from_user.id != ADMIN_ID:
            await cb.answer()
            return
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(t("admin_title", lang), reply_markup=admin_main_keyboard(lang))
        await cb.answer()
    elif action == "lang":
        await cb.message.edit_text(
            t("choose_language", lang),
            reply_markup=lang_keyboard(
                with_nav=True,
                nav_lang=lang,
                back_callback="menu:main",
                main_callback="menu:main",
            ),
        )
        await cb.answer()
    else:
        await cb.answer()


async def on_admin(cb: CallbackQuery):
    """Обработка админ-меню: только для ADMIN_ID."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer()
        return
    lang = _user_lang(cb)
    data = cb.data
    if data == "admin:cities":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))
        await cb.answer()
        return
    if data == "admin:games":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_games_title", lang),
            reply_markup=admin_games_keyboard(lang),
        )
        await cb.answer()
        return
    if data == "admin:settings21":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("coming_soon_settings_21", lang),
            reply_markup=admin_main_keyboard(lang),
        )
        await cb.answer()
        return
    if data == "admin:games:past":
        _admin_clear(cb.from_user.id)
        from db.queries import get_games_finished
        try:
            games = get_games_finished()
        except Exception:
            games = []
        if not games:
            await cb.message.edit_text(
                t("admin_games_current_empty", lang),
                reply_markup=admin_games_keyboard(lang),
            )
        else:
            await cb.message.edit_text(
                t("admin_btn_games_past", lang),
                reply_markup=admin_past_games_keyboard(lang),
            )
        await cb.answer()
        return
    if data == "admin:games:current":
        _admin_clear(cb.from_user.id)
        from db.queries import get_games_current
        try:
            games = get_games_current()
        except Exception:
            games = []
        if not games:
            await cb.message.edit_text(
                t("admin_games_current_empty", lang),
                reply_markup=admin_games_keyboard(lang),
            )
        else:
            await cb.message.edit_text(
                t("admin_btn_games_current", lang),
                reply_markup=admin_current_games_keyboard(lang),
            )
        await cb.answer()
        return
    if data.startswith("admin:game:"):
        parts = data.split(":")
        if len(parts) >= 3 and parts[1] == "game" and parts[2].isdigit():
            game_id = int(parts[2])
            if len(parts) == 3:
                from db.queries import get_game, get_prizes, get_city
                game = get_game(game_id)
                if not game:
                    await cb.answer()
                    return
                prizes = get_prizes(game_id)
                city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
                text = _format_game_detail(game, prizes, city_label, lang)
                kb = admin_finished_game_detail_keyboard(game_id, lang) if game.get("status") == "finish" else admin_game_detail_keyboard(game_id, lang)
                await cb.message.edit_text(text, reply_markup=kb)
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "players":
                from db.queries import get_participants_for_display
                try:
                    participants = get_participants_for_display(game_id)
                except Exception:
                    participants = []
                lines = [t("admin_players_title", lang), ""]
                for i, (uid, name) in enumerate(participants, 1):
                    safe = html.escape(name)
                    lines.append(f'{i}. <a href="tg://user?id={uid}">{safe}</a>')
                from db.queries import get_game
                game = get_game(game_id)
                back_target = f"admin:game:{game_id}"
                kb = InlineKeyboardBuilder()
                kb.add(
                    InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data=back_target),
                    InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
                )
                kb.adjust(2)
                await cb.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "prizes":
                from db.queries import get_prizes
                prizes = get_prizes(game_id)
                lines = [t("admin_prizes_title", lang), ""]
                for p in prizes:
                    name = (p.get("prize_name") or "").strip()
                    val = (p.get("coupon_text") or "").strip()
                    if name and val:
                        lines.append(f"{p['place_number']}. {html.escape(name)}: {html.escape(val)}")
                    elif name:
                        lines.append(f"{p['place_number']}. {html.escape(name)}")
                    else:
                        lines.append(f"{p['place_number']}. {html.escape(val)}")
                kb = InlineKeyboardBuilder()
                kb.add(
                    InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data=f"admin:game:{game_id}"),
                    InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
                )
                kb.adjust(2)
                await cb.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "winners":
                from db.queries import get_game_winners
                winners = get_game_winners(game_id)
                lines = [t("admin_winners_title", lang), ""]
                for place, uid, name, prize_name, _coupon_text in winners:
                    safe_name = html.escape(name)
                    if prize_name:
                        lines.append(f"{place}. <a href=\"tg://user?id={uid}\">{safe_name}</a> — {html.escape(prize_name)}")
                    else:
                        lines.append(f"{place}. <a href=\"tg://user?id={uid}\">{safe_name}</a>")
                kb = InlineKeyboardBuilder()
                kb.add(
                    InlineKeyboardButton(text=t("admin_btn_back", lang), callback_data=f"admin:game:{game_id}"),
                    InlineKeyboardButton(text=t("admin_btn_main", lang), callback_data="admin:main"),
                )
                kb.adjust(2)
                await cb.message.edit_text("\n".join(lines), reply_markup=kb.as_markup())
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "cancel":
                await cb.message.edit_text(
                    t("admin_confirm_cancel_game", lang),
                    reply_markup=admin_game_confirm_cancel_keyboard(game_id, lang),
                )
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "confirm_cancel":
                from db.queries import get_participants_user_ids, delete_game, get_game, update_game
                game = get_game(game_id)
                user_ids = get_participants_user_ids(game_id)
                status = (game or {}).get("status") or "draft"
                chat_id = (game or {}).get("chat_id")
                # Если игра уже идёт — отменяем без удаления (чтобы не ломать историю) и пишем в чат
                if status == "active":
                    try:
                        update_game(game_id, status="cancelled")
                    except Exception:
                        pass
                    # остановить текущую игру в рантайме
                    _round_state.pop(game_id, None)
                    if chat_id is not None:
                        _chat_to_game.pop(chat_id, None)
                        try:
                            await cb.bot.send_message(chat_id=chat_id, text=t("game_cancelled_in_chat", lang))
                        except Exception:
                            pass
                else:
                    # До начала — удаляем и уведомляем участников в личку
                    try:
                        delete_game(game_id)
                    except Exception:
                        pass
                    notify_text = t("game_cancelled_dm", lang)
                    for uid in user_ids:
                        try:
                            await cb.bot.send_message(chat_id=uid, text=notify_text)
                        except Exception:
                            pass
                await cb.message.edit_text(
                    t("admin_game_cancelled", lang),
                    reply_markup=admin_current_games_keyboard(lang),
                )
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "edit_type":
                await cb.message.edit_text(
                    t("admin_select_game_type", lang),
                    reply_markup=_admin_game_edit_type_keyboard(game_id, lang),
                )
                await cb.answer()
                return
            if len(parts) == 5 and parts[3] == "settype":
                new_type = parts[4]
                if new_type in ("dice", "bowling", "darts"):
                    from db.queries import get_game, update_game
                    game = get_game(game_id)
                    if game:
                        new_name = f"{new_type} {game['start_time'].strftime('%d.%m.%Y %H:%M')}"
                        update_game(game_id, game_type=new_type, name=new_name)
                    from db.queries import get_game, get_prizes, get_city
                    game = get_game(game_id)
                    if game:
                        prizes = get_prizes(game_id)
                        city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
                        await cb.message.edit_text(
                            _format_game_detail(game, prizes, city_label, lang),
                            reply_markup=admin_game_detail_keyboard(game_id, lang),
                        )
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "edit_participants":
                _admin_state[cb.from_user.id] = {"state": "admin_edit_game_participants", "game_id": game_id}
                await cb.message.edit_text(_admin_input_prompt("admin_enter_participants_new", lang))
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "edit_prize_places":
                _admin_state[cb.from_user.id] = {"state": "admin_edit_game_prize_places", "game_id": game_id}
                await cb.message.edit_text(_admin_input_prompt("admin_enter_prize_places_new", lang))
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "edit_prizes":
                from db.queries import get_game
                game = get_game(game_id)
                if game:
                    _admin_state[cb.from_user.id] = {
                        "state": "admin_edit_game_prizes",
                        "game_id": game_id,
                        "prize_places": game["prize_places"],
                        "prizes": [],
                        "mode": None,
                    }
                    await cb.message.edit_text(_admin_prizes_initial_prompt(lang, game["prize_places"], place=1, new=True))
                await cb.answer()
                return
            if len(parts) == 4 and parts[3] == "edit_datetime":
                _admin_state[cb.from_user.id] = {"state": "admin_edit_game_datetime", "game_id": game_id}
                await cb.message.edit_text(_admin_input_prompt("admin_enter_datetime_new", lang))
                await cb.answer()
                return
    if data == "admin:create_game":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_create_game_title", lang),
            reply_markup=admin_create_game_keyboard(lang),
        )
        await cb.answer()
        return
    if data == "admin:create_game:all":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_select_game_type", lang),
            reply_markup=admin_create_game_type_keyboard(lang),
        )
        await cb.answer()
        return
    if data.startswith("admin:create_game:type:"):
        game_type = data.split(":")[-1]  # dice, bowling, darts
        if game_type not in ("dice", "bowling", "darts"):
            await cb.answer()
            return
        prev = _admin_state.get(cb.from_user.id) or {}
        _admin_state[cb.from_user.id] = {
            "state": "admin_game_min_max",
            "game_type": game_type,
            "scope": prev.get("scope", "all"),
            "city_id": prev.get("city_id"),
        }
        await cb.message.edit_text(_admin_input_prompt("admin_enter_min_max", lang))
        await cb.answer()
        return
    if data == "admin:create_game:city":
        _admin_clear(cb.from_user.id)
        from db.queries import get_cities
        cities = get_cities()
        if not cities:
            await cb.message.edit_text(
                t("admin_no_cities", lang),
                reply_markup=admin_create_game_keyboard(lang),
            )
        else:
            await cb.message.edit_text(
                t("admin_select_city_game", lang),
                reply_markup=admin_create_game_city_keyboard(lang),
            )
        await cb.answer()
        return
    if data.startswith("admin:create_game:city:") and data != "admin:create_game:city":
        parts = data.split(":")
        if len(parts) >= 4 and parts[-1].isdigit():
            city_id = int(parts[-1])
            _admin_state[cb.from_user.id] = {
                "state": "admin_game_select_type",
                "scope": "city",
                "city_id": city_id,
            }
            await cb.message.edit_text(
                t("admin_select_game_type", lang),
                reply_markup=admin_create_game_type_keyboard(lang),
            )
        await cb.answer()
        return
    if data == "admin:create_game:confirm":
        state = _admin_state.get(cb.from_user.id) or {}
        if state.get("state") != "admin_game_confirm_create":
            await cb.answer()
            return
        from db.queries import create_game as db_create_game, add_prize, get_city
        game_type = state.get("game_type") or "dice"
        chat_id = CHAT_ID
        min_p = state.get("min_participants") or 2
        max_p = state.get("max_participants") or 50
        prize_places = state.get("prize_places") or 1
        prizes_coupons = state.get("prizes_coupons") or []
        start_time = state.get("start_time") or dt.now()
        city_id = state.get("city_id") if state.get("scope") == "city" else None
        name = f"{game_type} {start_time.strftime('%d.%m.%Y %H:%M')}"
        game_id = db_create_game(
            name=name,
            game_type=game_type,
            chat_id=chat_id,
            start_time=start_time,
            min_participants=min_p,
            max_participants=max_p,
            prize_places=prize_places,
            city_id=city_id,
        )
        if game_id:
            for place, raw_text in enumerate(prizes_coupons, 1):
                add_prize(game_id, place, raw_text)
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(t("admin_game_created", lang), reply_markup=admin_create_game_type_keyboard(lang))
        await cb.answer()
        return
    if data == "admin:create_game:cancel":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(t("admin_cancelled", lang), reply_markup=admin_create_game_keyboard(lang))
        await cb.answer()
        return
    if data == "admin:back":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(t("admin_title", lang), reply_markup=admin_main_keyboard(lang))
        await cb.answer()
        return
    if data == "admin:main":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("welcome_menu", lang),
            reply_markup=main_menu_keyboard(lang, cb.from_user.id),
        )
        await cb.answer()
        return
    if data == "admin:cities:create":
        _admin_state[cb.from_user.id] = {"state": "admin_create_city"}
        await cb.message.edit_text(_admin_input_prompt("admin_enter_city_name", lang))
        await cb.answer()
        return
    if data == "admin:cities:edit":
        _admin_state[cb.from_user.id] = {"state": "admin_edit_select"}
        from db.queries import get_cities
        cities = get_cities()
        if not cities:
            await cb.message.edit_text(t("admin_no_cities", lang), reply_markup=admin_cities_keyboard(lang))
            _admin_clear(cb.from_user.id)
        else:
            await cb.message.edit_text(
                t("admin_select_city_edit", lang),
                reply_markup=admin_cities_list_keyboard("admin:city:edit", lang),
            )
        await cb.answer()
        return
    if data == "admin:cities:delete":
        _admin_state[cb.from_user.id] = {"state": "admin_delete_select"}
        from db.queries import get_cities
        cities = get_cities()
        if not cities:
            await cb.message.edit_text(t("admin_no_cities", lang), reply_markup=admin_cities_keyboard(lang))
            _admin_clear(cb.from_user.id)
        else:
            await cb.message.edit_text(
                t("admin_select_city_delete", lang),
                reply_markup=admin_cities_list_keyboard("admin:city:delete", lang),
            )
        await cb.answer()
        return
    if data.startswith("admin:city:edit:"):
        try:
            city_id = int(data.split(":")[-1])
        except ValueError:
            await cb.answer()
            return
        _admin_state[cb.from_user.id] = {"state": "admin_edit_name", "city_id": city_id}
        await cb.message.edit_text(_admin_input_prompt("admin_enter_new_name", lang))
        await cb.answer()
        return
    if data.startswith("admin:city:delete:"):
        try:
            city_id = int(data.split(":")[-1])
        except ValueError:
            await cb.answer()
            return
        from db.queries import get_city
        city = get_city(city_id)
        if not city:
            await cb.answer()
            return
        _admin_state[cb.from_user.id] = {"state": "admin_delete_confirm", "city_id": city_id}
        await cb.message.edit_text(
            t("admin_confirm_delete", lang).format(name=city["city"]),
            reply_markup=admin_confirm_keyboard(city_id, lang),
        )
        await cb.answer()
        return
    if data.startswith("admin:confirm:yes:"):
        try:
            city_id = int(data.split(":")[-1])
        except ValueError:
            await cb.answer()
            return
        from db.queries import delete_city
        delete_city(city_id)
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_city_deleted", lang),
            reply_markup=admin_cities_keyboard(lang),
        )
        await cb.answer()
        return
    if data == "admin:confirm:no":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_cancelled", lang),
            reply_markup=admin_cities_keyboard(lang),
        )
        await cb.answer()
        return
    if data == "admin:cities:cancel":
        _admin_clear(cb.from_user.id)
        await cb.message.edit_text(
            t("admin_cities_title", lang),
            reply_markup=admin_cities_keyboard(lang),
        )
        await cb.answer()
        return
    await cb.answer()


async def on_admin_message(msg: Message):
    """Обработка текста от админа в состоянии создания/редактирования города."""
    if msg.from_user.id != ADMIN_ID:
        return
    state = _admin_state.get(msg.from_user.id)
    if not state:
        return
    lang = _user_lang(msg)
    text = (msg.text or msg.caption or "").strip()
    has_photo = bool(msg.photo)
    if text == "/back":
        if state.get("state") == "admin_game_min_max":
            _admin_clear(msg.from_user.id)
            await msg.answer(t("admin_select_game_type", lang), reply_markup=admin_create_game_type_keyboard(lang))
        elif state.get("state") == "admin_game_prize_places":
            _admin_state[msg.from_user.id] = {
                "state": "admin_game_min_max",
                "game_type": state.get("game_type"),
                "scope": state.get("scope"),
                "city_id": state.get("city_id"),
            }
            await msg.answer(_admin_input_prompt("admin_enter_min_max", lang))
        elif state.get("state") == "admin_game_prizes":
            _admin_state[msg.from_user.id] = {
                "state": "admin_game_prize_places",
                "game_type": state.get("game_type"),
                "scope": state.get("scope"),
                "city_id": state.get("city_id"),
                "min_participants": state.get("min_participants"),
                "max_participants": state.get("max_participants"),
            }
            await msg.answer(_admin_input_prompt("admin_enter_prize_places", lang))
        elif state.get("state") == "admin_game_datetime":
            # Назад к вводу призов (заново)
            _admin_state[msg.from_user.id] = {
                "state": "admin_game_prizes",
                "game_type": state.get("game_type"),
                "scope": state.get("scope"),
                "city_id": state.get("city_id"),
                "min_participants": state.get("min_participants"),
                "max_participants": state.get("max_participants"),
                "prize_places": state.get("prize_places"),
                "prizes": [],
                "mode": None,
            }
            await msg.answer(_admin_prizes_initial_prompt(lang, state.get("prize_places") or 0, place=1))
        elif state.get("state") in ("admin_edit_game_participants", "admin_edit_game_prize_places", "admin_edit_game_datetime"):
            game_id = state.get("game_id")
            _admin_clear(msg.from_user.id)
            if game_id:
                from db.queries import get_game, get_prizes, get_city
                game = get_game(game_id)
                if game:
                    prizes = get_prizes(game_id)
                    city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
                    await msg.answer(_format_game_detail(game, prizes, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
            return
        elif state.get("state") == "admin_edit_game_prizes":
            game_id = state.get("game_id")
            _admin_clear(msg.from_user.id)
            if game_id:
                from db.queries import get_game, get_prizes, get_city
                game = get_game(game_id)
                if game:
                    prizes = get_prizes(game_id)
                    city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
                    await msg.answer(_format_game_detail(game, prizes, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
            return
        else:
            _admin_clear(msg.from_user.id)
            await msg.answer(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))
        return
    if not text and not (state.get("state") in ("admin_game_prizes", "admin_edit_game_prizes") and has_photo):
        if state.get("state") == "admin_create_city":
            await msg.answer(_admin_input_prompt("admin_enter_city_name", lang))
        elif state.get("state") == "admin_edit_name":
            await msg.answer(_admin_input_prompt("admin_enter_new_name", lang))
        elif state.get("state") == "admin_game_min_max":
            await msg.answer(_admin_input_prompt("admin_enter_min_max", lang))
        elif state.get("state") == "admin_game_prize_places":
            await msg.answer(_admin_input_prompt("admin_enter_prize_places", lang))
        elif state.get("state") == "admin_game_prizes":
            prize_places = state.get("prize_places") or 0
            place = len(state.get("prizes") or []) + 1
            await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=place))
        elif state.get("state") == "admin_game_datetime":
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
        elif state.get("state") == "admin_edit_game_participants":
            await msg.answer(_admin_input_prompt("admin_enter_participants_new", lang))
        elif state.get("state") == "admin_edit_game_prize_places":
            await msg.answer(_admin_input_prompt("admin_enter_prize_places_new", lang))
        elif state.get("state") == "admin_edit_game_prizes":
            prize_places = state.get("prize_places") or 0
            place = len(state.get("prizes") or []) + 1
            await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=place, new=True))
        elif state.get("state") == "admin_edit_game_datetime":
            await msg.answer(_admin_input_prompt("admin_enter_datetime_new", lang))
        elif state.get("state") == "admin_game_confirm_create":
            # Назад к вводу даты и времени
            _admin_state[msg.from_user.id] = {
                **state,
                "state": "admin_game_datetime",
            }
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
        return
    name = text
    if state.get("state") == "admin_create_city":
        from db.queries import create_city
        city_id = create_city(name)
        _admin_clear(msg.from_user.id)
        if city_id:
            await msg.answer(t("admin_city_created", lang).format(name=name))
        await msg.answer(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))
        return
    if state.get("state") == "admin_edit_name":
        city_id = state.get("city_id")
        if not city_id:
            _admin_clear(msg.from_user.id)
            await msg.answer(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))
            return
        from db.queries import update_city
        update_city(city_id, name)
        _admin_clear(msg.from_user.id)
        await msg.answer(t("admin_city_updated", lang).format(name=name))
        await msg.answer(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))
        return
    if state.get("state") == "admin_game_min_max":
        parts = [p.strip() for p in text.split("/", 1)]
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            await msg.answer(
                t("admin_min_max_invalid", lang),
            )
            await msg.answer(_admin_input_prompt("admin_enter_min_max", lang))
            return
        min_p, max_p = int(parts[0]), int(parts[1])
        if min_p < 1 or max_p < 1 or min_p > max_p:
            await msg.answer(t("admin_min_max_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_min_max", lang))
            return
        await msg.answer(t("admin_min_max_accepted", lang).format(min=min_p, max=max_p))
        _admin_state[msg.from_user.id] = {
            "state": "admin_game_prize_places",
            "game_type": state.get("game_type"),
            "scope": state.get("scope"),
            "city_id": state.get("city_id"),
            "min_participants": min_p,
            "max_participants": max_p,
        }
        await msg.answer(_admin_input_prompt("admin_enter_prize_places", lang))
        return
    if state.get("state") == "admin_game_prize_places":
        max_p = state.get("max_participants")
        if not text.isdigit():
            await msg.answer(t("admin_prize_places_invalid", lang).format(max=max_p or "?"))
            await msg.answer(_admin_input_prompt("admin_enter_prize_places", lang))
            return
        n = int(text)
        if n < 1 or (max_p is not None and n > max_p):
            await msg.answer(t("admin_prize_places_invalid", lang).format(max=max_p or "?"))
            await msg.answer(_admin_input_prompt("admin_enter_prize_places", lang))
            return
        await msg.answer(t("admin_prize_places_accepted", lang))
        _admin_state[msg.from_user.id] = {
            "state": "admin_game_prizes",
            "game_type": state.get("game_type"),
            "scope": state.get("scope"),
            "city_id": state.get("city_id"),
            "min_participants": state.get("min_participants"),
            "max_participants": state.get("max_participants"),
            "prize_places": n,
            "prizes": [],
            "mode": None,
        }
        await msg.answer(_admin_prizes_initial_prompt(lang, n, place=1))
        return
    if state.get("state") == "admin_game_prizes":
        prize_places = state.get("prize_places") or 0
        prizes: List = state.get("prizes") or []
        mode = state.get("mode")
        if has_photo:
            if not text:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            if mode is None:
                mode = "one_by_one"
            if mode != "one_by_one":
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=1))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            if _parse_prize_pair(text)[0] is None:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            try:
                photo = msg.photo[-1]
                file = await msg.bot.get_file(photo.file_id)
                ext = "jpg"
                fname = f"prize_{msg.from_user.id}_{int(time.time())}_{len(prizes)}.{ext}"
                dest = PRIZES_DIR / fname
                await msg.bot.download_file(file.file_path, dest)
                rel_path = f"призы/{fname}"
                prizes.append({"path": rel_path, "text": text or None})
            except Exception as e:
                logger.exception("Download prize photo: %s", e)
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
        elif ("\n" in text or "\r" in text) and not has_photo:
            if mode is not None:
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(prizes)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            parts = [p.strip() for p in text.splitlines() if p.strip()]
            if len(parts) != prize_places:
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(parts)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=1))
                return
            # Проверяем формат каждого приза: «Название приза: сам приз»
            for p in parts:
                if _parse_prize_pair(p)[0] is None:
                    await msg.answer(t("admin_prize_format_invalid", lang))
                    await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=1))
                    return
            prizes_coupons = parts
            _admin_state[msg.from_user.id] = {
                **state,
                "state": "admin_game_datetime",
                "prizes_coupons": prizes_coupons,
            }
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
            return
        else:
            if mode is None:
                mode = "one_by_one"
            if mode != "one_by_one":
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(prizes)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            if _parse_prize_pair(text)[0] is None:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1))
                return
            prizes.append({"path": None, "text": text})
        if has_photo or (not has_photo and text):
            if len(prizes) < prize_places:
                _admin_state[msg.from_user.id] = {**state, "prizes": prizes, "mode": mode}
                next_place = len(prizes) + 1
                await msg.answer(
                    t("admin_prize_received", lang).format(current=len(prizes), total=prize_places)
                    + "\n\n"
                    + t("admin_send_prize_for_place", lang).format(place=next_place)
                )
                return
            prizes_coupons = []
            for i, p in enumerate(prizes):
                if isinstance(p, dict):
                    c = (p.get("path") or "") + ("\n" if p.get("path") and p.get("text") else "") + (p.get("text") or "")
                    prizes_coupons.append(c.strip())
                else:
                    prizes_coupons.append(str(p))
            _admin_state[msg.from_user.id] = {
                **state,
                "state": "admin_game_datetime",
                "prizes_coupons": prizes_coupons,
            }
            await msg.answer(t("admin_prize_received", lang).format(current=len(prizes), total=prize_places))
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
        return
    if state.get("state") == "admin_game_datetime":
        raw = text.strip()
        start_time = None
        for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                start_time = dt.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if not start_time:
            await msg.answer(t("admin_datetime_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
            return
        now = dt.now()
        if start_time <= now:
            await msg.answer(t("admin_datetime_past", lang))
            await msg.answer(_admin_input_prompt("admin_enter_datetime", lang))
            return
        # Сохраняем данные игры в состоянии и показываем предпросмотр с кнопками «Создать» / «Отменить»
        game_type = state.get("game_type") or "dice"
        min_p = state.get("min_participants") or 2
        max_p = state.get("max_participants") or 50
        prize_places = state.get("prize_places") or 1
        prizes_coupons = state.get("prizes_coupons") or []
        city_id = state.get("city_id") if state.get("scope") == "city" else None
        from db.queries import get_city
        if city_id:
            city_row = get_city(city_id)
            city_label = (city_row or {}).get("city") or str(city_id)
        else:
            city_label = t("admin_game_city_all", lang)
        game_type_label = _game_type_label(lang, game_type)
        # Названия призов (до двоеточия)
        prize_names = []
        for raw in prizes_coupons:
            name_val, _ = _parse_prize_pair(raw)
            if name_val:
                prize_names.append(name_val)
        lines = [
            f"{t('admin_game_label_type', lang)}: {game_type_label}",
            f"{t('admin_game_label_city', lang)}: {city_label}",
            f"{t('admin_game_label_participants', lang)}: {min_p}-{max_p}",
            f"{t('admin_game_label_prize_places', lang)}: {prize_places}",
            f"{t('admin_game_label_prizes', lang)}:",
        ]
        for i, name_val in enumerate(prize_names, 1):
            lines.append(f"  {i}. {name_val}")
        lines.append(f"{t('admin_game_label_date', lang)}: {start_time.strftime('%d.%m.%Y %H:%M')}")
        preview_text = "\n".join(lines)
        _admin_state[msg.from_user.id] = {
            **state,
            "state": "admin_game_confirm_create",
            "start_time": start_time,
        }
        keyboard = InlineKeyboardBuilder()
        keyboard.add(
            InlineKeyboardButton(text="Создать", callback_data="admin:create_game:confirm"),
            InlineKeyboardButton(text=t("admin_btn_cancel", lang) if t("admin_btn_cancel", lang) != "admin_btn_cancel" else "Отменить", callback_data="admin:create_game:cancel"),
        )
        keyboard.adjust(1, 1)
        await msg.answer(preview_text, reply_markup=keyboard.as_markup())
        return
    if state.get("state") == "admin_edit_game_participants":
        game_id = state.get("game_id")
        from db.queries import get_game, update_game, get_prizes, get_city
        game = get_game(game_id) if game_id else None
        if not game:
            _admin_clear(msg.from_user.id)
            return
        parts = [p.strip() for p in text.split("/", 1)]
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            await msg.answer(t("admin_min_max_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_participants_new", lang))
            return
        min_p, max_p = int(parts[0]), int(parts[1])
        if min_p < 1 or max_p < 1 or min_p > max_p:
            await msg.answer(t("admin_min_max_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_participants_new", lang))
            return
        if min_p < game["prize_places"]:
            await msg.answer(t("admin_min_max_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_participants_new", lang))
            return
        update_game(game_id, min_participants=min_p, max_participants=max_p)
        _admin_clear(msg.from_user.id)
        await msg.answer(t("admin_game_updated", lang))
        game = get_game(game_id)
        prizes = get_prizes(game_id)
        city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
        await msg.answer(_format_game_detail(game, prizes, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
        return
    if state.get("state") == "admin_edit_game_prize_places":
        game_id = state.get("game_id")
        from db.queries import get_game, update_game, get_prizes, get_city, delete_prizes
        game = get_game(game_id) if game_id else None
        if not game:
            _admin_clear(msg.from_user.id)
            return
        if not text.isdigit():
            await msg.answer(t("admin_prize_places_invalid", lang).format(max=game["max_participants"]))
            await msg.answer(_admin_input_prompt("admin_enter_prize_places_new", lang))
            return
        n = int(text)
        if n < 1 or n > game["max_participants"]:
            await msg.answer(t("admin_prize_places_invalid", lang).format(max=game["max_participants"]))
            await msg.answer(_admin_input_prompt("admin_enter_prize_places_new", lang))
            return
        update_game(game_id, prize_places=n)
        delete_prizes(game_id)
        _admin_state[msg.from_user.id] = {
            "state": "admin_edit_game_prizes",
            "game_id": game_id,
            "prize_places": n,
            "prizes": [],
            "mode": None,
        }
        await msg.answer(t("admin_prize_places_updated_enter_prizes", lang))
        await msg.answer(_admin_prizes_initial_prompt(lang, n, place=1, new=True))
        return
    if state.get("state") == "admin_edit_game_datetime":
        game_id = state.get("game_id")
        from db.queries import get_game, update_game, get_prizes, get_city
        raw = text.strip()
        start_time = None
        for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M"):
            try:
                start_time = dt.strptime(raw, fmt)
                break
            except ValueError:
                continue
        if not start_time:
            await msg.answer(t("admin_datetime_invalid", lang))
            await msg.answer(_admin_input_prompt("admin_enter_datetime_new", lang))
            return
        if start_time <= dt.now():
            await msg.answer(t("admin_datetime_past", lang))
            await msg.answer(_admin_input_prompt("admin_enter_datetime_new", lang))
            return
        game = get_game(game_id) if game_id else None
        if not game:
            _admin_clear(msg.from_user.id)
            return
        new_name = f"{game['game_type']} {start_time.strftime('%d.%m.%Y %H:%M')}"
        update_game(game_id, start_time=start_time, name=new_name)
        _admin_clear(msg.from_user.id)
        await msg.answer(t("admin_game_updated", lang))
        game = get_game(game_id)
        prizes = get_prizes(game_id)
        city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
        await msg.answer(_format_game_detail(game, prizes, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
        return
    if state.get("state") == "admin_edit_game_prizes":
        game_id = state.get("game_id")
        prize_places = state.get("prize_places") or 0
        prizes: List = state.get("prizes") or []
        mode = state.get("mode")
        if has_photo:
            if not text:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            if mode is None:
                mode = "one_by_one"
            if mode != "one_by_one":
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=1))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            if _parse_prize_pair(text)[0] is None:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            try:
                photo = msg.photo[-1]
                file = await msg.bot.get_file(photo.file_id)
                fname = f"prize_{msg.from_user.id}_{int(time.time())}_{len(prizes)}.jpg"
                dest = PRIZES_DIR / fname
                await msg.bot.download_file(file.file_path, dest)
                rel_path = f"призы/{fname}"
                prizes.append({"path": rel_path, "text": text or None})
            except Exception as e:
                logger.exception("Download prize photo: %s", e)
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
        elif ("\n" in text or "\r" in text) and not has_photo:
            if mode is not None:
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(prizes)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            parts = [p.strip() for p in text.splitlines() if p.strip()]
            if len(parts) != prize_places:
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(parts)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=1, new=True))
                return
            for p in parts:
                if _parse_prize_pair(p)[0] is None:
                    await msg.answer(t("admin_prize_format_invalid", lang))
                    await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=1, new=True))
                    return
            from db.queries import set_prizes, get_game, get_prizes, get_city
            set_prizes(game_id, parts)
            _admin_clear(msg.from_user.id)
            await msg.answer(t("admin_game_updated", lang))
            game = get_game(game_id)
            prizes_list = get_prizes(game_id)
            city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
            await msg.answer(_format_game_detail(game, prizes_list, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
            return
        else:
            if mode is None:
                mode = "one_by_one"
            if mode != "one_by_one":
                await msg.answer(t("admin_prizes_slash_invalid", lang).format(count=prize_places, got=len(prizes)))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            if _parse_prize_pair(text)[0] is None:
                await msg.answer(t("admin_prize_format_invalid", lang))
                await msg.answer(_admin_prizes_initial_prompt(lang, prize_places, place=len(prizes) + 1, new=True))
                return
            prizes.append({"path": None, "text": text})
        if has_photo or (not has_photo and text):
            if len(prizes) < prize_places:
                _admin_state[msg.from_user.id] = {**state, "prizes": prizes, "mode": mode}
                next_place = len(prizes) + 1
                await msg.answer(
                    t("admin_prize_received", lang).format(current=len(prizes), total=prize_places)
                    + "\n\n"
                    + t("admin_send_prize_for_place", lang).format(place=next_place)
                )
                return
            from db.queries import set_prizes, get_game, get_prizes, get_city
            prizes_coupons = []
            for p in prizes:
                if isinstance(p, dict):
                    c = (p.get("path") or "") + ("\n" if p.get("path") and p.get("text") else "") + (p.get("text") or "")
                    prizes_coupons.append(c.strip())
                else:
                    prizes_coupons.append(str(p))
            set_prizes(game_id, prizes_coupons)
            _admin_clear(msg.from_user.id)
            await msg.answer(t("admin_game_updated", lang))
            game = get_game(game_id)
            prizes_list = get_prizes(game_id)
            city_label = t("admin_game_city_all", lang) if not game.get("city_id") else (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
            await msg.answer(_format_game_detail(game, prizes_list, city_label, lang), reply_markup=admin_game_detail_keyboard(game_id, lang))
        return
    # состояние ожидания выбора кнопки (edit_select, delete_select, delete_confirm) — сбрасываем в меню городов
    _admin_clear(msg.from_user.id)
    await msg.answer(t("admin_cities_title", lang), reply_markup=admin_cities_keyboard(lang))


async def on_city_chosen(cb: CallbackQuery):
    from db.queries import get_user, save_user
    lang = _user_lang(cb)
    user_id = cb.from_user.id
    if get_user(user_id) is not None:
        await cb.answer(t("already_chose_city", lang))
        return
    value = cb.data.split(":", 1)[1]
    city_id = int(value) if value != "0" else None
    name = (cb.from_user.first_name or "") + (" " + cb.from_user.last_name if cb.from_user.last_name else "")
    saved_lang = _pending_lang.pop(user_id, None) or getattr(cb.from_user, "language_code", None)
    save_user(
        user_id=user_id,
        user_name=cb.from_user.username,
        name=name.strip() or None,
        city_id=city_id,
        language_code=saved_lang,
    )
    await cb.message.edit_text(
        t("welcome_menu", saved_lang),
        reply_markup=main_menu_keyboard(saved_lang, user_id),
    )
    await cb.answer()


async def send_5min_reminders(bot: Bot):
    """Отправить напоминание за 5 минут всем записанным на подходящие игры."""
    from db.queries import get_games_need_5min_reminder, get_participants_user_ids, set_game_reminder_5min_sent, get_user
    try:
        games = get_games_need_5min_reminder()
    except Exception as e:
        logger.exception("get_games_need_5min_reminder: %s", e)
        return
    for g in games:
        game_id = g["id"]
        chat_id = g["chat_id"]
        try:
            chat = await bot.get_chat(chat_id)
            chat_title = chat.title or str(chat_id)
        except Exception as e:
            logger.warning("get_chat %s: %s", chat_id, e)
            chat_title = str(chat_id)
        try:
            user_ids = get_participants_user_ids(game_id)
        except Exception as e:
            logger.exception("get_participants_user_ids: %s", e)
            continue
        text_by_lang = {}
        for uid in user_ids:
            try:
                user = get_user(uid)
                lang = (user or {}).get("language_code") or DEFAULT_LANG
                if lang not in text_by_lang:
                    text_by_lang[lang] = t("game_reminder_5min", lang).format(chat_title=chat_title)
                text = text_by_lang[lang]
                await bot.send_message(chat_id=uid, text=text)
            except Exception as e:
                logger.warning("reminder to user %s: %s", uid, e)
        try:
            set_game_reminder_5min_sent(game_id)
        except Exception as e:
            logger.exception("set_game_reminder_5min_sent: %s", e)


def _format_participants_list(participants: list, totals_by_uid: dict, lang: str, with_header: bool = True) -> str:
    """Текст закреплённого сообщения: (опционально) «Список участников» + нумерованный список с именами (ссылки) и очками по раундам (12/16/7 или 10/ выбыл)."""
    pending = t("round_score_pending", lang)
    lines = []
    if with_header:
        lines.extend([t("round_list_participants", lang), ""])
    for i, (uid, name) in enumerate(participants, 1):
        safe_name = html.escape(name)
        scores = totals_by_uid.get(uid)
        if scores is None:
            score_str = pending
        elif isinstance(scores, list):
            parts = []
            for s in scores:
                if s == ELIMINATED_MARKER:
                    parts.append(" выбыл")
                elif s is not None:
                    parts.append(str(s))
                else:
                    parts.append(pending)
            score_str = "/".join(parts)
        else:
            score_str = str(scores) if scores is not None else pending
        lines.append(f'{i}. <a href="tg://user?id={uid}">{safe_name}</a>   {score_str}')
    return "\n".join(lines)


def _build_totals_for_list(game_id: int, participants: list, current_index: int, round_number: int) -> dict:
    """Тоталы для списка (один раунд): из БД + 0 для пропустивших. Возвращает dict uid -> [score] для совместимости с форматом по раундам."""
    from db.queries import get_round_totals
    totals = get_round_totals(game_id, round_number)
    totals_by_uid = dict(totals)
    for i, (uid, _) in enumerate(participants):
        if i < current_index and uid not in totals_by_uid:
            totals_by_uid[uid] = 0
    return {uid: [v] for uid, v in totals_by_uid.items()}


def _build_totals_multiround(
    game_id: int,
    participants: list,
    current_index: int,
    round_number: int,
    playing_participants: list = None,
) -> dict:
    """Тоталы по раундам 1..round_number для списка: dict uid -> [r1, r2, ...], None = ещё не бросал/вылетел.
    Если передан playing_participants (кто играет в текущем раунде), то participants может быть полным списком
    (включая вылетевших); для текущего раунда у неиграющих будет None (—)."""
    from db.queries import get_round_totals
    result = {}
    for uid, _ in participants:
        result[uid] = []
    playing_uids = {p[0] for p in (playing_participants or [])}
    for r in range(1, round_number + 1):
        totals = get_round_totals(game_id, r)
        by_uid = dict(totals)
        for i, (uid, _) in enumerate(participants):
            if uid in by_uid:
                result[uid].append(by_uid[uid])
            elif r < round_number:
                result[uid].append(0)
            elif playing_participants is not None and r == round_number:
                if uid not in playing_uids:
                    result[uid].append(ELIMINATED_MARKER)
                else:
                    playing_idx = next(j for j, (u, _) in enumerate(playing_participants) if u == uid)
                    if uid in by_uid:
                        result[uid].append(by_uid[uid])
                    elif playing_idx < current_index:
                        result[uid].append(0)
                    else:
                        result[uid].append(None)
            elif i < current_index:
                result[uid].append(0)
            else:
                result[uid].append(None)
    return result


def _get_total_scores_so_far(game_id: int, max_round: int) -> dict:
    """Сумма очков по участникам за раунды 1..max_round: dict uid -> total."""
    from db.queries import get_round_totals
    result = {}
    for r in range(1, max_round + 1):
        for uid, total in get_round_totals(game_id, r):
            result[uid] = result.get(uid, 0) + total
    return result


async def announce_game_start(bot: Bot, game: dict):
    """Объявить в чате о старте игры, отправить правила, список участников (имена — ссылки), закрепить его и отправить эмодзи игры."""
    from db.queries import get_participants_for_display, get_city, update_game, get_prizes
    lang = DEFAULT_LANG
    game_type_label = _game_type_label(lang, game["game_type"])
    # Формулировка города: если игра для всех, явно пишем «для всех городов»
    if not game.get("city_id"):
        city_label = t("game_city_all_short", lang)
    else:
        city_label = (get_city(game["city_id"]) or {}).get("city") or str(game["city_id"])
    # Призы из БД
    try:
        prizes = get_prizes(game["id"])
    except Exception:
        prizes = []
    prizes_lines = []
    for p in prizes:
        name = (p.get("prize_name") or "").strip()
        if not name:
            raw = (p.get("coupon_text") or "").strip()
            name, _ = _parse_prize_pair(raw)
            name = name or ""
        if name:
            prizes_lines.append(name)
    prizes_block = ""
    if prizes_lines:
        prizes_block = "\n\nпризы:\n" + "\n".join(prizes_lines)
    msg1 = t("game_starts", lang).format(game_type=game_type_label, city=city_label) + prizes_block
    chat_id = game["chat_id"]
    try:
        await bot.send_message(chat_id=chat_id, text=msg1)
        await asyncio.sleep(5)
    except Exception as e:
        logger.warning("announce_game_start msg1 to %s: %s", chat_id, e)
        return
    # Примерные правила (пока только на русском, позже можно вынести в переводы)
    rules_text = (
        "Правила игры:\n\n"
        "1. В игре участвуют все зарегистрированные игроки из всех городов.\n"
        "2. Игра проходит по раундам. В каждом раунде каждый игрок делает по 3 броска.\n"
        "3. После каждого броска бот показывает ваш результат и суммарный результат за текущий раунд.\n"
        "4. По окончании раунда считается общий результат каждого игрока.\n"
        "5. Игроки, набравшие не меньше проходного балла, переходят в следующий раунд.\n"
        "6. В финальном раунде определяется победитель(и) по наибольшему общему результату.\n"
        "7. При равенстве результатов могут быть назначены дополнительные броски.\n"
        "8. Организатор оставляет за собой право изменять правила и проходной балл до начала следующей игры."
    )
    try:
        await bot.send_message(chat_id=chat_id, text=rules_text)
        await asyncio.sleep(5)
    except Exception as e:
        logger.warning("announce_game_start rules to %s: %s", chat_id, e)
    try:
        participants = get_participants_for_display(game["id"])
    except Exception as e:
        logger.exception("get_participants_for_display: %s", e)
        update_game(game["id"], status="active")
        return
    lang = DEFAULT_LANG
    # Первый раунд + список участников (сообщение закрепляем)
    list_text = "Первый раунд!\nСписок участников:\n\n" + _format_participants_list(participants, {}, lang, with_header=False)
    try:
        sent = await bot.send_message(chat_id=chat_id, text=list_text)
        await bot.pin_chat_message(chat_id=chat_id, message_id=sent.message_id)
        list_message_id = sent.message_id
    except Exception as e:
        logger.warning("announce_game_start msg2/pin to %s: %s", chat_id, e)
        list_message_id = None
    # Сообщение с типом игры: отправляем реальный "бросок" (dice), а не текст
    emoji = GAME_TYPE_EMOJI.get(game["game_type"], "🎲")
    try:
        await asyncio.sleep(5)
        await bot.send_dice(chat_id=chat_id, emoji=emoji)
    except Exception as e:
        logger.warning("announce_game_start emoji to %s: %s", chat_id, e)
    try:
        update_game(game["id"], status="active")
    except Exception as e:
        logger.exception("update_game status active: %s", e)
    asyncio.create_task(delayed_start_round_1(bot, game["id"], chat_id, game["game_type"], list_message_id))


async def delayed_start_round_1(bot: Bot, game_id: int, chat_id: int, game_type: str, list_message_id: int = None):
    """Через небольшую паузу вызвать первого участника первого раунда."""
    await asyncio.sleep(5)
    from db.queries import get_participants_for_display
    lang = DEFAULT_LANG
    emoji = GAME_TYPE_EMOJI.get(game_type, "🎲")
    try:
        participants = get_participants_for_display(game_id)
    except Exception as e:
        logger.exception("get_participants_for_display: %s", e)
        return
    if not participants:
        return
    # Определяем, финальный ли это раунд (сразу призовой), исходя из числа участников и призов
    try:
        from db.queries import get_game
        game = get_game(game_id)
        prize_places = (game or {}).get("prize_places") or 1
    except Exception:
        prize_places = 1
    is_final_round = len(participants) < 8 or len(participants) < (prize_places * 3)
    _round_state[game_id] = {
        "participant_ids": participants,
        "round_participants": list(participants),
        "all_participants": list(participants),
        "current_index": 0,
        "throw_count": 0,
        "chat_id": chat_id,
        "game_type": game_type,
        "round_number": 1,
        "list_message_id": list_message_id,
        "is_final_round": is_final_round,
        "turn_id": 0,
    }
    _chat_to_game[chat_id] = game_id
    uid0, name0 = participants[0]
    name_link = f'<a href="tg://user?id={uid0}">{html.escape(name0)}</a>'
    text = t("round_do_3_throws", lang).format(name=name_link, emoji=emoji)
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        _round_state[game_id]["turn_id"] = 1
        asyncio.create_task(_timeout_for_turn(bot, game_id, 0, 1, 1))
    except Exception as e:
        logger.warning("delayed_start_round_1: %s", e)
        _round_state.pop(game_id, None)
        _chat_to_game.pop(chat_id, None)


async def _timeout_for_turn(bot: Bot, game_id: int, participant_index: int, turn_id: int, expected_round: int):
    """Таймаут хода (2 мин по умолчанию, 1 мин для опоздавших): пропуск → 0, обновить список, следующий или _check_catchup_or_finish.
    turn_id защищает от срабатывания старых задач таймаута в новом ходе/раунде."""
    timeout_sec = 120
    state = _round_state.get(game_id)
    if state:
        timeout_sec = state.get("timeout_seconds", 120)
    await asyncio.sleep(timeout_sec)
    state = _round_state.get(game_id)
    if not state:
        return
    # Если уже начался другой ход/раунд — игнорируем этот таймаут
    if state.get("turn_id") != turn_id:
        return
    if state.get("round_number") != expected_round:
        return
    participants = state["participant_ids"]
    if state["current_index"] != participant_index or participant_index >= len(participants):
        return
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    list_msg_id = state.get("list_message_id")
    current_index = state["current_index"]
    list_participants = state.get("all_participants") or participants
    list_index = (current_index + 1) if not state.get("is_missed_pass") else len(list_participants)
    playing = participants if len(list_participants) > len(participants) else None
    totals_by_uid = _build_totals_multiround(game_id, list_participants, list_index, state["round_number"], playing_participants=playing)
    if list_msg_id is not None:
        try:
            await bot.edit_message_text(
                chat_id=chat_id, message_id=list_msg_id,
                text=_format_participants_list(list_participants, totals_by_uid, lang),
            )
        except Exception as e:
            logger.warning("timeout_for_turn edit list: %s", e)
    skipped_uid, skipped_name = participants[participant_index]
    skipped_link = f'<a href="tg://user?id={skipped_uid}">{html.escape(skipped_name)}</a>'
    try:
        await bot.send_message(
            chat_id=chat_id,
            text=t("round_participant_skipped", lang).format(name=skipped_link),
        )
    except Exception as e:
        logger.warning("timeout_2min skipped msg: %s", e)
    state["current_index"] = current_index + 1
    state["throw_count"] = 0
    if state["current_index"] >= len(participants):
        # Опоздавшим даём только один шанс за раунд; после прохода по ним — сразу завершаем раунд
        if state.get("is_missed_pass"):
            await _finish_round_and_maybe_next(bot, game_id, state)
        else:
            await _check_catchup_or_finish(bot, game_id, state)
        return
    emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    next_uid, next_name = participants[state["current_index"]]
    name_link = f'<a href="tg://user?id={next_uid}">{html.escape(next_name)}</a>'
    text = t("round_do_3_throws", lang).format(name=name_link, emoji=emoji)
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        state["turn_id"] = int(state.get("turn_id") or 0) + 1
        asyncio.create_task(_timeout_for_turn(bot, game_id, state["current_index"], state["turn_id"], state["round_number"]))
    except Exception as e:
        logger.warning("timeout_for_turn next: %s", e)


async def on_game_emoji_text(msg: Message):
    """Эмодзи текстом (🎲/🎳/🎯 или несколько подряд) — засчитываем один бросок со случайным значением 1–6."""
    user_id = msg.from_user.id if msg.from_user else None
    text_preview = (msg.text or "")[:20].replace("\n", " ")
    logger.info("on_game_emoji_text: chat_id=%s user_id=%s text=%r", msg.chat.id, user_id, text_preview)
    if not msg.text or msg.chat.id not in _chat_to_game:
        logger.info("on_game_emoji_text: skip — нет text или чат не игровой")
        return
    game_id = _chat_to_game[msg.chat.id]
    state = _round_state.get(game_id)
    if not state:
        logger.info("on_game_emoji_text: skip — нет state для game_id=%s", game_id)
        return
    text = (msg.text or "").strip()
    expected_emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    if not text.startswith(expected_emoji):
        logger.info("on_game_emoji_text: skip — текст не начинается с %r", expected_emoji)
        return
    if user_id is None:
        logger.info("on_game_emoji_text: skip — нет from_user")
        return
    # Если идёт тай-брейк, обрабатываем этот бросок как дополнительный (по текстовому эмодзи)
    if state.get("phase") == "tiebreak":
        if user_id != state.get("tiebreak_wait_uid"):
            return
        from db.queries import add_throw
        value = random.randint(1, 6)
        throw_idx = state.get("tiebreak_next_throw_index", 3)
        add_throw(game_id, user_id, state["round_number"], throw_idx, value)
        try:
            name = next((n for u, n in (state.get("tiebreak_tied_group") or []) if u == user_id), str(user_id))
            name_link = f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'
            await msg.bot.send_message(
                chat_id=msg.chat.id,
                text=t("round_tiebreak_result", DEFAULT_LANG).format(name=name_link, value=value),
            )
        except Exception:
            pass
        state["tiebreak_next_throw_index"] = throw_idx + 1
        tied = state.get("tiebreak_tied_group") or []
        state["tiebreak_index"] = state.get("tiebreak_index", 0) + 1
        if state["tiebreak_index"] < len(tied):
            await _start_tiebreak_turn(msg.bot, game_id, state, state["tiebreak_index"])
        else:
            state["phase"] = None
            state["tiebreak_cycle_completed"] = True
            from db.queries import get_game
            game = get_game(game_id)
            prize_places = (game or {}).get("prize_places") or 1
            await _do_tiebreak_and_winners(msg.bot, game_id, state, [], prize_places)
        return
    participants = state["participant_ids"]
    current_index = state["current_index"]
    if current_index >= len(participants):
        logger.info("on_game_emoji_text: skip — current_index=%s >= len(participants)=%s", current_index, len(participants))
        return
    current_uid = participants[current_index][0]
    if user_id != current_uid:
        logger.info("on_game_emoji_text: skip — не твой ход: user_id=%s, текущий current_uid=%s", user_id, current_uid)
        return
    value = random.randint(1, 6)
    logger.info("on_game_emoji_text: принимаем бросок (текст) user_id=%s value=%s", user_id, value)
    await _process_one_throw(msg.bot, msg.chat.id, game_id, state, user_id, value)


async def _process_one_throw(bot: Bot, chat_id: int, game_id: int, state: dict, user_id: int, value: int):
    """Обработать один бросок: записать в БД, ответить; при 3-м — вызвать следующего. Бросают вручную (dice в чате)."""
    lang = DEFAULT_LANG
    participants = state["participant_ids"]
    current_index = state["current_index"]
    current_name = participants[current_index][1]
    emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    from db.queries import add_throw, get_round_totals
    throw_count = state["throw_count"]
    add_throw(game_id, user_id, state["round_number"], throw_count, value)
    state["throw_count"] += 1
    throw_count = state["throw_count"]
    result_line = t("round_your_result", lang).format(value=value)
    if throw_count == 1:
        msg_text = f"{result_line}\n{t('round_throw_2_more', lang).format(emoji=emoji)}"
        try:
            await bot.send_message(chat_id=chat_id, text=msg_text)
        except TelegramRetryAfter as e:
            logger.warning("send_message flood (throw 1): %s", e)
        except Exception as e:
            logger.warning("send_message error (throw 1): %s", e)
        return
    if throw_count == 2:
        msg_text = f"{result_line}\n{t('round_throw_1_more', lang).format(emoji=emoji)}"
        try:
            await bot.send_message(chat_id=chat_id, text=msg_text)
        except TelegramRetryAfter as e:
            logger.warning("send_message flood (throw 2): %s", e)
        except Exception as e:
            logger.warning("send_message error (throw 2): %s", e)
        return
    if throw_count == 3:
        totals = get_round_totals(game_id, state["round_number"])
        current_total = next((t for u, t in totals if u == user_id), 0)
        # Имя в сообщении, чтобы в чате было понятно, чей это итог (сумма трёх бросков этого игрока)
        name_link = f'<a href="tg://user?id={user_id}">{html.escape(current_name)}</a>'
        third_throw_text = f"{t('round_your_result', lang).format(value=value)}\n{name_link}, спасибо! Ваш результат в этом раунде: {current_total}"
        try:
            await bot.send_message(chat_id=chat_id, text=third_throw_text)
        except TelegramRetryAfter as e:
            logger.warning("send_message flood (thanks_total): %s", e)
        except Exception as e:
            logger.warning("send_message error (thanks_total): %s", e)
        list_participants = state.get("all_participants") or state["participant_ids"]
        list_index = (state["current_index"] + 1) if not state.get("is_missed_pass") else len(list_participants)
        playing = state["participant_ids"] if len(list_participants) > len(state["participant_ids"]) else None
        totals_by_uid = _build_totals_multiround(game_id, list_participants, list_index, state["round_number"], playing_participants=playing)
        list_msg_id = state.get("list_message_id")
        if list_msg_id is not None:
            try:
                new_list_text = _format_participants_list(list_participants, totals_by_uid, lang)
                await bot.edit_message_text(
                    chat_id=chat_id, message_id=list_msg_id, text=new_list_text
                )
            except Exception as e:
                logger.warning("edit pinned list: %s", e)
        state["current_index"] += 1
        state["throw_count"] = 0
        if state["current_index"] >= len(participants):
            # Опоздавшим даём только один шанс за раунд; после прохода по ним — сразу завершаем раунд
            if state.get("is_missed_pass"):
                await _finish_round_and_maybe_next(bot, game_id, state)
            else:
                await _check_catchup_or_finish(bot, game_id, state)
            return
        next_uid, next_name = participants[state["current_index"]]
        name_link = f'<a href="tg://user?id={next_uid}">{html.escape(next_name)}</a>'
        text = t("round_do_3_throws", lang).format(name=name_link, emoji=emoji)
        try:
            await asyncio.sleep(5)
            await bot.send_message(chat_id=chat_id, text=text)
            state["turn_id"] = int(state.get("turn_id") or 0) + 1
            asyncio.create_task(_timeout_for_turn(bot, game_id, state["current_index"], state["turn_id"], state["round_number"]))
        except Exception as e:
            logger.warning("on_dice next participant: %s", e)


async def on_dice_message(msg: Message):
    """Обработка броска (dice) в чате: текущий участник раунда или фаза доп. времени (catchup)."""
    chat_id = msg.chat.id
    user_id = msg.from_user.id if msg.from_user else None
    dice_emoji = getattr(msg.dice, "emoji", None) if getattr(msg, "dice", None) else None
    dice_value = getattr(msg.dice, "value", None) if getattr(msg, "dice", None) else None
    logger.info("on_dice_message: chat_id=%s user_id=%s has_dice=%s dice_emoji=%s dice_value=%s", chat_id, user_id, getattr(msg, "dice", None) is not None, dice_emoji, dice_value)
    game_id = _chat_to_game.get(chat_id)
    if game_id is None:
        logger.info("on_dice_message: skip — chat_id %s не в _chat_to_game", chat_id)
        return
    state = _round_state.get(game_id)
    if not state:
        logger.info("on_dice_message: skip — нет state для game_id=%s", game_id)
        return
    if user_id is None:
        logger.info("on_dice_message: skip — нет from_user")
        return
    if state.get("phase") == "tiebreak":
        if user_id != state.get("tiebreak_wait_uid"):
            return
        if not msg.dice:
            return
        emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
        if msg.dice.emoji != emoji:
            return
        from db.queries import add_throw
        throw_idx = state.get("tiebreak_next_throw_index", 3)
        add_throw(game_id, user_id, state["round_number"], throw_idx, msg.dice.value)
        try:
            name = next((n for u, n in (state.get("tiebreak_tied_group") or []) if u == user_id), str(user_id))
            name_link = f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'
            await msg.bot.send_message(
                chat_id=chat_id,
                text=t("round_tiebreak_result", DEFAULT_LANG).format(name=name_link, value=msg.dice.value),
            )
        except Exception:
            pass
        state["tiebreak_next_throw_index"] = throw_idx + 1
        tied = state.get("tiebreak_tied_group") or []
        state["tiebreak_index"] = state.get("tiebreak_index", 0) + 1
        if state["tiebreak_index"] < len(tied):
            await _start_tiebreak_turn(msg.bot, game_id, state, state["tiebreak_index"])
        else:
            state["phase"] = None
            state["tiebreak_cycle_completed"] = True
            from db.queries import get_game
            game = get_game(game_id)
            prize_places = (game or {}).get("prize_places") or 1
            await _do_tiebreak_and_winners(msg.bot, game_id, state, [], prize_places)
        return
    participants = state["participant_ids"]
    current_index = state["current_index"]
    if current_index >= len(participants):
        logger.info("on_dice_message: skip — current_index=%s >= len(participants)=%s", current_index, len(participants))
        return
    current_user_id, current_name = participants[current_index]
    if user_id != current_user_id:
        logger.info("on_dice_message: skip — не твой ход: user_id=%s, текущий current_uid=%s (%s)", user_id, current_user_id, current_name)
        return
    if not msg.dice:
        logger.info("on_dice_message: skip — msg.dice пустой")
        return
    emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    if msg.dice.emoji != emoji:
        logger.info("on_dice_message: skip — эмодзи не совпадает: пришло %r, игра %s ожидает %r", msg.dice.emoji, state.get("game_type"), emoji)
        return
    logger.info("on_dice_message: принимаем бросок user_id=%s value=%s", user_id, msg.dice.value)
    await _process_one_throw(msg.bot, msg.chat.id, game_id, state, user_id, msg.dice.value)


async def _check_catchup_or_finish(bot: Bot, game_id: int, state: dict):
    """После обхода всех: если есть с 0 очков — тот же цикл по ним (1 мин на ход); иначе — завершение раунда. Опоздавшие только из тех, кто играл в этом раунде (participant_ids), выбывших не вызываем."""
    from db.queries import get_round_totals, get_user
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    # Кто играл в этом раунде — только они могут быть «опоздавшими»; выбывшие (не в participant_ids) не вызываем
    participants_this_round = state["participant_ids"]
    round_number = state["round_number"]
    totals = get_round_totals(game_id, round_number)
    totals_by_uid = dict(totals)
    for uid, _ in participants_this_round:
        totals_by_uid.setdefault(uid, 0)
    missed = [uid for uid, _ in participants_this_round if totals_by_uid[uid] == 0]
    if not missed:
        await _finish_round_and_maybe_next(bot, game_id, state)
        return
    lines = [t("round_participants_missed", lang), ""]
    missed_with_names = []
    for uid in missed:
        user = get_user(uid)
        name = (user or {}).get("name") or (user or {}).get("user_name") or str(uid)
        safe_name = html.escape(name)
        lines.append(f'• <a href="tg://user?id={uid}">{safe_name}</a>')
        missed_with_names.append((uid, name))
    lines.append("")
    lines.append(t("round_catchup_5min", lang))
    try:
        await bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        logger.warning("check_catchup missed list: %s", e)
    state["participant_ids"] = missed_with_names
    # round_participants не трогаем: это исходный список игроков этого раунда
    state["current_index"] = 0
    state["throw_count"] = 0
    state["timeout_seconds"] = 60
    state["is_missed_pass"] = True
    # Тот же цикл: первый опоздавший — «сделайте 3 броска», кнопка, таймаут 1 мин
    uid0, name0 = missed_with_names[0]
    name_link = f'<a href="tg://user?id={uid0}">{html.escape(name0)}</a>'
    emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    text = t("round_do_3_throws", lang).format(name=name_link, emoji=emoji)
    try:
        await asyncio.sleep(5)
        await bot.send_message(chat_id=chat_id, text=text)
        state["turn_id"] = int(state.get("turn_id") or 0) + 1
        asyncio.create_task(_timeout_for_turn(bot, game_id, 0, state["turn_id"], state["round_number"]))
    except Exception as e:
        logger.warning("check_catchup start missed turn: %s", e)


async def _finish_round_and_maybe_next(bot: Bot, game_id: int, state: dict):
    """Раунд закончен: средний балл, список прошедших. Если проходных < 10 или < призов*2 — финал. Далее: следующий раунд или финал+тай-брейк+победители."""
    from db.queries import get_round_totals, get_user, get_game
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    # Участники, которые реально играли в этом раунде (без учёта списка "опоздавших" в catchup)
    participants_this_round = state.get("round_participants") or state["participant_ids"]
    # Полный список для закрепа (может включать вылетевших)
    participants = state.get("all_participants") or participants_this_round
    round_number = state["round_number"]
    totals = get_round_totals(game_id, round_number)
    # totals_by_uid: только те, кто реально бросал в этом раунде
    totals_by_uid = dict(totals)
    # Средний балл считаем только по тем, кто бросал (без пропустивших)
    values = [total for _, total in totals]
    avg = sum(values) / len(values) if values else 0
    # Проходной балл: целая часть среднего (например 10.2 -> 10), включительно
    passing_score = int(avg)
    # В финальном раунде не показываем проходной балл (сразу объявляем победителей одним сообщением)
    if not state.get("is_final_round"):
        round_label = t("round_1_finished", lang) if round_number == 1 else t("round_N_finished", lang).format(round=round_number)
        lines = [
            round_label,
            t("round_passing_score", lang).format(score=passing_score),
        ]
        try:
            await bot.send_message(chat_id=chat_id, text="\n".join(lines))
        except Exception as e:
            logger.warning("finish_round_and_maybe_next send: %s", e)
    game = get_game(game_id)
    prize_places = (game or {}).get("prize_places") or 1

    # Если текущий раунд помечен как финальный — разыгрываем призы по результатам ЭТОГО раунда
    if state.get("is_final_round"):
        state["participant_ids"] = participants_this_round
        started = await _do_tiebreak_and_winners(bot, game_id, state, [], prize_places)
        # Если начался тай-брейк, состояние должно остаться, чтобы принимать броски
        if not started:
            _round_state.pop(game_id, None)
            _chat_to_game.pop(chat_id, None)
        return

    # Прошедшие — те, кто бросал и набрал >= проходного (включительно), для НЕ финальных раундов
    passed = [(uid, total) for uid, total in totals_by_uid.items() if total >= passing_score]
    passed.sort(key=lambda x: -x[1])
    if not passed:
        _round_state.pop(game_id, None)
        _chat_to_game.pop(chat_id, None)
        return
    passed_with_names = []
    for uid, _ in passed:
        u = get_user(uid)
        name = (u or {}).get("name") or (u or {}).get("user_name") or str(uid)
        passed_with_names.append((uid, name))

    # Финальный ли СЛЕДУЮЩИЙ раунд: если прошло < 8 ИЛИ < (призы * 3)
    num_next = len(passed_with_names)
    is_final_next = num_next < 8 or num_next < (prize_places * 3)

    # Отдельным сообщением — список прошедших (в следующий / финальный раунд)
    header_key = "round_list_passed_final" if is_final_next else "round_list_passed"
    lines_passed = [
        t(header_key, lang),
        "",
    ]
    for i, (uid, total) in enumerate(passed, 1):
        user = get_user(uid)
        name = (user or {}).get("name") or (user or {}).get("user_name") or str(uid)
        safe_name = html.escape(name)
        lines_passed.append(f'{i}. <a href="tg://user?id={uid}">{safe_name}</a>')
    try:
        await bot.send_message(chat_id=chat_id, text="\n".join(lines_passed))
    except Exception as e:
        logger.warning("finish_round_and_maybe_next passed list: %s", e)

    # Полный список участников раунда (включая вылетевших) — для закреплённого сообщения в след. раунде
    all_display = state.get("all_participants") or state["participant_ids"]
    await _start_round_N(
        bot,
        game_id,
        state["chat_id"],
        state["game_type"],
        state.get("list_message_id"),
        passed_with_names,
        round_number + 1,
        is_final_round=is_final_next,
        all_display_participants=all_display,
    )


async def _start_round_N(bot: Bot, game_id: int, chat_id: int, game_type: str, list_message_id: int, participants: list, round_number: int, is_final_round: bool = False, all_display_participants: list = None):
    """Старт раунда N: обновить закреп (все участники, отсортированы по баллам; финал так же), вызвать первого участника."""
    lang = DEFAULT_LANG
    emoji = GAME_TYPE_EMOJI.get(game_type, "🎲")
    display_list = all_display_participants if all_display_participants is not None else list(participants)
    if round_number > 1:
        # Сортируем по результату ПРЕДЫДУЩЕГО раунда (round_number - 1), чтобы в закрепе первыми шли прошедшие из прошлого раунда
        from db.queries import get_round_totals
        prev_totals = dict(get_round_totals(game_id, round_number - 1))
        display_list = sorted(display_list, key=lambda p: -prev_totals.get(p[0], 0))
    _round_state[game_id] = {
        "participant_ids": participants,
        "round_participants": list(participants),
        "all_participants": display_list,
        "current_index": 0,
        "throw_count": 0,
        "chat_id": chat_id,
        "game_type": game_type,
        "round_number": round_number,
        "list_message_id": list_message_id,
        "is_final_round": is_final_round,
        "turn_id": 0,
    }
    _chat_to_game[chat_id] = game_id
    if all_display_participants is not None:
        totals_by_uid = _build_totals_multiround(game_id, display_list, 0, round_number, playing_participants=participants)
    else:
        totals_by_uid = _build_totals_multiround(game_id, participants, 0, round_number)
    list_text = _format_participants_list(display_list, totals_by_uid, lang)
    if list_message_id is not None:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=list_message_id, text=list_text)
        except Exception as e:
            logger.warning("start_round_N edit list: %s", e)
    uid0, name0 = participants[0]
    name_link = f'<a href="tg://user?id={uid0}">{html.escape(name0)}</a>'
    text = t("round_do_3_throws", lang).format(name=name_link, emoji=emoji)
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        _round_state[game_id]["turn_id"] = 1
        asyncio.create_task(_timeout_for_turn(bot, game_id, 0, 1, round_number))
    except Exception as e:
        logger.warning("start_round_N: %s", e)
        _round_state.pop(game_id, None)
        _chat_to_game.pop(chat_id, None)


async def _start_tiebreak_turn(bot: Bot, game_id: int, state: dict, index: int):
    """Вызвать участника тай-брейка по индексу: «Name, сделайте 1 бросок», таймаут 2 мин."""
    tied = state.get("tiebreak_tied_group") or []
    if index >= len(tied):
        return
    uid, name = tied[index]
    state["tiebreak_wait_uid"] = uid
    state["tiebreak_index"] = index
    state["phase"] = "tiebreak"
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    emoji = GAME_TYPE_EMOJI.get(state["game_type"], "🎲")
    name_link = f'<a href="tg://user?id={uid}">{html.escape(name)}</a>'
    text = t("round_tiebreak_throw", lang).format(name=name_link, emoji=emoji)
    try:
        await bot.send_message(chat_id=chat_id, text=text)
        asyncio.create_task(_timeout_tiebreak_turn(bot, game_id, index))
    except Exception as e:
        logger.warning("start_tiebreak_turn: %s", e)


async def _timeout_tiebreak_turn(bot: Bot, game_id: int, waiting_index: int):
    """Через 2 минуты: если участник тай-брейка не бросил — 0, следующий или пересчёт порядка."""
    await asyncio.sleep(120)
    state = _round_state.get(game_id)
    if not state or state.get("phase") != "tiebreak":
        return
    tied = state.get("tiebreak_tied_group") or []
    if waiting_index >= len(tied) or state.get("tiebreak_index") != waiting_index:
        return
    from db.queries import add_throw
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    skipped_uid, skipped_name = tied[waiting_index]
    add_throw(game_id, skipped_uid, state["round_number"], state.get("tiebreak_next_throw_index", 3), 0)
    state["tiebreak_next_throw_index"] = state.get("tiebreak_next_throw_index", 3) + 1
    state["tiebreak_index"] = waiting_index + 1
    skipped_link = f'<a href="tg://user?id={skipped_uid}">{html.escape(skipped_name)}</a>'
    try:
        await bot.send_message(chat_id=chat_id, text=t("round_participant_skipped", lang).format(name=skipped_link))
    except Exception as e:
        logger.warning("timeout_tiebreak skipped: %s", e)
    if state["tiebreak_index"] >= len(tied):
        state["phase"] = None
        state["tiebreak_cycle_completed"] = True
        from db.queries import get_game
        game = get_game(game_id)
        prize_places = (game or {}).get("prize_places") or 1
        await _do_tiebreak_and_winners(bot, game_id, state, [], prize_places)
        return
    await _start_tiebreak_turn(bot, game_id, state, state["tiebreak_index"])


async def _do_tiebreak_and_winners(bot: Bot, game_id: int, state: dict, passed: list, prize_places: int) -> bool:
    """После финального раунда: при равенстве очков — по 1 броску по очереди (2 мин на бросок), повторять пока не определится порядок. Затем список победителей."""
    from db.queries import get_round_totals, get_round_tiebreak_totals, get_user
    lang = DEFAULT_LANG
    chat_id = state["chat_id"]
    round_number = state["round_number"]
    base_totals = get_round_totals(game_id, round_number)
    by_uid_base = dict(base_totals)
    extra_totals = get_round_tiebreak_totals(game_id, round_number)
    by_uid_extra = dict(extra_totals)
    for uid, _ in state["participant_ids"]:
        by_uid_base.setdefault(uid, 0)
        by_uid_extra.setdefault(uid, 0)
    # Сортировка: сначала базовые очки (3 броска), затем тай-брейк (доп. броски)
    ordered = sorted(state["participant_ids"], key=lambda p: (-by_uid_base.get(p[0], 0), -by_uid_extra.get(p[0], 0)))

    def _place_range_str(start_idx: int, end_exclusive: int) -> str:
        """start_idx/end_exclusive: 0-based позиции в ordered, end_exclusive не включительно."""
        a = start_idx + 1
        b = end_exclusive
        return f"{a} места" if a == b else f"{a}-{b} места"

    async def _announce_places(start_idx: int, end_exclusive: int):
        lines = ["Результаты:", ""]
        for pos in range(start_idx, end_exclusive):
            uid, name = ordered[pos]
            safe = html.escape(name)
            lines.append(f"{pos + 1}. <a href=\"tg://user?id={uid}\">{safe}</a>")
        try:
            await bot.send_message(chat_id=chat_id, text="\n".join(lines))
        except Exception:
            pass

    # Если завершили цикл тай-брейка — либо повторяем его для той же группы, либо объявляем занятые места
    if state.get("tiebreak_cycle_completed") and state.get("tiebreak_target_from") is not None and state.get("tiebreak_target_to") is not None:
        target_from = int(state["tiebreak_target_from"])
        target_to = int(state["tiebreak_target_to"])
        tied_group = state.get("tiebreak_tied_group") or []
        slots_count = max(1, target_to - target_from)
        ranked = sorted(
            tied_group,
            key=lambda p: (-by_uid_extra.get(p[0], 0), p[1].lower()),
        )
        if len(ranked) <= slots_count:
            # мест хватает на всех из группы — спор снят
            await _announce_places(target_from, target_to)
            state.pop("tiebreak_cycle_completed", None)
            state.pop("tiebreak_target_from", None)
            state.pop("tiebreak_target_to", None)
            state.pop("tiebreak_tied_group", None)
            state.pop("tiebreak_wait_uid", None)
            state.pop("tiebreak_ordered", None)
            state.pop("phase", None)
        else:
            boundary_score = by_uid_extra.get(ranked[slots_count - 1][0], 0)
            higher = [p for p in ranked if by_uid_extra.get(p[0], 0) > boundary_score]
            equal = [p for p in ranked if by_uid_extra.get(p[0], 0) == boundary_score]
            remaining_slots = slots_count - len(higher)
            if remaining_slots > 0 and len(equal) > remaining_slots:
                # Повторяем тай-брейк только среди тех, кто реально остаётся в споре на границе мест
                next_target_from = target_from + len(higher)
                next_target_to = next_target_from + remaining_slots
                try:
                    lines = [t("round_tiebreak", lang), f"Для определения {_place_range_str(next_target_from, next_target_to)}", ""]
                    for uid, name in equal:
                        safe_name = html.escape(name)
                        lines.append(f'• <a href="tg://user?id={uid}">{safe_name}</a>')
                    await bot.send_message(chat_id=chat_id, text="\n".join(lines))
                except Exception:
                    pass
                state["tiebreak_tied_group"] = equal
                state["tiebreak_target_from"] = next_target_from
                state["tiebreak_target_to"] = next_target_to
                state["tiebreak_index"] = 0
                state["phase"] = "tiebreak"
                state["tiebreak_cycle_completed"] = False
                state.setdefault("tiebreak_next_throw_index", 3)
                await _start_tiebreak_turn(bot, game_id, state, 0)
                return True
            else:
                # Спор снят: места в текущем диапазоне определены
                await _announce_places(target_from, target_to)
                state.pop("tiebreak_cycle_completed", None)
                state.pop("tiebreak_target_from", None)
                state.pop("tiebreak_target_to", None)
                state.pop("tiebreak_tied_group", None)
                state.pop("tiebreak_wait_uid", None)
                state.pop("tiebreak_ordered", None)
                state.pop("phase", None)
    # Тай-брейк только если ничья влияет на распределение призовых мест.
    # Если спорных зон несколько — начинаем с "нижних" мест (ближе к границе призов).
    while True:
        groups = []
        i = 0
        while i < len(ordered):
            j = i + 1
            key_i = (by_uid_base.get(ordered[i][0], 0), by_uid_extra.get(ordered[i][0], 0))
            while j < len(ordered):
                key_j = (by_uid_base.get(ordered[j][0], 0), by_uid_extra.get(ordered[j][0], 0))
                if key_j != key_i:
                    break
                j += 1
            if j - i > 1:
                groups.append((i, j, ordered[i:j]))
            i = j
        # Берём группу с максимальным start, которая затрагивает призовую зону
        candidates = [(s, e, g) for (s, e, g) in groups if s < prize_places]
        if not candidates:
            break
        tied_from, tied_to, tied = max(candidates, key=lambda x: x[0])
        target_to = min(tied_to, prize_places)
        try:
            lines = [t("round_tiebreak", lang), f"Для определения {_place_range_str(tied_from, target_to)}", ""]
            for uid, name in tied:
                safe_name = html.escape(name)
                lines.append(f'• <a href="tg://user?id={uid}">{safe_name}</a>')
            await bot.send_message(chat_id=chat_id, text="\n".join(lines))
        except Exception:
            pass
        state["tiebreak_ordered"] = ordered
        state["tiebreak_tied_group"] = tied
        state["tiebreak_index"] = 0
        state["tiebreak_target_from"] = tied_from
        state["tiebreak_target_to"] = target_to
        state["tiebreak_cycle_completed"] = False
        state.setdefault("tiebreak_next_throw_index", 3)
        await _start_tiebreak_turn(bot, game_id, state, 0)
        return True
    header_lines = []
    if state.get("is_final_round"):
        header_lines.append(t("round_final_finished", lang))
        header_lines.append("")
    lines = header_lines + [t("round_winners", lang), ""]
    from db.queries import get_prizes
    try:
        prizes = get_prizes(game_id)
        prize_names = {p["place_number"]: (p.get("prize_name") or "").strip() for p in prizes}
    except Exception:
        prizes = []
        prize_names = {}
    for i, (uid, name) in enumerate(ordered[:prize_places], 1):
        safe_name = html.escape(name)
        prize_label = prize_names.get(i)
        if prize_label:
            lines.append(f'{i}. <a href="tg://user?id={uid}">{safe_name}</a> — {html.escape(prize_label)}')
        else:
            lines.append(f'{i}. <a href="tg://user?id={uid}">{safe_name}</a>')
    list_message_id = state.get("list_message_id")
    try:
        await bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        logger.warning("do_tiebreak_and_winners: %s", e)
    from db.queries import get_prizes
    try:
        prizes = get_prizes(game_id)
        by_place = {
            p["place_number"]: (p.get("prize_name") or "").strip()
            + (": " + (p.get("coupon_text") or "").strip() if (p.get("coupon_text") or "").strip() else "")
            for p in prizes
        }
        raw_by_place = {
            p["place_number"]: {
                "prize_name": (p.get("prize_name") or "").strip(),
                "coupon_text": (p.get("coupon_text") or "").strip(),
            }
            for p in prizes
        }
        from db.queries import add_user_prize
        for i in range(min(prize_places, len(ordered))):
            uid = ordered[i][0]
            prize_text = by_place.get(i + 1, "").strip()
            raw = raw_by_place.get(i + 1, {"prize_name": "", "coupon_text": ""})
            try:
                add_user_prize(uid, game_id, i + 1, raw.get("prize_name") or "", raw.get("coupon_text") or "")
            except Exception as e:
                logger.warning("save user prize user=%s game=%s place=%s: %s", uid, game_id, i + 1, e)
            if prize_text:
                try:
                    await bot.send_message(chat_id=uid, text=prize_text)
                except Exception as e:
                    logger.warning("send prize to winner %s: %s", uid, e)
    except Exception as e:
        logger.warning("get_prizes/send prizes: %s", e)
    # Игра завершена: после отправки призов переводим статус в finish
    try:
        from db.queries import update_game
        update_game(game_id, status="finish")
    except Exception as e:
        logger.warning("update game status finish: %s", e)
    if list_message_id is not None:
        try:
            await bot.unpin_chat_message(chat_id=chat_id, message_id=list_message_id)
        except Exception as e:
            logger.warning("unpin message: %s", e)
    _round_state.pop(game_id, None)
    _chat_to_game.pop(chat_id, None)
    return False


async def process_games_start(bot: Bot):
    """Найти игры, время которых наступило, объявить в чате и перевести в active."""
    from db.queries import get_games_to_start
    try:
        games = get_games_to_start()
    except Exception as e:
        logger.exception("get_games_to_start: %s", e)
        return
    for g in games:
        try:
            await announce_game_start(bot, g)
        except Exception as e:
            logger.exception("announce_game_start game %s: %s", g.get("id"), e)


async def reminder_loop(bot: Bot):
    """Каждую минуту: напоминания за 5 минут и объявления о старте игр."""
    first_run = True
    while True:
        try:
            await send_5min_reminders(bot)
            if not first_run:
                await process_games_start(bot)
            first_run = False
        except Exception as e:
            logger.exception("reminder_loop: %s", e)
        await asyncio.sleep(60)


async def on_startup(bot: Bot):
    """Запуск фоновой задачи напоминаний за 5 минут до игры."""
    asyncio.create_task(reminder_loop(bot))


async def main():
    if not BOT_TOKEN:
        raise SystemExit("Задай BOT_TOKEN в .env")
    if not CHAT_ID:
        raise SystemExit("Задай CHAT_ID в .env")

    try:
        from db.init_db import create_tables
        create_tables()
        logger.info("Таблицы БД проверены/созданы.")
    except Exception as e:
        logger.warning("БД недоступна или таблицы не созданы: %s", e)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()
    dp.startup.register(on_startup)
    dp.message.middleware(ChatFilter(CHAT_ID))
    dp.message.middleware(log_game_chat_message)
    dp.callback_query.middleware(ChatFilter(CHAT_ID))
    dp.message.register(cmd_start, F.text == "/start")
    # Сначала броски из меню (dice): и админ, и игроки — бросок должен обрабатывать игра, а не админка
    dp.message.register(on_dice_message, (F.content_type == ContentType.DICE) | F.dice)
    # Текст от админа (создание игры, призы и т.д.)
    dp.message.register(on_admin_message, F.from_user.id == ADMIN_ID)
    # Текст 🎲/🎳/🎯 (эмодзи вручную)
    dp.message.register(on_game_emoji_text, F.text)
    dp.callback_query.register(on_lang_chosen, F.data.startswith("lang:"))
    dp.callback_query.register(on_city_chosen, F.data.startswith("city:"))
    dp.callback_query.register(on_menu, F.data.startswith("menu:"))
    dp.callback_query.register(on_admin, F.data.startswith("admin:"))
    logger.info("Бот запущен. Работает только в чате: %s", CHAT_ID)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
