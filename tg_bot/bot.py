import configparser
from typing import Union

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from pathlib import Path
import urllib.parse
import re

from states import BindStates, ParkrunBind, S95Bind, AdminBroadcast
from keyboards import (
    main_menu, consent_kb, confirm_profile_kb, clubs_kb,
    profile5v_actions_kb, confirm_unlink_club_kb,
    settings_kb, dashboards_root_kb, dashboards_cat_kb,
    profile_root_kb, profile_pr_actions_kb,
    confirm_parkrun_kb,
    profile_c95_actions_kb, confirm_s95_kb,
)
from db import (
    TZ, ensure_user_row, get_profile, set_consent,
    log_action, parse_user_id_from_text, user_exists, find_latest_name_for_user,
    can_change, bind_profile, list_clubs_distinct,
    set_user_club, delete_user_club, get_current_club, unlink_profile,
    set_news_subscribed, mark_first_start,
    get_parkrun_user, ensure_parkrun_user_row,
    bind_parkrun_profile, unlink_parkrun_profile,
    get_5v_runs_count, get_parkrun_runs_count, get_s95_runs_count,
    get_s95_runner, ensure_s95_runner_row, bind_s95_profile, unlink_s95_profile,
    get_s95_by_barcode, is_5v_profile_bound, is_parkrun_profile_bound, is_s95_profile_bound,
    get_last_5v_run, get_last_parkrun_run, get_last_s95_run, get_news_subscribed_tg_ids,
    set_january_notification, get_january_subscribed_tg_ids,
    get_bot_stats, get_last_started_users,
)

# --- Telegram token ---

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

cfg = configparser.ConfigParser()
cfg.read(CONFIG_PATH)

TOKEN = cfg['telegram']['token']

# Список администраторов (для /message и тестовой рассылки)
ADMINS_RAW = cfg['telegram'].get('admins', '').strip()
if ADMINS_RAW:
    ADMIN_IDS = [
        int(x) for x in re.split(r"[,\s]+", ADMINS_RAW) if x
    ]
else:
    ADMIN_IDS = []

def get_broadcast_targets() -> list[int]:
    """
    На первой итерации рассылаем только администраторам — для теста.

    Когда решишь отправлять всем подписчикам из БД,
    ЗАМЕНИ одну строку внутри функции на:
        return get_news_subscribed_tg_ids()
    и больше ничего трогать не нужно.
    """
    #return ADMIN_IDS
    return get_news_subscribed_tg_ids()

bot = Bot(TOKEN)
dp = Dispatcher()

DASHBOARD_URL = "http://run5k.run/d/03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst"
AUTHOR_HANDLE = "@Popov_Dmitry"
AUTHOR_CHANNEL = "https://t.me/popov_way"

CONSENT_TEXT = (
    "Мини-оферта:\n\n"
    "Вы соглашаетесь на обработку персональных данных, включая ваш Telegram-идентификатор "
    "и ссылки на ваши профили участника на сайтах 5verst, s95, parkrun, runpark, для обеспечения работы персонализированных "
    "функций на сайте run5k.run.\n\n"
    "Автор гарантирует, что данные не будут передаваться для использования в коммерческих целях. "
    "Передача данных возможна только представителям указанных сайтов и другим проверенным "
    "проектам, которые занимаются сбором, обработкой и анализом статистики пробежек в вышеуказанных системах"
)

def url_5v_profile(uid: Union[str, int]) -> str:
    return f"https://5verst.ru/userstats/{uid}/"


def url_5v_challenges(uid: Union[str, int]) -> str:
    return (
        "https://run5k.run/d/"
        "3e54a2d8-ef9f-4743-8117-4a2ddb47d6a7/chellendzhi"
        f"?var-name={uid}"
    )


def url_5v_club_dashboard(club: str) -> str:
    encoded = urllib.parse.quote(club)
    return (
        "https://run5k.run/d/"
        "03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst"
        f"?var-Club5={encoded}"
    )

def consent_flag(tg_id: int) -> bool:
    row = get_profile(tg_id)
    return bool(row and row.get('consent_accepted'))

def mk_menu(tg_id: int):
    # всегда вернёт корректное меню с/без кнопки "📝 Согласие"
    return main_menu(consent_accepted=consent_flag(tg_id))

# ===== Helpers =====
async def must_consent(message: Message) -> bool:
    row = get_profile(message.from_user.id)
    if not row or not row.get('consent_accepted'):
        await message.answer(
            "Сначала примите оферту в разделе «⚙️ Настройки» → «Согласие».",
            reply_markup=mk_menu(message.from_user.id),
            disable_web_page_preview=True
        )
        return False
    return True

async def enforce_change_limit(
    field: str,
    tg_id: int,
    action_code: str,
    ctx: Union[Message, CallbackQuery],
    text_prefix: str,
) -> bool:
    """
    Универсальная проверка лимита «раз в 24 часа».

    field       — имя поля в tg_user_profile (last_*_change_at)
    tg_id       — Telegram ID пользователя
    action_code — код для log_action, например "PROFILE_CHANGE_DENIED_LIMIT"
    ctx         — Message или CallbackQuery
    text_prefix — текст перед датой «Следующая попытка после ...»
    """
    can, nt = can_change(field, tg_id)
    if can:
        return True

    log_action(tg_id, action_code, False, {"next_time": nt.isoformat()})

    text = (
        f"{text_prefix}\n"
        f"Следующая попытка после: {nt.astimezone(TZ):%Y-%m-%d %H:%M}."
    )

    if isinstance(ctx, CallbackQuery):
        await ctx.message.answer(text, disable_web_page_preview=True)
        await ctx.answer()
    else:
        await ctx.answer(text, disable_web_page_preview=True)

    return False

async def suggest_parkrun_from_s95(cb: CallbackQuery, s95_id: str):
    """
    После успешной привязки С95 пытаемся найти связанный профиль parkrun
    по s95_barcode и предложить привязку.
    """
    s95_row = get_s95_runner(s95_id) or {}
    barcode = s95_row.get("s95_barcode")
    if not barcode:
        return

    profile = get_profile(cb.from_user.id) or {}
    if profile.get("parkrun_user_id"):
        # parkrun уже привязан — ничего не предлагаем
        return

    pr_row = get_parkrun_user(barcode)
    runs_pr = get_parkrun_runs_count(barcode)
    runs_pr_text = pluralize_ru(runs_pr, ("пробежка", "пробежки", "пробежек"))
    url = f"https://www.parkrun.org.uk/parkrunner/{barcode}/all/"

    name_pr = (
            pr_row.get("actual_name_runner")
            or pr_row.get("name_runner")
            or f"ID {barcode}"
    ) if pr_row else f"ID {barcode}"

    # Последняя пробежка parkrun
    last_run = get_last_parkrun_run(barcode)
    last_part = ""
    if last_run:
        dt = last_run["date_event"]
        try:
            dt_str = dt.strftime("%d.%m.%Y")
        except AttributeError:
            dt_str = str(dt)
        last_part = f"\nПоследняя пробежка: {dt_str} в {last_run['name_point']}"

    text = (
        "<b>Найден связанный профиль parkrun</b>\n\n"
        "Этот ID также используется в системе parkrun:\n"
        f"{name_pr} ({url}) - {runs_pr_text}."
        f"{last_part}\n\n"
        "Привязать этот профиль parkrun к вашему Telegram-аккаунту?"
    )

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=confirm_parkrun_kb(barcode),
        disable_web_page_preview=True,
    )

async def suggest_s95_from_parkrun(cb: CallbackQuery, parkrun_id: str):
    """
    После привязки parkrun пытаемся найти связанный профиль С95
    по s95_barcode и предложить привязку.
    """
    s95_row = get_s95_by_barcode(parkrun_id)
    if not s95_row:
        return

    profile = get_profile(cb.from_user.id) or {}
    if profile.get("s95_user_id"):
        # С95 уже привязан — не трогаем
        return

    s95_id = s95_row.get("s95_id")
    if not s95_id:
        return

    runs_s95 = get_s95_runs_count(s95_id)
    runs_s95_text = pluralize_ru(runs_s95, ("пробежка", "пробежки", "пробежек"))

    url = f"https://s95.ru/athletes/{s95_id}"
    name_s95 = s95_row.get("name_runner") or f"ID {s95_id}"

    # Последняя пробежка С95
    last_run = get_last_s95_run(s95_id)
    last_part = ""
    if last_run:
        dt = last_run["date_event"]
        try:
            dt_str = dt.strftime("%d.%m.%Y")
        except AttributeError:
            dt_str = str(dt)
        last_part = f"\nПоследняя пробежка: {dt_str} в {last_run['name_point']}"

    text = (
        "<b>Найден связанный профиль С95</b>\n\n"
        "Этот ID также используется в системе С95:\n"
        f"{name_s95} ({url}) - {runs_s95_text}."
        f"{last_part}\n\n"
        "Привязать этот профиль С95 к вашему Telegram-аккаунту?"
    )

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=confirm_s95_kb(s95_id),
        disable_web_page_preview=True,
    )

# ===== Handlers =====
@dp.message(CommandStart())
async def on_start(message: Message):
    ensure_user_row(message.from_user.id, message.from_user.username, message.chat.id)
    mark_first_start(message.from_user.id)
    row = get_profile(message.from_user.id)

    has_consent = bool(row and row.get("consent_accepted"))

    uid_5v = row.get("user_id_5v") if row else None
    pr_id = row.get("parkrun_user_id") if row else None
    s95_id = row.get("s95_user_id") if row else None

    has_any_profile = bool(uid_5v or pr_id or s95_id)

    intro = (
        "Привет! Это бот проекта run5k.run со статистикой, дэшбордами и рейтингами о субботних "
        "парковых пробежках.\n\n"
        "<b>В боте вы можете:</b>  \n"
        "🔹Привязать профили в системах парковых пробежек 5 вёрст, С95, parkrun\n"
        "🔹Вступить в клуб 5 вёрст и следить за пробежками группы участников\n"
        "🔹Воспользоваться навигацией по дэшбордам автора\n\n"
        "Новости и обновления: https://t.me/popov_way\n"
        "Контакт автора: @Popov_Dmitry"
    )

    # 1. Согласие не дано — всё как раньше
    if not has_consent:
        tail = (
            "\n\nЧтобы продолжить, откройте «⚙️ Настройки» и примите оферту "
            "для использования функционала бота."
        )

    # 2. Согласие есть, но НИ ОДИН профиль не привязан
    elif has_consent and not has_any_profile:
        tail = (
            "\n\nСогласие уже принято ✅\n\n"
            "Сейчас у вас не привязано ни одного профиля в системах 5 вёрст, parkrun и С95.\n"
            "Рекомендую привязать хотя бы один профиль, чтобы бот смог показывать "
            "персональную статистику.\n\n"
            "Сделать это можно в разделе «👤 Мой профиль» — выберите нужную систему "
            "и нажмите «Привязать профиль»."
        )

    # 3. Согласие есть и хотя бы один профиль привязан — показываем сводную статистику
    else:
        summary = build_profile_summary(row or {})
        tail = "\n\n" + summary

        # Подсказка о подписке на новости — только если ещё не подписан
        if not row.get("news_subscribed"):
            tail += (
                "\n\nЧтобы получать уведомления об обновлениях проекта, "
                "вы можете включить рассылку в разделе «⚙️ Настройки»."
            )

    await message.answer(
        intro + tail,
        parse_mode="HTML",
        reply_markup=main_menu(consent_accepted=has_consent),
        disable_web_page_preview=True,
    )


@dp.message(Command("message"))
async def admin_message_cmd(message: Message, state: FSMContext):
    # Только админы
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Эта команда доступна только администратору бота.")
        return

    await message.answer(
        "Пришлите сообщение, которое нужно разослать пользователям.\n\n"
        "Можно отправить текст, фото, документ и т.п.\n"
        "Ссылки будут отправлены без превью.",
        disable_web_page_preview=True,
    )
    await state.set_state(AdminBroadcast.waiting_message)

@dp.message(Command("stats"))
async def admin_stats_cmd(message: Message):
    # Только админы
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Эта команда доступна только администратору бота.")
        return

    stats = get_bot_stats()

    lines = [
        "<b>Статистика по боту</b>",
        "",
        f"1. Всего чел запускали бота: <b>{stats.get('total_users', 0)}</b>",
        f"2. Новых пользователей за последнюю неделю: <b>{stats.get('new_last_7d', 0)}</b>",
        f"3. Новых пользователей за последний день: <b>{stats.get('new_last_1d', 0)}</b>",
        "",
        f"4. Приняли оферту: <b>{stats.get('consent_accepted', 0)}</b>",
        f"5. Подписались на новости: <b>{stats.get('news_subscribed', 0)}</b>",
        f"6. Подписались на уведомления 1 января: <b>{stats.get('january_notification', 0)}</b>",
        "",
        f"7. Привязали 5 вёрст ID: <b>{stats.get('bound_5v', 0)}</b>",
        f"8. Привязали parkrun ID: <b>{stats.get('bound_parkrun', 0)}</b>",
        f"9. Привязали С95 ID: <b>{stats.get('bound_s95', 0)}</b>",
        "",
        f"10. Привязали все 3 системы: <b>{stats.get('bound_all_three', 0)}</b>",
        f"11. 5 вёрст + С95, без parkrun: <b>{stats.get('bound_5v_s95_only', 0)}</b>",
        f"12. 5 вёрст + parkrun, без С95: <b>{stats.get('bound_5v_parkrun_only', 0)}</b>",
        f"13. parkrun + С95, без 5 вёрст: <b>{stats.get('bound_parkrun_s95_only', 0)}</b>",
    ]

    await message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    # Отдельным сообщением — последние 5 пользователей
    last_users = get_last_started_users(5)
    if not last_users:
        return

    lines2 = [
        "<b>Последние 5 запусков бота</b>",
        "",
    ]

    for row in last_users:
        username = row.get("tg_username")
        chat_id = row.get("tg_chat_id")
        started = row.get("first_start_ts")

        if username:
            user_repr = f"@{username}"
        elif chat_id:
            user_repr = f"<a href=\"tg://user?id={chat_id}\">👤 {chat_id}</a>"
        else:
            user_repr = f"tg_user_id {row.get('tg_user_id')}"

        # Красиво форматируем дату, если это datetime
        try:
            # если есть tz — приводим к TZ, если нет — просто форматируем
            started_local = started.astimezone(TZ) if getattr(started, "tzinfo", None) else started
            started_str = started_local.strftime("%Y-%m-%d %H:%M")
        except Exception:
            started_str = str(started)

        lines2.append(f"{user_repr} — {started_str}")

    await message.answer(
        "\n".join(lines2),
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

@dp.message(AdminBroadcast.waiting_message)
async def admin_message_collect(message: Message, state: FSMContext):
    # На всякий случай ещё раз убеждаемся, что это админ
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("Эта функция доступна только администратору бота.")
        await state.clear()
        return

    # Не даём случайно разослать команду
    if message.text and message.text.startswith("/"):
        await message.answer(
            "Похоже, вы отправили команду.\n"
            "Пришлите, пожалуйста, обычное сообщение (текст, фото и т.п.), "
            "которое нужно разослать.",
            disable_web_page_preview=True,
        )
        return

    # Запоминаем, какое именно сообщение нужно будет разослать
    await state.update_data(
        broadcast_chat_id=message.chat.id,
        broadcast_message_id=message.message_id,
        broadcast_text=message.text if message.text else None,
    )

    # 1) Превью — копируем это сообщение тебе же
    await bot.copy_message(
        chat_id=message.chat.id,
        from_chat_id=message.chat.id,
        message_id=message.message_id,
        # В copy_message нет disable_web_page_preview
    )

    # 2) Спрашиваем подтверждение
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Запустить рассылку",
                    callback_data="broadcast:confirm",
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="broadcast:cancel",
                )
            ],
        ]
    )

    await message.answer(
        "Отправить это сообщение всем получателям?",
        reply_markup=kb,
        disable_web_page_preview=True,
    )

@dp.callback_query(F.data.startswith("broadcast:"))
async def admin_broadcast_cb(cb: CallbackQuery, state: FSMContext):
    # Только админы могут нажимать эти кнопки
    if cb.from_user.id not in ADMIN_IDS:
        await cb.answer("Эта кнопка доступна только администратору.", show_alert=True)
        return

    action = cb.data.split(":")[1]

    if action == "cancel":
        await state.clear()
        await cb.message.answer("Рассылка отменена.")
        await cb.answer()
        return

    if action != "confirm":
        await cb.answer()
        return

    # confirm
    data = await state.get_data()
    src_chat_id = data.get("broadcast_chat_id")
    src_message_id = data.get("broadcast_message_id")
    broadcast_text = data.get("broadcast_text")

    if not src_chat_id or not src_message_id:
        await cb.answer(
            "Не найдено сообщение для рассылки, начните заново командой /message.",
            show_alert=True,
        )
        await state.clear()
        return

    targets = get_broadcast_targets()
    total_targets = len(targets)

    sent = 0
    failed = 0

    for uid in targets:
        try:
            if broadcast_text:
                # Чистый текст — шлём send_message и режем превью ссылок
                await bot.send_message(
                    uid,
                    broadcast_text,
                    disable_web_page_preview=True,
                )
            else:
                # Всё остальное — копируем как есть
                await bot.copy_message(
                    chat_id=uid,
                    from_chat_id=src_chat_id,
                    message_id=src_message_id,
                )
            sent += 1
        except Exception:
            failed += 1

    await state.clear()

    await cb.message.answer(
        "Рассылка завершена.\n"
        f"Всего получателей по списку: {total_targets}\n"
        f"Успешно отправлено: {sent}\n"
        f"Ошибок при отправке: {failed}",
        disable_web_page_preview=True,
    )
    await cb.answer()

def build_profile_summary(row, show_hint: bool = False) -> str:
    uid_5v = row.get("user_id_5v") if row else None
    pr_id = row.get("parkrun_user_id") if row else None
    s95_id = row.get("s95_user_id") if row else None

    # Если вообще ни одной системы не привязано — отдельный текст
    if not (uid_5v or pr_id or s95_id):
        text = (
            "<b>Мой профиль</b>\n\n"
            "У вас пока не привязано ни одного профиля."
        )
        if show_hint:
            text += (
                "\n\nВыберите систему ниже и нажмите «Привязать профиль», "
                "чтобы указать свои учетные записи 5 вёрст, parkrun и С95."
            )
        return text

    parts = ["<b>Мой профиль</b>\n"]

    # --- 5 вёрст ---
    parts.append("\n<i>5 вёрст:</i>")
    if uid_5v:
        # Имя берём из нашей базы 5 вёрст
        name_5v = find_latest_name_for_user(uid_5v) or f"ID {uid_5v}"
        runs_5v = get_5v_runs_count(uid_5v)
        runs_5v_text = pluralize_ru(runs_5v, ("пробежка", "пробежки", "пробежек"))
        club_5v = get_current_club(uid_5v)

        profile_url_5v = f"https://5verst.ru/userstats/{uid_5v}/"
        challenge_url_5v = (
            "https://run5k.run/d/"
            "3e54a2d8-ef9f-4743-8117-4a2ddb47d6a7/chellendzhi"
            f"?var-name={uid_5v}"
        )

        parts.append(
            f"\nПрофиль: <a href=\"{profile_url_5v}\">{name_5v}</a> - {runs_5v_text}"
        )
        parts.append(f"\nЧелленджи: <a href=\"{challenge_url_5v}\">ссылка</a>")

        if club_5v:
            encoded = urllib.parse.quote(club_5v)
            club_url = (
                "https://run5k.run/d/"
                "03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst"
                f"?var-Club5={encoded}"
            )
            parts.append(f"\nКлубы: <a href=\"{club_url}\">{club_5v}</a>")
        else:
            parts.append("\nКлубы: не указан")
    else:
        parts.append("\nПрофиль: не привязан")
        parts.append("\nЧелленджи: не привязан")
        parts.append("\nКлубы: не привязан")

    # --- parkrun ---
    parts.append("\n\n<i>parkrun:</i>")
    if pr_id:
        pr_user = get_parkrun_user(pr_id) or {}
        name_pr = (
            pr_user.get("actual_name_runner")
            or pr_user.get("name_runner")
            or f"ID {pr_id}"
        )
        runs_pr = get_parkrun_runs_count(pr_id)
        runs_pr_text = pluralize_ru(runs_pr, ("пробежка", "пробежки", "пробежек"))
        pr_url = f"https://www.parkrun.org.uk/parkrunner/{pr_id}/all/"

        parts.append(
            f"\nПрофиль: <a href=\"{pr_url}\">{name_pr}</a> - {runs_pr_text}"
        )
    else:
        parts.append("\nПрофиль: не привязан")

    # --- С95 ---
    parts.append("\n\n<i>С95:</i>")
    if s95_id:
        s95_row = get_s95_runner(s95_id) or {}
        name_s95 = s95_row.get("name_runner") or f"ID {s95_id}"
        runs_s95 = get_s95_runs_count(s95_id)
        runs_s95_text = pluralize_ru(runs_s95, ("пробежка", "пробежки", "пробежек"))
        s95_url = f"https://s95.ru/athletes/{s95_id}"

        parts.append(
            f"\nПрофиль: <a href=\"{s95_url}\">{name_s95}</a> - {runs_s95_text}"
        )
    else:
        parts.append("\nПрофиль: не привязан")

    # Подсказку внизу показываем только там, где она уместна
    if show_hint:
        parts.append(
            "\n\nВыберите учетную запись ниже, чтобы посмотреть детали или изменить привязку."
        )

    return "".join(parts)

@dp.callback_query(F.data == "profile:back")
async def profile_back(cb: CallbackQuery):
    # Возвращаемся в "Мой профиль" как при нажатии кнопки в меню
    row = get_profile(cb.from_user.id)
    text = build_profile_summary(row or {}, show_hint=True)

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=profile_root_kb(),
        disable_web_page_preview=True,
    )
    await cb.answer()

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: Message):
    # Проверяем, что есть согласие
    if not await must_consent(message):
        return

    row = get_profile(message.from_user.id)
    text = build_profile_summary(row or {}, show_hint=True)

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=profile_root_kb(),
        disable_web_page_preview=True,
    )

@dp.message(F.text == "⚙️ Настройки")
async def settings(message: Message):
    ensure_user_row(message.from_user.id, message.from_user.username, message.chat.id)
    row = get_profile(message.from_user.id)
    consent = bool(row and row.get('consent_accepted'))
    news = bool(row and row.get('news_subscribed'))
    january = bool(row and row.get('january_notification'))

    text = (
        "<b>Настройки</b>\n\n"
        "Здесь можно управлять согласием на обработку данных и подпиской на новости проекта."
    )
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=settings_kb(consent, news, january),
        disable_web_page_preview=True
    )

@dp.message(F.text == "📝 Согласие")
async def consent(message: Message):
    ensure_user_row(message.from_user.id, message.from_user.username, message.chat.id)
    row = get_profile(message.from_user.id)

    if row and row.get('consent_accepted'):
        await message.answer(
            "Согласие уже принято ✅",
            reply_markup=mk_menu(message.from_user.id),
            disable_web_page_preview=True
        )
        return

    await message.answer(CONSENT_TEXT, disable_web_page_preview=True)
    await message.answer(
        "Принять условия?",
        reply_markup=consent_kb(),
        disable_web_page_preview=True
    )

@dp.callback_query(F.data.startswith("consent:"))
async def consent_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]
    row = get_profile(cb.from_user.id) or {}
    news = bool(row.get("news_subscribed"))
    january = bool(row.get("january_notification"))


    # 1. Принятие согласия
    if action == "accept":
        set_consent(cb.from_user.id, True)
        log_action(cb.from_user.id, "CONSENT_ACCEPTED", True, {})

        row = get_profile(cb.from_user.id) or {}
        news = bool(row.get("news_subscribed"))
        january = bool(row.get("january_notification"))

        text = "Спасибо! Согласие принято ✅"

        if not news:
            text += (
                "\n\nПодпишитесь на рассылку с новостями проекта, чтобы не пропустить "
                "важные обновления, новые дэшборды и появление новых функций.\n"
                "\nИли можете сразу перейти к привязке профиля 5 вёрст по кнопке 🪪 в меню."
            )

        await cb.message.answer(
            text,
            reply_markup=settings_kb(True, news, january),
            disable_web_page_preview=True
        )

        await cb.message.answer(
            "Главное меню обновлено.",
            reply_markup=main_menu(consent_accepted=True),
            disable_web_page_preview=True
        )

        await cb.answer()
        return

    # 2. Отклонение согласия
    elif action == "decline":
        set_consent(cb.from_user.id, False)
        log_action(cb.from_user.id, "CONSENT_DECLINED", True, {})

        await cb.message.answer(
            "Без согласия продолжение невозможно. Вернитесь, когда будете готовы.",
            reply_markup=main_menu(consent_accepted=False),
            disable_web_page_preview=True
        )

        await cb.answer()
        return

    # 3. Отзыв согласия
    elif action == "revoke":
        set_consent(cb.from_user.id, False)
        log_action(cb.from_user.id, "CONSENT_REVOKED", True, {})

        # 1) Показываем настройки с обновлёнными флагами
        await cb.message.answer(
            "Согласие отозвано. Функционал бота будет ограничен до повторного принятия оферты.",
            reply_markup=settings_kb(
                consent_accepted=False,
                news_subscribed=news,
                january_subscribed=january,
            ),
            disable_web_page_preview=True,
        )

        # 2) Обновляем главное меню (кнопка профиля должна исчезнуть)
        await cb.message.answer(
            "Главное меню обновлено.",
            reply_markup=main_menu(consent_accepted=False),
            disable_web_page_preview=True,
        )

        await cb.answer()
        return


    # 4. Оставить согласие без изменений
    elif action == "keep":
        await cb.message.answer(
            "Оставляем согласие без изменений.",
            reply_markup=settings_kb(
                consent_accepted=True,
                news_subscribed=news,
                january_subscribed=january,
            ),
            disable_web_page_preview=True,
        )

        await cb.answer()
        return


@dp.callback_query(F.data == "profile:pr")
async def profile_pr(cb: CallbackQuery):
    row = get_profile(cb.from_user.id)
    pr_id = row.get("parkrun_user_id") if row else None
    has_parkrun = bool(pr_id)

    text = "<b>Учетная запись parkrun</b>\n\n"

    if has_parkrun:
        url = f"https://www.parkrun.org.uk/parkrunner/{pr_id}/all/"
        text += (
            "Сейчас к вашему Telegram-аккаунту привязан профиль parkrun:\n"
            f"<a href=\"{url}\">ID {pr_id}</a>\n\n"
            "Вы можете изменить или отвязать его не чаще одного раза в 24 часа."
        )
    else:
        text += (
            "У вас пока не привязан parkrun ID.\n\n"
            "Вы можете указать его, чтобы в будущем использовать статистику с сайта parkrun.\n"
            "Для начала привязки нажмите кнопку «Привязать профиль» ниже."
        )

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=profile_pr_actions_kb(has_parkrun),
        disable_web_page_preview=True,
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("news:"))
async def news_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]

    if action == "subscribe":
        set_news_subscribed(cb.from_user.id, True)
        log_action(cb.from_user.id, "NEWS_SUBSCRIBE", True, {})

        row = get_profile(cb.from_user.id) or {}
        consent = bool(row.get("consent_accepted"))
        january = bool(row.get("january_notification"))

        await cb.message.answer(
            "Вы подписались на рассылку новостей проекта ✅",
            reply_markup=settings_kb(consent, True, january),
            disable_web_page_preview=True,
        )

        await cb.answer()
        return

    elif action == "unsubscribe":
        set_news_subscribed(cb.from_user.id, False)
        log_action(cb.from_user.id, "NEWS_UNSUBSCRIBE", True, {})

        row = get_profile(cb.from_user.id) or {}
        consent = bool(row.get("consent_accepted"))
        january = bool(row.get("january_notification"))

        await cb.message.answer(
            "Вы отписались от рассылки новостей.",
            reply_markup=settings_kb(consent, False, january),

            disable_web_page_preview=True,
        )

    elif action == "cancel":
        await cb.message.answer(
            "Действие с рассылкой отменено.",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True,
        )

    await cb.answer()

@dp.callback_query(F.data.startswith("january:"))
async def january_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]
    row = get_profile(cb.from_user.id)

    if action == "subscribe":
        set_january_notification(cb.from_user.id, True)
        await cb.message.answer(
            "Вы подписались на уведомления о стартах 1 января.",
            reply_markup=settings_kb(
                consent_accepted=row.get("consent_accepted"),
                news_subscribed=row.get("news_subscribed"),
                january_subscribed=True
            )
        )
    elif action == "unsubscribe":
        set_january_notification(cb.from_user.id, False)
        await cb.message.answer(
            "Вы отписались от уведомлений 1 января.",
            reply_markup=settings_kb(
                consent_accepted=row.get("consent_accepted"),
                news_subscribed=row.get("news_subscribed"),
                january_subscribed=False
            )
        )
    else:
        await cb.message.answer(
            "Действие отменено.",
            reply_markup=mk_menu(cb.from_user.id),
        )

    await cb.answer()

@dp.callback_query(F.data.startswith("settings:"))
async def settings_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]
    row = get_profile(cb.from_user.id)
    consent = bool(row and row.get('consent_accepted'))
    news = bool(row and row.get('news_subscribed'))
    january = bool(row and row.get('january_notification'))


    if action == "close":
        await cb.message.edit_reply_markup(reply_markup=None)
        await cb.message.answer(
            "Возвращаю в главное меню.",
            reply_markup=main_menu(consent_accepted=consent),
            disable_web_page_preview=True,
        )
        await cb.answer()
        return

    if action == "consent":
        if not consent:
            await cb.message.answer(CONSENT_TEXT, disable_web_page_preview=True)
            await cb.message.answer("Принять условия?", reply_markup=consent_kb(), disable_web_page_preview=True)
        else:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Отозвать согласие", callback_data="consent:revoke")],
                    [InlineKeyboardButton(text="Отмена", callback_data="consent:keep")],
                ]
            )
            await cb.message.answer("Сейчас согласие уже дано. Хотите его отозвать?", reply_markup=kb, disable_web_page_preview=True)
        await cb.answer()
        return

    if action == "news":
        if not news:
            text = (
                "Вы можете предоставить согласие на рассылку новостей по проекту, "
                "спамить не будем — только самое важное и интересное 😊\n\n"
                "Также новости с комментариями автора доступны на его личном телеграм-канале t.me/popov_way"
            )
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Подписаться на обновления", callback_data="news:subscribe")],
                    [InlineKeyboardButton(text="Отмена", callback_data="news:cancel")],
                ]
            )
            await cb.message.answer(text, reply_markup=kb, disable_web_page_preview=True)
        else:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Отписаться от рассылки", callback_data="news:unsubscribe")],
                    [InlineKeyboardButton(text="Отмена", callback_data="news:cancel")],
                ]
            )
            await cb.message.answer("Вы уже подписаны на рассылку. Хотите отписаться?", reply_markup=kb, disable_web_page_preview=True)
        await cb.answer()
        return

    if action == "january":
        if not row.get("january_notification"):
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Подписаться", callback_data="january:subscribe")],
                    [InlineKeyboardButton(text="Отмена", callback_data="january:cancel")],
                ]
            )
            await cb.message.answer(
                "Хотите получать уведомления об изменении времени стартов 1 января?",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        else:
            kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="Отписаться", callback_data="january:unsubscribe")],
                    [InlineKeyboardButton(text="Отмена", callback_data="january:cancel")],
                ]
            )
            await cb.message.answer(
                "Вы уже подписаны на уведомления. Хотите отписаться?",
                reply_markup=kb,
                disable_web_page_preview=True,
            )
        await cb.answer()
        return


@dp.callback_query(F.data == "p5v:club:no_profile")
async def no_profile_club(cb: CallbackQuery):
    await cb.answer("Сначала привяжите профиль 5 вёрст.", show_alert=True)

@dp.message(S95Bind.waiting_input)
async def s95_receive_input(message: Message, state: FSMContext):
    raw = message.text.strip()
    upper = raw.upper()

    # 1. Проверяем, не ссылка ли это сразу
    link_match = re.search(r"(?:https?://)?s95\.ru/athletes/(\d+)", raw)
    if link_match:
        # Пользователь уже прислал ссылку на профиль
        s95_id = link_match.group(1)  # только цифры из URL
        is_link = True
    else:
        # 2. Иначе достаём любые цифры (ID или QR)
        m = re.search(r"(\d+)", upper)
        if not m:
            await message.answer(
                "<b>Учетная запись С95</b>\n\n"
                "Не смог распознать данные.\n\n"
                "Поддерживаемые варианты:\n"
                "• Ссылка на профиль: "
                "<code>https://s95.ru/athletes/5207</code>\n"
                "• Короткий ID: <code>5207</code>\n"
                "• QR-код: <code>7035519</code> или <code>A7035519</code>.",
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            return
        s95_id = m.group(1)  # только цифры
        is_link = False

    # 3. Ищем в локальной базе по s95_id и s95_barcode (всегда только цифры)
    row = get_s95_runner(s95_id)

    if row:
        # Если нашли — используем canon s95_id из БД (то, что и будем писать в tg_user_profile)
        canonical_id = row.get("s95_id") or s95_id
        name = row.get("name_runner") or f"ID {canonical_id}"
        url = f"https://s95.ru/athletes/{canonical_id}"

        runs_s95 = get_s95_runs_count(canonical_id)
        runs_s95_text = pluralize_ru(runs_s95, ("пробежка", "пробежки", "пробежек"))
        last_run = get_last_s95_run(canonical_id)
        last_part = ""
        if last_run:
            dt = last_run["date_event"]
            try:
                dt_str = dt.strftime("%d.%m.%Y")
            except AttributeError:
                dt_str = str(dt)
            last_part = f'\nПоследняя пробежка: {dt_str} в {last_run["name_point"]}'

        text = (
            "<b>Учетная запись С95</b>\n\n"
            f"Найден участник: <b>{name}</b> - {runs_s95_text}{last_part}\n"
            f"Ссылка на профиль: {url}\n\n"
            "Это вы? Привязать эту учетную запись к вашему Telegram-профилю?"
        )

        await message.answer(
            text,
            parse_mode="HTML",
            reply_markup=confirm_s95_kb(canonical_id),
            disable_web_page_preview=True,
        )
        await state.clear()
        return

    # 4. В базе ничего не нашли
    if not is_link:
        # Пришёл ID / QR, но в таблице нет совпадений — просим ссылку
        await message.answer(
            "<b>Учетная запись С95</b>\n\n"
            "В локальной базе пока нет участника с таким ID / QR-кодом.\n\n"
            "Пожалуйста, пришлите ссылку на ваш профиль на сайте в формате:\n"
            "<code>https://s95.ru/athletes/5207</code>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await state.set_state(S95Bind.waiting_link)
        return

    # Уже была ссылка, но в БД нет записи — сразу предлагаем подтвердить по ссылке
    url = f"https://s95.ru/athletes/{s95_id}"
    text = (
        "<b>Учетная запись С95</b>\n\n"
        "В локальной базе пока нет участника с таким ID.\n\n"
        "Пожалуйста, откройте ссылку:\n"
        f"{url}\n\n"
        "Если страница профиля открывается и это ваша учетная запись — "
        "нажмите «Да, привязать», и мы добавим заявку на загрузку данных."
    )

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=confirm_s95_kb(s95_id),
        disable_web_page_preview=True,
    )
    await state.clear()


@dp.message(S95Bind.waiting_link)
async def s95_receive_link(message: Message, state: FSMContext):
    raw = message.text.strip()
    link_match = re.search(r"https?://s95\.ru/athletes/(\d+)", raw)
    if not link_match:
        await message.answer(
            "Ожидаю ссылку вида <code>https://s95.ru/athletes/5207</code>.\n"
            "Пожалуйста, пришлите корректную ссылку из вашего профиля.",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        return

    s95_id = link_match.group(1)
    runs_s95 = get_s95_runs_count(s95_id)
    runs_s95_text = pluralize_ru(runs_s95, ("пробежка", "пробежки", "пробежек"))

    url = f"https://s95.ru/athletes/{s95_id}"

    text = (
        "<b>Учетная запись С95</b>\n\n"
        "Проверьте, пожалуйста, ссылку:\n"
        f"{url}\n\n"
        f"По этому ID в протоколах найдено: {runs_s95_text}.\n\n"
        "Это ваш профиль? Если да — нажмите «Да, привязать», "
        "и мы добавим заявку на загрузку данных."
    )

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=confirm_s95_kb(s95_id),
        disable_web_page_preview=True,
    )
    await state.clear()

@dp.message(ParkrunBind.waiting_id)
async def pr_receive_id(message: Message, state: FSMContext):
    raw = message.text.strip().upper()

    # Достаем цифры из формата "7035519" или "A7035519"
    m = re.search(r'(\d+)', raw)
    if not m:
        await message.answer(
            "Не смог распознать ID.\n\n"
            "Поддерживаемые форматы:\n"
            "• 7035519\n"
            "• A7035519\n\n"
            "Проверьте, пожалуйста, данные, которые вы присылаете."
        )
        return

    user_id = m.group(1)
    url = f"https://www.parkrun.org.uk/parkrunner/{user_id}/all/"

    runs_pr = get_parkrun_runs_count(user_id)
    runs_pr_text = pluralize_ru(runs_pr, ("пробежка", "пробежки", "пробежек"))

    last_run = get_last_parkrun_run(user_id)
    last_part = ""
    if last_run:
        dt = last_run["date_event"]
        try:
            dt_str = dt.strftime("%d.%m.%Y")
        except AttributeError:
            dt_str = str(dt)
        last_part = f'\nПоследняя пробежка: {dt_str} в {last_run["name_point"]}'

    row = get_parkrun_user(user_id)

    if row:
        display_name = row.get("actual_name_runner") or row.get("name_runner") or f"ID {user_id}"
        text = (
            "<b>Учетная запись parkrun</b>\n\n"
            f"Найден участник: <b>{display_name}</b> - {runs_pr_text}{last_part}\n"
            f"Ссылка на профиль: {url}\n\n"
            "Это вы? Привязать эту учетную запись к вашему Telegram-профилю?"
        )
    else:
        text = (
            "<b>Учетная запись parkrun</b>\n\n"
            "В локальной базе пока нет участника с таким ID.\n\n"
            "Пожалуйста, откройте ссылку:\n"
            f"{url}\n\n"
            "Если страница профиля открывается и это ваша учетная запись — "
            "нажмите «Да, привязать», и мы добавим заявку на загрузку данных."
        )

    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=confirm_parkrun_kb(user_id),
        disable_web_page_preview=True,
    )

    # Состояние больше не нужно — дальше всё по callback
    await state.clear()

@dp.callback_query(
    F.data.startswith("c95:confirm") | F.data.startswith("c95:cancel")
)
async def c95_bind_cb(cb: CallbackQuery):
    parts = cb.data.split(":")
    action = parts[1]

    if action == "cancel":
        await cb.message.answer(
            "Привязка учетной записи С95 отменена.",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True,
        )
        await cb.answer()
        return

    if action == "confirm":
        if len(parts) < 3:
            await cb.answer("Ошибка: не передан ID.", show_alert=True)
            return

        s95_id = re.sub(r"\D", "", parts[2])  # на всякий случай ещё раз только цифры

        runs_s95 = get_s95_runs_count(s95_id)
        runs_s95_text = pluralize_ru(runs_s95, ("пробежка", "пробежки", "пробежек"))

        s95_row = get_s95_runner(s95_id) or {}
        name_s95 = s95_row.get("name_runner") or f"ID {s95_id}"

        # Гарантируем строку в s95_runners по s95_id
        ensure_s95_runner_row(s95_id)

        ok, msg = bind_s95_profile(cb.from_user.id, s95_id)
        if not ok:
            await cb.message.answer(f"Ошибка: {msg}")
            await cb.answer()
            return

        log_action(cb.from_user.id, "S95_PROFILE_BOUND", True, {"s95_user_id": s95_id})

        url = f"https://s95.ru/athletes/{s95_id}"
        await cb.message.answer(
            "Учетная запись С95 привязана ✅\n\n"
            f"<b>Профиль С95:</b> <a href=\"{url}\">{name_s95}</a> - {runs_s95_text}\n\n"
            "Если возникнут вопросы по загрузке статистики — напишите автору бота.",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )

        await suggest_parkrun_from_s95(cb, s95_id)

        await cb.answer()


@dp.callback_query(
    F.data.startswith("pr:confirm") | F.data.startswith("pr:cancel")
)
async def parkrun_bind_cb(cb: CallbackQuery):

    parts = cb.data.split(":")
    action = parts[1]

    if action == "cancel":
        await cb.message.answer(
            "Привязка учетной записи parkrun отменена.",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True,
        )
        await cb.answer()
        return

    if action == "confirm":
        if len(parts) < 3:
            await cb.answer("Ошибка: не передан ID.", show_alert=True)
            return

        try:
            user_id = parts[2]
        except ValueError:
            await cb.answer("Ошибка формата ID.", show_alert=True)
            return

            # Проверка: профиль parkrun уже привязан к другой УЗ TG?
            if is_parkrun_profile_bound(user_id, cb.from_user.id):
                url = f"https://www.parkrun.org.uk/parkrunner/{user_id}/all/"
                await cb.message.answer(
                    "Этот профиль parkrun уже привязан к другой учетной записи Telegram.\n\n"
                    "Проверьте, пожалуйста, профиль по ссылке:\n"
                    f"{url}\n\n"
                    f"Если этот профиль действительно принадлежит вам, "
                    f"напишите автору {AUTHOR_HANDLE} для выяснения обстоятельств.",
                    disable_web_page_preview=True,
                )
                await cb.answer()
                return

        runs_pr = get_parkrun_runs_count(user_id)
        runs_pr_text = pluralize_ru(runs_pr, ("пробежка", "пробежки", "пробежек"))

        # Смотрим, есть ли участник в локальной базе
        pr_user = get_parkrun_user(user_id)

        # Убедимся, что строка в parkrun_users есть (создаём-заглушку при необходимости)
        ensure_parkrun_user_row(user_id)

        # Привязываем к профилю
        bind_parkrun_profile(cb.from_user.id, user_id)
        log_action(cb.from_user.id, "PARKRUN_PROFILE_BOUND", True, {"parkrun_user_id": user_id})

        url = f"https://www.parkrun.org.uk/parkrunner/{user_id}/all/"

        if pr_user:
            # Участник уже есть в локальной базе — показываем обычный текст со статистикой
            name_pr = (
                    pr_user.get("actual_name_runner")
                    or pr_user.get("name_runner")
                    or f"ID {user_id}"
            )
            text = (
                "Учетная запись parkrun привязана ✅\n\n"
                f"<b>Профиль parkrun:</b> <a href=\"{url}\">{name_pr}</a> - {runs_pr_text}\n\n"
                "Если возникнут вопросы по загрузке статистики — напишите автору @Popov_Dmitry."
            )
        else:
            # В локальной базе ещё нет данных — информируем про будущую загрузку
            name_pr = f"ID {user_id}"
            text = (
                "Учетная запись parkrun привязана ✅\n\n"
                f"Профиль parkrun <a href=\"{url}\">{name_pr}</a> успешно привязан.\n\n"
                "Ранее этого участника не было в локальной базе, "
                "данные о ваших пробежках будут загружены в базу данных в ближайшее время.\n\n"
                "Если возникнут вопросы по загрузке статистики — напишите автору @Popov_Dmitry."
            )

        await cb.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True,
        )

        await suggest_s95_from_parkrun(cb, user_id)
        await cb.answer()

@dp.message(F.text == "📊 Дэшборды")
async def description(message: Message):
    text = (
        "<b>Описание</b>\n"
        "Это бот проекта run5k.run со статистикой, дэшбордами и рейтингами о субботних парковых пробежках.\n\n"
        "Ниже можно выбрать тематику дэшбордов.\n\n"
        f"Новости и обновления: <a href=\"{AUTHOR_CHANNEL}\">{AUTHOR_CHANNEL}</a>\n"
        f"Контакт автора: {AUTHOR_HANDLE}\n"
    )
    await message.answer(
        text,
        parse_mode="HTML",
        reply_markup=dashboards_root_kb(),
        disable_web_page_preview=True
    )

@dp.callback_query(F.data == "dash:root")
async def dash_root_cb(cb: CallbackQuery):
    await cb.message.edit_reply_markup(reply_markup=dashboards_root_kb())
    await cb.answer()


@dp.callback_query(F.data.startswith("dash:cat:"))
async def dash_cat_cb(cb: CallbackQuery):
    category = cb.data.split(":")[2]
    await cb.message.edit_reply_markup(reply_markup=dashboards_cat_kb(category))
    await cb.answer()

@dp.message(BindStates.waiting_profile)
async def bind_receive(message: Message, state: FSMContext):
    uid = parse_user_id_from_text(message.text)
    if uid is None:
        await message.answer(
            "Не распознал ID. Пришлите ссылку https://5verst.ru/userstats/<id>/ или сам <id>.",
            disable_web_page_preview=True,
        )
        return

    if not user_exists(uid):
        log_action(message.from_user.id, "PROFILE_NOT_FOUND", False, {"user_id_5v": uid})
        await message.answer(
            "Такой пользователь не найден в базе финишей/волонтёрств. Проверьте ID.",
            disable_web_page_preview=True,
        )
        return

    name = find_latest_name_for_user(uid) or "имя не найдено"
    runs_5v = get_5v_runs_count(uid)
    runs_5v_text = pluralize_ru(runs_5v, ("пробежка", "пробежки", "пробежек"))

    last_run = get_last_5v_run(uid)
    last_part = ""
    if last_run:
        dt = last_run["date_event"]
        try:
            dt_str = dt.strftime("%d.%m.%Y")
        except AttributeError:
            dt_str = str(dt)
        last_part = f'\nПоследняя пробежка: {dt_str} в {last_run["name_point"]}'

    await message.answer(
        f"Нашли профиль: *{name}* (ID {uid}), {runs_5v_text}.{last_part}\n"
        f"Привязать этот профиль 5 вёрст?",
        parse_mode="Markdown",
        reply_markup=confirm_profile_kb(uid),
        disable_web_page_preview=True,
    )
    await state.clear()

@dp.callback_query(F.data.startswith("bind:"))
async def bind_confirm(cb: CallbackQuery):
    parts = cb.data.split(":")
    action = parts[1]  # 'confirm' или 'cancel'

    # Отмена привязки
    if action == "cancel":
        await cb.message.answer(
            "Привязка отменена.",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True
        )
        await cb.answer()
        return

    # Подтверждение привязки
    if action == "confirm":
        if len(parts) < 3:
            await cb.answer("Ошибка: не передан UID.", show_alert=True)
            return

        uid = parts[2]

        # Проверка: профиль 5 вёрст уже привязан к другой УЗ TG?
        if is_5v_profile_bound(uid, cb.from_user.id):
            profile_url = url_5v_profile(uid)
            await cb.message.answer(
                "Этот профиль 5 вёрст уже привязан к другой учетной записи Telegram.\n\n"
                "Проверьте, пожалуйста, профиль по ссылке:\n"
                f"{profile_url}\n\n"
                f"Если этот профиль действительно принадлежит вам, "
                f"напишите автору {AUTHOR_HANDLE} для выяснения обстоятельств.",
                disable_web_page_preview=True,
            )
            await cb.answer()
            return

        profile_url = url_5v_profile(uid)
        ok, msg = bind_profile(cb.from_user.id, uid, profile_url)
        if not ok:
            ...
            return

        log_action(cb.from_user.id, "PROFILE_BOUND", True, {"user_id_5v": uid})

        # Загружаем данные после привязки
        row = get_profile(cb.from_user.id)
        name_txt = find_latest_name_for_user(uid) or uid
        club = get_current_club(uid)
        has_club = bool(club)

        runs_5v = get_5v_runs_count(uid)
        runs_5v_text = pluralize_ru(runs_5v, ("пробежка", "пробежки", "пробежек"))

        challenge_url = url_5v_challenges(uid)

        text = (
            "<b>Профиль 5 вёрст</b>\n\n"
            "Профиль привязан ✅\n"
            f"<b>Профиль:</b> <a href=\"{profile_url}\">{name_txt}</a> - {runs_5v_text}\n"
            f"<b>Челленджи:</b> <a href=\"{challenge_url}\">перейти</a>\n"
        )

        if has_club:
            encoded = urllib.parse.quote(club)
            club_url = (
                "https://run5k.run/d/"
                "03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst"
                f"?var-Club5={encoded}"
            )
            text += (
                f"<b>Клуб:</b> <a href=\"{club_url}\">{club}</a>\n"
                "\nВы уже состоите в клубе."
            )
        else:
            text += (
                "<b>Клуб:</b> не выбран\n\n"
                "Вы можете вступить в клуб, чтобы отображаться на дэшборде:\n"
                "<a href=\"https://run5k.run/d/"
                "03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst\">"
                "Клубы 5 вёрст</a>\n"
                "Для этого нажмите 'Привязать / изменить клуб'"
            )

        await cb.message.answer(
            text,
            parse_mode="HTML",
            reply_markup=profile5v_actions_kb(True, has_club),
            disable_web_page_preview=True
        )
        await cb.answer()
        return

@dp.callback_query(F.data.startswith("clubs:action:"))
async def clubs_action(cb: CallbackQuery):
    action = cb.data.split(":")[2]

    # Отмена
    if action == "cancel":
        await cb.message.answer("Действие отменено.", reply_markup=mk_menu(cb.from_user.id))
        await cb.answer()
        return

    # Привязать / изменить клуб
    if action == "set":
        ok = await enforce_change_limit(
            field="last_club_change_at",
            tg_id=cb.from_user.id,
            action_code="CLUB_CHANGE_DENIED_LIMIT",
            ctx=cb,
            text_prefix="Сменять клуб можно раз в 24 часа.",
        )
        if not ok:
            return

        clubs = list_clubs_distinct()
        if not clubs:
            await cb.message.answer("Список клубов пуст. Напишите автору @Popov_Dmitry.")
            await cb.answer()
            return

        await cb.message.answer(
            "Выберите клуб из списка ниже.\n"
            "Если вашего клуба нет — напишите автору @Popov_Dmitry.\n"
            f"Где использовать клубы: {DASHBOARD_URL}",
            reply_markup=clubs_kb(clubs, page=0),
            disable_web_page_preview=True
        )
        await cb.answer()
        return

    # Отвязать клуб — спрашиваем подтверждение
    if action == "unlink":
        uid = get_profile(cb.from_user.id).get('user_id_5v')
        current = get_current_club(uid)
        if not current:
            await cb.message.answer("У вас сейчас не выбран клуб.", reply_markup=mk_menu(cb.from_user.id))
            await cb.answer()
            return
        await cb.message.answer(
            f"Вы точно хотите отвязать клуб <b>{current}</b> из своего профиля?",
            parse_mode="HTML",
            reply_markup=confirm_unlink_club_kb()
        )
        await cb.answer()

@dp.callback_query(F.data == "club:confirm_unlink")
async def club_confirm_unlink(cb: CallbackQuery):
    can, nt = can_change('last_club_change_at', cb.from_user.id)
    if not can:
        log_action(cb.from_user.id, "CLUB_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
        await cb.answer("Лимит 24 часа.", show_alert=True)
        return

    uid = get_profile(cb.from_user.id).get('user_id_5v')
    ok = delete_user_club(cb.from_user.id, uid)
    if ok:
        log_action(cb.from_user.id, "CLUB_UNLINKED", True, {"user_id_5v": uid})
        await cb.message.answer("Клуб отвязан.", reply_markup=mk_menu(cb.from_user.id))
    else:
        log_action(cb.from_user.id, "CLUB_UNLINK_NOOP", False, {"user_id_5v": uid})
        await cb.message.answer("Клуб не был привязан.", reply_markup=mk_menu(cb.from_user.id))
    await cb.answer()


@dp.callback_query(F.data == "club:cancel_unlink")
async def club_cancel_unlink(cb: CallbackQuery):
    await cb.message.answer("Отмена отвязки клуба.", reply_markup=mk_menu(cb.from_user.id))
    await cb.answer()

@dp.callback_query(F.data == "profile:5v")
async def p5v_root_cb(cb: CallbackQuery):
    # проверяем согласие по tg_user_id
    row = get_profile(cb.from_user.id)
    if not row or not row.get("consent_accepted"):
        await cb.message.answer(
            "Сначала примите оферту в разделе «⚙️ Настройки» → «Согласие».",
            reply_markup=mk_menu(cb.from_user.id),
            disable_web_page_preview=True,
        )
        await cb.answer()
        return

    uid = row.get("user_id_5v") if row else None
    has_profile = bool(uid)
    club = get_current_club(uid) if uid else None
    has_club = bool(club)

    text = "<b>Профиль 5 вёрст</b>\n\n"

    if has_profile:
        profile_url = url_5v_profile(uid)
        challenge_url = url_5v_challenges(uid)
        name_txt = find_latest_name_for_user(uid) or f"ID {uid}"

        text += f"<b>Профиль:</b> <a href=\"{profile_url}\">{name_txt}</a>\n"
        text += f"<b>Челленджи:</b> <a href=\"{challenge_url}\">перейти</a>\n"

        if has_club:
            club_url = url_5v_club_dashboard(club)
            text += f"<b>Клуб:</b> <a href=\"{club_url}\">{club}</a>\n"
        else:
            text += "<b>Клуб:</b> не выбран\n"
            text += (
                "\nВы можете вступить в клуб, чтобы отображаться на дэшборде:\n"
                "<a href=\"https://run5k.run/d/"
                "03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst\">"
                "Клубы 5 вёрст</a>\n"
            )
    else:
        text += (
            "Профиль ещё не привязан.\n\n"
            "Привяжите профиль с сайта «5 вёрст», чтобы вступать в клубы и видеть расширенную статистику."
        )

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=profile5v_actions_kb(has_profile, has_club),
        disable_web_page_preview=True,
    )
    await cb.answer()

@dp.callback_query(F.data.startswith("p5v:action:"))
async def p5v_action(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split(":")[2]

    # Привязать / изменить профиль 5 вёрст
    if action == "bind":
        ok = await enforce_change_limit(
            field="last_profile_change_at",
            tg_id=cb.from_user.id,
            action_code="PROFILE_CHANGE_DENIED_LIMIT",
            ctx=cb,
            text_prefix="Менять профиль 5 вёрст можно раз в 24 часа.",
        )
        if not ok:
            return

        # Просим пользователя прислать ID/ссылку и переводим в состояние ожидания
        await cb.message.answer(
            "<b>Профиль 5 вёрст</b>\n\n"
            "Пришлите ссылку на ваш профиль или ID участника.\n"
            "Поддерживаемые форматы:\n"
            "• Ссылка: <code>https://5verst.ru/userstats/790103773/</code>\n"
            "• ID: <code>790103773</code>\n"
            "• ID с буквой: <code>А790103773</code>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await state.set_state(BindStates.waiting_profile)
        await cb.answer()
        return

    # Отвязать профиль 5 вёрст
    if action == "unbind":
        can, nt = can_change('last_profile_change_at', cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "PROFILE_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.answer("Лимит 24 часа.", show_alert=True)
            return

        ok = unlink_profile(cb.from_user.id)
        if ok:
            log_action(cb.from_user.id, "PROFILE_UNBOUND", True, {})
            await cb.message.answer(
                "Профиль отвязан.",
                reply_markup=main_menu(consent_accepted=consent_flag(cb.from_user.id)),
                disable_web_page_preview=True,
            )
        else:
            log_action(cb.from_user.id, "PROFILE_UNBOUND_NOOP", False, {})
            await cb.message.answer(
                "У вас и так не привязан профиль.",
                reply_markup=main_menu(consent_accepted=consent_flag(cb.from_user.id)),
                disable_web_page_preview=True,
            )
        await cb.answer()

@dp.callback_query(F.data.startswith("pr:action:"))
async def pr_action(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split(":")[2]

    # Привязать / изменить профиль
    if action == "bind":
        ok = await enforce_change_limit(
            field="last_parkrun_change_at",
            tg_id=cb.from_user.id,
            action_code="PARKRUN_CHANGE_DENIED_LIMIT",
            ctx=cb,
            text_prefix="Менять привязку учетной записи parkrun можно раз в 24 часа.",
        )
        if not ok:
            return

        await cb.message.answer(
            "<b>Учетная запись parkrun</b>\n\n"
            "Пришлите ваш ID участника parkrun.\n"
            "Поддерживаемые форматы:\n"
            "• Числа: <code>7035519</code>\n"
            "• Формат с буквой: <code>A7035519</code>",
            parse_mode="HTML",
        )
        await state.set_state(ParkrunBind.waiting_id)
        await cb.answer()
        return

    # Отвязать профиль
    if action == "unbind":
        can, nt = can_change("last_parkrun_change_at", cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "PARKRUN_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.answer("Менять привязку можно раз в 24 часа.", show_alert=True)
            return

        ok = unlink_parkrun_profile(cb.from_user.id)
        if ok:
            log_action(cb.from_user.id, "PARKRUN_PROFILE_UNBOUND", True, {})
            await cb.message.answer(
                "Профиль parkrun отвязан.",
                reply_markup=mk_menu(cb.from_user.id),
                disable_web_page_preview=True,
            )
        else:
            log_action(cb.from_user.id, "PARKRUN_PROFILE_UNBOUND_NOOP", False, {})
            await cb.message.answer(
                "У вас не был привязан профиль parkrun.",
                reply_markup=mk_menu(cb.from_user.id),
                disable_web_page_preview=True,
            )
        await cb.answer()
        return

@dp.callback_query(F.data.startswith("c95:action:"))
async def c95_action(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split(":")[2]

    # Привязать / изменить профиль
    if action == "bind":
        can, nt = can_change("last_s95_change_at", cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "S95_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.message.answer(
                "Менять привязку учетной записи С95 можно раз в 24 часа.\n"
                f"Следующая попытка после: {nt.astimezone(TZ):%Y-%m-%d %H:%M}.",
                disable_web_page_preview=True,
            )
            await cb.answer()
            return

        await cb.message.answer(
            "<b>Учетная запись С95</b>\n\n"
            "Пришлите ссылку на профиль или ID / QR-код.\n"
            "Поддерживаемые форматы:\n"
            "• Ссылка: <code>https://s95.ru/athletes/5207</code>\n"
            "• ID: <code>5207</code>\n"
            "• QR-код: <code>7035519</code> или <code>A7035519</code>",
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        await state.set_state(S95Bind.waiting_input)
        await cb.answer()
        return

    # Отвязать профиль
    if action == "unbind":
        can, nt = can_change("last_s95_change_at", cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "S95_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.answer("Менять привязку можно раз в 24 часа.", show_alert=True)
            return

        ok = unlink_s95_profile(cb.from_user.id)
        if ok:
            log_action(cb.from_user.id, "S95_PROFILE_UNBOUND", True, {})
            await cb.message.answer(
                "Профиль С95 отвязан.",
                reply_markup=mk_menu(cb.from_user.id),
                disable_web_page_preview=True,
            )
        else:
            log_action(cb.from_user.id, "S95_PROFILE_UNBOUND_NOOP", False, {})
            await cb.message.answer(
                "У вас не был привязан профиль С95.",
                reply_markup=mk_menu(cb.from_user.id),
                disable_web_page_preview=True,
            )
        await cb.answer()
        return

@dp.callback_query(F.data.startswith("clubs:page:"))
async def clubs_page(cb: CallbackQuery):
    page = int(cb.data.split(":")[2])
    clubs = list_clubs_distinct()
    await cb.message.edit_reply_markup(reply_markup=clubs_kb(clubs, page=page))
    await cb.answer()

@dp.callback_query(F.data.startswith("club:set:"))
async def club_set(cb: CallbackQuery):
    club = cb.data.split(":", 2)[2]
    row = get_profile(cb.from_user.id)
    uid = row.get('user_id_5v')

    can, nt = can_change('last_club_change_at', cb.from_user.id)
    if not can:
        log_action(cb.from_user.id, "CLUB_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
        await cb.answer("Лимит 24 часа.", show_alert=True)
        return

    set_user_club(cb.from_user.id, uid, club)
    log_action(cb.from_user.id, "CLUB_SET", True, {"club": club, "user_id_5v": uid})

    club_url = url_5v_club_dashboard(club)

    await cb.message.answer(
        f"Готово! Вы в клубе «{club}».\n"
        f"<a href=\"{club_url}\">Посмотреть статистику по клубу</a>",
        parse_mode="HTML",
        reply_markup=mk_menu(cb.from_user.id),
        disable_web_page_preview=True
    )
    await cb.answer()

@dp.callback_query(F.data == "profile:c95")
async def profile_c95(cb: CallbackQuery):
    row = get_profile(cb.from_user.id)
    s95_id = row.get("s95_user_id") if row else None
    has_c95 = bool(s95_id)

    text = "<b>Учетная запись С95</b>\n\n"

    if has_c95:
        url = f"https://s95.ru/athletes/{s95_id}"
        text += (
            "Сейчас к вашему Telegram-аккаунту привязан профиль в системе С95:\n"
            f"<a href=\"{url}\">ID {s95_id}</a>\n\n"
            "Вы можете изменить или отвязать его не чаще одного раза в 24 часа."
        )
    else:
        text += (
            "У вас пока не привязана учетная запись в системе С95.\n\n"
            "Вы можете указать её, чтобы в будущем использовать статистику с сайта s95.\n"
            "Для начала привязки нажмите кнопку «Привязать профиль» ниже."
        )

    await cb.message.answer(
        text,
        parse_mode="HTML",
        reply_markup=profile_c95_actions_kb(has_c95),
        disable_web_page_preview=True,
    )
    await cb.answer()


def pluralize_ru(number: int, forms: tuple[str, str, str]) -> str:
    """
    Склонение существительного по числу.
    forms = ("пробежка", "пробежки", "пробежек")
    Возвращает строку вида "127 пробежек".
    """
    n = abs(number)

    # 11–19 -> третья форма
    if 11 <= (n % 100) <= 19:
        form = forms[2]
    else:
        last = n % 10
        if last == 1:
            form = forms[0]
        elif 2 <= last <= 4:
            form = forms[1]
        else:
            form = forms[2]

    return f"{number} {form}"

@dp.message(lambda m: m.text and m.text.startswith("/") and m.text.strip() != "/start")
async def unknown_slash_command(message: Message):
    await message.answer(
        "Я не знаю такую команду 🤔\n\n"
        "Функционал бота был обновлён, некоторые команды больше не используются.\n"
        "Пожалуйста, нажмите /start, чтобы открыть актуальное меню.",
        reply_markup=main_menu(consent_accepted=consent_flag(message.from_user.id)),
        disable_web_page_preview=True,
    )

@dp.message()
async def unknown_message(message: Message, state: FSMContext):
    # Если бот сейчас ожидает данных — НЕ ловим
    current_state = await state.get_state()
    if current_state is not None:
        return  # бот ждёт ID/ссылку — не вмешиваемся

    # Иначе — универсальный ответ
    await message.answer(
        "Я не знаю такую команду 🤔\n\n"
        "Функционал бота был обновлён, некоторые команды больше не используются.\n"
        "Пожалуйста, нажмите /start, чтобы открыть актуальное меню.",
        reply_markup=main_menu(consent_accepted=consent_flag(message.from_user.id)),
        disable_web_page_preview=True,
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
