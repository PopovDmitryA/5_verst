import configparser
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from pathlib import Path

from states import BindStates
from keyboards import main_menu, consent_kb, confirm_profile_kb, clubs_kb, delete_club_kb, \
    clubs_actions_kb, profile5v_actions_kb
from db import (
    TZ, ensure_user_row, get_profile, set_consent,
    log_action, parse_user_id_from_text, user_exists, find_latest_name_for_user,
    can_change, bind_profile, list_clubs_distinct,
    set_user_club, delete_user_club, get_current_club, unlink_profile
)

from aiogram.fsm.context import FSMContext

# --- Telegram token ---

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
CONFIG_PATH = PROJECT_ROOT / "5_verst.ini"

cfg = configparser.ConfigParser()
cfg.read(CONFIG_PATH)

TOKEN = cfg['telegram']['token']

bot = Bot(TOKEN)
dp = Dispatcher()

DASHBOARD_URL = "http://run5k.run/d/03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst"
AUTHOR_HANDLE = "@Popov_Dmitry"
AUTHOR_CHANNEL = "https://t.me/popov_way"

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
        await message.answer("Сначала примите оферту в разделе «📝 Согласие».", reply_markup=mk_menu(message.from_user.id), disable_web_page_preview=True)
        return False
    return True

async def must_bound(message: Message) -> bool:
    row = get_profile(message.from_user.id)
    if not row.get('user_id_5v'):
        await message.answer("Сначала привяжите профиль 5 Вёрст.", reply_markup=mk_menu(message.from_user.id), disable_web_page_preview=True)
        return False
    return True


# ===== Handlers =====
@dp.message(CommandStart())
async def on_start(message: Message):
    ensure_user_row(message.from_user.id, message.from_user.username, message.chat.id)
    row = get_profile(message.from_user.id)
    has_consent = bool(row and row.get('consent_accepted'))
    uid = row.get('user_id_5v') if row else None
    club = None
    name_txt = None

    if uid:
        name_txt = find_latest_name_for_user(uid) or ""
        club = get_current_club(uid)

    intro = (
        "Привет! Это «Бот статистики парковых пробежек». "
        "Функционал находится в разработке: сейчас можно привязать учётную запись на сайте «5 вёрст» "
        "и вступить в беговой клуб для просмотра статистики в этом "
        f"(<a href=\"{DASHBOARD_URL}\">дэшборде</a>)."
    )

    if not has_consent:
        tail = "\n\nЧтобы продолжить, дайте согласие на обработку данных: нажмите «📝 Согласие»."
    elif has_consent and not uid:
        tail = "\n\nРекомендую привязать профиль с сайта «5 вёрст»: раздел «🪪 Профиль 5 вёрст» → «Привязать / изменить профиль»."
    else:
        # Есть и согласие, и привязка
        fio = f"<b>{name_txt}</b>" if name_txt else f"ID <code>{uid}</code>"
        club_txt = f"\nКлуб: <b>{club}</b>" if club else "\nКлуб: не выбран"
        tail = f"\n\nПрофиль: {fio}{club_txt}"

    await message.answer(intro + tail, parse_mode="HTML",
                         reply_markup=main_menu(consent_accepted=has_consent), disable_web_page_preview=True)

@dp.message(F.text == "📝 Согласие")
async def consent(message: Message):
    ensure_user_row(message.from_user.id, message.from_user.username, message.chat.id)
    row = get_profile(message.from_user.id)
    if row and row.get('consent_accepted'):
        await message.answer("Согласие уже принято ✅", reply_markup=mk_menu(message.from_user.id), disable_web_page_preview=True)
        return
    text = (
        "Мини-оферта:\n\n"
        "Вы соглашаетесь на обработку персональных данных, включая ваш Telegram-идентификатор и ссылку "
        "на профиль участника сайта «5 вёрст», исключительно для корректной работы функций "
        "бота статистики парковых пробежек.\n\n"
        "Автор гарантирует, что эти данные не будут использоваться в коммерческих или иных корыстных целях "
        "и не передаются третьим лицам."
    )

    await message.answer(text, disable_web_page_preview=True)
    await message.answer("Принять условия?", reply_markup=consent_kb(), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("consent:"))
async def consent_cb(cb: CallbackQuery):
    action = cb.data.split(":")[1]
    if action == "accept":
        set_consent(cb.from_user.id, True)
        log_action(cb.from_user.id, "CONSENT_ACCEPTED", True, {})
        await cb.message.answer("Спасибо! Согласие принято ✅", reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True)
    else:
        set_consent(cb.from_user.id, False)
        log_action(cb.from_user.id, "CONSENT_DECLINED", True, {})
        await cb.message.answer("Без согласия продолжение невозможно. Вернитесь, когда будете готовы.",
                                reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True)
    await cb.answer()

@dp.message(F.text == "🔗 Привязать/изменить профиль")
async def bind_start(message: Message, state: FSMContext):
    if not await must_consent(message):
        return
    can, nt = can_change('last_profile_change_at', message.from_user.id)
    if not can:
        log_action(message.from_user.id, "PROFILE_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
        await message.answer(f"Менять профиль можно раз в 24 часа. Следующая попытка после: {nt.astimezone(TZ):%Y-%m-%d %H:%M}.", disable_web_page_preview=True)
        return
    await message.answer("Пришлите ссылку вида https://5verst.ru/userstats/<id>/ или просто числовой ID.", disable_web_page_preview=True)
    await state.set_state(BindStates.waiting_profile)

@dp.message(F.text == "👤 Профиль")
async def profile_info(message: Message):
    row = get_profile(message.from_user.id)
    has_consent = bool(row and row.get('consent_accepted'))
    uid = row.get('user_id_5v') if row else None
    club = get_current_club(uid) if uid else None
    name_txt = find_latest_name_for_user(uid) if uid else None

    lines = ["<b>Профиль</b>"]
    lines.append(f"• Учётка в TG: @{message.from_user.username or '—'}")

    lines.append("• Учётка на сайте 5 вёрст:")
    if uid:
        url = f"https://5verst.ru/userstats/{uid}/"
        pretty = name_txt or f"ID {uid}"
        lines.append(f"  — <a href=\"{url}\">{pretty}</a>")
    else:
        lines.append("  — не привязана")

    lines.append("• Клуб участника:")
    if club:
        lines.append(f"  — {club}")
    else:
        lines.append("  — не выбран")

    if not has_consent:
        lines.append("\nЧтобы продолжить — дайте согласие: «📝 Согласие».")

    await message.answer("\n".join(lines), parse_mode="HTML",
                         reply_markup=main_menu(consent_accepted=has_consent), disable_web_page_preview=True)

@dp.message(F.text == "ℹ️ Описание")
async def description(message: Message):
    text = (
        "<b>Описание</b>\n"
        "Это бот проекта run5k.run со статистикой, дэшбордами и рейтингами о субботних парковых пробежках.\n\n"
        f"Новости и обновления: <a href=\"{AUTHOR_CHANNEL}\">{AUTHOR_CHANNEL}</a>\n"
        f"Контакт автора: {AUTHOR_HANDLE}\n"
    )
    await message.answer(text, parse_mode="HTML",
                         reply_markup=main_menu(consent_accepted=consent_flag(message.from_user.id)), disable_web_page_preview=True)

@dp.message(BindStates.waiting_profile)
async def bind_receive(message: Message, state: FSMContext):
    uid = parse_user_id_from_text(message.text)
    if uid is None:
        await message.answer("Не распознал ID. Пришлите ссылку https://5verst.ru/userstats/<id>/ или сам <id>.", disable_web_page_preview=True)
        return
    if not user_exists(uid):
        log_action(message.from_user.id, "PROFILE_NOT_FOUND", False, {"user_id_5v": uid})
        await message.answer("Такой пользователь не найден в базе финишей/волонтёрств. Проверьте ID.", disable_web_page_preview=True)
        return
    name = find_latest_name_for_user(uid) or "имя не найдено"
    await message.answer(f"Нашли профиль: *{name}* (ID {uid}). Привязать?", parse_mode="Markdown",
                         reply_markup=confirm_profile_kb(uid), disable_web_page_preview=True)
    await state.clear()

@dp.callback_query(F.data.startswith("profile:"))
async def bind_confirm(cb: CallbackQuery):
    _, action, *rest = cb.data.split(":")
    if action == "cancel":
        await cb.message.answer("Привязка отменена.", reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True)
        await cb.answer()
        return
    if action == "confirm":
        uid = int(rest[0])
        can, nt = can_change('last_profile_change_at', cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "PROFILE_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.answer("Лимит 24 часа.", show_alert=True)
            return
        profile_url = f"https://5verst.ru/userstats/{uid}/"
        ok, msg = bind_profile(cb.from_user.id, uid, profile_url)
        if ok:
            log_action(cb.from_user.id, "PROFILE_BOUND", True, {"user_id_5v": uid, "profile_url": profile_url})
            await cb.message.answer(f"Профиль привязан: {profile_url}", reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True)
        else:
            log_action(cb.from_user.id, "PROFILE_BOUND_ERROR", False, {"user_id_5v": uid, "error": msg})
            await cb.message.answer(msg, reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True)
        await cb.answer()

@dp.message(F.text == "👥 Клубы")
async def clubs_root(message: Message):
    if not await must_consent(message):
        return
    if not await must_bound(message):
        return

    uid = get_profile(message.from_user.id).get('user_id_5v')
    has_club = bool(get_current_club(uid))

    await message.answer(
        "Выберите действие с клубом:",
        reply_markup=clubs_actions_kb(has_club)
    )


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
        can, nt = can_change('last_club_change_at', cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "CLUB_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.message.answer(
                f"Сменять клуб можно раз в 24 часа. Следующая попытка после: "
                f"{nt.astimezone(TZ):%Y-%m-%d %H:%M}.",
                disable_web_page_preview=True
            )
            await cb.answer()
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

@dp.message(F.text == "🪪 Профиль 5 вёрст")
async def p5v_root(message: Message):
    if not await must_consent(message): return
    await message.answer("Действия с профилем 5 вёрст:", reply_markup=profile5v_actions_kb(), disable_web_page_preview=True)

@dp.callback_query(F.data.startswith("p5v:action:"))
async def p5v_action(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split(":")[2]
    if action == "cancel":
        await cb.message.answer("Действие отменено.",
                                reply_markup=main_menu(consent_accepted=consent_flag(cb.from_user.id)))
        await cb.answer()
        return

    if action == "bind":
        # запускаем существующий сценарий привязки
        can, nt = can_change('last_profile_change_at', cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "PROFILE_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.message.answer(f"Менять профиль можно раз в 24 часа. Следующая попытка после: {nt.astimezone(TZ):%Y-%m-%d %H:%M}.")
            await cb.answer()
            return
        await cb.message.answer("Пришлите ссылку вида https://5verst.ru/userstats/<id>/ или просто числовой ID.", disable_web_page_preview=True)
        await state.set_state(BindStates.waiting_profile)
        await cb.answer()
        return

    if action == "unbind":
        # отвязка профиля
        can, nt = can_change('last_profile_change_at', cb.from_user.id)
        if not can:
            log_action(cb.from_user.id, "PROFILE_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
            await cb.answer("Лимит 24 часа.", show_alert=True)
            return
        ok = unlink_profile(cb.from_user.id)
        if ok:
            log_action(cb.from_user.id, "PROFILE_UNBOUND", True, {})
            await cb.message.answer("Профиль отвязан.",
                                    reply_markup=main_menu(consent_accepted=consent_flag(cb.from_user.id)))
        else:
            log_action(cb.from_user.id, "PROFILE_UNBOUND_NOOP", False, {})
            await cb.message.answer("У вас и так не привязан профиль.",
                                    reply_markup=main_menu(consent_accepted=consent_flag(cb.from_user.id)))
        await cb.answer()

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
    await cb.message.answer(
        f"Готово! Вы в клубе «{club}».\n"
        "Если вашего клуба нет — напишите автору @Popov_Dmitry.\n"
        "Где использовать клубы: http://run5k.run/d/03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst",
        reply_markup=mk_menu(cb.from_user.id), disable_web_page_preview=True
    )
    await cb.answer()

@dp.message(F.text == "❌ Удалить клуб")
async def club_delete_start(message: Message):
    if not await must_consent(message): return
    if not await must_bound(message): return

    uid = get_profile(message.from_user.id).get('user_id_5v')
    cur = get_current_club(uid)
    if not cur:
        await message.answer("У вас сейчас не выбран клуб.", reply_markup=mk_menu(message.from_user.id))
        return

    await message.answer(f"Сейчас выбран клуб: «{cur}». Удалить?", reply_markup=delete_club_kb(), disable_web_page_preview=True)

@dp.callback_query(F.data == "club:delete")
async def club_delete(cb: CallbackQuery):
    can, nt = can_change('last_club_change_at', cb.from_user.id)
    if not can:
        log_action(cb.from_user.id, "CLUB_CHANGE_DENIED_LIMIT", False, {"next_time": nt.isoformat()})
        await cb.answer("Лимит 24 часа.", show_alert=True)
        return
    uid = get_profile(cb.from_user.id).get('user_id_5v')
    ok = delete_user_club(cb.from_user.id, uid)
    if ok:
        log_action(cb.from_user.id, "CLUB_DELETED", True, {"user_id_5v": uid})
        await cb.message.answer("Клуб удалён.", reply_markup=mk_menu(cb.from_user.id))
    else:
        log_action(cb.from_user.id, "CLUB_DELETE_NOOP", False, {"user_id_5v": uid})
        await cb.message.answer("У вас и так не выбран клуб.", reply_markup=mk_menu(cb.from_user.id))
    await cb.answer()

@dp.callback_query(F.data == "club:cancel")
async def club_cancel(cb: CallbackQuery):
    await cb.message.answer("Действие отменено.", reply_markup=mk_menu(cb.from_user.id))
    await cb.answer()

@dp.message(F.text == "ℹ️ Помощь")
async def help_msg(message: Message):
    await message.answer(
        "Доступные действия:\n"
        "• 📝 Согласие — принять мини-оферту\n"
        "• 🔗 Привязать/изменить профиль — раз в 24 часа, с подтверждением по имени\n"
        "• 👥 Вступить/сменить клуб — раз в 24 часа\n"
        "• ❌ Удалить клуб — раз в 24 часа\n\n"
        "Если клуба нет — напишите @Popov_Dmitry\n"
        "Подробнее о клубах: http://run5k.run/d/03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst",
        reply_markup=mk_menu(message.from_user.id), disable_web_page_preview=True
    )

if __name__ == "__main__":
    import asyncio
    asyncio.run(dp.start_polling(bot))
