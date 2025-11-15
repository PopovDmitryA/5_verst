from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

def main_menu(consent_accepted: bool):
    rows = []
    # Согласие показываем ТОЛЬКО если ещё не дано
    if not consent_accepted:
        rows.append([KeyboardButton(text="📝 Согласие")])
    rows.extend([
        [KeyboardButton(text="👤 Профиль")],
        [KeyboardButton(text="🪪 Профиль 5 вёрст")],
        [KeyboardButton(text="👥 Клубы")],
        [KeyboardButton(text="ℹ️ Описание")],
        [KeyboardButton(text="ℹ️ Помощь")],
    ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def consent_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Принять", callback_data="consent:accept"),
             InlineKeyboardButton(text="Отклонить", callback_data="consent:decline")]
        ]
    )

def confirm_profile_kb(uid: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да, привязать", callback_data=f"profile:confirm:{uid}")],
            [InlineKeyboardButton(text="Нет", callback_data="profile:cancel")]
        ]
    )

def clubs_kb(clubs: list[str], page: int = 0, per_page: int = 12):
    total = len(clubs)
    start = page * per_page
    end = min(start + per_page, total)
    rows = []
    for c in clubs[start:end]:
        rows.append([InlineKeyboardButton(text=c, callback_data=f"club:set:{c}")])
    nav = []
    if start > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"clubs:page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"clubs:page:{page+1}"))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)

def delete_club_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Удалить клуб", callback_data="club:delete")],
            [InlineKeyboardButton(text="Отмена", callback_data="club:cancel")]
        ]
    )

def clubs_actions_kb(has_club: bool):
    buttons = [[InlineKeyboardButton(text="Привязать / изменить клуб", callback_data="clubs:action:set")]]
    if has_club:
        buttons.append([InlineKeyboardButton(text="Отвязать клуб", callback_data="clubs:action:unlink")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="clubs:action:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def profile5v_actions_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Привязать / изменить профиль", callback_data="p5v:action:bind")],
            [InlineKeyboardButton(text="Отвязать профиль", callback_data="p5v:action:unbind")],
            [InlineKeyboardButton(text="Отмена", callback_data="p5v:action:cancel")],
        ]
    )

def confirm_unlink_club_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да", callback_data="club:confirm_unlink"),
             InlineKeyboardButton(text="Нет", callback_data="club:cancel_unlink")]
        ]
    )
