from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

def main_menu(consent_accepted: bool):
    rows = []
    if not consent_accepted:
        rows.append([KeyboardButton(text="⚙️ Настройки")])
        rows.append([KeyboardButton(text="📊 Дэшборды")])
        return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

    # Когда согласие есть:
    rows.extend([
        # [KeyboardButton(text="👤 Профиль")]  # пока скрыто
        [KeyboardButton(text="👤 Мой профиль")],
        [KeyboardButton(text="📊 Дэшборды")],
        [KeyboardButton(text="⚙️ Настройки")],
    ])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def profile_root_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Учетная запись 5 вёрст", callback_data="profile:5v")],
            [InlineKeyboardButton(text="Учетная запись parkrun", callback_data="profile:pr")],
            [InlineKeyboardButton(text="Учетная запись С95", callback_data="profile:c95")],
        ]
    )


def settings_kb(consent_accepted: bool, news_subscribed: bool):
    consent_icon = "✅" if consent_accepted else "❌"
    news_icon = "✅" if news_subscribed else "❌"

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Согласие на обработку перс. данных: {consent_icon}", callback_data="settings:consent")],
            [InlineKeyboardButton(text=f"Рассылка о новостях: {news_icon}", callback_data="settings:news")],
            [InlineKeyboardButton(text="Вернуться в главное меню", callback_data="settings:close")],
        ]
    )

def consent_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Принять", callback_data="consent:accept"),
             InlineKeyboardButton(text="Отклонить", callback_data="consent:decline")]
        ]
    )

def confirm_profile_kb(uid: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да", callback_data=f"bind:confirm:{uid}"),
                InlineKeyboardButton(text="Нет", callback_data="bind:cancel"),
            ]
        ]
    )


def clubs_kb(clubs: list[str], page: int = 0, per_page: int = 6):
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

    # Всегда видимая кнопка "Назад" к профилю 5 вёрст
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:5v")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def clubs_actions_kb(has_club: bool):
    first_text = "Привязать клуб" if not has_club else "Поменять клуб"
    buttons = [[InlineKeyboardButton(text=first_text, callback_data="clubs:action:set")]]
    if has_club:
        buttons.append([InlineKeyboardButton(text="Отвязать клуб", callback_data="clubs:action:unlink")])
    buttons.append([InlineKeyboardButton(text="Отмена", callback_data="clubs:action:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def confirm_parkrun_kb(user_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Да, привязать", callback_data=f"pr:confirm:{user_id}"),
                InlineKeyboardButton(text="Отмена", callback_data="pr:cancel"),
            ]
        ]
    )

def profile5v_actions_kb(has_profile: bool, has_club: bool):
    rows = []

    # Профиль
    profile_text = "Привязать профиль" if not has_profile else "Привязать другой профиль"
    rows.append([InlineKeyboardButton(
        text=profile_text,
        callback_data="p5v:action:bind"
    )])

    if has_profile:
        rows.append([InlineKeyboardButton(text="Отвязать профиль", callback_data="p5v:action:unbind")])

    # Клубы – показываем ВСЕГДА
    if not has_profile:
        rows.append([InlineKeyboardButton(text="Клубы (недоступно)", callback_data="p5v:club:no_profile")])
    else:
        club_text = "Привязать клуб" if not has_club else "Поменять клуб"
        rows.append([InlineKeyboardButton(text=club_text, callback_data="clubs:action:set")])
        if has_club:
            rows.append([InlineKeyboardButton(text="Отвязать клуб", callback_data="clubs:action:unlink")])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="profile:back")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def profile_pr_actions_kb(has_parkrun: bool):
    rows = []

    profile_text = "Привязать профиль" if not has_parkrun else "Привязать другой профиль"
    rows.append([
        InlineKeyboardButton(
            text=profile_text,
            callback_data="pr:action:bind"
        )
    ])

    if has_parkrun:
        rows.append([
            InlineKeyboardButton(
                text="Отвязать профиль",
                callback_data="pr:action:unbind"
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="Назад",
            callback_data="profile:back"
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def profile_c95_actions_kb(has_c95: bool):
    rows = []

    profile_text = "Привязать профиль" if not has_c95 else "Привязать другой профиль"
    rows.append([
        InlineKeyboardButton(
            text=profile_text,
            callback_data="c95:action:bind",
        )
    ])

    if has_c95:
        rows.append([
            InlineKeyboardButton(
                text="Отвязать профиль",
                callback_data="c95:action:unbind",
            )
        ])

    rows.append([
        InlineKeyboardButton(
            text="Назад",
            callback_data="profile:back",
        )
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def confirm_s95_kb(s95_id: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Да, привязать",
                    callback_data=f"c95:confirm:{s95_id}",
                ),
                InlineKeyboardButton(
                    text="Отмена",
                    callback_data="c95:cancel",
                ),
            ]
        ]
    )

def confirm_unlink_club_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Да", callback_data="club:confirm_unlink"),
             InlineKeyboardButton(text="Нет", callback_data="club:cancel_unlink")]
        ]
    )

def dashboards_root_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📍 Локации", callback_data="dash:cat:loc")],
            [InlineKeyboardButton(text="🧳 Паркран-туристы", callback_data="dash:cat:tour")],
            [InlineKeyboardButton(text="🏃 Все участники", callback_data="dash:cat:all")],
        ]
    )


def dashboards_cat_kb(category: str):
    rows = []
    if category == "loc":
        rows = [
            [InlineKeyboardButton(text="Статистика по локациям",
                                  url="https://run5k.run/d/bepnuz4ecveo0f/statistika-po-lokacijam")],
            [InlineKeyboardButton(text="Рейтинг участников и волонтёров внутри локации",
                                  url="https://run5k.run/d/ae5xf2cebu3gga/rejting-uchastnikov-i-volontjorov-vnutri-lokacii")],
            [InlineKeyboardButton(text="Долгая пауза",
                                  url="https://run5k.run/d/cea88eb2-47e4-4334-bfd6-e13ad11f5e3a/dolgaja-pauza")],
            [InlineKeyboardButton(text="Календарь первых стартов локаций 5 вёрст",
                                  url="https://run5k.run/d/eeqquzpgqp88wd/kalendar--pervyh-startov-lokacij-5-vjorst")],
        ]
    elif category == "tour":
        rows = [
            [InlineKeyboardButton(text="Карта туристов",
                                  url="https://run5k.run/d/de1hu8dabny80c/karta-turistov")],
            [InlineKeyboardButton(text="Карта туристов волонтёров",
                                  url="https://run5k.run/d/de96ruht0r0n4c/karta-turistov-volonterov")],
            [InlineKeyboardButton(text="Рейтинг по количеству уникальных локаций",
                                  url="https://run5k.run/d/fehx3pjkvj56oa/rejting-po-kolichestvu-unikal-nyh-lokacij")],
            [InlineKeyboardButton(text="Рейтинг расстояния посещённых локаций от Москвы",
                                  url="https://run5k.run/d/dekvyyrwadwjkb/89bc50f")],
            [InlineKeyboardButton(text="Прогноз даты завершения туризма",
                                  url="https://run5k.run/d/eednttn3wos1sf/prognoz-daty-zavershenija-turizma")],
        ]
    elif category == "all":
        rows = [
            [InlineKeyboardButton(text="Рейтинг количества пробежек",
                                  url="https://run5k.run/d/beb3dpef24r28a/rejting-kolichestva-probezhek")],
            [InlineKeyboardButton(text="Рейтинг количества волонтёрств",
                                  url="https://run5k.run/d/feb3hdye0fhtse/rejting-kolichestva-volonterstv")],
            [InlineKeyboardButton(text="Счёт по личным встречам (пересечения)",
                                  url="https://run5k.run/d/86bf8188-e70b-4e14-8997-6a8893142f55/schjot-po-lichnym-vstrecham")],
            [InlineKeyboardButton(text="Челленджи",
                                  url="https://run5k.run/d/3e54a2d8-ef9f-4743-8117-4a2ddb47d6a7/chellendzhi")],
            [InlineKeyboardButton(text="Клубы 5 вёрст",
                                  url="https://run5k.run/d/03450385-0269-4509-873f-1423067b5c7f/kluby-5-vjorst")],
            [InlineKeyboardButton(text="Рекорды по возрастным группам в локациях",
                                  url="https://run5k.run/d/d615a771-0ea5-4559-ac97-536e08662a96/rekordy-po-vozrastnym-gruppam-v-lokacijah")],
            [InlineKeyboardButton(text="Рейтинг победителей на пробежках",
                                  url="https://run5k.run/d/feitbfpcwwb28a/rejting-pobeditelej-na-probezhkah")],
            [InlineKeyboardButton(text="Рейтинг по времени финиша",
                                  url="https://run5k.run/d/deprgii19fdoga/rejting-po-vremeni-finisha")],
            [InlineKeyboardButton(text="Единый протокол",
                                  url="https://run5k.run/d/4a385e6f-5cb6-4e7d-914f-8fbee0b34bba/edinyj-protokol")],
        ]
    # Кнопка назад на корневой уровень
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="dash:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)
