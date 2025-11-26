from aiogram.fsm.state import StatesGroup, State

class BindStates(StatesGroup):
    waiting_profile = State()


class ParkrunBind(StatesGroup):
    waiting_id = State()


class S95Bind(StatesGroup):
    # Сначала ждём любое представление: ссылка / ID / QR
    waiting_input = State()
    # Если по ID/QR совпадений нет — отдельно ждём именно ссылку
    waiting_link = State()


class AdminBroadcast(StatesGroup):
    # Ждём, пока админ пришлёт текст рассылки
    waiting_message = State()
