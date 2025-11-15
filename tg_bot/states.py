from aiogram.fsm.state import StatesGroup, State

class BindStates(StatesGroup):
    waiting_profile = State()
