# src/handlers/__init__.py
from telegram.ext import Application, MessageHandler, CommandHandler, filters

from .messages import on_msg
from .commands import cmd_t, cmd_b, get_chat_id
from .security import blocked


def register_handlers(app: Application, chat_whitelist: filters.BaseFilter) -> None:
    """
    Регистрирует все хэндлеры приложения.
    chat_whitelist — filters.Chat(chat_id=...) для белого списка.
    """
    app.add_handler(MessageHandler(chat_whitelist & filters.TEXT & (~filters.COMMAND), on_msg))

    app.add_handler(CommandHandler("get_id", get_chat_id))
    app.add_handler(CommandHandler("t", cmd_t, filters=chat_whitelist))
    app.add_handler(CommandHandler("b", cmd_b, filters=chat_whitelist))

    app.add_handler(MessageHandler(~chat_whitelist, blocked), group=99)
