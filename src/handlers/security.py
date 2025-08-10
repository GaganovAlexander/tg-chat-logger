from telegram import Update
from telegram.ext import ContextTypes

from src.db.logger import log_event


async def blocked(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Хэндлер на все сообщения вне whitelist:
    логируем попытку и отправляем предупреждение.
    """
    chat = update.effective_chat
    user = update.effective_user

    log_event({
        "type": "security.blocked_chat",
        "chat_id": getattr(chat, "id", None),
        "chat_type": getattr(chat, "type", None),
        "chat_title": getattr(chat, "title", None),
        "user_id": getattr(user, "id", None),
        "username": getattr(user, "username", None),
        "reason": "not in whitelist",
    })

    cid = getattr(chat, "id", None)
    if cid is None:
        return

    msg = (
        "⚠️ Этот бот привязан к приватным чатам, разрешённым владельцем.\n"
        "Его нельзя использовать здесь. Если вы администратор чата (или это личные сообщения), удалите бота "
        "или попросите владельца добавить этот чат в whitelist.\n"
        "Если вы не знаете владельца, бот для вас бесполезен."
    )
    try:
        if update.effective_message:
            await update.effective_message.reply_text(msg)
        else:
            await ctx.bot.send_message(chat_id=cid, text=msg)
    except Exception:
        pass
