import datetime as dt

from telegram import Update
from telegram.ext import ContextTypes

from src.schemas import Msg
from src.db.users import upsert_user
from src.db.messages import insert_message


async def on_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Логирует обычные текстовые сообщения (только текст, без медиа/реплаев).
    """
    m = update.effective_message
    if not m or not m.text:
        return

    u = m.from_user
    upsert_user(
        user_id=u.id,
        username=u.username or "",
        first_name=u.first_name or "",
        last_name=u.last_name or "",
    )

    insert_message(
        Msg(
            tg_msg_id=m.message_id,
            user_id=u.id,
            text=m.text,
            ts=m.date.astimezone(dt.timezone.utc).replace(tzinfo=None),
        )
    )
