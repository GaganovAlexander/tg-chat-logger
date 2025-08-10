import asyncio
import datetime as dt

from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes

from src.schemas import Msg
from src.db import upsert_user, insert_message
from src.workers import summarizer_loop
from src.configs import BOT_TOKEN


async def on_msg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    m = update.effective_message
    if not m or not m.text:
        return
    u = m.from_user
    upsert_user(
        user_id=u.id,
        username=u.username or '',
        first_name=u.first_name or '',
        last_name=u.last_name or ''
    )
    insert_message(Msg(
        tg_msg_id=m.message_id,
        user_id=u.id,
        text=m.text,
        ts=m.date.astimezone(dt.timezone.utc).replace(tzinfo=None)
    ))


async def main():
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), on_msg))
    task = asyncio.create_task(summarizer_loop())
    await app.initialize()
    await app.start()
    try:
        await app.updater.start_polling()
        await task
    finally:
        await app.updater.stop()
        await app.stop()
        await app.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
