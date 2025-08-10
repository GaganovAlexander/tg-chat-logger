import asyncio

from telegram.ext import Application, filters

from src.configs import BOT_TOKEN, ALLOWED_CHAT_IDS
from src.handlers import register_handlers
from src.workers import summarizer_loop


async def main():
    app = Application.builder().token(BOT_TOKEN).build()

    chat_whitelist = filters.Chat(chat_id=list(ALLOWED_CHAT_IDS))

    register_handlers(app, chat_whitelist)

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
