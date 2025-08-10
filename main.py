import asyncio
import datetime as dt
import json
from math import ceil

from telegram import Update
from telegram.ext import (
    Application,
    MessageHandler,
    CommandHandler,
    filters,
    ContextTypes
)
from telegram.constants import ParseMode

from src.schemas import Msg
from src.db.users import upsert_user
from src.db.messages import (
    insert_message,
    fetch_last_messages,
    tool_fetch_messages_like
)
from src.db.summaries import fetch_last_summaries, tool_fetch_recent_summaries
from src.db.contexts import fetch_last_contexts, tool_fetch_recent_contexts
from src.db.logger import (
    log_llm_tool_request, log_event
)
from src.llm import (
    summarize_messages,
    summarize_summaries,
    RAG_SYSTEM,
    _chat_complete,
    parse_tool_call
)
from src.workers import summarizer_loop
from src.configs import BOT_TOKEN, ALLOWED_CHAT_IDS, N, K


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

async def cmd_t(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        n = int((ctx.args or ["0"])[0])
    except Exception:
        await update.effective_chat.send_message("Формат: /t {n}")
        return

    if n <= 0:
        await update.effective_chat.send_message("n должно быть > 0, например \"/t 100\"")
        return

    if n <= N:
        msgs = fetch_last_messages(n)
        if not msgs:
            await update.effective_chat.send_message("Сообщений пока нет.")
            return
        text, _, _ = await summarize_messages(msgs)
        await update.effective_chat.send_message(text[:4000], ParseMode.MARKDOWN)
        return

    s_needed = ceil(n / N)
    if s_needed < K:
        sums = fetch_last_summaries(s_needed)
        if sums:
            text, _, _ = await summarize_summaries(sums)
            await update.effective_chat.send_message(text[:4000])
            return
        msgs = fetch_last_messages(min(n, N))
        if msgs:
            text, _, _ = await summarize_messages(msgs)
            await update.effective_chat.send_message(text[:4000], ParseMode.MARKDOWN)
        else:
            await update.effective_chat.send_message("Недостаточно данных.")
        return

    ctx_cnt = ceil(s_needed / K)
    ctx_texts = fetch_last_contexts(ctx_cnt)
    if ctx_texts:
        text, _, _ = await summarize_summaries(ctx_texts)
        await update.effective_chat.send_message(text[:4000], ParseMode.MARKDOWN)
        return

    sums = fetch_last_summaries(min(s_needed, K-1))
    if sums:
        text, _, _ = await summarize_summaries(sums)
        await update.effective_chat.send_message(text[:4000], ParseMode.MARKDOWN)
        return

    msgs = fetch_last_messages(min(n, N))
    if msgs:
        text, _, _ = await summarize_messages(msgs)
        await update.effective_chat.send_message(text[:4000], ParseMode.MARKDOWN)
    else:
        await update.effective_chat.send_message("Недостаточно данных.")


async def cmd_b(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = " ".join(ctx.args or []).strip()
    if not q:
        await update.effective_chat.send_message("Формат: /b {запрос}")
        return

    first = [
        {"role": "system", "content": RAG_SYSTEM},
        {"role": "user", "content": q}
    ]
    draft, _, _ = await _chat_complete(first, temperature=0.2)

    tool = parse_tool_call(draft)
    if not tool:
        await update.effective_chat.send_message(draft[:4000], ParseMode.MARKDOWN)
        return
    log_llm_tool_request(draft, tool)

    name = tool.get("name")
    args = tool.get("args", {})

    if name == "fetch_messages_like":
        data = tool_fetch_messages_like(
            query=args.get("query",""),
            limit=int(args.get("limit", 50)),
            days=int(args.get("days", 30))
        )
    elif name == "fetch_recent_summaries":
        data = tool_fetch_recent_summaries(limit=int(args.get("limit", 10)))
    elif name == "fetch_recent_contexts":
        data = tool_fetch_recent_contexts(limit=int(args.get("limit", 5)))
    else:
        await update.effective_chat.send_message("Неизвестный инструмент.")
        return

    second = [
        {"role": "system", "content": "Сформируй краткий и точный ответ по данным ниже."},
        {"role": "user", "content": q},
        {"role": "assistant", "content": draft},
        {"role": "user", "content": "Данные из БД:\n" + json.dumps(data, ensure_ascii=False)[:12000]}
    ]
    final, _, _ = await _chat_complete(second, temperature=0.2)
    await update.effective_chat.send_message(final[:4000], parse_mode=ParseMode.MARKDOWN)


async def get_chat_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    return await update.effective_chat.send_message(f"`{update.effective_chat.id}`", parse_mode=ParseMode.MARKDOWN)


async def blocked(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
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
        "Его нельзя использовать здесь. Если вы администратор чата(или это частные сообщения), удалите бота "
        "или попросите владельца добавить этот чат в whitelist.\n"
        "Если вы не знаете владельца, то и сам бот для вас бесполезен."
    )
    try:
        if update.effective_message:
            await update.effective_message.reply_text(msg)
        else:
            await ctx.bot.send_message(chat_id=cid, text=msg)
    except Exception:
        pass


async def main():
    app = Application.builder().token(BOT_TOKEN).build()
    chat_whitelist = filters.Chat(chat_id=list(ALLOWED_CHAT_IDS))
    app.add_handler(MessageHandler(chat_whitelist & filters.TEXT & (~filters.COMMAND), on_msg))
    app.add_handler(CommandHandler("get_id", get_chat_id, filters=chat_whitelist))
    app.add_handler(CommandHandler("t", cmd_t, filters=chat_whitelist))
    app.add_handler(CommandHandler("b", cmd_b, filters=chat_whitelist))
    app.add_handler(MessageHandler(~chat_whitelist, blocked), group=99)
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
