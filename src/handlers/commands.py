import json

from telegram import Update
from telegram.ext import ContextTypes
from telegram.constants import ParseMode

from src.schemas import Msg
from src.db import get_ch
from src.db.logger import log_event, log_llm_tool_request
from src.db.messages import tool_get_messages_window, tool_search_messages
from src.db.summaries import tool_get_summaries
from src.db.contexts import tool_get_contexts
from src.db.users import load_display_names
from src.t_materials import build_materials_for_last_n
from src.llm import RAG_SYSTEM, _chat_complete, parse_tool_call

_last_t_cache = {"n": 0, "upto_msg_id": 0, "text": ""}


async def cmd_t(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /t {n} — кратко пересказать последние n сообщений:
    приоритет материалов: контексты → выжимки → хвост сырых сообщений.
    """
    try:
        n = int((ctx.args or ["0"])[0])
    except Exception:
        await update.effective_chat.send_message("Формат: /t {n}")
        return
    if n <= 0:
        await update.effective_chat.send_message("n должно быть > 0")
        return

    row = get_ch().query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_msg_id = int(row[0][0] or 0)

    DELTA_MAX = 20
    if (
        _last_t_cache["text"]
        and _last_t_cache["n"] >= n
        and (last_msg_id - _last_t_cache["upto_msg_id"]) <= DELTA_MAX
    ):
        rows = get_ch().query(
            """
            SELECT tg_msg_id, user_id, text, ts
            FROM tg_messages
            WHERE tg_msg_id > %(from_id)s AND lengthUTF8(text) > 0
            ORDER BY tg_msg_id ASC
            """,
            parameters={"from_id": _last_t_cache["upto_msg_id"]},
        ).result_rows

        tail = [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]

        names = load_display_names(m.user_id for m in tail)
        for m in tail:
            m.author = names.get(m.user_id, str(m.user_id))

        if tail:
            tail_lines = "\n".join(
                f"- {m.ts.isoformat()}Z | {m.author}: {m.text}" 
                for m in tail[:10]
            )
            resp = (
                f"{_last_t_cache['text']}\n\nДополнение (новые {len(tail)} сообщений):\n"
                f"{tail_lines}\n\n(Основной обзор не пересчитывался — добавлено только новое)."
            )
        else:
            resp = _last_t_cache["text"] + "\n\n(Новых сообщений почти не было.)"
        await update.effective_chat.send_message(resp[:4000])
        return

    ctx_texts, sum_texts, raw_msgs = build_materials_for_last_n(n)
    if not (ctx_texts or sum_texts or raw_msgs):
        await update.effective_chat.send_message("Недостаточно данных.")
        return

    blocks = []
    if ctx_texts:
        blocks.append("Контексты:\n" + "\n\n".join(ctx_texts))
    if sum_texts:
        blocks.append("Выжимки:\n" + "\n\n".join(sum_texts))
    if raw_msgs:
        tail_lines = "\n".join(
            f"{m.ts.isoformat()}Z | {getattr(m, 'author', m.user_id)}: {m.text}"
            for m in raw_msgs
        )
        blocks.append("Последние сообщения (хвост):\n" + tail_lines)

    content = (
        "Составь ЕДИНУЮ хронологию ключевых событий и фактов из всех материалов ниже.\n"
        "Используй только важную информацию, без лишних деталей и повторов.\n"
        "Объедини данные из контекстов, выжимок и хвоста, сортируя их строго по времени.\n"
        "Приоритет источников: контексты > выжимки > хвост.\n"
        "Игнорируй факты, которые уже упоминались ранее.\n"
        "Вывод — в формате 8–15 буллетов, без лишнего оформления и лишних заголовков.\n"
        "Не используй спецсимволов или markdown разметку.\n\n"
        "Материалы:\n" + "\n\n---\n\n".join(blocks)
    )

    messages = [
        {
            "role": "system",
            "content": "Ты опытный аналитик, который делает сжатые хронологические выжимки диалогов."
        },
        {"role": "user", "content": content}
    ]
    text, _, _ = await _chat_complete(messages, temperature=0.2)

    log_event(
        {
            "type": "llm.t_route",
            "route": "contexts>summaries>raw",
            "counts": {
                "contexts": len(ctx_texts),
                "summaries": len(sum_texts),
                "raw": len(raw_msgs),
            },
            "n_requested": n,
        }
    )

    _last_t_cache.update({"n": n, "upto_msg_id": last_msg_id, "text": text})
    await update.effective_chat.send_message(text[:4000])


async def cmd_b(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /b {text} — прямой промпт с возможностью запросить данные из БД
    через протокол TOOL: {...}
    """
    q = " ".join(ctx.args or []).strip()
    if not q:
        await update.effective_chat.send_message("Формат: /b {запрос}")
        return

    first = [
        {"role": "system", "content": RAG_SYSTEM},
        {"role": "user", "content": q},
    ]
    draft, _, _ = await _chat_complete(first, temperature=0.2)

    tool = parse_tool_call(draft)
    if not tool:
        await update.effective_chat.send_message(draft[:4000])
        return

    log_llm_tool_request(draft, tool)

    name = tool.get("name")
    args = tool.get("args", {}) or {}

    if name == "get_contexts":
        data = tool_get_contexts(limit=int(args.get("limit", 5)))
    elif name == "get_summaries":
        data = tool_get_summaries(limit=int(args.get("limit", 10)))
    elif name == "get_messages_window":
        data = tool_get_messages_window(n=int(args.get("n", 200)))
    elif name == "search_messages":
        data = tool_search_messages(
            query=args.get("query", ""),
            window=int(args.get("window", 5000)),
            limit=int(args.get("limit", 50)),
        )
    else:
        await update.effective_chat.send_message("Нейросеть попыталась использовать неизвестный инструмент.")
        return

    second = [
        {"role": "system", "content": (
            "Отвечай кратко и конкретно. "
            "Если пришли и контексты/выжимки и сырые сообщения, доверяй контекстам и выжимкам, "
            "а сырые используй только для уточнения/цитаты."
        )},
        {"role": "user", "content": q},
        {"role": "assistant", "content": draft},
        {"role": "user", "content": "Данные из БД:\n" + json.dumps(data, ensure_ascii=False)[:12000]},
    ]
    final, _, _ = await _chat_complete(second, temperature=0.2)
    await update.effective_chat.send_message(final[:4000])


async def get_chat_id(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    /get_id — вернуть chat.id (удобно для whitelisting)
    """
    return await update.effective_chat.send_message(
        f"`{update.effective_chat.id}`", parse_mode=ParseMode.MARKDOWN
    )
