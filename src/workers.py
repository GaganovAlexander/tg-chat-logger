import asyncio

from .db import get_ch
from .db.summaries import get_last_summarized_msg_id, insert_summary, get_next_batch
from .db.contexts import get_last_context_batch_id, insert_context
from .llm import summarize_messages, summarize_summaries
from .configs import N, K


async def summarizer_loop():
    while True:
        last_to = get_last_summarized_msg_id()
        msgs = get_next_batch(last_to, N)
        if len(msgs) < N:
            await asyncio.sleep(2)
            continue
        batch_id = (msgs[-1].tg_msg_id // N)
        text, ti, to = await summarize_messages(msgs)
        insert_summary(batch_id, msgs, text, ti, to)
        await maybe_make_context()


async def maybe_make_context():
    last_ctx_to = get_last_context_batch_id()
    rows = get_ch().query(
        "SELECT batch_id, text, from_ts, to_ts "
        "FROM tg_summaries WHERE batch_id > %(b)s ORDER BY batch_id ASC LIMIT %(k)s",
        parameters={'b': last_ctx_to, 'k': K}
    ).result_rows
    if len(rows) < K:
        return
    sums_texts = [r[1] for r in rows]
    ctx_text, ti, to = await summarize_summaries(sums_texts)
    context_id = rows[-1][0] // K
    insert_context(context_id, rows, ctx_text, ti, to)
