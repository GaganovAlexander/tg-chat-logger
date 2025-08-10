from __future__ import annotations

from typing import List

from . import get_ch
from ..schemas import Msg
from .users import load_display_names


def get_last_summarized_msg_id() -> int:
    ch = get_ch()
    r = ch.query("SELECT max(to_msg_id) FROM tg_summaries").result_rows
    return int(r[0][0] or 0)

def get_next_batch(last_to: int, limit: int) -> List[Msg]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE tg_msg_id > %(x)s AND lengthUTF8(text) > 0
        ORDER BY tg_msg_id ASC
        LIMIT %(lim)s
        """,
        parameters={'x': last_to, 'lim': limit}
    ).result_rows
    msgs = [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]
    names = load_display_names(m.user_id for m in msgs)
    for m in msgs:
        m.author = names.get(m.user_id, str(m.user_id))
    return msgs

def insert_summary(batch_id: int, msgs: List[Msg], text: str, tokens_in: int, tokens_out: int) -> None:
    ch = get_ch()
    ch.insert(
        'tg_summaries',
        [(batch_id,
          msgs[0].tg_msg_id,
          msgs[-1].tg_msg_id,
          msgs[0].ts,
          msgs[-1].ts,
          text,
          tokens_in,
          tokens_out)],
        column_names=[
            'batch_id',
            'from_msg_id',
            'to_msg_id',
            'from_ts',
            'to_ts',
            'text',
            'tokens_in',
            'tokens_out'
        ]
    )

def fetch_last_summaries(k: int) -> list[str]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT text
        FROM tg_summaries
        ORDER BY batch_id DESC
        LIMIT %(k)s
        """,
        parameters={"k": k}
    ).result_rows
    return [r[0] for r in rows[::-1]]

def tool_fetch_recent_summaries(limit: int = 5) -> list[dict]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT context_id, from_ts, to_ts, text
        FROM tg_summaries
        ORDER BY batch_id DESC
        LIMIT %(lim)s
        """,
        parameters={"lim": limit}
    ).result_rows
    return [{"summary_id": r[0], "from_ts": r[1].isoformat()+"Z", "to_ts": r[2].isoformat()+"Z", "text": r[3]} for r in rows]
