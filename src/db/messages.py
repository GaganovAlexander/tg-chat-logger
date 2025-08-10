from __future__ import annotations

from typing import List

from . import get_ch
from ..schemas import Msg
from .users import load_display_names


def insert_message(m: Msg) -> None:
    ch = get_ch()
    ch.insert(
        'tg_messages',
        [(m.tg_msg_id, m.user_id, m.text, m.ts)],
        column_names=['tg_msg_id','user_id','text','ts']
    )

def fetch_last_messages(n: int) -> List[Msg]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE lengthUTF8(text) > 0
        ORDER BY tg_msg_id DESC
        LIMIT %(n)s
        """,
        parameters={"n": n}
    ).result_rows
    rows.reverse()
    msgs = [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]
    names = load_display_names(m.user_id for m in msgs)
    for m in msgs:
        m.author = names.get(m.user_id, str(m.user_id))
    return msgs

def tool_fetch_messages_like(query: str, limit: int = 50, days: int = 30) -> list[dict]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE ts >= now() - INTERVAL %(days)s DAY
          AND positionCaseInsensitiveUTF8(text, %(q)s) > 0
        ORDER BY ts DESC
        LIMIT %(lim)s
        """,
        parameters={"days": days, "q": query, "lim": limit}
    ).result_rows
    return [{"tg_msg_id": r[0], "user_id": r[1], "text": r[2], "ts": r[3].isoformat()+"Z"} for r in rows]
