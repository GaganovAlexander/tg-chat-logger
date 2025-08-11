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
    row = ch.query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_id = int(row[0][0] or 0)
    if last_id == 0 or n <= 0:
        return []

    from_id = max(0, last_id - n)

    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE tg_msg_id > %(from_id)s
          AND lengthUTF8(text) > 0
        ORDER BY tg_msg_id ASC
        """,
        parameters={"from_id": from_id},
    ).result_rows

    msgs = [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]

    names = load_display_names(m.user_id for m in msgs)
    for m in msgs:
        m.author = names.get(m.user_id, str(m.user_id))

    return msgs


def tool_get_messages_window(n: int = 200) -> list[dict]:
    """
    Последние n сообщений по ОКНУ message_id: (max_id - n; max_id].
    Возвращает в хронологическом порядке.
    """
    ch = get_ch()
    row = ch.query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_id = int(row[0][0] or 0)
    if last_id == 0 or n <= 0:
        return []
    from_id = max(0, last_id - n)

    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE tg_msg_id > %(from_id)s AND lengthUTF8(text) > 0
        ORDER BY tg_msg_id ASC
        """,
        parameters={"from_id": from_id}
    ).result_rows

    msgs = [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]
    names = load_display_names(m.user_id for m in msgs)
    data = []
    for m in msgs:
        data.append({
            "tg_msg_id": m.tg_msg_id,
            "user_id": m.user_id,
            "author": names.get(m.user_id, str(m.user_id)),
            "text": m.text,
            "ts": m.ts.isoformat() + "Z",
        })
    return data


def tool_search_messages(query: str, window: int = 5000, limit: int = 50) -> list[dict]:
    """
    Поиск по тексту в ОКНЕ последних `window` сообщений по message_id.
    Возвращаем самые релевантные недавние (по убыванию id).
    """
    ch = get_ch()
    row = ch.query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_id = int(row[0][0] or 0)
    if last_id == 0:
        return []
    from_id = max(0, last_id - max(0, int(window)))

    rows = ch.query(
        """
        SELECT tg_msg_id, user_id, text, ts
        FROM tg_messages
        WHERE tg_msg_id > %(from_id)s
          AND lengthUTF8(text) > 0
          AND positionCaseInsensitiveUTF8(text, %(q)s) > 0
        ORDER BY tg_msg_id DESC
        LIMIT %(lim)s
        """,
        parameters={"from_id": from_id, "q": query, "lim": limit}
    ).result_rows

    return [
        {"tg_msg_id": r[0], "user_id": r[1], "text": r[2], "ts": r[3].isoformat()+"Z"}
        for r in rows
    ]
