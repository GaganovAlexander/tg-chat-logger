from __future__ import annotations

from typing import List, Tuple, Optional
from datetime import datetime

from . import get_ch
from ..schemas import Msg
from .users import load_display_names


def get_oldest_ts_of_last_n(n: int) -> Optional[datetime]:
    ch = get_ch()
    row = ch.query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_id = int(row[0][0] or 0)
    if last_id == 0 or n <= 0:
        return

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
    return rows[0][0] if rows and rows[0][0] is not None else None

def fetch_contexts_since(oldest_ts: datetime) -> List[Tuple[str, datetime]]:
    """Возвращает [(text, to_ts)] контекстов, которые пересекают интервал [oldest_ts, now]."""
    ch = get_ch()
    rows = ch.query(
        """
        SELECT text, to_ts
        FROM tg_contexts
        WHERE to_ts >= %(from_ts)s   -- заканчиваются не раньше начала интервала
        ORDER BY context_id ASC
        """,
        parameters={"from_ts": oldest_ts}
    ).result_rows
    return [(r[0], r[1]) for r in rows]

def fetch_summaries_since(from_ts: datetime) -> List[Tuple[str, datetime]]:
    """[(text, to_ts)] выжимок, начинаем после from_ts (обычно конец контекста)."""
    ch = get_ch()
    rows = ch.query(
        """
        SELECT text, to_ts
        FROM tg_summaries
        WHERE from_ts > %(from_ts)s
        ORDER BY batch_id ASC
        """,
        parameters={"from_ts": from_ts}
    ).result_rows
    return [(r[0], r[1]) for r in rows]

def fetch_raw_since(from_ts: datetime, limit: int) -> List[Msg]:
    """Хвост сырых сообщений после from_ts (лимитируем, чтобы не разбухать контекст)."""
    ch = get_ch()
    row = ch.query("SELECT max(tg_msg_id) FROM tg_messages").result_rows
    last_id = int(row[0][0] or 0)
    if last_id == 0 or limit <= 0:
        return []

    from_id = max(0, last_id - limit)

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
