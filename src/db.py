from typing import List, Tuple, Iterable, Dict
import datetime as dt

import clickhouse_connect

from .schemas import Msg
from .configs import (
    CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD
)


ch = clickhouse_connect.get_client(
    host="localhost", port=8123,
    database=CLICKHOUSE_DB,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD
)

def upsert_user(user_id:int, username:str, first_name:str, last_name:str):
    now = dt.datetime.now(dt.timezone.utc)
    ch.insert(
        'tg_users',
        [(user_id, username or '', first_name or '', last_name or '', now, now)],
        column_names=['user_id','username','first_name','last_name','first_seen','last_seen']
    )

def insert_message(m: Msg):
    ch.insert(
        'tg_messages',
        [(m.tg_msg_id, m.user_id, m.text, m.ts)],
        column_names=['tg_msg_id','user_id','text','ts']
    )

def get_last_summarized_msg_id() -> int:
    r = ch.query("SELECT max(to_msg_id) FROM tg_summaries").result_rows
    return int(r[0][0] or 0)

def get_next_batch(last_to: int, limit: int) -> List[Msg]:
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

def insert_summary(batch_id: int, msgs: List[Msg], text: str, tokens_in: int, tokens_out: int):
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

def get_last_context_batch_id() -> int:
    r = ch.query("SELECT max(to_batch_id) FROM tg_contexts").result_rows
    return int(r[0][0] or 0)

def insert_context(context_id: int, batches: List[Tuple[int, str, dt.datetime, dt.datetime]], text: str, ti: int, to: int):
    from_id = batches[0][0]
    to_id = batches[-1][0]
    from_ts = batches[0][2]
    to_ts = batches[-1][3]
    ch.insert(
        'tg_contexts',
        [(context_id,
          from_id,
          to_id,
          from_ts,
          to_ts,
          text,
          ti,
          to)],
        column_names=[
            'context_id',
            'from_batch_id',
            'to_batch_id',
            'from_ts',
            'to_ts',
            'text',
            'tokens_in',
            'tokens_out'
        ]
    )


def load_display_names(user_ids: Iterable[int]) -> Dict[int, str]:
    ids = list(set(int(x) for x in user_ids))
    if not ids:
        return {}
    rows = ch.query(
        """
        SELECT
          user_id,
          argMax(username, last_seen)   AS username,
          argMax(first_name, last_seen) AS first_name,
          argMax(last_name,  last_seen) AS last_name
        FROM tg_users
        WHERE user_id IN %(ids)s
        GROUP BY user_id
        """,
        parameters={"ids": ids}
    ).result_rows

    out: Dict[int, str] = {}
    for uid, uname, fn, ln in rows:
        fn = (fn or "").strip()
        ln = (ln or "").strip()
        if fn or ln:
            disp = (fn + " " + ln).strip()
        elif uname:
            disp = uname
        else:
            disp = str(uid)
        out[int(uid)] = disp

    for uid in ids:
        out.setdefault(uid, str(uid))
    return out


def fetch_last_messages(n: int) -> List[Msg]:
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

def fetch_last_summaries(k: int) -> list[str]:
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

def fetch_last_contexts(c: int) -> list[str]:
    rows = ch.query(
        """
        SELECT text
        FROM tg_contexts
        ORDER BY context_id DESC
        LIMIT %(c)s
        """,
        parameters={"c": c}
    ).result_rows
    return [r[0] for r in rows[::-1]]


def tool_fetch_messages_like(query: str, limit: int = 50, days: int = 30) -> list[dict]:
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

def tool_fetch_recent_summaries(limit: int = 10) -> list[dict]:
    rows = ch.query(
        """
        SELECT batch_id, from_ts, to_ts, text
        FROM tg_summaries
        ORDER BY batch_id DESC
        LIMIT %(lim)s
        """,
        parameters={"lim": limit}
    ).result_rows
    return [{"batch_id": r[0], "from_ts": r[1].isoformat()+"Z", "to_ts": r[2].isoformat()+"Z", "text": r[3]} for r in rows]

def tool_fetch_recent_contexts(limit: int = 5) -> list[dict]:
    rows = ch.query(
        """
        SELECT context_id, from_ts, to_ts, text
        FROM tg_contexts
        ORDER BY context_id DESC
        LIMIT %(lim)s
        """,
        parameters={"lim": limit}
    ).result_rows
    return [{"context_id": r[0], "from_ts": r[1].isoformat()+"Z", "to_ts": r[2].isoformat()+"Z", "text": r[3]} for r in rows]
