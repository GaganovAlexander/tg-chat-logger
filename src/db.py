from typing import List, Tuple
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
        "SELECT tg_msg_id, user_id, text, ts "
        "FROM tg_messages WHERE tg_msg_id > %(x)s AND lengthUTF8(text) > 0 "
        "ORDER BY tg_msg_id ASC LIMIT %(lim)s",
        parameters={'x': last_to, 'lim': limit}
    ).result_rows
    return [Msg(tg_msg_id=r[0], user_id=r[1], text=r[2], ts=r[3]) for r in rows]

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
