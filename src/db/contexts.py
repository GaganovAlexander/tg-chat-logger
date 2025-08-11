from __future__ import annotations

from typing import List, Tuple
import datetime as dt

from . import get_ch
from ..configs import N


def get_last_context_batch_id() -> int:
    ch = get_ch()
    r = ch.query("SELECT max(to_batch_id) FROM tg_contexts").result_rows
    return int(r[0][0] or 0)

def insert_context(context_id: int, batches: List[Tuple[int, str, dt.datetime, dt.datetime]], text: str, ti: int, to: int) -> None:
    ch = get_ch()
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

def fetch_last_contexts(c: int) -> list[str]:
    ch = get_ch()
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


def tool_get_contexts(limit: int = 5) -> list[dict]:
    ch = get_ch()
    rows = ch.query(
        """
        SELECT context_id, from_batch_id, to_batch_id, from_ts, to_ts, text
        FROM tg_contexts
        ORDER BY context_id DESC
        LIMIT %(lim)s
        """,
        parameters={"lim": limit}
    ).result_rows
    rows.reverse()
    out = []
    for ctx_id, from_b, to_b, from_ts, to_ts, text in rows:
        approx = max(1, (to_b - from_b + 1)) * N
        out.append({
            "context_id": ctx_id,
            "from_ts": from_ts.isoformat() + "Z",
            "to_ts": to_ts.isoformat() + "Z",
            "approx_messages": approx,
            "text": text
        })
    return out
