from __future__ import annotations

from typing import Iterable, Dict

from . import get_ch


def upsert_user(user_id: int, username: str, first_name: str, last_name: str) -> None:
    ch = get_ch()
    ch.insert(
        'tg_users',
        [(user_id, username or '', first_name or '', last_name or '', None, None)],
        column_names=[
            'user_id',
            'username',
            'first_name',
            'last_name',
            'first_seen',
            'last_seen'
        ]
    )

def load_display_names(user_ids: Iterable[int]) -> Dict[int, str]:
    ch = get_ch()
    ids = list({int(x) for x in user_ids})
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
