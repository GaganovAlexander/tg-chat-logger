from __future__ import annotations

from .db.materials import (
    get_oldest_ts_of_last_n, fetch_contexts_since, fetch_summaries_since, fetch_raw_since
)


def build_materials_for_last_n(n: int, raw_tail_limit: int = 200) -> tuple[list[str], list[str], list]:
    """
    Возвращает (contexts_texts, summaries_texts, raw_msgs)
    Логика: покрыть интервал последних n сообщений по времени:
      1) контексты, что пересекают этот интервал,
      2) затем выжимки после последнего контекста,
      3) затем хвост сырых (лимитируем).
    """
    oldest = get_oldest_ts_of_last_n(n)
    if not oldest:
        return [], [], []

    contexts = fetch_contexts_since(oldest)
    ctx_texts = [t for (t, _) in contexts]
    ctx_end = max((t for (_, t) in contexts), default=oldest)

    sums = fetch_summaries_since(ctx_end)
    sum_texts = [t for (t, _) in sums]
    sum_end = max((t for (_, t) in sums), default=ctx_end)

    raw_msgs = fetch_raw_since(sum_end, limit=raw_tail_limit)

    return ctx_texts, sum_texts, raw_msgs
