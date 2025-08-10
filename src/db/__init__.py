from __future__ import annotations

import clickhouse_connect
from typing import Optional

from ..configs import CLICKHOUSE_DB, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD


__ch_client: Optional[clickhouse_connect.driver.Client] = None

def get_ch() -> clickhouse_connect.driver.Client:
    global __ch_client
    if __ch_client is None:
        __ch_client = clickhouse_connect.get_client(
            host="localhost",
            port=8123,
            database=CLICKHOUSE_DB,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    return __ch_client


from .users import upsert_user, load_display_names        # noqa: E402
from .messages import insert_message, fetch_last_messages, tool_fetch_messages_like  # noqa: E402
from .summaries import (
    get_last_summarized_msg_id, get_next_batch, insert_summary, fetch_last_summaries
)  # noqa: E402
from .contexts import (
    get_last_context_batch_id, insert_context, fetch_last_contexts, tool_fetch_recent_contexts
)  # noqa: E402
