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
