from pydantic import BaseModel
import datetime as dt


class Msg(BaseModel):
    tg_msg_id: int
    user_id: int
    text: str
    ts: dt.datetime
    