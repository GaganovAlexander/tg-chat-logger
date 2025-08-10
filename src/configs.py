from os import getenv

from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv(), override=True)

BOT_TOKEN = getenv("BOT_TOKEN")

N = int(getenv("N", "100"))
K = int(getenv("K", "10"))


LLM_PROVIDER = getenv("LLM_PROVIDER", "groq").lower()

GROQ_API_KEY = getenv("GROQ_API_KEY", "")
GROQ_MODEL   = getenv("GROQ_MODEL", "llama-3.1-8b-instant")
GROQ_BASEURL = "https://api.groq.com/openai/v1"

OPENAI_API_KEY = getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = getenv("OPENAI_MODEL", "gpt-5")
OPENAI_BASEURL = getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")


CLICKHOUSE_DB = getenv("CLICKHOUSE_DB", "default")
CLICKHOUSE_USER = getenv("CLICKHOUSE_USER","default")
CLICKHOUSE_PASSWORD = getenv("CLICKHOUSE_PASSWORD","")


def _parse_ids(val: str) -> set[int]:
    if not val:
        return set()
    out = set()
    for part in val.split(","):
        part = part.strip()
        if not part:
            continue
        out.add(int(part))
    return out

ALLOWED_CHAT_IDS = _parse_ids(getenv("ALLOWED_CHAT_IDS", ""))
single = getenv("ALLOWED_CHAT_ID")
if single:
    ALLOWED_CHAT_IDS.add(int(single))
