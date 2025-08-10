from typing import List, Tuple
import json
import time

import httpx

from .schemas import Msg
from .configs import (
    LLM_PROVIDER,
    GROQ_API_KEY, GROQ_BASEURL, GROQ_MODEL,
    OPENAI_API_KEY, OPENAI_BASEURL, OPENAI_MODEL
)
from .db.logger import (
    log_llm_chat_start, log_llm_chat_end, log_llm_tool_request, log_llm_tool_result, log_exception
)


async def _chat_complete(messages: list, temperature: float = 0.2) -> Tuple[str, int, int]:
    if LLM_PROVIDER == "groq":
        url = f"{GROQ_BASEURL}/chat/completions"
        headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
        model = GROQ_MODEL
        provider = "groq"
    elif LLM_PROVIDER == "openai":
        url = f"{OPENAI_BASEURL}/chat/completions"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        model = OPENAI_MODEL
        provider = "openai"
    else:
        raise RuntimeError(f"Неизвестный LLM_PROVIDER: {LLM_PROVIDER}")

    payload = {"model": model, "messages": messages, "temperature": temperature}

    req_meta = log_llm_chat_start(provider, model, messages, temperature)
    t0 = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(url, headers=headers, json=payload)
            r.raise_for_status()
            data = r.json()

        text = (data["choices"][0]["message"]["content"] or "").strip()
        usage = data.get("usage") or {}
        tin  = int(usage.get("prompt_tokens") or 0)
        tout = int(usage.get("completion_tokens") or 0)
        dt_ms = int((time.perf_counter() - t0) * 1000)

        log_llm_chat_end(provider, model, req_meta, text, usage, latency_ms=dt_ms, ok=True)
        return text, tin, tout

    except Exception:
        dt_ms = int((time.perf_counter() - t0) * 1000)
        log_llm_chat_end(provider, model, req_meta, response_text="", usage=None, latency_ms=dt_ms, ok=False, error="HTTP/Parse error")
        log_exception(ctx=f"_chat_complete provider={provider} model={model}")
        raise


async def summarize_messages(msgs: List[Msg]) -> tuple[str,int,int]:
    lines = [f"{m.ts.isoformat()}Z | {m.author}: {m.text}" for m in msgs]
    content = (
        "Ты — ассистент, который делает краткие выжимки переписок Telegram.\n"
        "Сожми сообщения ниже в 5–10 пунктов: ключевые факты, решения, договорённости, вопросы, ссылки.\n"
        "Опусти оффтоп/шутки. Будь конкретным и кратким.\n\n"
        + "\n".join(lines)
    )
    messages = [
        {"role": "system", "content": "Ты делаешь точные и лаконичные саммари чатов."},
        {"role": "user", "content": content},
    ]
    return await _chat_complete(messages, temperature=0.2)


async def summarize_summaries(sums: List[str]) -> Tuple[str, int, int]:
    joined = "\n\n".join(f"- Выжимка {i}:\n{s}" for i, s in enumerate(sums, 1))
    content = (
        "Сверни ряд выжимок в общий контекст (10–15 коротких пунктов):\n"
        "• темы и решения по порядку времени,\n"
        "• важные изменения и договорённости,\n"
        "• открытые вопросы/TODO.\n\n"
        + joined
    )
    messages = [
        {"role": "system", "content": "Ты агрегируешь выжимки в компактную хронику."},
        {"role": "user", "content": content},
    ]
    return await _chat_complete(messages, temperature=0.2)


RAG_SYSTEM = (
    "Ты — помощник с доступом к базе чата. "
    "Если тебе нужны данные, верни ЕДИНСТВЕННЫЙ блок в формате:\n"
    "TOOL: {\"name\":\"fetch_messages_like\",\"args\":{\"query\":\"...\",\"limit\":50,\"days\":30}}\n"
    "или\n"
    "TOOL: {\"name\":\"fetch_recent_summaries\",\"args\":{\"limit\":10}}\n"
    "или\n"
    "TOOL: {\"name\":\"fetch_recent_contexts\",\"args\":{\"limit\":5}}\n"
    "Если данных достаточно — сразу дай ответ без TOOL."
)

def parse_tool_call(text: str) -> dict | None:
    marker = "TOOL:"
    idx = text.find(marker)
    if idx == -1:
        return None
    js = text[idx+len(marker):].strip()
    try:
        return json.loads(js)
    except Exception:
        return None
