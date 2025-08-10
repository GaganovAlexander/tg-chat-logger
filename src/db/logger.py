from __future__ import annotations

import datetime as dt
import json
import traceback
from typing import Any, Dict, List, Optional

from . import get_ch


_TABLE = "logs"

def _clip(s: str | None, max_chars: int = 20_000) -> str:
    if not s:
        return ""
    return s if len(s) <= max_chars else s[:max_chars] + f"... [truncated {len(s)-max_chars} chars]"

def _safe_json(obj: Any, max_chars: int = 200_000) -> str:
    try:
        raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        raw = json.dumps({"_unserializable": str(type(obj))}, ensure_ascii=False)
    return _clip(raw, max_chars)

def log_event(meta: Dict[str, Any]) -> None:
    """Пишет произвольный JSON в logs.meta_raw (никогда не бросает исключений)."""
    try:
        ch = get_ch()
        payload = _safe_json(meta)
        ch.insert(
            _TABLE,
            [(dt.datetime.utcnow(), payload)],
            column_names=["event_time", "meta_raw"]
        )
    except Exception:
        pass


def log_llm_chat_start(provider: str, model: str, messages: List[Dict[str, str]], temperature: float) -> Dict[str, Any]:
    meta = {
        "type": "llm.chat_start",
        "provider": provider,
        "model": model,
        "temperature": temperature,
        "request": {"messages": messages},
    }
    log_event(meta)
    return meta

def log_llm_chat_end(provider: str, model: str, request_meta: Dict[str, Any],
                     response_text: str, usage: Optional[Dict[str, Any]],
                     latency_ms: int, ok: bool = True, error: Optional[str] = None) -> None:
    meta = {
        "type": "llm.chat_end",
        "provider": provider,
        "model": model,
        "ok": ok,
        "latency_ms": latency_ms,
        "response": {"text": _clip(response_text), "usage": usage or {}},
        "request_ref": {"temperature": request_meta.get("temperature")},
    }
    if not ok and error:
        meta["error"] = _clip(error, 8000)
    log_event(meta)

def log_llm_tool_request(draft_text: str, tool_json: Dict[str, Any]) -> None:
    log_event({
        "type": "llm.tool_request",
        "llm_draft": _clip(draft_text),
        "tool": tool_json,
    })

def log_llm_tool_result(name: str, args: Dict[str, Any], result: Any) -> None:
    preview: Any = result
    try:
        if isinstance(result, list):
            preview = {"count": len(result), "sample": result[:3]}
    except Exception:
        preview = {"repr": repr(result)}
    log_event({
        "type": "llm.tool_result",
        "tool": {"name": name, "args": args},
        "result_preview": preview,
    })

def log_exception(ctx: str) -> None:
    log_event({
        "type": "exception",
        "context": ctx,
        "traceback": _clip(traceback.format_exc())
    })
