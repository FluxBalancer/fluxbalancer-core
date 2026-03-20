from collections.abc import Mapping
from typing import Any


def _normalize_path(path: str) -> str:
    path = (path or "").strip().lower()
    if not path:
        return "unknown"

    parts = [part for part in path.split("/") if part]
    if not parts:
        return "root"

    return parts[0]


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _seconds_bucket(seconds: float | None) -> str:
    if seconds is None:
        return "unknown"

    if seconds <= 1.5:
        return "xs"
    if seconds <= 3.0:
        return "s"
    if seconds <= 6.0:
        return "m"
    return "l"


def build_request_profile(path: str, query: Mapping[str, Any] | None) -> str:
    endpoint = _normalize_path(path)
    query = query or {}

    seconds = _safe_float(query.get("seconds"))
    seconds_key = f"{seconds:.2f}" if seconds is not None else "unknown"

    if endpoint == "cpu":
        return f"cpu:{seconds_key}"

    if endpoint == "mem":
        mb = _safe_float(query.get("mb"))
        mb_key = f"{mb:.0f}" if mb is not None else "unknown"
        return f"mem:{seconds_key}:{mb_key}"

    return endpoint
