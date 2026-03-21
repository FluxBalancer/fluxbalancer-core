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

    return str(int(seconds))


def build_request_profile(path: str, query: Mapping[str, Any] | None) -> str:
    endpoint = _normalize_path(path)
    query = query or {}

    seconds = _safe_float(query.get("seconds"))
    bucket = _seconds_bucket(seconds)

    if endpoint == "cpu":
        return f"cpu:{bucket}"

    if endpoint == "mem":
        mb = _safe_float(query.get("mb"))
        mb_bucket = (
            "low" if mb is not None and mb < 128 else
            "mid" if mb is not None and mb < 512 else
            "high"
        )
        return f"mem:{bucket}:{mb_bucket}"

    return endpoint
