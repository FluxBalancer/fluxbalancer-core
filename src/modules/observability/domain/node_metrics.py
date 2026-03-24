from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class NodeMetrics:
    timestamp: str
    node_id: str
    cpu_util: float
    mem_util: float
    net_in_bytes: int
    net_out_bytes: int
    latency_ms: float | None = None

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)
