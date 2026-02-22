from dataclasses import dataclass
from typing import Mapping


@dataclass(slots=True)
class ExecutionResult:
    node_id: str
    status: int
    body: bytes
    headers: Mapping[str, str] | None = None
    latency_ms: float = 0.0
