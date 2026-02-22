from typing import Protocol


class LatencyRecorder(Protocol):
    async def record(self, node_id: str, latency_ms: float) -> None: ...
