from modules.observability.application.ports.metrics_repository import MetricsRepository
from modules.replication.application.ports.outbound.latency_recorder import (
    LatencyRecorder,
)


class MetricsRepositoryLatencyRecorder(LatencyRecorder):
    def __init__(self, repo: MetricsRepository):
        self.repo = repo

    async def record(self, node_id: str, latency_ms: float) -> None:
        await self.repo.add_latency(node_id, latency_ms)
