from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.observability.domain.node_metrics import NodeMetrics


class MetricsSnapshotBuilder:
    def __init__(self, repo: MetricsRepository):
        self.repo = repo

    async def build(self) -> list[dict]:
        snapshot = []
        for metric in await self.repo.list_latest():
            metric: NodeMetrics

            snapshot.append(
                {
                    "node_id": metric.node_id,
                    "cpu": metric.cpu_util,
                    "mem": metric.mem_util,
                    "net_in": metric.net_in_bytes,
                    "net_out": metric.net_out_bytes,
                    "latency": metric.latency_ms,
                }
            )
        return snapshot
