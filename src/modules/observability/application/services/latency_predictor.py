import numpy as np

from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)


class LatencyPredictor:
    """
    Прогнозирует latency узла.

    Использует:
    - p95 latency
    - EMA
    - небольшой safety factor
    """

    def __init__(self, repo: MetricsRepository):
        self.repo = repo

    async def predict(self, node_id: str) -> float:
        samples = await self.repo.get_latency_samples(node_id)

        if not samples:
            return float("inf")

        arr = np.asarray(samples)

        p95 = np.percentile(arr, 95)
        return p95
