from collections import defaultdict, deque
from threading import RLock

import numpy as np

from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.observability.domain.node_metrics import NodeMetrics


class InMemoryMetricsRepository(MetricsRepository):
    """
    Хранит историю по каждому node_id отдельно.

    - NodeMetrics (CPU/MEM/NET) → snapshot history
    - latency_ms → event history (sliding window)
    """

    def __init__(
        self,
        history_limit: int = 512,
        latency_window: int = 512,
    ):
        self._lock = RLock()
        self._history: dict[str, deque[NodeMetrics]] = defaultdict(
            lambda: deque(maxlen=history_limit)
        )
        self._latency: dict[str, dict[str, deque[float]]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=latency_window))
        )

    def _latency_p90(self, node_id: str) -> float | None:
        values = self._collect_latency_values(node_id=node_id, profile=None)
        if not values:
            return None
        return float(np.percentile(values, 90))

    def _with_latency(self, m: NodeMetrics) -> NodeMetrics:
        return NodeMetrics(
            timestamp=m.timestamp,
            node_id=m.node_id,
            cpu_util=m.cpu_util,
            mem_util=m.mem_util,
            net_in_bytes=m.net_in_bytes,
            net_out_bytes=m.net_out_bytes,
            latency_ms=self._latency_p90(m.node_id),
        )

    async def upsert(self, metrics: NodeMetrics) -> None:
        with self._lock:
            self._history[metrics.node_id].append(metrics)

    async def get_latest(self, node_id: str) -> NodeMetrics | None:
        with self._lock:
            h = self._history.get(node_id)
            if not h:
                return None
            return self._with_latency(h[-1])

    async def get_prev(self, node_id: str) -> NodeMetrics | None:
        with self._lock:
            h = self._history.get(node_id)
            if not h or len(h) < 2:
                return None
            return self._with_latency(h[-2])

    async def list_latest(self) -> list[NodeMetrics]:
        with self._lock:
            return [self._with_latency(h[-1]) for h in self._history.values() if h]

    async def add_latency(
        self,
        node_id: str,
        latency_ms: float,
        profile: str | None = None,
    ) -> None:
        key = profile or "__all__"
        with self._lock:
            self._latency[node_id][key].append(latency_ms)

    async def get_latency_samples(
        self,
        node_id: str,
        profile: str | None = None,
    ) -> list[float]:
        with self._lock:
            return self._collect_latency_values(node_id=node_id, profile=profile)

    async def clear(self) -> None:
        with self._lock:
            self._history.clear()
            self._latency.clear()

    def _collect_latency_values(
        self,
        node_id: str,
        profile: str | None = None,
    ) -> list[float]:
        profiles = self._latency.get(node_id)
        if not profiles:
            return []

        if profile is not None:
            window = profiles.get(profile)
            return list(window) if window else []

        values: list[float] = []
        for window in profiles.values():
            values.extend(window)
        return values
