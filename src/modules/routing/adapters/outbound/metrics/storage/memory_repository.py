from __future__ import annotations

from collections import defaultdict, deque
from threading import RLock

import numpy as np

from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.domain.entities.node.node_metrics import NodeMetrics


class InMemoryMetricsRepository(MetricsRepository):
    """
    Хранит историю по каждому node_id отдельно.

    - NodeMetrics (CPU/MEM/NET) → snapshot history
    - latency_ms → event history (sliding window)
    """

    def __init__(
        self,
        history_limit: int = 32,
        latency_window: int = 100,
    ):
        self._lock = RLock()
        self._history: dict[str, deque[NodeMetrics]] = defaultdict(
            lambda: deque(maxlen=history_limit)
        )
        self._latency: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=latency_window)
        )

    def _latency_p95(self, node_id: str) -> float | None:
        window = self._latency.get(node_id)
        if not window:
            return None

        return float(np.percentile(window, 95))

    def _with_latency(self, m: NodeMetrics) -> NodeMetrics:
        return NodeMetrics(
            timestamp=m.timestamp,
            node_id=m.node_id,
            cpu_util=m.cpu_util,
            mem_util=m.mem_util,
            net_in_bytes=m.net_in_bytes,
            net_out_bytes=m.net_out_bytes,
            latency_ms=self._latency_p95(m.node_id),
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

    async def add_latency(self, node_id: str, latency_ms: float) -> None:
        """
        Добавляет latency-событие для узла.
        """
        with self._lock:
            self._latency[node_id].append(latency_ms)
