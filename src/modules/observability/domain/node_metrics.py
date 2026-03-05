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

    def to_vector(
        self,
        interval: float,
        prev: NodeMetrics | None = None,
    ) -> list[float]:
        """Преобразует метрику в числовой вектор для MCDM.

        Args:
            prev: Предыдущий снимок той же ноды.
            interval: Шаг измерения (секунды).
        """
        cpu = self.cpu_util
        mem = self.mem_util

        if prev:
            delta_in = self.net_in_bytes - prev.net_in_bytes
            delta_out = self.net_out_bytes - prev.net_out_bytes
            net_Bps = max(delta_in, delta_out) / max(interval, 1e-6)
        else:
            net_Bps = 0.0

        net_util = net_Bps

        lat = self.latency_ms if self.latency_ms is not None else float("inf")

        return [cpu, mem, net_util, lat]

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)
