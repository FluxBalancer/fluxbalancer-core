from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

SLA_MAX_LATENCY_MS = 500


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
        self, interval: float, prev: NodeMetrics | None = None, nic_gbps: int = 1
    ) -> list[float]:
        """Преобразует метрику в числовой вектор для MCDM.

        Args:
            prev: Предыдущий снимок той же ноды.
            interval: Шаг измерения (секунды).
            nic_gbps: Пропускная способность сетевого интерфейса.

        Returns:
            Вектор [cpu_util, mem_util, net_util] в диапазоне [0,1].
        """
        cpu = self.cpu_util
        mem = self.mem_util

        # --- считаем сетевую активность как Δбайт/с --- #
        if prev:
            delta_in = self.net_in_bytes - prev.net_in_bytes
            delta_out = self.net_out_bytes - prev.net_out_bytes
            net_Bps = max(delta_in, delta_out) / max(interval, 1e-6)
        else:
            net_Bps = 0.0
        nic_Bps = nic_gbps * 125_000_000  # 1 Gb/s = 125 MB/s
        net_util = min(net_Bps / nic_Bps, 1.0)  # clamp в [0,1]

        lat: float = (self.latency_ms or 0.0) / SLA_MAX_LATENCY_MS

        return [cpu, mem, net_util, lat]

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)
