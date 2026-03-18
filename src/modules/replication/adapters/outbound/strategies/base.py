from typing import Protocol

from modules.replication.domain.model.replication_plan import ReplicationPlan


class ReplicationStrategy(Protocol):
    """Стратегия формирования плана репликации."""

    async def build(
        self,
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
        tau_ms: int | None = None,
        latency_samples_per_node: list[list[float]] | None = None
    ) -> ReplicationPlan:
        """Формирует план репликации.

        Args:
            ranked: Отранжированные узлы (node_id, host, port) по лучшему к худшему.
            max_replicas: Верхняя граница числа реплик.
            tau_ms:
            latency_samples_per_node:

        Returns:
            ReplicationPlan.
        """
        ...
