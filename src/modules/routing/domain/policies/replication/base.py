from typing import Protocol

from src.modules.replication.domain.replication_plan import ReplicationPlan


class ReplicationStrategy(Protocol):
    """Стратегия формирования плана репликации."""

    async def build(
        self,
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
    ) -> ReplicationPlan:
        """Формирует план репликации.

        Args:
            ranked: Отранжированные узлы (node_id, host, port) по лучшему к худшему.
            max_replicas: Верхняя граница числа реплик.

        Returns:
            ReplicationPlan.
        """
        ...
