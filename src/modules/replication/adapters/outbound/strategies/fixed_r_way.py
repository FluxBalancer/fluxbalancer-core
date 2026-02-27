from dataclasses import dataclass

from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.services.fixed_r_way import fixed_r_way


@dataclass(slots=True)
class FixedParallelReplication(ReplicationStrategy):
    """Фиксированная параллельная репликация (fixed r-way).

    Все реплики запускаются одновременно.

    Args:
        r: Число реплик.
    """

    r: int

    async def build(
        self,
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
        tau_ms: int | None = None,
    ) -> ReplicationPlan:
        return fixed_r_way(
            replication_count=self.r, ranked=ranked, max_replicas=max_replicas
        )
