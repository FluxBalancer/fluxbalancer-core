from dataclasses import dataclass

from modules.replication.domain.services.fixed_r_way import fixed_r_way
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy


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
        # TODO: replace [str, str, int] to normal dataclass
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
    ) -> ReplicationPlan:
        return fixed_r_way(
            replication_count=self.r, ranked=ranked, max_replicas=max_replicas
        )
