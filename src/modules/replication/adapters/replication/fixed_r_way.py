from dataclasses import dataclass

from src.modules.replication.adapters.algorithms.fixed_r_way import fixed_r_way
from src.modules.replication.domain.replication_plan import ReplicationPlan
from src.modules.replication.adapters.replication.base import ReplicationStrategy


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
