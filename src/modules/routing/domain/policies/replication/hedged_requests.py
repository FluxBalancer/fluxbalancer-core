from dataclasses import dataclass

from src.modules.replication.algorithms.hedged_requests import hedged_requests
from src.modules.replication.domain.replication_plan import ReplicationPlan
from src.modules.routing.domain.policies.replication.base import ReplicationStrategy


@dataclass(slots=True)
class HedgedReplication(ReplicationStrategy):
    """Отложенная репликация (hedged requests).

    Строит последовательность целей: первая сразу, остальные с шагом tau_ms.

    Args:
        r_max: Максимум реплик.
        tau_ms: Шаг запуска дополнительной реплики.
    """

    r_max: int
    tau_ms: int

    async def build(
        self,
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
    ) -> ReplicationPlan:
        return hedged_requests(
            replication_max_count=self.r_max,
            time_delta_ms=self.tau_ms,
            ranked=ranked,
            max_replicas=max_replicas,
        )
