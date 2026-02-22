from dataclasses import dataclass

from modules.replication.domain.services.hedged_requests import hedged_requests
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy


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
