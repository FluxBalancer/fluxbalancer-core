from dataclasses import dataclass

from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.services.hedged_requests import hedged_requests


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
        tau_ms: int | None = None,
        latency_samples_per_node: list[list[float]] | None = None,
        backup_max_inflight: int | None
    ) -> ReplicationPlan:
        effective_tau = int(tau_ms) if tau_ms is not None else int(self.tau_ms)
        return hedged_requests(
            replication_max_count=self.r_max,
            time_delta_ms=effective_tau,
            ranked=ranked,
            max_replicas=max_replicas,
            latency_samples_per_node=latency_samples_per_node,
            backup_max_inflight=backup_max_inflight,
        )
