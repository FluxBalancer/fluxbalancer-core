from dataclasses import dataclass

from src.modules.replication.algorithms.speculative_execution import (
    speculative_execution,
)
from src.modules.replication.domain.replication_plan import ReplicationPlan
from src.modules.routing.domain.policies.replication.base import ReplicationStrategy


@dataclass(slots=True)
class SpeculativeReplication(ReplicationStrategy):
    """Спекулятивная репликация (speculative execution).

    Практически в онлайн-прокси без CDF/квантилей чаще реализуется
    как «запустить доп. реплики после порога», то есть близко к hedged.
    Отличие: порог может задаваться более “жёстко” и/или зависеть от наблюдаемой задержки.

    Здесь базовая реализация: первая сразу, остальные с порогом threshold_ms.

    Args:
        r_max: Максимум реплик.
        threshold_ms: Порог ожидания, после которого разрешается запуск следующей реплики.
    """

    r_max: int
    threshold_ms: int

    async def build(
        self,
        ranked: list[tuple[str, str, int]],
        *,
        max_replicas: int,
    ) -> ReplicationPlan:
        return speculative_execution(
            replication_max_count=self.r_max,
            threshold_ms=self.threshold_ms,
            ranked=ranked,
            max_replicas=max_replicas,
        )
