from enum import StrEnum

from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.adapters.outbound.strategies.fixed_r_way import (
    FixedParallelReplication,
)
from modules.replication.adapters.outbound.strategies.hedged_requests import (
    HedgedReplication,
)
from modules.replication.adapters.outbound.strategies.speculative_execution import (
    SpeculativeReplication,
)


class ReplicationAlgorithmName(StrEnum):
    FIXED = "fixed"
    HEDGED = "hedged"
    SPECULATIVE = "speculative"


class ReplicationStrategyRegistry:
    """Реестр стратегий репликации.

    СУЩЕСТВЕННЫЙ совет:
    Делай значения по умолчанию здесь, а не размазывай по middleware.
    """

    def __init__(self):
        self._map: dict[str, ReplicationStrategy] = {
            ReplicationAlgorithmName.FIXED: FixedParallelReplication(r=2),
            ReplicationAlgorithmName.HEDGED: HedgedReplication(r_max=3, tau_ms=80),
            ReplicationAlgorithmName.SPECULATIVE: SpeculativeReplication(
                r_max=3, threshold_ms=120
            ),
        }

    def get(self, name: str | None) -> ReplicationStrategy:
        if name is None:
            return self._map[ReplicationAlgorithmName.FIXED]

        try:
            key = ReplicationAlgorithmName(name.strip().lower())
            return self._map[key]
        except (Exception, KeyError) as e:
            raise ValueError(f"Неизвестная стратегия репликации: {name}") from e
