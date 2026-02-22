from enum import StrEnum

from src.modules.decision.adapters.mcdm import AIRMStrategy
from src.modules.decision.adapters.mcdm.electre import (
    ELECTREStrategy,
)
from src.modules.decision.adapters.mcdm.lc import (
    LinearScalarizationStrategy,
)
from src.modules.decision.adapters.mcdm.saw import SAWStrategy
from src.modules.decision.adapters.mcdm.topsis import TopsisStrategy
from src.modules.routing.application.ports.outbound.strategy.balancer_strategy_provider import (
    BalancerStrategyProvider,
)
from src.modules.decision.domain.ranking_strategy import RankingStrategy


class AlgorithmName(StrEnum):
    AIRM = "airm"
    SAW = "saw"
    TOPSIS = "topsis"
    ELECTRE = "electre"
    LinearScalarization = "lc"


class BalancerStrategyRegistry(BalancerStrategyProvider):
    def __init__(self):
        self._algos: dict[AlgorithmName, RankingStrategy] = {
            AlgorithmName.TOPSIS: TopsisStrategy(),
            AlgorithmName.SAW: SAWStrategy(),
            AlgorithmName.AIRM: AIRMStrategy(),
            AlgorithmName.ELECTRE: ELECTREStrategy(),
            AlgorithmName.LinearScalarization: LinearScalarizationStrategy(),
        }

    def get(self, name: str):
        try:
            key = AlgorithmName(name.strip().lower())
            return self._algos[key]
        except (Exception, KeyError) as e:
            raise ValueError(f"Неизвестный алгоритм: {name}") from e
