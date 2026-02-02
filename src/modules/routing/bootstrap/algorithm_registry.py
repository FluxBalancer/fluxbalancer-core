from enum import StrEnum

from src.modules.routing.adapters.outbound.algorithms.mcdm.airm import AIRMStrategy
from src.modules.routing.adapters.outbound.algorithms.mcdm.electre import (
    ELECTREStrategy,
)
from src.modules.routing.adapters.outbound.algorithms.mcdm.lc import (
    LinearScalarizationStrategy,
)
from src.modules.routing.adapters.outbound.algorithms.mcdm.saw import SAWStrategy
from src.modules.routing.adapters.outbound.algorithms.mcdm.topsis import TopsisStrategy
from src.modules.routing.application.ports.outbound.strategy.strategy_provider import (
    StrategyProvider,
)
from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class AlgorithmName(StrEnum):
    AIRM = "airm"
    SAW = "saw"
    TOPSIS = "topsis"
    ELECTRE = "electre"
    LinearScalarization = "lc"


class AlgorithmRegistry(StrategyProvider):
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
            return self._algos[name]
        except KeyError:
            raise ValueError(f"Неизвестный алгоритм: {name}")
