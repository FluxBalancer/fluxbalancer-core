from enum import StrEnum

from modules.decision.adapters.outbound.strategies.airm_strategy import AIRMStrategy
from modules.decision.adapters.outbound.strategies.electre_strategy import (
    ELECTREStrategy,
)
from modules.decision.adapters.outbound.strategies.lc_strategy import (
    LinearScalarizationStrategy,
)
from modules.decision.adapters.outbound.strategies.saw_strategy import SAWStrategy
from modules.decision.adapters.outbound.strategies.topsis_strategy import TopsisStrategy
from modules.decision.application.ports.outbound.strategy_provider import (
    StrategyProvider,
)
from src.modules.decision.domain.ranking_strategy import RankingStrategy


class AlgorithmName(StrEnum):
    AIRM = "airm"
    SAW = "saw"
    TOPSIS = "topsis"
    ELECTRE = "electre"
    LinearScalarization = "lc"


class BalancerStrategyRegistry(StrategyProvider[RankingStrategy]):
    def __init__(self):
        self._algos: dict[AlgorithmName, RankingStrategy] = {
            AlgorithmName.TOPSIS: TopsisStrategy(),
            AlgorithmName.SAW: SAWStrategy(),
            AlgorithmName.AIRM: AIRMStrategy(),
            AlgorithmName.ELECTRE: ELECTREStrategy(),
            AlgorithmName.LinearScalarization: LinearScalarizationStrategy(),
        }

    def get(self, name: str) -> RankingStrategy:
        try:
            key = AlgorithmName(name.strip().lower())
            return self._algos[key]
        except (Exception, KeyError) as e:
            raise ValueError(f"Неизвестный алгоритм: {name}") from e
