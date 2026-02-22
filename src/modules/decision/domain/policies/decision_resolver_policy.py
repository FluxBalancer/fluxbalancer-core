from typing import Protocol

from modules.decision.domain.weights_strategy import WeightsStrategy
from modules.gateway.application.dto.brs import BRSRequest
from src.modules.decision.domain.ranking_strategy import RankingStrategy


class DecisionResolverPolicy(Protocol):
    """
    Отвечает за интерпретацию BRS и выбор:
      - стратегии балансировки
      - стратегии расчёта весов
    """

    def resolve_balancer(self, brs: BRSRequest) -> RankingStrategy: ...

    def resolve_weights(self, brs: BRSRequest) -> WeightsStrategy: ...
