from typing import Protocol

from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.decision.application.ports.weights_provider import (
    WeightsProvider,
)
from src.modules.decision.domain.ranking_strategy import RankingStrategy


class DecisionPolicyResolver(Protocol):
    """
    Отвечает за интерпретацию BRS и выбор:
      - стратегии балансировки
      - стратегии расчёта весов
    """

    def resolve_balancer(self, brs: BRSRequest) -> RankingStrategy: ...

    def resolve_weights(self, brs: BRSRequest) -> WeightsProvider: ...
