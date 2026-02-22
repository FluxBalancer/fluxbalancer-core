from modules.decision.application.ports.outbound.strategy_provider import (
    StrategyProvider,
)
from modules.decision.domain.weights_strategy import WeightsStrategy
from modules.gateway.application.dto.brs import BRSRequest
from modules.decision.domain.policies.decision_resolver_policy import (
    DecisionResolverPolicy,
)
from src.modules.decision.domain.ranking_strategy import RankingStrategy


class DefaultDecisionResolver(DecisionResolverPolicy):
    """
    Простая политика:
    - если стратегия указана в BRS → берём её
    - иначе → default
    """

    def __init__(
        self,
        balancer_provider: StrategyProvider[RankingStrategy],
        weights_provider: StrategyProvider[WeightsStrategy],
        default_balancer: RankingStrategy,
        default_weights: WeightsStrategy,
    ):
        self.balancer_provider = balancer_provider
        self.weights_provider = weights_provider

        self.default_balancer = default_balancer
        self.default_weights = default_weights

    def resolve_balancer(self, brs: BRSRequest) -> RankingStrategy:
        if brs.balancer_strategy_name is None:
            return self.default_balancer

        try:
            return self.balancer_provider.get(brs.balancer_strategy_name)
        except ValueError as e:
            raise ValueError(
                f"BRS: неизвестная стратегия балансировки: {brs.balancer_strategy_name}"
            ) from e

    def resolve_weights(self, brs: BRSRequest) -> WeightsStrategy:
        if brs.weights_strategy_name is None:
            return self.default_weights

        try:
            return self.weights_provider.get(brs.weights_strategy_name)
        except ValueError as e:
            raise ValueError(
                f"BRS: неизвестная стратегия весов: {brs.weights_strategy_name}"
            ) from e
