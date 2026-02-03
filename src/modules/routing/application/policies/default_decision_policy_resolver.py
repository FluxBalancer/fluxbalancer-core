from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.ports.outbound.strategy.balancer_strategy_provider import (
    BalancerStrategyProvider,
)
from src.modules.routing.application.ports.outbound.strategy.weight_strategy_provider import (
    WeightStrategyProvider,
)
from src.modules.routing.application.ports.outbound.weights.weights_provider import (
    WeightsProvider,
)
from src.modules.routing.application.ports.policies.decision_policy_resolver import DecisionPolicyResolver
from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class DefaultDecisionPolicyResolver(DecisionPolicyResolver):
    """
    Простая политика:
    - если стратегия указана в BRS → берём её
    - иначе → default
    """

    def __init__(
            self,
            balancer_provider: BalancerStrategyProvider,
            weights_provider: WeightStrategyProvider,
            default_balancer: RankingStrategy,
            default_weights: WeightsProvider,
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

    def resolve_weights(self, brs: BRSRequest) -> WeightsProvider:
        if brs.weights_strategy_name is None:
            return self.default_weights

        try:
            return self.weights_provider.get(brs.weights_strategy_name)
        except ValueError as e:
            raise ValueError(
                f"BRS: неизвестная стратегия весов: {brs.weights_strategy_name}"
            ) from e
