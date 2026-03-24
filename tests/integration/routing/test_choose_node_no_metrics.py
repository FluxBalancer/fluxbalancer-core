from unittest.mock import AsyncMock, Mock

import pytest

from modules.decision.adapters.outbound.strategies.topsis_strategy import TopsisStrategy
from modules.decision.adapters.outbound.weights.entropy_weights_strategy import (
    EntropyWeightsStrategy,
)
from modules.decision.domain.policies.decision_resolver_policy import (
    DecisionResolverPolicy,
)
from modules.decision.domain.ranking_strategy import RankingStrategy
from modules.decision.domain.weights_strategy import WeightsStrategy
from modules.gateway.application.dto.brs import BRSRequest
from modules.routing.application.usecase.choose_node import ChooseNodeUseCase


class FakeDecisionPolicy(DecisionResolverPolicy):
    def resolve_balancer(self, brs: BRSRequest) -> RankingStrategy:
        return TopsisStrategy()

    def resolve_weights(self, brs: BRSRequest) -> WeightsStrategy:
        return EntropyWeightsStrategy()


@pytest.mark.asyncio
async def test_choose_node_raises_when_no_metrics_available():
    metrics_repo = AsyncMock()
    metrics_repo.list_latest.return_value = []

    node_registry = Mock()

    uc = ChooseNodeUseCase(
        metrics_repo=metrics_repo,
        node_registry=node_registry,
        decision_policy=FakeDecisionPolicy(),
    )

    with pytest.raises(RuntimeError, match="Нет метрик"):
        await uc.execute(
            brs=BRSRequest(
                service=None,
                replications_count=None,
                replicate_all=False,
                deadline_ms=1000,
                balancer_strategy_name=None,
                weights_strategy_name=None,
                replication_strategy_name=None,
                completion_strategy_name=None,
                completion_k=None,
                replications_adaptive=None,
            )
        )
