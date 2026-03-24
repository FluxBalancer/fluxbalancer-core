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
from modules.observability.domain.node_metrics import NodeMetrics
from modules.routing.application.usecase.choose_node import ChooseNodeUseCase


class FakeDecisionPolicy(DecisionResolverPolicy):
    def resolve_balancer(self, brs: BRSRequest) -> RankingStrategy:
        return TopsisStrategy()

    def resolve_weights(self, brs: BRSRequest) -> WeightsStrategy:
        return EntropyWeightsStrategy()


@pytest.mark.asyncio
async def test_choose_node_returns_best():
    metrics_repo = AsyncMock()
    metrics_repo.list_latest.return_value = [
        NodeMetrics("t", "node1", 0.9, 0.9, 0, 0, 200),  # хуже
        NodeMetrics("t", "node2", 0.1, 0.1, 0, 0, 50),  # лучше
    ]
    metrics_repo.get_prev.return_value = None
    metrics_repo.get_latency_samples.side_effect = [
        [200, 210, 220],  # node1
        [50, 60, 70],  # node2
    ]

    node_registry = Mock()
    node_registry.get_endpoint.side_effect = [
        ("host1", 8001),
        ("host2", 8002),
    ]

    uc = ChooseNodeUseCase(
        metrics_repo=metrics_repo,
        node_registry=node_registry,
        decision_policy=FakeDecisionPolicy(),
    )
    result = await uc.execute(
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
    assert result[0] == "node2"
