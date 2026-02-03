from __future__ import annotations

from src.modules.routing.adapters.outbound.metrics.docker.docker_collector import (
    DockerMetricsCollector,
)
from src.modules.routing.adapters.outbound.metrics.docker.extractors.cpu import (
    CpuExtractor,
)
from src.modules.routing.adapters.outbound.metrics.docker.extractors.memory import (
    MemoryExtractor,
)
from src.modules.routing.adapters.outbound.metrics.docker.extractors.network import (
    NetworkExtractor,
)
from src.modules.routing.adapters.outbound.metrics.storage.memory_aggregation_repository import (
    InMemoryMetricsAggregationRepository,
)
from src.modules.routing.adapters.outbound.metrics.storage.memory_repository import (
    InMemoryMetricsRepository,
)
from src.modules.routing.adapters.outbound.registry.docker_node_registry import (
    DockerNodeRegistry,
)
from src.modules.routing.adapters.outbound.strategy.balancer_strategy_registry import (
    BalancerStrategyRegistry,
    AlgorithmName,
)
from src.modules.routing.adapters.outbound.strategy.weight_strategy_registry import WeightsProviderRegistry, \
    WeightsAlgorithmName
from src.modules.routing.application.policies.default_decision_policy_resolver import DefaultDecisionPolicyResolver
from src.modules.routing.application.usecase.metrics.metrics_updater import (
    MetricsUpdater,
)
from src.modules.routing.application.usecase.node.choose_node import ChooseNodeUseCase
from src.modules.routing.config.settings import settings


class RoutingModule:
    """
    Содержит все "склеенные" зависимости модуля routing,
    чтобы main.py был максимально тонким.
    """

    def __init__(self):
        self.repo = InMemoryMetricsRepository(history_limit=32)
        self.registry = DockerNodeRegistry()

        self.balancer_registry = BalancerStrategyRegistry()
        self.weights_registry = WeightsProviderRegistry()

        self.decision_policy = DefaultDecisionPolicyResolver(
            balancer_provider=self.balancer_registry,
            weights_provider=self.weights_registry,
            default_balancer=self.balancer_registry.get(AlgorithmName.TOPSIS),
            default_weights=self.weights_registry.get(WeightsAlgorithmName.ENTROPY),
        )

        self.choose_node_uc = ChooseNodeUseCase(
            metrics_repo=self.repo,
            node_registry=self.registry,
            decision_policy=self.decision_policy,
        )

        extractors = [
            CpuExtractor(),
            MemoryExtractor(),
            NetworkExtractor(),
        ]

        self.metrics_agg = InMemoryMetricsAggregationRepository()

        self.collector = DockerMetricsCollector(
            repo=self.repo,
            registry_updater=self.registry,
            extractors=extractors,
        )

        self.updater = MetricsUpdater(
            collector=self.collector,
            collector_interval=settings.collector_interval,
        )
