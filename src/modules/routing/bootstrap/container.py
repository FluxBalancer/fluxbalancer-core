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
from src.modules.routing.adapters.outbound.weights.weights_provider import (
    EntropyWeightsProvider,
)
from src.modules.routing.application.usecase.metrics.metrics_updater import (
    MetricsUpdater,
)
from src.modules.routing.application.usecase.node.choose_node import ChooseNodeUseCase
from src.modules.routing.bootstrap.algorithm_registry import (
    AlgorithmRegistry,
    AlgorithmName,
)
from src.modules.routing.config.settings import settings
from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class RoutingModule:
    """
    Содержит все "склеенные" зависимости модуля routing,
    чтобы main.py был максимально тонким.
    """

    def __init__(self):
        self.repo = InMemoryMetricsRepository(history_limit=32)
        self.registry = DockerNodeRegistry()
        self.weights = EntropyWeightsProvider()

        self.algorithm_registry = AlgorithmRegistry()
        self.strategy: RankingStrategy = self.algorithm_registry.get(
            AlgorithmName.TOPSIS
        )

        self.choose_node_uc = ChooseNodeUseCase(
            repo=self.repo,
            registry=self.registry,
            weights=self.weights,
            strategy=self.strategy,
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
