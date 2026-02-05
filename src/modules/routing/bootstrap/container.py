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
from src.modules.routing.adapters.outbound.metrics.storage.redis_repository import (
    RedisMetricsRepository,
)
from src.modules.routing.adapters.outbound.registry.docker_node_registry import (
    DockerNodeRegistry,
)
from src.modules.routing.adapters.outbound.strategy.balancer_strategy_registry import (
    BalancerStrategyRegistry,
    AlgorithmName,
)
from src.modules.routing.adapters.outbound.strategy.weight_strategy_registry import (
    WeightsProviderRegistry,
    WeightsAlgorithmName,
)
from src.modules.routing.application.policies.default_decision_policy_resolver import (
    DefaultDecisionPolicyResolver,
)
from src.modules.routing.application.usecase.metrics.metrics_updater import (
    MetricsUpdater,
)
from src.modules.routing.application.usecase.node.choose_node import ChooseNodeUseCase
from src.modules.routing.config.settings import settings, MetricsBackend


class RoutingModule:
    """
    Содержит все "склеенные" зависимости модуля routing,
    чтобы main.py был максимально тонким.
    """

    def __init__(self):
        self._init_repositories()
        self._init_registry()
        self._init_strategies()
        self._init_decision_policy()
        self._init_use_cases()

        self._init_metrics_collector()

    def _init_repositories(self) -> None:
        if settings.metrics.backend is MetricsBackend.REDIS:
            self.repo = self._create_redis_metrics_repo()
        else:
            self.repo = InMemoryMetricsRepository()

        self.metrics_agg = InMemoryMetricsAggregationRepository()

    def _create_redis_metrics_repo(self) -> RedisMetricsRepository:
        import redis.asyncio as redis

        cfg = settings.metrics.redis

        redis_client = redis.from_url(
            cfg.url,
            decode_responses=True,
        )

        return RedisMetricsRepository(
            redis_client=redis_client,
            history_limit=cfg.history_limit,
            latency_window=cfg.latency_window,
        )

    def _init_registry(self) -> None:
        self.registry = DockerNodeRegistry()

    def _init_strategies(self) -> None:
        self.balancer_registry = BalancerStrategyRegistry()
        self.weights_registry = WeightsProviderRegistry()

    def _init_decision_policy(self) -> None:
        self.decision_policy = DefaultDecisionPolicyResolver(
            balancer_provider=self.balancer_registry,
            weights_provider=self.weights_registry,
            default_balancer=self.balancer_registry.get(AlgorithmName.TOPSIS),
            default_weights=self.weights_registry.get(WeightsAlgorithmName.ENTROPY),
        )

    def _init_use_cases(self) -> None:
        self.choose_node_uc = ChooseNodeUseCase(
            metrics_repo=self.repo,
            node_registry=self.registry,
            decision_policy=self.decision_policy,
        )

    def _init_metrics_collector(self) -> None:
        extractors = self._create_metric_extractors()

        self.collector = DockerMetricsCollector(
            repo=self.repo,
            registry_updater=self.registry,
            extractors=extractors,
        )

        self.updater = MetricsUpdater(
            collector=self.collector,
            collector_interval=settings.collector_interval,
        )

    def _create_metric_extractors(self):
        return [
            CpuExtractor(),
            MemoryExtractor(),
            NetworkExtractor(),
        ]
