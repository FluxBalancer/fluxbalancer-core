from src.modules.decision.application.decision_policy_resolver import (
    DecisionPolicyResolver,
)
from src.modules.decision.application.ports.balancer_strategy_provider import (
    BalancerStrategyProvider,
)
from src.modules.decision.application.ports.weight_strategy_provider import (
    WeightStrategyProvider,
)
from src.modules.observability.adapters.docker.docker_collector import (
    DockerMetricsCollector,
)
from src.modules.observability.adapters.docker.extractors.network import (
    NetworkExtractorPolicy,
)
from src.modules.observability.adapters.storage.memory_aggregation_repository import (
    InMemoryMetricsAggregationRepository,
)
from src.modules.observability.application.ports.collector import CollectorManager
from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.observability.application.usecase.metrics_updater import MetricsUpdater
from src.modules.replication.adapters.registries.replication_strategy_registry import (
    ReplicationStrategyRegistry,
)
from src.modules.replication.application.replication_executor import (
    ReplicationExecutor,
)
from src.modules.replication.application.replication_manager import (
    ReplicationManager,
)
from src.modules.replication.application.replication_planner import (
    ReplicationPlanner,
    PlannerConfig,
)
from src.modules.replication.domain.replication_policy import ReplicationPolicy
from src.modules.routing.application.ports.choose_node_port import (
    ChooseNodePort,
)
from src.modules.routing.application.usecase.choose_node import ChooseNodeUseCase
from src.modules.routing.config.settings import settings, MetricsBackend


class RoutingModule:
    """
    Содержит все "склеенные" зависимости модуля routing,
    чтобы main.py был максимально тонким.
    """

    def __init__(self):
        self.metrics_repo: MetricsRepository
        self.metrics_agg: InMemoryMetricsAggregationRepository
        self.registry: NodeRegistry
        self.balancer_registry: BalancerStrategyProvider
        self.weights_registry: WeightStrategyProvider
        self.decision_policy: DecisionPolicyResolver
        self.choose_node_uc: ChooseNodePort
        self.collector: CollectorManager
        self.updater: MetricsUpdater

        self.replication_policy: ReplicationPolicy
        self.replication_planner: ReplicationPlanner
        self.replication_executor: ReplicationExecutor
        self.replication_manager: ReplicationManager
        self.replication_strategy_registry: ReplicationStrategyRegistry

        self._init_repositories()
        self._init_registry()
        self._init_strategies()
        self._init_decision_policy()

        self._init_use_cases()
        self._init_replication_policy()

        # self._init_metrics_collector()

    def _init_repositories(self) -> None:
        if settings.metrics.backend is MetricsBackend.REDIS:
            self.metrics_repo = self._create_redis_metrics_repo()
        else:
            self.metrics_repo = InMemoryMetricsRepository()

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
        self.registry = InMemoryNodeRegistry()

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
            metrics_repo=self.metrics_repo,
            node_registry=self.registry,
            decision_policy=self.decision_policy,
        )

    def _init_metrics_collector(self) -> None:
        extractors = self._create_metric_extractors()

        self.collector = DockerMetricsCollector(
            repo=self.metrics_repo,
            registry_updater=self.registry,
            extractors=extractors,
        )

        self.updater = MetricsUpdater(
            collector=self.collector,
            collector_interval=settings.collector_interval,
        )

    def _create_metric_extractors(self):
        return [
            CpuExtractorPolicy(),
            MemoryExtractorPolicy(),
            NetworkExtractorPolicy(),
        ]

    def _init_replication_policy(self):
        self.replication_strategy_registry = ReplicationStrategyRegistry()
        self.replication_policy = ReplicationPolicy(
            default_replicas=1,
            max_replicas=5,
        )

        self.replication_planner = ReplicationPlanner(
            chooser=self.choose_node_uc,
            policy=self.replication_policy,
            strategy_registry=self.replication_strategy_registry,
            config=PlannerConfig(adaptive=True, lambda_cost=1.0),
        )

        self.replication_executor = ReplicationExecutor(
            metrics_repo=self.metrics_repo,
        )

        self.replication_manager = ReplicationManager(
            planner=self.replication_planner,
            executor=self.replication_executor,
        )
