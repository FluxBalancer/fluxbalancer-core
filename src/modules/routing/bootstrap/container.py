from aiohttp import ClientSession

from modules.decision.adapters.outbound.registries.balancer_strategy_registry import (
    BalancerStrategyRegistry,
    AlgorithmName,
)
from modules.decision.adapters.outbound.registries.weight_strategy_registry import (
    WeightsProviderRegistry,
    WeightsAlgorithmName,
)
from modules.decision.application.services.default_decision_resolver import (
    DefaultDecisionResolver,
)
from modules.decision.application.ports.outbound.strategy_provider import (
    StrategyProvider,
)
from modules.decision.domain.ranking_strategy import RankingStrategy
from modules.decision.domain.weights_strategy import WeightsStrategy
from modules.discovery.adapters.memory_node_registry import InMemoryNodeRegistry
from modules.discovery.application.ports.outbound.node_registry import NodeRegistry
from modules.gateway.application.use_cases.proxy_request import ProxyRequestUseCase
from modules.observability.adapters.outbound.replication_latency_recorder import (
    MetricsRepositoryLatencyRecorder,
)
from modules.observability.adapters.outbound.storage.memory_aggregation_repository import (
    InMemoryMetricsAggregationRepository,
)
from modules.observability.adapters.outbound.storage.memory_repository import (
    InMemoryMetricsRepository,
)
from modules.observability.adapters.outbound.storage.redis_repository import (
    RedisMetricsRepository,
)
from modules.replication.adapters.outbound.http.aiohttp_replication_runner import (
    AiohttpReplicationRunner,
)
from modules.replication.adapters.outbound.registry.replication_strategy_registry import (
    ReplicationStrategyRegistry,
)
from modules.replication.application.ports.outbound.latency_recorder import (
    LatencyRecorder,
)
from modules.replication.application.services.replication_planner import (
    ReplicationPlanner,
    PlannerConfig,
)
from modules.replication.domain.policies.replication_policy import ReplicationPolicy
from modules.decision.domain.policies.decision_resolver_policy import (
    DecisionResolverPolicy,
)
from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from modules.replication.application.services.replication_manager import (
    ReplicationManager,
)
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
        self.latency_recorder: LatencyRecorder

        self.registry: NodeRegistry
        self.balancer_registry: StrategyProvider[RankingStrategy]
        self.weights_registry: StrategyProvider[WeightsStrategy]

        self.decision_policy: DecisionResolverPolicy
        self.choose_node_uc: ChooseNodePort

        self.replication_policy: ReplicationPolicy
        self.replication_planner: ReplicationPlanner
        self.replication_executor: AiohttpReplicationRunner = None
        self.replication_manager: ReplicationManager = None
        self.replication_strategy_registry: ReplicationStrategyRegistry

        self.proxy_use_case: ProxyRequestUseCase = None

        self._init_repositories()
        self._init_node_registry()
        self._init_registries()
        self._init_decision_policy()

        self._init_use_cases()
        self._init_replication_policy()

    async def init_async(self, client_session: ClientSession):
        self.replication_executor = AiohttpReplicationRunner(
            client=client_session,
            latency_recorder=self.latency_recorder,
        )

        self.replication_manager = ReplicationManager(
            planner=self.replication_planner,
            executor=self.replication_executor,
        )

        self.proxy_use_case = ProxyRequestUseCase(
            choose_node=self.choose_node_uc,
            replication_manager=self.replication_manager,
            metrics_repo=self.metrics_repo,
            client=client_session,
        )

    def _init_repositories(self) -> None:
        self.metrics_agg = InMemoryMetricsAggregationRepository()

        if settings.metrics.backend is MetricsBackend.REDIS:
            self.metrics_repo = self._create_redis_metrics_repo()
        else:
            self.metrics_repo = InMemoryMetricsRepository()

        self.latency_recorder = MetricsRepositoryLatencyRecorder(repo=self.metrics_repo)

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

    async def close_redis_if_exist(self) -> None:
        if settings.metrics.backend is MetricsBackend.REDIS and isinstance(
            self.metrics_repo, RedisMetricsRepository
        ):
            await self.metrics_repo.redis.aclose()

    def _init_node_registry(self) -> None:
        self.registry = InMemoryNodeRegistry()

    def _init_registries(self) -> None:
        self.balancer_registry = BalancerStrategyRegistry()
        self.weights_registry = WeightsProviderRegistry()

    def _init_decision_policy(self) -> None:
        self.decision_policy = DefaultDecisionResolver(
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

        self.replication_executor = None
        self.replication_manager = None
