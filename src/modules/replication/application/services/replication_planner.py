from dataclasses import dataclass

from core.application.ports.strategy_provider import StrategyProvider
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import MetricsRepository
from modules.observability.domain.node_metrics import NodeMetrics
from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.policies.adaptive_replication_selector_policy import (
    AdaptiveReplicationSelector,
)
from modules.replication.domain.policies.replication_policy import ReplicationDecision
from modules.replication.domain.policies.replication_policy import ReplicationPolicy
from modules.replication.domain.services.work_amplification.universal_wa import (
    UniversalWAEstimator,
)
from src.modules.routing.application.usecase.choose_node import ChooseNodeUseCase


@dataclass(slots=True)
class PlannerConfig:
    """Конфигурация планировщика репликации."""

    adaptive: bool = False
    lambda_cost: float = 1.0  # цена WA (под SLA)


class ReplicationPlanner:
    """
    Формирует план репликации запроса.

    Examples:
        ranked:   [N1, N2, N3, N4, N5]

        base_r = 3   (хотим 3 реплики)

        адаптация:
          N1: 100ms
          N2: 110ms
          N3: 400ms

        добавление N3 почти не снижает min latency
        → r_eff = 2

    """

    def __init__(
        self,
        chooser: ChooseNodeUseCase,
        policy: ReplicationPolicy,
        strategy_registry: StrategyProvider[ReplicationStrategy],
        metrics_repository: MetricsRepository,
        *,
        config: PlannerConfig = PlannerConfig(),
    ):
        self.metrics_repository = metrics_repository
        self.chooser = chooser
        self.policy = policy
        self.strategy_registry = strategy_registry
        self.config = config

    async def build(self, brs: BRSRequest) -> ReplicationPlan:
        """Строит план репликации.

        Args:
            brs: DTO запроса BRS.

        Returns:
            ReplicationPlan.

        Raises:
            RuntimeError: Если нет доступных узлов.
        """
        ranked: list[tuple[str, str, int]] = await self.chooser.rank_nodes(brs)
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        base_replication_count: int = self.policy.resolve_count(
            ReplicationDecision(
                replicate_all=brs.replicate_all, requested_count=brs.replications_count
            ),
            available_nodes=len(ranked),
        )
        strategy: ReplicationStrategy = self.strategy_registry.get(
            brs.replication_strategy_name
        )

        # строим черновой план на base_r, чтобы получить delay_ms
        ranked_base: list[tuple[str, str, int]] = ranked[:base_replication_count]
        draft_plan: ReplicationPlan = await strategy.build(
            ranked_base, max_replicas=base_replication_count
        )
        delays_ms: list[int] = [t.delay_ms for t in draft_plan.targets]

        best_replication_count: int = base_replication_count

        if self.config.adaptive and base_replication_count > 1:
            latency_hat: list[float] = []
            for node_id, _, _ in ranked_base:
                latest: NodeMetrics | None = await self.metrics_repository.get_latest(
                    node_id
                )
                if latest and latest.latency_ms is not None:
                    latency_hat.append(float(latest.latency_ms))
                else:
                    latency_hat.append(float("inf"))

            selector = AdaptiveReplicationSelector(
                lambda_cost=self.config.lambda_cost,
                # TODO: давать не p95 latency, а просто latency
                wa_estimator=UniversalWAEstimator(
                    latency_samples_ms=self._get_latency_samples_for_wa(latency_hat)
                ),
            )

            best_replication_count = selector.choose_r(
                latency_hat, r_max=base_replication_count, delays_ms=delays_ms
            )

        # ограничиваем ranked списком best_replication_count (для fixed), но hedged/speculative сами решат delay
        ranked_cut = ranked[:best_replication_count]
        return await strategy.build(ranked_cut, max_replicas=best_replication_count)

    def _get_latency_samples_for_wa(self, latency_hat: list[float]) -> list[float]:
        # временный костыль: пока нет реальных сэмплов — хотя бы так
        # (ниже объясню, что лучше)
        return [x for x in latency_hat if x != float("inf")]
