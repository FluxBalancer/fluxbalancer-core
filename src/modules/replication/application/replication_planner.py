from dataclasses import dataclass

from src.modules.replication.adapters.algorithms import FixedWAEstimator
from src.modules.replication.domain.replication_plan import ReplicationPlan
from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.usecase.choose_node import ChooseNodeUseCase
from src.modules.replication.adapters.replication.adaptive_replication_selector_policy import (
    AdaptiveReplicationSelector,
)
from src.modules.replication.domain.replication_policy import ReplicationPolicy


@dataclass(slots=True)
class PlannerConfig:
    """Конфигурация планировщика репликации."""

    adaptive: bool = False
    lambda_cost: float = 1.0  # цена WA (под SLA)


class ReplicationPlanner:
    """Формирует план репликации запроса."""

    def __init__(
        self,
        chooser: ChooseNodeUseCase,
        policy: ReplicationPolicy,
        strategy_registry,
        *,
        config: PlannerConfig = PlannerConfig(),
    ):
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
        ranked = await self.chooser.rank_nodes(brs)
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        base_r = self.policy.resolve_count(brs, available_nodes=len(ranked))
        strategy = self.strategy_registry.get(brs.replication_strategy_name)

        r_eff = base_r

        if self.config.adaptive and base_r > 1:
            # latency_hat берём из метрик: ранжирование уже учитывает latency,
            # но для формулы нужен список прогнозов. Минимально: берём "порты" и
            # не усложняем: используем base_r.
            #
            # СУЩЕСТВЕННЫЙ совет:
            # Здесь стоит реально подать список latency_ms от metrics_repo по node_id.
            selector = AdaptiveReplicationSelector(
                lambda_cost=self.config.lambda_cost,
                wa_estimator=FixedWAEstimator(),
            )
            # заглушка: “чем дальше в ранге, тем хуже”
            # TODO: fix to metrics_repository
            latency_hat = [float(i) for i in range(len(ranked))]
            r_eff = selector.choose_r(latency_hat, r_max=base_r)

        # ограничиваем ranked списком r_eff (для fixed), но hedged/speculative сами решат delay
        ranked_cut = ranked[:r_eff]
        return await strategy.build(ranked_cut, max_replicas=r_eff)
