import random
from dataclasses import dataclass

import numpy as np

from core.application.ports.strategy_provider import StrategyProvider
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import MetricsRepository
from modules.observability.application.services.latency_predictor import LatencyPredictor
from modules.observability.domain.node_metrics import NodeMetrics
from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.adapters.outbound.strategies.hedged_requests import (
    HedgedReplication,
)
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


# TODO: extract adaptive and lambda to BRS
@dataclass(slots=True)
class PlannerConfig:
    """Конфигурация планировщика репликации."""

    lambda_cost: float = 0.5  # цена WA (под SLA)


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
        self.predictor = LatencyPredictor(metrics_repository)
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
        ranked_base: list[tuple[str, str, int]] = ranked[:base_replication_count]

        latency_hat: list[float] = []
        samples: list[float] = []

        for node_id, _, _ in ranked_base:
            predicted: float = await self.predictor.predict(node_id)
            latency_hat.append(predicted)

            node_samples = await self.metrics_repository.get_latency_samples(node_id)
            samples.extend(node_samples)

        tau_ms: int | None = None
        if isinstance(strategy, HedgedReplication):
            tau_ms = compute_tau_ms(brs=brs, latency_samples_ms=samples)

        best_replication_count: int = base_replication_count

        if brs.replications_adaptive and base_replication_count > 1:
            selector = AdaptiveReplicationSelector(
                lambda_cost=self.config.lambda_cost,
                wa_estimator=UniversalWAEstimator(
                    latency_samples_ms=samples
                ),
            )

            draft_plan: ReplicationPlan = await strategy.build(
                ranked_base, max_replicas=base_replication_count, tau_ms=tau_ms
            )
            delays_ms: list[int] = [t.delay_ms for t in draft_plan.targets]

            best_replication_count = selector.choose_r(
                latency_hat, r_max=base_replication_count, delays_ms=delays_ms
            )

        # ограничиваем ranked списком best_replication_count (для fixed), но hedged/speculative сами решат delay
        ranked_cut: list[tuple[str, str, int]] = ranked[:best_replication_count]
        plan: ReplicationPlan = await strategy.build(
            ranked_cut, max_replicas=best_replication_count, tau_ms=tau_ms
        )
        plan.r_eff = best_replication_count
        return plan


def compute_tau_ms(
        brs: BRSRequest,
        *,
        latency_samples_ms: list[float],
        default_tau_ms: int = 80,
        min_tau_ms: int = 20,
        max_tau_ms: int = 5000,
        percentile: float = 50,
        jitter: float = 0.10,
) -> int:
    """
    Вычисляет задержку запуска дополнительной реплики (τ).

    Логика:
    - τ ≈ median latency (p50)
    - если мало данных → fallback на default_tau_ms
    - τ ограничивается долей дедлайна
    - добавляется небольшой jitter для предотвращения синхронизации

    Args:
        brs:
        latency_samples_ms: наблюдения latency
        default_tau_ms:
        min_tau_ms:
        max_tau_ms:
        percentile: используемый процентиль (обычно 50–60)
        jitter:
    """

    samples = np.asarray(latency_samples_ms, dtype=float)

    samples = samples[np.isfinite(samples)]
    samples = samples[samples > 0]

    if samples.size == 0:
        tau = default_tau_ms
    else:
        tau = float(np.percentile(samples, percentile))

    # ограничение по дедлайну
    if brs.deadline_ms:
        try:
            dl = float(brs.deadline_ms)

            if dl > 0:
                tau = min(tau, dl * 0.2)
        except Exception:
            pass

    tau = int(np.clip(tau, min_tau_ms, max_tau_ms))

    if jitter > 0:
        tau = int(tau * random.uniform(1 - jitter, 1 + jitter))
        tau = int(np.clip(tau, min_tau_ms, max_tau_ms))

    return tau
