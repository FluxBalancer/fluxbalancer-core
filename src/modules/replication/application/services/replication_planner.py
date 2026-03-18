import random
from dataclasses import dataclass

import numpy as np

from core.application.ports.strategy_provider import StrategyProvider
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import MetricsRepository
from modules.observability.application.services.latency_predictor import (
    LatencyPredictor,
)
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

    lambda_cost: float = 0.1
    min_samples_for_adaptive: int = 5


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

        latency_samples_per_node: list[list[float]] = []
        for node_id, _, _ in ranked_base:
            node_samples = await self.metrics_repository.get_latency_samples(node_id)
            node_samples = [
                float(sample)
                for sample in node_samples
                if np.isfinite(sample) and sample > 0
            ]
            latency_samples_per_node.append(node_samples)

        primary_samples = (
            latency_samples_per_node[0] if latency_samples_per_node else []
        )
        tau_ms = compute_tau_ms(
            brs=brs,
            latency_samples_ms=primary_samples,
        )

        best_replication_count: int = base_replication_count

        enough_history_for_adaptive = (
            brs.replications_adaptive is True
            and base_replication_count > 1
            and len(latency_samples_per_node) >= 2
            and all(
                len(samples) >= self.config.min_samples_for_adaptive
                for samples in latency_samples_per_node[:base_replication_count]
            )
        )
        if enough_history_for_adaptive:
            selector = AdaptiveReplicationSelector(
                lambda_cost=self.config.lambda_cost,
                wa_estimator=UniversalWAEstimator(
                    latency_samples_per_node=latency_samples_per_node
                ),
            )

            draft_plan: ReplicationPlan = await strategy.build(
                ranked_base, max_replicas=base_replication_count, tau_ms=tau_ms
            )
            delays_ms: list[int] = [t.delay_ms for t in draft_plan.targets]

            best_replication_count = selector.choose_r(
                latency_samples_per_node,
                r_max=base_replication_count,
                delays_ms=delays_ms,
            )

        # ограничиваем ranked списком best_replication_count (для fixed), но hedged/speculative сами решат delay
        ranked_cut: list[tuple[str, str, int]] = ranked[:best_replication_count]
        plan: ReplicationPlan = await strategy.build(
            ranked_cut,
            max_replicas=best_replication_count,
            tau_ms=tau_ms,
            latency_samples_per_node=latency_samples_per_node,
        )
        plan.r_eff = best_replication_count
        return plan


def compute_tau_ms(
    brs: BRSRequest,
    *,
    latency_samples_ms: list[float],
    default_tau_ms: int = 220,
    warmup_min_samples: int = 8,
    jitter: float = 0.05,
) -> int:
    samples = np.asarray(latency_samples_ms, dtype=float)
    samples = samples[np.isfinite(samples)]
    samples = samples[samples > 0]

    if samples.size == 0:
        tau = float(default_tau_ms)

    elif samples.size < warmup_min_samples:
        p50 = float(np.percentile(samples, 50))
        tau = 0.5 * p50

    else:
        p50 = float(np.percentile(samples, 50))
        p95 = float(np.percentile(samples, 95))

        tail_ratio = (p95 - p50) / max(p50, 1.0)

        if tail_ratio > 1.0:
            tau = 0.4 * p50
        elif tail_ratio > 0.5:
            tau = 0.6 * p50
        else:
            tau = 0.8 * p50

    tau = max(tau, 5.0)

    if brs.deadline_ms:
        tau = min(tau, 0.4 * brs.deadline_ms)

    if jitter > 0:
        tau *= random.uniform(1.0 - jitter, 1.0 + jitter)

    return int(max(1, round(tau)))
