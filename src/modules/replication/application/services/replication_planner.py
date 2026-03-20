import logging
from dataclasses import dataclass

import numpy as np

from core.application.ports.strategy_provider import StrategyProvider
from modules.gateway.application.dto.brs import BRSRequest
from modules.observability.application.ports.metrics_repository import MetricsRepository
from modules.replication.adapters.outbound.registries.replication_strategy_registry import (
    ReplicationAlgorithmName,
)
from modules.replication.adapters.outbound.strategies.base import ReplicationStrategy
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.policies.adaptive_replication_selector_policy import AdaptiveReplicationSelector
from modules.replication.domain.policies.replication_policy import ReplicationDecision
from modules.replication.domain.policies.replication_policy import ReplicationPolicy
from modules.replication.domain.services.work_amplification.universal_wa import UniversalWAEstimator
from src.modules.routing.application.usecase.choose_node import ChooseNodeUseCase

logger = logging.getLogger("replication.planner")


@dataclass(slots=True)
class PlannerConfig:
    """
    Конфигурация строгого planner'а.

    hedge_quantile:
        Квантиль, по которому запускается backup.
        В академической и промышленной практике для hedged requests
        обычно используют p95 latency для класса запросов.

    min_samples:
        Минимум наблюдений, после которого можно доверять эмпирическому квантилю.

    max_adaptive_replicas:
        Для уменьшения tail latency при completion=first достаточно primary + 1 backup.
        Большее число реплик резко увеличивает work amplification.
    """

    lambda_cost: float = 0.25
    adaptive_min_samples: int = 8

    # hedge_quantile: float = 84 # p99 ci95 = [-0.22; 9.94] -- wa=1.186 -- 4 версия  ::: score = 4.09
    # hedge_quantile: float = 65 # p99 ci95 = [9.06; 14.61] -- wa=1.496 -- 6 версия  ::: score = 7.91
    # hedge_quantile: float = 50 # p99 ci95 = [6.45; 17.51] -- wa=1.548 -- 7 версия  ::: score = 7.74
    # hedge_quantile: float = 40 # p99 ci95 = [8.97 – 19.87] -- wa=1.571 -- 8 версия  ::: score = 9.17
    # hedge_quantile: float = 30 # p99 ci95 = [2.9; 23.3] -- wa=1.619 -- 9 версия  ::: score = 8.09
    # hedge_quantile: float = 38 # p99 ci95 = [] -- wa= --  версия  ::: score
    # hedge_quantile: float = 39 # p99 ci95 = [9.58; 15.21] -- wa=1.679 -- 12 версия  ::: score = 7.509
                                 # p99 ci95 = [8.5 ; 15.02] -- wa=1.602 -- 12 версия  ::: score = 7.342 (adaptive)
    hedge_quantile: float = 41 # p99 ci95 = [14.61; 19.61] -- wa=1.679 -- 13 версия  ::: score = 10.18
                               # p99 ci95 = [13.87; 22.07] -- wa=1.509 -- 13 версия  ::: score = 11.9 (adaptive)

    min_samples: int = 16
    max_adaptive_replicas: int = 3

    backup_max_inflight: int = 6


class ReplicationPlanner:
    """
    Строгий планировщик репликации.

    Для hedged/speculative:
      - порог запуска backup = эмпирический p95
        по request-profile для primary node;
      - если данных недостаточно, hedging отключается;
      - для first-valid tail mitigation используем не более 2 реплик.

    Без эвристик tail_ratio / lambda_cost / Monte-Carlo.
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

    async def build(
            self,
            brs: BRSRequest,
            request_profile: str | None = None,
    ) -> ReplicationPlan:
        ranked: list[tuple[str, str, int]] = await self.chooser.rank_nodes(
            brs,
            request_profile=request_profile,
        )
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        base_replication_count: int = self.policy.resolve_count(
            ReplicationDecision(
                replicate_all=brs.replicate_all, requested_count=brs.replications_count
            ),
            available_nodes=len(ranked),
        )

        strategy_name = (
            (brs.replication_strategy_name or ReplicationAlgorithmName.FIXED.value)
            .strip()
            .lower()
        )
        strategy: ReplicationStrategy = self.strategy_registry.get(strategy_name)

        ranked_base = ranked[:base_replication_count]

        # FIXED оставляем как есть.
        if strategy_name == ReplicationAlgorithmName.FIXED.value:
            plan = await strategy.build(
                ranked_base,
                max_replicas=base_replication_count,
                tau_ms=None,
                latency_samples_per_node=None,
                backup_max_inflight=None,
            )
            plan.r_eff = len(plan.targets)
            return plan

        # Для hedged/speculative нужен строгий эмпирический timeout backup-запуска.
        primary_node_id = ranked_base[0][0]
        hedge_delay_ms = await self._estimate_backup_delay_ms(
            node_id=primary_node_id,
            request_profile=request_profile,
        )

        # Недостаточно данных -> не хеджируем.
        if hedge_delay_ms is None:
            node_id, host, port = ranked[0]
            logger.info(
                "hedging disabled: not enough samples for node=%s profile=%s",
                node_id,
                request_profile,
            )
            return await strategy.build(
                ranked=[(node_id, host, port)],
                max_replicas=1,
                tau_ms=None,
                latency_samples_per_node=None,
                backup_max_inflight=None,
            )

        # Число реплик, которые вообще могут стартовать до дедлайна.
        deadline_ms = int(brs.deadline_ms)
        effective_delay = max(1, int(hedge_delay_ms))

        launch_horizon_ms = max(1, int(deadline_ms * 0.75))

        max_r_by_deadline = 1 + ((launch_horizon_ms - 1) // effective_delay)
        hard_cap = min(
            base_replication_count,
            max_r_by_deadline,
            self.config.max_adaptive_replicas
        )

        ranked_cut = ranked[:hard_cap]

        latency_samples_per_node: list[list[float]] = []
        for node_id, _, _ in ranked_cut:
            node_samples = await self.metrics_repository.get_latency_samples(
                node_id,
                profile=request_profile,
            )
            node_samples = self._sanitize_samples(node_samples)
            latency_samples_per_node.append(node_samples)

        if (
            brs.replications_adaptive
            and len(ranked_cut) >= 2
            and all(len(samples) >= self.config.adaptive_min_samples for samples in latency_samples_per_node)
        ):
            delays_ms: list[int] = [i * effective_delay for i in range(len(ranked_cut))]
            wa_estimator = UniversalWAEstimator(
                latency_samples_per_node=latency_samples_per_node
            )
            selector = AdaptiveReplicationSelector(
                lambda_cost=self.config.lambda_cost,
                wa_estimator=wa_estimator,
            )
            effective_replication_count = selector.choose_r(
                samples_per_node=latency_samples_per_node,
                r_max=hard_cap,
                delays_ms=delays_ms,
            )
        else:
            effective_replication_count = hard_cap

        plan = await strategy.build(
            ranked_cut,
            max_replicas=effective_replication_count,
            tau_ms=effective_delay,
            latency_samples_per_node=latency_samples_per_node,
            backup_max_inflight=self.config.backup_max_inflight,
        )
        plan.r_eff = len(plan.targets)

        logger.info(
            "replication plan: strategy=%s deadline_ms=%s hedge_delay_ms=%s targets=%s",
            strategy_name,
            brs.deadline_ms,
            effective_delay,
            [(t.node_id, t.delay_ms) for t in plan.targets],
        )

        return plan

    async def _estimate_backup_delay_ms(
            self,
            *,
            node_id: str,
            request_profile: str | None,
    ) -> int | None:
        """
        Оценка порога backup-запуска.

        Приоритет:
        1. p95 по request_profile на primary node
        2. p95 по всем запросам на primary node
        3. иначе None (hedging отключаем)

        Это лучше, чем смешивать все узлы/все классы запросов в один tau.
        """
        if request_profile is not None:
            profile_samples = await self.metrics_repository.get_latency_samples(
                node_id=node_id,
                profile=request_profile,
            )
            profile_samples = self._sanitize_samples(profile_samples)

            if len(profile_samples) >= self.config.min_samples:
                return self._quantile_ms(profile_samples, self.config.hedge_quantile)

        global_samples = await self.metrics_repository.get_latency_samples(
            node_id=node_id
        )
        global_samples = self._sanitize_samples(global_samples)

        if len(global_samples) >= self.config.min_samples:
            return self._quantile_ms(global_samples, self.config.hedge_quantile)

        return None

    @staticmethod
    def _sanitize_samples(samples: list[float]) -> list[float]:
        result: list[float] = []
        for value in samples:
            value = float(value)
            if np.isfinite(value) and value > 0:
                result.append(value)
        return result

    @staticmethod
    def _quantile_ms(samples: list[float], q: float) -> int:
        return max(1, int(round(float(np.percentile(np.asarray(samples), q)))))
