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
from modules.replication.domain.policies.replication_policy import ReplicationDecision
from modules.replication.domain.policies.replication_policy import ReplicationPolicy
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

    max_tail_replicas:
        Для уменьшения tail latency при completion=first достаточно primary + 1 backup.
        Большее число реплик резко увеличивает work amplification.
    """

    # hedge_quantile: float = 84 # p99 = [1.34; 13.75]
    hedge_quantile: float = 83
    min_samples: int = 32
    max_tail_replicas: int = 4

    backup_max_inflight: int = 4


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
                backup_max_inflight=None
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
                backup_max_inflight=None
            )

        # Число реплик, которые вообще могут стартовать до дедлайна.
        max_r_by_deadline = 1 + max(
            0, (int(brs.deadline_ms) - 1) // max(1, hedge_delay_ms)
        )
        effective_replication_count = min(base_replication_count, max_r_by_deadline)

        ranked_cut = ranked[:effective_replication_count]

        latency_samples_per_node: list[list[float]] = []
        for node_id, _, _ in ranked_cut:
            node_samples = await self.metrics_repository.get_latency_samples(
                node_id,
                profile=request_profile,
            )
            node_samples = self._sanitize_samples(node_samples)
            latency_samples_per_node.append(node_samples)

        plan = await strategy.build(
            ranked_cut,
            max_replicas=effective_replication_count,
            tau_ms=hedge_delay_ms,
            latency_samples_per_node=latency_samples_per_node,
            backup_max_inflight=self.config.backup_max_inflight
        )
        plan.r_eff = len(plan.targets)

        logger.info(
            "replication plan: strategy=%s deadline_ms=%s hedge_delay_ms=%s targets=%s",
            strategy_name,
            brs.deadline_ms,
            hedge_delay_ms,
            [(t.node_id, t.delay_ms, t.require_idle) for t in plan.targets],
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
