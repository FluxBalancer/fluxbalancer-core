import logging

import numpy as np

from config.settings import settings
from modules.decision.domain.normalization import normalize_cost
from modules.decision.domain.policies.decision_resolver_policy import (
    DecisionResolverPolicy,
)
from modules.discovery.application.ports.outbound.node_registry import (
    NodeRegistry,
)
from modules.gateway.application.dto.brs import BRSRequest
from src.modules.decision.domain.ranking_strategy import RankingStrategy
from src.modules.decision.domain.weights_strategy import (
    WeightsStrategy,
)
from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.observability.domain.node_metrics import NodeMetrics
from src.modules.routing.application.ports.choose_node_port import (
    ChooseNodePort,
)
from src.modules.types.numpy import Vector, Matrix

logger = logging.getLogger("decision")


class ChooseNodeUseCase(ChooseNodePort):
    def __init__(
        self,
        metrics_repo: MetricsRepository,
        node_registry: NodeRegistry,
        decision_policy: DecisionResolverPolicy,
    ):
        self.metrics_repo = metrics_repo
        self.node_registry = node_registry
        self.decision_policy = decision_policy

    async def execute(
        self,
        brs: BRSRequest,
        request_profile: str | None = None,
    ) -> tuple[str, str, int]:
        ranked = await self.rank_nodes(brs, request_profile)
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        return ranked[0]

    async def rank_nodes(
        self,
        brs: BRSRequest,
        request_profile: str | None = None,
    ) -> list[tuple[str, str, int]]:
        """Ранжирует узлы и возвращает список endpoint'ов (лучший→худший).

        Args:
            brs: Параметры запроса BRS.

        Returns:
            Список кортежей (node_id, host, port) по убыванию предпочтительности.
        """
        balancer_strategy: RankingStrategy = self.decision_policy.resolve_balancer(brs)
        weights_strategy: WeightsStrategy = self.decision_policy.resolve_weights(brs)

        metrics: list[NodeMetrics] = await self.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = []
        for m in metrics:
            prev: NodeMetrics | None = await self.metrics_repo.get_prev(m.node_id)

            profile_samples = []
            if request_profile is not None:
                profile_samples = await self.metrics_repo.get_latency_samples(
                    m.node_id,
                    profile=request_profile,
                )

            if profile_samples:
                latency_ms = float(np.percentile(profile_samples, 95))
            else:
                global_samples = await self.metrics_repo.get_latency_samples(m.node_id)
                latency_ms = (
                    float(np.percentile(global_samples, 95))
                    if global_samples
                    else float("inf")
                )

            cpu: float = m.cpu_util
            mem: float = m.mem_util

            if prev:
                delta_in = m.net_in_bytes - prev.net_in_bytes
                delta_out = m.net_out_bytes - prev.net_out_bytes
                net_Bps = max(delta_in, delta_out) / max(
                    settings.collector_interval, 1e-6
                )
            else:
                net_Bps = 0.0

            net_util: float = net_Bps / (1 * 125_000_000)

            vectors.append([cpu, mem, net_util, latency_ms])

        X_raw: Matrix = np.vstack(vectors).astype(float)
        X_norm: Matrix = normalize_cost(X_raw)
        weights: Vector = weights_strategy.compute(X_norm)

        scores = balancer_strategy.score_all(X_norm, weights)
        order = np.argsort(-scores)

        ranked: list[tuple[str, str, int]] = []
        for idx in order.tolist():
            node: NodeMetrics = metrics[int(idx)]
            host, port = self.node_registry.get_endpoint(node.node_id)
            ranked.append((node.node_id, host, port))
        return ranked
