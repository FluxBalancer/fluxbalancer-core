import logging

import numpy as np

from modules.discovery.application.ports.outbound.node_registry import (
    NodeRegistry,
)
from modules.gateway.application.dto.brs import BRSRequest
from modules.decision.domain.policies.decision_resolver_policy import (
    DecisionResolverPolicy,
)
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
from src.modules.routing.config.settings import settings
from src.modules.types.numpy import Vector, Matrix

logger = logging.getLogger("decision")

SLA_MAX_LATENCY_MS = 500


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

    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        ranked = await self.rank_nodes(brs)
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        return ranked[0]

    async def rank_nodes(self, brs: BRSRequest) -> list[tuple[str, str, int]]:
        """Ранжирует узлы и возвращает список endpoint'ов (лучший→худший).

        Args:
            brs: Параметры запроса BRS.

        Returns:
            Список кортежей (node_id, host, port) по убыванию предпочтительности.
        """
        balancer_strategy: RankingStrategy = self.decision_policy.resolve_balancer(brs)
        weights_provider: WeightsStrategy = self.decision_policy.resolve_weights(brs)

        metrics: list[NodeMetrics] = await self.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = [
            m.to_vector(
                interval=settings.collector_interval,
                sla_latency_ms=SLA_MAX_LATENCY_MS,
                prev=await self.metrics_repo.get_prev(m.node_id),
            )
            for m in metrics
        ]
        X: Matrix = np.vstack(vectors).astype(float)
        weights: Vector = weights_provider.compute(X)

        scores = balancer_strategy.score_all(X, weights)
        order = np.argsort(-scores)

        ranked: list[tuple[str, str, int]] = []
        for idx in order.tolist():
            node = metrics[int(idx)]
            host, port = self.node_registry.get_endpoint(node.node_id)
            ranked.append((node.node_id, host, port))
        return ranked
