import logging
from dataclasses import asdict

import numpy as np

from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.application.ports.inbound.node.choose_node_port import (
    ChooseNodePort,
)
from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.application.ports.outbound.node.node_registry import (
    NodeRegistry,
)
from src.modules.routing.application.ports.outbound.strategy.strategy_provider import (
    StrategyProvider,
)
from src.modules.routing.application.ports.outbound.weights.weights_provider import (
    WeightsProvider,
)
from src.modules.routing.config.settings import settings
from src.modules.routing.domain.entities.node.node_metrics import NodeMetrics
from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy
from src.modules.types.numpy import Vector, Matrix

logger = logging.getLogger("decision")


class ChooseNodeUseCase(ChooseNodePort):
    def __init__(
        self,
        repo: MetricsRepository,
        registry: NodeRegistry,
        weights: WeightsProvider,
        strategy_provider: StrategyProvider,
        strategy: RankingStrategy | None,
    ):
        self.repo = repo
        self.registry = registry
        self.weights = weights
        self.strategy_provider = strategy_provider
        self.default_strategy = strategy

    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        strategy: RankingStrategy = self.__resolve_strategy(brs.balancer_strategy_name)

        metrics: list[NodeMetrics] = self.repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = [
            m.to_vector(
                interval=settings.collector_interval, prev=self.repo.get_prev(m.node_id)
            )
            for m in metrics
        ]

        X: Matrix = np.vstack(vectors).astype(float)
        w: Vector = self.weights.compute(X)

        chosen_idx: int = strategy.choose(X, w)
        chosen_node: NodeMetrics = metrics[chosen_idx]
        host, port = self.registry.get_endpoint(chosen_node.node_id)

        logger.info(
            {
                "message": "chosen_node",
                "data": {
                    "algorithm": strategy.__class__.__name__,
                    "node_id": chosen_node.node_id,
                    "cpu": chosen_node.cpu_util,
                    "mem": chosen_node.mem_util,
                    "latency": chosen_node.latency_ms,
                },
                "brs": asdict(brs),
            }
        )

        return chosen_node.node_id, host, port

    def __resolve_strategy(self, name: str | None) -> RankingStrategy:
        if name is None:
            return self.default_strategy

        try:
            return self.strategy_provider.get(name)
        except ValueError as e:
            raise ValueError(f"BRS: неизвестная стратегия балансировки: {name}") from e
