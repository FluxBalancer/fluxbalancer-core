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
from src.modules.routing.application.ports.outbound.strategy.balancer_strategy_provider import (
    BalancerStrategyProvider,
)
from src.modules.routing.application.ports.outbound.strategy.weight_strategy_provider import WeightStrategyProvider
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
        metrics_repo: MetricsRepository,
        node_registry: NodeRegistry,
        weights_strategy_provider: WeightStrategyProvider,
        balancer_strategy_provider: BalancerStrategyProvider,
        balancer_strategy: RankingStrategy | None,
        weight_strategy: WeightsProvider | None,
    ):
        self.metrics_repo = metrics_repo
        self.node_registry = node_registry

        self.weights_strategy_provider = weights_strategy_provider
        self.balancer_strategy_provider = balancer_strategy_provider

        self.default_balancer_strategy = balancer_strategy
        self.default_weight_strategy = weight_strategy

    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        balancer_strategy: RankingStrategy = self.__resolve_strategy(brs.balancer_strategy_name)

        metrics: list[NodeMetrics] = self.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = [
            m.to_vector(
                interval=settings.collector_interval, prev=self.metrics_repo.get_prev(m.node_id)
            )
            for m in metrics
        ]

        X: Matrix = np.vstack(vectors).astype(float)

        weights_provider = self.__resolve_weights_provider(brs.weights_strategy_name)
        w: Vector = weights_provider.compute(X)

        chosen_idx: int = balancer_strategy.choose(X, w)
        chosen_node: NodeMetrics = metrics[chosen_idx]
        host, port = self.node_registry.get_endpoint(chosen_node.node_id)

        logger.info(
            {
                "message": "chosen_node",
                "data": {
                    "algorithm": balancer_strategy.__class__.__name__,
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
            return self.default_balancer_strategy

        try:
            return self.balancer_strategy_provider.get(name)
        except ValueError as e:
            raise ValueError(f"BRS: неизвестная стратегия балансировки: {name}") from e

    def __resolve_weights_provider(self, name: str | None) -> WeightsProvider:
        if name is None:
            return self.default_weight_strategy
        try:
            return self.weights_strategy_provider.get(name)
        except ValueError as e:
            raise ValueError(f"BRS: неизвестная стратегия весов: {name}") from e