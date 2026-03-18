import logging
import math

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

    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        ranked = await self.rank_nodes(brs)
        if not ranked:
            raise RuntimeError("Нет доступных нод")

        return ranked[0]

    async def rank_nodes(self, brs: BRSRequest) -> list[tuple[str, str, int]]:
        balancer_strategy: RankingStrategy = self.decision_policy.resolve_balancer(brs)
        weights_strategy: WeightsStrategy = self.decision_policy.resolve_weights(brs)

        metrics: list[NodeMetrics] = await self.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = []
        valid_nodes: list[NodeMetrics] = []

        for m in metrics:
            prev = await self.metrics_repo.get_prev(m.node_id)
            latency_samples = await self.metrics_repo.get_latency_samples(m.node_id)

            vector = self._build_feature_vector(
                metric=m,
                prev=prev,
                latency_samples=latency_samples,
                interval=settings.collector_interval,
            )

            if vector is None:
                continue

            vectors.append(vector)
            valid_nodes.append(m)

        if not vectors:
            raise RuntimeError("Нет валидных нод для ранжирования")

        X_raw: Matrix = np.vstack(vectors).astype(float)
        X_norm: Matrix = normalize_cost(X_raw)
        weights: Vector = weights_strategy.compute(X_norm)

        scores = balancer_strategy.score_all(X_norm, weights)
        order = np.argsort(-scores)

        ranked: list[tuple[str, str, int]] = []
        for idx in order.tolist():
            node: NodeMetrics = valid_nodes[int(idx)]
            host, port = self.node_registry.get_endpoint(node.node_id)
            ranked.append((node.node_id, host, port))
        return ranked

    def _build_feature_vector(
        self,
        *,
        metric: NodeMetrics,
        prev: NodeMetrics | None,
        latency_samples: list[float],
        interval: float,
    ) -> list[float] | None:
        """
        Все признаки — cost-критерии: меньше = лучше.
        Вектор:
            [cpu, mem, net, p50, p95, p99, tail_ratio, latency_trend]
        """
        cpu = self._safe(metric.cpu_util, fallback=1.0)
        mem = self._safe(metric.mem_util, fallback=1.0)

        if prev:
            delta_in = metric.net_in_bytes - prev.net_in_bytes
            delta_out = metric.net_out_bytes - prev.net_out_bytes
            net_Bps = max(delta_in, delta_out) / max(interval, 1e-6)
        else:
            net_Bps = 0.0

        # нормировка в util
        net_util = self._safe(net_Bps / (1 * 125_000_000), fallback=1.0)

        clean = np.asarray(
            [x for x in latency_samples if math.isfinite(x) and x > 0],
            dtype=float,
        )

        if clean.size == 0:
            # новый/холодный узел — не убиваем его бесконечностью,
            # но даём плохой, а не катастрофический профиль
            p50 = 2_000.0
            p95 = 4_000.0
            p99 = 5_000.0
            tail_ratio = 2.0
            trend = 1_000.0
        else:
            p50 = float(np.percentile(clean, 50))
            p95 = float(np.percentile(clean, 95))
            p99 = float(np.percentile(clean, 99))

            tail_ratio = p95 / max(p50, 1.0)

            if clean.size >= 8:
                head = clean[: clean.size // 2]
                tail = clean[clean.size // 2 :]
                prev_mean = float(np.mean(head)) if head.size else p50
                last_mean = float(np.mean(tail)) if tail.size else p50
                trend = max(0.0, last_mean - prev_mean)
            else:
                trend = 0.0

        vector = [
            cpu,
            mem,
            net_util,
            self._safe(p50, fallback=5_000.0),
            self._safe(p95, fallback=5_000.0),
            self._safe(p99, fallback=5_000.0),
            self._safe(tail_ratio, fallback=5.0),
            self._safe(trend, fallback=5_000.0),
        ]

        if not all(math.isfinite(x) for x in vector):
            return None

        return vector

    @staticmethod
    def _safe(value: float | int | None, fallback: float) -> float:
        if value is None:
            return fallback

        v = float(value)
        if not math.isfinite(v):
            return fallback

        return v
