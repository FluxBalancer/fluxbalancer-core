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
from src.modules.routing.application.ports.outbound.weights.weights_provider import (
    WeightsProvider,
)
from src.modules.routing.application.ports.policies.decision_policy_resolver import (
    DecisionPolicyResolver,
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
        decision_policy: DecisionPolicyResolver,
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
        weights_provider: WeightsProvider = self.decision_policy.resolve_weights(brs)

        metrics: list[NodeMetrics] = await self.metrics_repo.list_latest()
        if not metrics:
            raise RuntimeError("Нет метрик: коллектор ещё не собрал данные")

        vectors: list[list[float]] = [
            m.to_vector(
                interval=settings.collector_interval,
                prev=await self.metrics_repo.get_prev(m.node_id),
            )
            for m in metrics
        ]
        X: Matrix = np.vstack(vectors).astype(float)
        weights: Vector = weights_provider.compute(X)

        # ВАЖНО: текущие стратегии выбирают только argmax.
        # Для репликации нам нужен полный ранжир.
        # Решение: считать "score" в адаптере. Для простоты — делаем ранжирование здесь
        # через "частичный" расчёт для каждого алгоритма.
        # Самый чистый вариант — расширить интерфейс RankingStrategy (см. совет ниже).
        scores = self._score_all(balancer_strategy, X, weights)
        order = np.argsort(-scores)  # по убыванию

        ranked: list[tuple[str, str, int]] = []
        for idx in order.tolist():
            node = metrics[int(idx)]
            host, port = self.node_registry.get_endpoint(node.node_id)
            ranked.append((node.node_id, host, port))
        return ranked

    def _score_all(self, strategy: RankingStrategy, X: Matrix, w: Vector) -> Vector:
        """Возвращает score для всех альтернатив для сортировки.

        СУЩЕСТВЕННЫЙ совет:
        Лучше расширить протокол RankingStrategy:
            - choose()
            - score_all()
        Тогда эта функция уйдёт в стратегии-адаптеры, и use-case станет чище.
        """
        name = strategy.__class__.__name__.lower()

        # SAW / LinearScalarization: score = X@w
        if "saw" in name or "linear" in name or "scalar" in name:
            return X @ w

        # TOPSIS: score = C_i (как в твоей реализации topsis())
        if "topsis" in name:
            norm = np.linalg.norm(X, axis=0)
            norm[norm == 0] = 1.0
            R = X / norm
            V = R * w
            A_pos = V.min(axis=0)
            A_neg = V.max(axis=0)
            D_pos = np.linalg.norm(V - A_pos, axis=1)
            D_neg = np.linalg.norm(V - A_neg, axis=1)
            denom = D_pos + D_neg
            C = np.divide(D_neg, denom, out=np.zeros_like(denom), where=denom != 0)
            return C

        # ELECTRE: score = число "побеждённых"
        if "electre" in name:
            m = X.shape[0]
            conc_th = 0.6
            disc_th = 0.4
            concordance = np.zeros((m, m))
            discordance = np.zeros((m, m))
            for i in range(m):
                for j in range(m):
                    if i == j:
                        continue
                    mask = X[i] <= X[j]
                    concordance[i, j] = w[mask].sum()
                    diff = (X[i] - X[j]) / (X.max(axis=0) - X.min(axis=0) + 1e-12)
                    discordance[i, j] = diff.max()
            outrank = (concordance >= conc_th) & (discordance <= disc_th)
            return outrank.sum(axis=1).astype(float)

        # AIRM: score = частота побед (Monte-Carlo)
        if "airm" in name:
            # простой Monte Carlo — дорого, но m маленькое (12).
            # Для ранжирования: считаем counts и нормируем в [0,1]
            n_iter = 500
            alpha = w * 5
            rng = np.random.default_rng()
            counts = np.zeros(X.shape[0], dtype=float)

            # нормализация критериев в [0,1] как benefit (в твоём airm())
            X_adj = X.copy()
            # все критерии у тебя cost -> benefit через инверсию по max
            X_adj = X_adj.max(axis=0) - X_adj
            col_min = X_adj.min(axis=0)
            col_max = X_adj.max(axis=0)
            denom = np.where(col_max == col_min, 1.0, col_max - col_min)
            X_norm = (X_adj - col_min) / denom

            for _ in range(n_iter):
                w_rand = rng.dirichlet(alpha)
                s = X_norm @ w_rand
                counts[int(s.argmax())] += 1.0
            return counts / max(counts.max(), 1.0)

        # fallback: выбрать только победителя как score=1, остальные 0
        out = np.zeros(X.shape[0], dtype=float)
        out[int(strategy.choose(X, w))] = 1.0
        return out
