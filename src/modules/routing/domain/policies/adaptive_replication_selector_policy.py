from dataclasses import dataclass

from src.modules.replication.algorithms.adaptive_selecctor import (
    adaptive_selector_replicas,
)
from src.modules.replication.domain.wa_estimator import WAEstimator


@dataclass(slots=True)
class AdaptiveReplicationSelector:
    """Адаптивный выбор числа реплик по правилу ΔL_r >= λ·ΔWA_r.

    Важно: в реальном прокси точное E[min(T)] дорого. Здесь используется аппроксимация:
        E[T(R_r)] ≈ min(T_hat_j) по ранжированному списку.

    Args:
        lambda_cost: Коэффициент "цены" репликации (чем меньше, тем агрессивнее репликация).
        wa_estimator: Оценщик прироста WA.
    """

    lambda_cost: float
    wa_estimator: WAEstimator

    def choose_r(
        self,
        latency_hat_ms: list[float],
        *,
        r_max: int,
    ) -> int:
        """Выбирает число реплик.

        Args:
            latency_hat_ms: Прогнозные задержки узлов в порядке ранга (лучший→худший).
            r_max: Верхняя граница реплик.

        Returns:
            Оптимальное r* (минимум 1).
        """
        return adaptive_selector_replicas(
            replication_count_max=r_max,
            lambda_cost=self.lambda_cost,
            wa_estimator=self.wa_estimator,
            latency_hat_ms=latency_hat_ms,
        )
