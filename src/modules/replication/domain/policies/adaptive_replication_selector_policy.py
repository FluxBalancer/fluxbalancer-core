from dataclasses import dataclass

from modules.replication.domain.policies.wa_estimator import WAEstimator
from modules.replication.domain.services.adaptive_selecctor import (
    adaptive_selector_replicas,
)


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
        self, latency_hat_ms: list[float], *, r_max: int, delays_ms: list[int]
    ) -> int:
        """Выбирает число реплик.

        Args:
            latency_hat_ms: Прогнозные задержки узлов в порядке ранга (лучший→худший).
            r_max: Верхняя граница реплик.
            delays_ms:

        Returns:
            Оптимальное r* (минимум 1).
        """
        return adaptive_selector_replicas(
            r_max=r_max,
            lambda_cost=self.lambda_cost,
            wa_estimator=self.wa_estimator,
            latency_hat_ms=latency_hat_ms,
            delays_ms=delays_ms,
        )
