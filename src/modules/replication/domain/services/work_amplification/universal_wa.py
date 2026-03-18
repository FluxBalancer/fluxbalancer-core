import bisect
import math
from dataclasses import dataclass

from modules.replication.domain.policies.wa_estimator import WAEstimator


@dataclass
class UniversalWAEstimator(WAEstimator):
    """
    Универсальный оценщик ΔWA_r.

    ΔWA_r = P(T > delay_ms)
    где:
        T — случайная величина времени завершения запроса,
        F(t) = P(T <= t) — функция распределения,
        S(t) = 1 - F(t) — survival-функция.

    Мы оцениваем S(t) либо эмпирически по наблюдениям, либо через экспоненциальную модель.
    """

    latency_samples_per_node: list[list[float]]

    def delta_wa(
        self,
        *,
        delay_ms: float,
        prev_finish_hat_ms: float,
        active_prefix: int,
        delays_ms: list[int],
    ) -> float:
        if delay_ms <= 0:
            return 1.0

        survival_prod = 1.0

        for i in range(active_prefix):
            node_samples = self.latency_samples_per_node[i]
            effective_delay = delay_ms - delays_ms[i]
            if effective_delay <= 0:
                continue

            S = self._survival(node_samples, effective_delay, prev_finish_hat_ms)
            survival_prod *= S

        return survival_prod

    def _survival(self, samples: list[float], t: float, fallback_mu: float) -> float:
        samples = [x for x in samples if math.isfinite(x) and x >= 0]

        if not samples:
            # fallback: экспоненциальная модель
            mu = max(float(fallback_mu), 1e-6)
            return math.exp(-t / mu)

        samples_sorted = sorted(samples)
        idx = bisect.bisect_right(samples_sorted, t)
        F = idx / len(samples_sorted)
        return max(0.0, min(1.0, 1.0 - F))
