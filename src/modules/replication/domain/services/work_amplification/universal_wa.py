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

    latency_samples_ms: list[float]


    def delta_wa(self, *, delay_ms: float, prev_finish_hat_ms: float) -> float:
        if delay_ms <= 0:
            return 1.0  # старт сразу => почти наверняка "запустится"

        # если есть эмпирические данные — считаем S(t) = P(T>t) = 1 - F(t)
        samples = [x for x in self.latency_samples_ms if math.isfinite(x) and x >= 0]
        if samples:
            # эмпирическая CDF:
            # F(t) = (время наблюдений <= t) / N
            samples_sorted = sorted(samples)
            idx = bisect.bisect_right(samples_sorted, delay_ms)
            F = idx / len(samples_sorted)

            # survival-функция
            S = 1.0 - F
            return max(0.0, min(1.0, S))

        # fallback: экспоненциальная модель
        # P(T > t) = exp(-t / μ)
        mu = max(float(prev_finish_hat_ms), 1e-6)
        return math.exp(-float(delay_ms) / mu)
