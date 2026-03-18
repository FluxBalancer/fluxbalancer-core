import numpy as np

from modules.replication.domain.policies.wa_estimator import WAEstimator


def _estimate_tail_latency(
    samples_per_node: list[list[float]],
    delays_ms: list[int],
    *,
    q: float = 0.99,
    mc_samples: int = 1000,
) -> float:
    """
    Оценивает q-квантиль времени завершения:
        min(T1, δ2 + T2, δ3 + T3, ...)

    через Monte-Carlo выборку.
    """

    if not samples_per_node:
        return float("inf")

    r = len(samples_per_node)
    draws = []

    for _ in range(mc_samples):
        times = []
        for i in range(r):
            samples = samples_per_node[i]
            if not samples:
                continue

            t = samples[np.random.randint(len(samples))]
            delay = delays_ms[i] if i < len(delays_ms) else 0

            times.append(t + delay)
        if times:
            draws.append(min(times))
    if not draws:
        return float("inf")
    return float(np.percentile(draws, q * 100))


def adaptive_selector_replicas(
    r_max: int,
    lambda_cost: float,
    wa_estimator: WAEstimator,
    delays_ms: list[int] | None,
    samples_per_node: list[list[float]],
) -> int:
    """
    Выбор числа реплик через оценку tail latency.
    """
    if not samples_per_node:
        return 1
    if delays_ms is None:
        delays_ms = [0] * len(samples_per_node)

    r_max = min(r_max, len(samples_per_node))
    best_r = 1
    prev_tail = _estimate_tail_latency(
        samples_per_node[:1],
        delays_ms[:1],
    )

    for r in range(2, r_max + 1):
        cur_tail = _estimate_tail_latency(
            samples_per_node[:r],
            delays_ms[:r],
        )
        if prev_tail <= 0:
            delta_L = 0.0
        else:
            delta_L = (prev_tail - cur_tail) / prev_tail

        delay_r = delays_ms[r - 1] if r - 1 < len(delays_ms) else 0
        delta_WA = wa_estimator.delta_wa(
            delay_ms=delay_r,
            prev_finish_hat_ms=prev_tail,
            active_prefix=r - 1,
            delays_ms=delays_ms,
        )

        if delta_L >= lambda_cost * delta_WA:
            best_r = r
            prev_tail = cur_tail
        else:
            break

    return best_r
