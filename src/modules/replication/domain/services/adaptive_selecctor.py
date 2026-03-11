from modules.replication.domain.policies.wa_estimator import WAEstimator


def adaptive_selector_replicas(
    r_max: int,
    lambda_cost: float,
    wa_estimator: WAEstimator,
    latency_hat_ms: list[float],  # T^_1, T^_2, ...
    delays_ms: list[int] | None = None,  # δ_1, δ_2, ...
) -> int:
    if not latency_hat_ms:
        return 1
    if delays_ms is None:
        delays_ms = []

    r_max = min(r_max, len(latency_hat_ms))
    best_r = 1
    prev = latency_hat_ms[0]

    for r in range(2, r_max + 1):
        cur = min(prev, latency_hat_ms[r - 1])
        delta_L = prev - cur
        delay_r = delays_ms[r - 1]

        delta_WA = wa_estimator.delta_wa(delay_ms=delay_r, prev_finish_hat_ms=prev)

        if delta_L >= lambda_cost * delta_WA:
            best_r = r
            prev = cur
        else:
            break

    return best_r
