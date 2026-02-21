from src.modules.replication.domain.wa_estimator import WAEstimator


def adaptive_selector_replicas(
    replication_count_max: int,
    lambda_cost: float,
    wa_estimator: WAEstimator,
    latency_hat_ms: list[float],
) -> int:
    if not latency_hat_ms:
        return 1

    r_max = min(replication_count_max, len(latency_hat_ms))
    # базовое r=1
    best_r = 1
    prev = latency_hat_ms[0]

    for r in range(2, r_max + 1):
        cur = min(prev, latency_hat_ms[r - 1])
        delta_L = prev - cur
        delta_WA = wa_estimator.delta_wa(r)

        if delta_L >= lambda_cost * delta_WA:
            best_r = r
            prev = cur
        else:
            break

    return best_r
