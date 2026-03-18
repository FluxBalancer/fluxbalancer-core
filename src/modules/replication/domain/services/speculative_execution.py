import numpy as np

from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def speculative_execution(
    replication_max_count: int,
    threshold_ms: int,
    ranked: list[tuple[str, str, int]],
    *,
    max_replicas: int,
    tau_ms: int,
    latency_samples_per_node: list[list[float]] | None = None,
) -> ReplicationPlan:
    """
    Нормальная ступенчатая speculative-стратегия:
    - первая реплика: сразу
    - каждая следующая: с накопленной задержкой
    - дополнительные реплики не стартуют все в один и тот же момент
    """
    r_eff = min(replication_max_count, max_replicas, len(ranked))
    if r_eff <= 0:
        return ReplicationPlan(targets=[])

    targets: list[ReplicationTarget] = []
    # fallback
    if not latency_samples_per_node or len(latency_samples_per_node) == 0:
        effective_tau = tau_ms or threshold_ms
        effective_tau = max(1, int(effective_tau))

        for i, (nid, h, p) in enumerate(ranked[:r_eff]):
            delay = i * effective_tau
            targets.append(
                ReplicationTarget(
                    node_id=nid,
                    host=h,
                    port=p,
                    delay_ms=delay,
                )
            )
        return ReplicationPlan(targets=targets)

    primary_samples = np.asarray(latency_samples_per_node[0], dtype=float)
    primary_samples = primary_samples[np.isfinite(primary_samples)]
    primary_samples = primary_samples[primary_samples > 0]

    if primary_samples.size == 0:
        base_delay = tau_ms or threshold_ms
    else:
        p50 = float(np.percentile(primary_samples, 50))
        p95 = float(np.percentile(primary_samples, 95))
        p99 = float(np.percentile(primary_samples, 99))

        tail_ratio = (p95 - p50) / max(p50, 1.0)

        if tail_ratio > 1.0:
            base_delay = 0.5 * p50
        elif tail_ratio > 0.5:
            base_delay = 0.7 * p50
        else:
            base_delay = 0.9 * p50

        base_delay = min(base_delay, 0.8 * p99)

    base_delay = max(1, int(base_delay))

    # --- строим план ---
    for i, (nid, h, p) in enumerate(ranked[:r_eff]):
        delay = int(i * base_delay)

        targets.append(
            ReplicationTarget(
                node_id=nid,
                host=h,
                port=p,
                delay_ms=delay,
            )
        )

    return ReplicationPlan(targets=targets)
