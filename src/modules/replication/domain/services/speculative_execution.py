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
    Здесь speculative делаем не эвристическим по p50/p95/p99,
    а в том же строгом стиле, что и hedged:
    - первая реплика сразу
    - следующая через tau_ms
    - backup запускается только если нода idle (через require_idle=True)

    threshold_ms оставлен в сигнатуре ради совместимости,
    но реальный порог приходит через tau_ms из planner.
    """
    r_eff = min(replication_max_count, max_replicas, len(ranked))
    if r_eff <= 0:
        return ReplicationPlan(targets=[])

    effective_tau = max(1, int(tau_ms or threshold_ms))

    targets: list[ReplicationTarget] = []
    for i, (nid, h, p) in enumerate(ranked[:r_eff]):
        targets.append(
            ReplicationTarget(
                node_id=nid,
                host=h,
                port=p,
                delay_ms=i * effective_tau,
                require_idle=(i > 0),
            )
        )

    return ReplicationPlan(targets=targets)
