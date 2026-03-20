from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def hedged_requests(
    replication_max_count: int,
    time_delta_ms: int,
    ranked: list[tuple[str, str, int]],
    max_replicas: int,
    latency_samples_per_node: list[list[float]] | None = None,
    backup_max_inflight: int | None = None,
) -> ReplicationPlan:
    """
    Строгая hedged-схема:
    - primary: сразу
    - каждый следующий backup: с задержкой i * time_delta_ms
    - backup-реплики помечаются require_idle=True
    """
    r_eff = min(replication_max_count, max_replicas, len(ranked))

    targets: list[ReplicationTarget] = []
    for i, (nid, h, p) in enumerate(ranked[:r_eff]):
        delay = i * int(time_delta_ms)
        targets.append(
            ReplicationTarget(
                node_id=nid,
                host=h,
                port=p,
                delay_ms=delay,
                max_inflight=(backup_max_inflight if i > 0 else None),
            )
        )
    return ReplicationPlan(targets=targets)
