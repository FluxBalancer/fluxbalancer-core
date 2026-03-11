from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def hedged_requests(
        replication_max_count: int,
        time_delta_ms: int,
        ranked: list[tuple[str, str, int]],
        max_replicas: int,
) -> ReplicationPlan:
    r_eff = min(replication_max_count, max_replicas, len(ranked))
    targets: list[ReplicationTarget] = []
    for i, (nid, h, p) in enumerate(ranked[:r_eff]):
        delay = 0 if i == 0 else int(time_delta_ms)
        targets.append(
            ReplicationTarget(
                node_id=nid,
                host=h,
                port=p,
                delay_ms=delay
            )
        )
    return ReplicationPlan(targets=targets)
