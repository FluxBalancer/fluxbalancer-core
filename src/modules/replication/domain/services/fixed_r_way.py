from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def fixed_r_way(
    replication_count: int,
    ranked: list[tuple[str, str, int]],
    *,
    max_replicas: int,
) -> ReplicationPlan:
    r_eff = min(replication_count, max_replicas, len(ranked))
    targets = [
        ReplicationTarget(node_id=nid, host=h, port=p, delay_ms=0)
        for nid, h, p in ranked[:r_eff]
    ]

    return ReplicationPlan(targets=targets)
