from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def speculative_execution(
    replication_max_count: int,
    threshold_ms: int,
    ranked: list[tuple[str, str, int]],
    *,
    max_replicas: int,
) -> ReplicationPlan:
    r_eff = min(replication_max_count, max_replicas, len(ranked))
    targets: list[ReplicationTarget] = []
    for i, (nid, h, p) in enumerate(ranked[:r_eff]):
        delay = 0 if i == 0 else i * int(threshold_ms)
        targets.append(ReplicationTarget(node_id=nid, host=h, port=p, delay_ms=delay))

    return ReplicationPlan(targets=targets)
