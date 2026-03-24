from modules.replication.domain.services.fixed_r_way import fixed_r_way
from modules.replication.domain.services.hedged_requests import hedged_requests
from modules.replication.domain.services.speculative_execution import (
    speculative_execution,
)


def test_fixed_r_way_limits_targets_by_max_replicas():
    ranked = [
        ("n1", "h1", 1),
        ("n2", "h2", 2),
        ("n3", "h3", 3),
    ]

    plan = fixed_r_way(
        replication_count=5,
        ranked=ranked,
        max_replicas=2,
    )

    assert len(plan.targets) == 2
    assert [t.node_id for t in plan.targets] == ["n1", "n2"]
    assert all(t.delay_ms == 0 for t in plan.targets)


def test_hedged_requests_assigns_delays_and_backup_limits():
    ranked = [
        ("n1", "h1", 1),
        ("n2", "h2", 2),
        ("n3", "h3", 3),
    ]

    plan = hedged_requests(
        replication_max_count=3,
        time_delta_ms=50,
        ranked=ranked,
        max_replicas=3,
        backup_max_inflight=8,
    )

    assert len(plan.targets) == 3
    assert plan.targets[0].delay_ms == 0
    assert plan.targets[1].delay_ms == 50
    assert plan.targets[2].delay_ms == 100

    assert plan.targets[0].max_inflight is None
    assert plan.targets[1].max_inflight == 8
    assert plan.targets[2].max_inflight == 8


def test_speculative_execution_uses_tau_ms_when_provided():
    ranked = [
        ("n1", "h1", 1),
        ("n2", "h2", 2),
    ]

    plan = speculative_execution(
        replication_max_count=2,
        threshold_ms=500,
        ranked=ranked,
        max_replicas=2,
        tau_ms=40,
        backup_max_inflight=3,
    )

    assert len(plan.targets) == 2
    assert plan.targets[0].delay_ms == 0
    assert plan.targets[1].delay_ms == 40
    assert plan.targets[1].max_inflight == 3
