from modules.replication.domain.policies.replication_policy import (
    ReplicationPolicy,
    ReplicationDecision,
)


def test_default_replicas():
    default_replicas = 3
    policy = ReplicationPolicy(default_replicas=default_replicas, max_replicas=5)

    r: int = policy.resolve_count(
        ReplicationDecision(replicate_all=False, requested_count=None),
        available_nodes=10,
    )

    assert r == default_replicas


def test_replicate_all():
    max_replicas = 9
    policy = ReplicationPolicy(max_replicas=max_replicas)

    r: int = policy.resolve_count(
        ReplicationDecision(replicate_all=True, requested_count=None),
        available_nodes=10,
    )

    assert r == max_replicas


def test_replicate_requested_count():
    requested_count = 8
    policy = ReplicationPolicy(default_replicas=2, max_replicas=requested_count + 1)

    r: int = policy.resolve_count(
        ReplicationDecision(replicate_all=False, requested_count=requested_count),
        available_nodes=10,
    )

    assert r == requested_count


def test_replicate_zero_nodes():
    available_nodes = 0
    policy = ReplicationPolicy(default_replicas=2, max_replicas=1)

    r: int = policy.resolve_count(
        ReplicationDecision(replicate_all=False, requested_count=None),
        available_nodes=0,
    )

    assert r == available_nodes
