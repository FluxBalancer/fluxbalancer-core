import pytest

from modules.replication.domain.completion import (
    FirstValidPolicy,
    KOutOfNPolicy,
    MajorityPolicy,
    QuorumPolicy,
    ReplicaReply,
)


def _reply(
    *,
    node_id: str,
    value: str,
    ok: bool,
    status: int = 200,
    latency_ms: float = 10.0,
) -> ReplicaReply:
    return ReplicaReply(
        node_id=node_id,
        socket=f"{node_id}:8080",
        ok=ok,
        value=value,
        raw_body=value.encode(),
        status=status,
        latency_ms=latency_ms,
    )


def test_first_valid_policy_ignores_invalid_and_picks_first_valid():
    policy = FirstValidPolicy()

    policy.push(_reply(node_id="n1", value="err", ok=False, status=500, latency_ms=5))
    assert policy.is_done() is False

    winner = _reply(node_id="n2", value="ok-1", ok=True, latency_ms=20)
    policy.push(winner)

    assert policy.is_done() is True
    assert policy.choose() == winner


def test_k_out_of_n_policy_waits_for_k_valid_replies():
    policy = KOutOfNPolicy(k=2)

    policy.push(_reply(node_id="n1", value="a", ok=False, status=500, latency_ms=5))
    assert policy.is_done() is False

    fast = _reply(node_id="n2", value="x", ok=True, latency_ms=15)
    slow = _reply(node_id="n3", value="y", ok=True, latency_ms=30)

    policy.push(fast)
    assert policy.is_done() is False

    policy.push(slow)
    assert policy.is_done() is True
    assert policy.choose() == fast


def test_majority_policy_requires_majority_of_equal_valid_values():
    policy = MajorityPolicy(expected_n=3)

    r1 = _reply(node_id="n1", value="same", ok=True, latency_ms=30)
    r2 = _reply(node_id="n2", value="same", ok=True, latency_ms=10)
    r3 = _reply(node_id="n3", value="other", ok=True, latency_ms=5)

    policy.push(r1)
    assert policy.is_done() is False

    policy.push(r3)
    assert policy.is_done() is False

    policy.push(r2)
    assert policy.is_done() is True

    # среди совпавшего большинства выбирается самый быстрый
    assert policy.choose() == r2


def test_majority_policy_does_not_count_invalid_replies():
    policy = MajorityPolicy(expected_n=3)

    policy.push(_reply(node_id="n1", value="same", ok=False, status=500))
    policy.push(_reply(node_id="n2", value="same", ok=True))
    policy.push(_reply(node_id="n3", value="other", ok=True))

    assert policy.is_done() is False
    with pytest.raises(RuntimeError, match="большинство не достигнуто"):
        policy.choose()


def test_quorum_policy_requires_quorum_for_same_value():
    policy = QuorumPolicy(quorum_size=2)

    slow = _reply(node_id="n1", value="payload", ok=True, latency_ms=50)
    fast = _reply(node_id="n2", value="payload", ok=True, latency_ms=10)

    policy.push(slow)
    assert policy.is_done() is False

    policy.push(fast)
    assert policy.is_done() is True
    assert policy.choose() == fast


def test_quorum_policy_rejects_choose_before_quorum():
    policy = QuorumPolicy(quorum_size=2)
    policy.push(_reply(node_id="n1", value="a", ok=True))

    with pytest.raises(RuntimeError, match="кворум не достигнут"):
        policy.choose()
