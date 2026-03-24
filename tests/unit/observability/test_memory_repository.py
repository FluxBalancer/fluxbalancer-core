import pytest

from modules.observability.adapters.outbound.storage.memory_repository import (
    InMemoryMetricsRepository,
)
from modules.observability.domain.node_metrics import NodeMetrics


@pytest.mark.asyncio
async def test_memory_repository_returns_latest_and_prev_with_latency_p90():
    repo = InMemoryMetricsRepository(history_limit=10, latency_window=10)

    first = NodeMetrics(
        timestamp="t1",
        node_id="node-1",
        cpu_util=0.2,
        mem_util=0.3,
        net_in_bytes=100,
        net_out_bytes=200,
    )
    second = NodeMetrics(
        timestamp="t2",
        node_id="node-1",
        cpu_util=0.4,
        mem_util=0.5,
        net_in_bytes=150,
        net_out_bytes=250,
    )

    await repo.upsert(first)
    await repo.upsert(second)

    await repo.add_latency("node-1", 10, profile="cpu:1")
    await repo.add_latency("node-1", 20, profile="cpu:1")
    await repo.add_latency("node-1", 100, profile="mem:1:mid")

    latest = await repo.get_latest("node-1")
    prev = await repo.get_prev("node-1")

    assert latest is not None
    assert latest.timestamp == "t2"
    assert latest.node_id == "node-1"
    assert latest.latency_ms is not None
    assert 20 <= latest.latency_ms <= 100

    assert prev is not None
    assert prev.timestamp == "t1"
    assert prev.latency_ms is not None


@pytest.mark.asyncio
async def test_memory_repository_profile_samples_are_isolated():
    repo = InMemoryMetricsRepository(history_limit=10, latency_window=10)

    await repo.add_latency("node-1", 10, profile="cpu:1")
    await repo.add_latency("node-1", 20, profile="cpu:1")
    await repo.add_latency("node-1", 30, profile="mem:1:mid")

    cpu_samples = await repo.get_latency_samples("node-1", profile="cpu:1")
    mem_samples = await repo.get_latency_samples("node-1", profile="mem:1:mid")
    all_samples = await repo.get_latency_samples("node-1")

    assert cpu_samples == [10, 20]
    assert mem_samples == [30]
    assert sorted(all_samples) == [10, 20, 30]


@pytest.mark.asyncio
async def test_memory_repository_clear_removes_metrics_and_latency():
    repo = InMemoryMetricsRepository(history_limit=10, latency_window=10)

    await repo.upsert(
        NodeMetrics(
            timestamp="t1",
            node_id="node-1",
            cpu_util=0.1,
            mem_util=0.2,
            net_in_bytes=0,
            net_out_bytes=0,
        )
    )
    await repo.add_latency("node-1", 42.0, profile="cpu:1")

    await repo.clear()

    assert await repo.get_latest("node-1") is None
    assert await repo.get_prev("node-1") is None
    assert await repo.get_latency_samples("node-1") == []
    assert await repo.list_latest() == []
