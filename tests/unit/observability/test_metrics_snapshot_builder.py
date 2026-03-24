from unittest.mock import AsyncMock

import pytest

from modules.observability.application.services.metrics_snapshot_builder import (
    MetricsSnapshotBuilder,
)
from modules.observability.domain.node_metrics import NodeMetrics


@pytest.mark.asyncio
async def test_metrics_snapshot_builder_builds_expected_payload():
    repo = AsyncMock()
    repo.list_latest.return_value = [
        NodeMetrics(
            timestamp="t1",
            node_id="node-1",
            cpu_util=0.1,
            mem_util=0.2,
            net_in_bytes=100,
            net_out_bytes=200,
            latency_ms=30.5,
        ),
        NodeMetrics(
            timestamp="t2",
            node_id="node-2",
            cpu_util=0.3,
            mem_util=0.4,
            net_in_bytes=300,
            net_out_bytes=400,
            latency_ms=None,
        ),
    ]

    builder = MetricsSnapshotBuilder(repo=repo)

    snapshot = await builder.build()

    assert snapshot == [
        {
            "node_id": "node-1",
            "cpu": 0.1,
            "mem": 0.2,
            "net_in": 100,
            "net_out": 200,
            "latency": 30.5,
        },
        {
            "node_id": "node-2",
            "cpu": 0.3,
            "mem": 0.4,
            "net_in": 300,
            "net_out": 400,
            "latency": None,
        },
    ]
